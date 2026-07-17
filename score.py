import json
import os
import sys
import time
from urllib import response

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

MODELS = ["llama3.2:3b"]
REQUEST_PAUSE = 6.5   # free tier ~10 req/min; stay under it

SYSTEM_PROMPT = """You are a financial analyst assessing Turkish public disclosures (KAP filings) for their likely short-term impact on the disclosing company's stock price.

You are given a disclosure's subject line, in Turkish. Judge its likely market sentiment for that company's shares, and return a JSON verdict.

How to decide:
1. If the subject is a GENERIC procedural or category label with no directional meaning, return neutral (0.0)....Examples: "Özel Durum Açıklaması (Genel)", "Pay Bazında Devre Kesici Bildirimi", "Pay Alım Satım Bildirimi" (direction not stated), "Kredi Derecelendirmesi" (direction not stated), "Hak Kullanımı", "Merkezi Kayıt Kuruluşu Duyurusu", exchange-system notices. Examples: "Özel Durum Açıklaması (Genel)", "Pay Bazında Devre Kesici Bildirimi", "Kredi Derecelendirmesi" (direction not stated), "Merkezi Kayıt Kuruluşu Duyurusu", exchange-system notices.
2. If the subject DOES name a concrete event with a natural market direction, score it accordingly — do NOT default to neutral in that case:
   - Dividend / cash payment to shareholders ("Temettü", "Kâr Payı", "Nakit Ödeme", "Mali Hak Kullanımı") → positive, +0.3 to +0.5
   - Won contract, new investment, capacity expansion, buyback → positive, +0.4 to +0.6
   - Announced profit / strong results → positive, +0.5
   - Announced loss, rating downgrade, investigation, lawsuit, default → negative, -0.4 to -0.6
   - Capital increase via rights issue ("Bedelli Sermaye Artırımı") → mildly negative, -0.2 (dilution)

Be decisive when a real signal exists; be neutral only when the subject genuinely lacks direction.

Return ONLY a JSON object, no other text:
{
  "sentiment": "positive" | "neutral" | "negative",
  "score": number from -1.0 to 1.0,
  "reasoning": "one short English sentence"
}"""

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def unscored_disclosures(cur):
    """Disclosures that don't yet have a sentiment row."""
    cur.execute(
        """
        SELECT d.id, d.subject, i.symbol
        FROM disclosures d
        JOIN instruments i ON i.id = d.instrument_id
        LEFT JOIN disclosure_sentiment s ON s.disclosure_id = d.id
        WHERE s.disclosure_id IS NULL AND d.subject IS NOT NULL
        ORDER BY d.id
        """
    )
    return cur.fetchall()


def score_one(client, model, symbol, subject, max_retries=4):
    """Send one disclosure to the model. Retries transient errors with backoff."""
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",
                     "content": f"Company: {symbol}\nDisclosure subject: {subject}"},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            data = json.loads(response.choices[0].message.content)
            sentiment = str(data["sentiment"]).lower()
            score = float(data["score"])
            reasoning = str(data.get("reasoning", ""))[:500]

            if sentiment not in ("positive", "neutral", "negative"):
                raise ValueError(f"bad sentiment: {sentiment}")
            if not -1.0 <= score <= 1.0:
                raise ValueError(f"score out of range: {score}")

            return sentiment, score, reasoning

        except (json.JSONDecodeError, KeyError, ValueError):
            # The model returned something malformed — retrying won't help.
            raise
        except Exception as exc:
            # Transient (network/server). Back off and retry.
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt * 3
            print(f"    {symbol}: {type(exc).__name__}, retry in {wait}s", file=sys.stderr)
            time.sleep(wait)

def main():
    client = OpenAI(base_url="http://localhost:11434/v1", api_key="ollama")
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute("INSERT INTO scoring_runs DEFAULT VALUES RETURNING id")
        run_id = cur.fetchone()[0]
        conn.commit()

        todo = unscored_disclosures(cur)

    print(f"Scoring {len(todo)} disclosures with {MODELS}\n")

    scored, failed = 0, 0
    for disclosure_id, subject, symbol in todo:
        try:
            result, used_model, last_err = None, None, None
            for model in MODELS:
                try:
                    result = score_one(client, model, symbol, subject)
                    used_model = model
                    break
                except Exception as exc:
                    last_err = exc
            if result is None:
                raise last_err
            sentiment, score, reasoning = result

            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO disclosure_sentiment
                        (disclosure_id, sentiment, score, reasoning, model)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (disclosure_id) DO NOTHING
                    """,
                    (disclosure_id, sentiment, score, reasoning, used_model),
                )
                conn.commit()
            scored += 1
            print(f"  [{symbol}] {score:+.2f} {sentiment:8} | {subject[:50]}")
        except Exception as exc:
            failed += 1
            print(f"  [{symbol}] FAILED - {exc}", file=sys.stderr)

        time.sleep(REQUEST_PAUSE)

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE scoring_runs
            SET status='success', finished_at=now(), scored=%s, failed=%s
            WHERE id=%s
            """,
            (scored, failed, run_id),
        )
        conn.commit()

    print(f"\nDone. {scored} scored, {failed} failed.")
    conn.close()


if __name__ == "__main__":
    main()