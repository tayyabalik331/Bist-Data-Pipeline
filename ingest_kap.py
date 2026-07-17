import os
import sys
import time
from datetime import date, datetime, timedelta

import httpx
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

BASE = "https://www.kap.org.tr"
QUERY_URL = f"{BASE}/tr/api/disclosure/members/byCriteria"
WARMUP_URL = f"{BASE}/tr/bildirim-sorgu"
HEADERS = {
    "User-Agent": "bist-data-pipeline/0.1 (student research project)",
    "Referer": WARMUP_URL,
    "Content-Type": "application/json",
}

TICKERS = ["THYAO", "GARAN", "AKBNK", "ASELS", "KCHOL",
           "SISE", "EREGL", "TUPRS", "BIMAS", "FROTO"]
TICKER_SET = set(TICKERS)

BACKFILL_DAYS = 90          # how far back to pull
WINDOW_DAYS = 7             # KAP caps at 2000 rows/request; keep windows small
MAX_RELATED = 3            # skip bulk notices naming more than this many tickers
REQUEST_PAUSE = 0.6        # ~2 req/s, polite


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def instrument_ids(cur):
    """Map ticker -> instrument_id for the tickers we track."""
    cur.execute("SELECT id, symbol FROM instruments WHERE symbol = ANY(%s)", (TICKERS,))
    return {symbol: iid for iid, symbol in cur.fetchall()}


def related_tickers(d):
    rs = d.get("relatedStocks") or ""
    return {s.strip().upper() for s in rs.split(",") if s.strip()}


def parse_publish(d):
    # KAP format: "16.07.2026 16:25:20"
    raw = d.get("publishDate")
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d.%m.%Y %H:%M:%S")
    except ValueError:
        return None


def fetch_window(client, start, end):
    body = {"fromDate": start.isoformat(), "toDate": end.isoformat(),
            "mkkMemberOidList": [], "subjectList": []}
    r = client.post(QUERY_URL, json=body)
    r.raise_for_status()
    return r.json()


def main():
    end = date.today()
    start = end - timedelta(days=BACKFILL_DAYS)

    conn = get_connection()
    with conn.cursor() as cur:
        ids = instrument_ids(cur)
    print(f"Tracking {len(ids)} instruments. Backfill {start} .. {end}\n")

    collected = []   # (instrument_id, disclosure_index, publish_dt, subject, summary, related)

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        client.get(WARMUP_URL)   # warm-up: sets WAF cookies

        cursor = start
        while cursor < end:
            chunk_end = min(cursor + timedelta(days=WINDOW_DAYS), end)
            try:
                rows = fetch_window(client, cursor, chunk_end)
            except Exception as exc:
                print(f"  {cursor}..{chunk_end}: FAILED - {exc}", file=sys.stderr)
                cursor = chunk_end + timedelta(days=1)
                time.sleep(REQUEST_PAUSE)
                continue

            kept = 0
            for d in rows:
                rel = related_tickers(d)
                mine = rel & TICKER_SET
                if not mine:
                    continue
                if len(rel) > MAX_RELATED:      # bulk exchange notice, not company news
                    continue
                for sym in mine:
                    collected.append((
                        ids[sym],
                        d.get("disclosureIndex"),
                        parse_publish(d),
                        d.get("subject"),
                        d.get("summary"),
                        d.get("relatedStocks"),
                    ))
                    kept += 1

            print(f"  {cursor}..{chunk_end}: {len(rows)} rows, kept {kept}")
            cursor = chunk_end + timedelta(days=1)
            time.sleep(REQUEST_PAUSE)

    # Upsert. Idempotent on (instrument_id, disclosure_index).
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO disclosures
                (instrument_id, disclosure_index, publish_date, subject, summary, related_stocks)
            VALUES %s
            ON CONFLICT (instrument_id, disclosure_index) DO NOTHING
            """,
            collected,
        )
        conn.commit()

    print(f"\nUpserted {len(collected)} disclosure rows.")
    conn.close()


if __name__ == "__main__":
    main()