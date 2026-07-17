import os
from datetime import date

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

app = FastAPI(title="BIST Market Data API")


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/instruments")
def list_instruments():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT i.symbol,
                       COUNT(o.ts) AS bars,
                       MIN(o.ts)   AS first_date,
                       MAX(o.ts)   AS last_date
                FROM instruments i
                LEFT JOIN ohlcv_daily o ON o.instrument_id = i.id
                GROUP BY i.symbol
                ORDER BY i.symbol
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/instruments/{symbol}/candles")
def get_candles(
    symbol: str,
    start: date = Query(..., description="YYYY-MM-DD"),
    end: date = Query(..., description="YYYY-MM-DD"),
):
    if start > end:
        raise HTTPException(400, "start must be before end")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM instruments WHERE symbol = %s", (symbol.upper(),))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, f"Unknown symbol: {symbol}")

            cur.execute(
                """
                SELECT ts, open, high, low, close, adj_close, volume
                FROM ohlcv_daily
                WHERE instrument_id = %s AND ts BETWEEN %s AND %s
                ORDER BY ts
                """,
                (row["id"], start, end),
            )
            candles = cur.fetchall()

        return {"symbol": symbol.upper(), "count": len(candles), "candles": candles}
    finally:
        conn.close()

@app.get("/instruments/{symbol}/disclosures")
def get_disclosures(symbol: str, limit: int = 20):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM instruments WHERE symbol = %s", (symbol.upper(),))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, f"Unknown symbol: {symbol}")

            cur.execute(
                """
                SELECT d.publish_date, d.subject,
                       s.sentiment, s.score, s.reasoning, s.model
                FROM disclosures d
                LEFT JOIN disclosure_sentiment s ON s.disclosure_id = d.id
                WHERE d.instrument_id = %s
                ORDER BY d.publish_date DESC NULLS LAST
                LIMIT %s
                """,
                (row["id"], limit),
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/instruments/{symbol}/sentiment")
def get_sentiment_score(symbol: str):
    """Aggregate sentiment for a ticker — the mini F-RAY score."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM instruments WHERE symbol = %s", (symbol.upper(),))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, f"Unknown symbol: {symbol}")

            cur.execute(
                """
                SELECT
                    COUNT(s.disclosure_id)                        AS scored_disclosures,
                    ROUND(AVG(s.score), 3)                        AS avg_score,
                    COUNT(*) FILTER (WHERE s.sentiment='positive') AS positive,
                    COUNT(*) FILTER (WHERE s.sentiment='neutral')  AS neutral,
                    COUNT(*) FILTER (WHERE s.sentiment='negative') AS negative,
                    MAX(d.publish_date)                           AS latest_disclosure
                FROM disclosures d
                JOIN disclosure_sentiment s ON s.disclosure_id = d.id
                WHERE d.instrument_id = %s
                """,
                (row["id"],),
            )
            agg = cur.fetchone()

        signal = "neutral"
        if agg["avg_score"] is not None:
            if agg["avg_score"] > 0.1:
                signal = "bullish"
            elif agg["avg_score"] < -0.1:
                signal = "bearish"

        return {"symbol": symbol.upper(), "signal": signal, **agg}
    finally:
        conn.close()