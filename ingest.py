import os
import sys
from datetime import date

import psycopg2
import psycopg2.extras
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

BIST30 = [
    "THYAO", "GARAN", "AKBNK", "ASELS", "KCHOL",
    "SISE", "EREGL", "TUPRS", "BIMAS", "FROTO",
]


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def upsert_instrument(cur, symbol: str) -> int:
    """Insert the instrument if new, and return its id either way."""
    cur.execute(
        """
        INSERT INTO instruments (symbol)
        VALUES (%s)
        ON CONFLICT (symbol) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING id
        """,
        (symbol,),
    )
    return cur.fetchone()[0]


def fetch_ohlcv(symbol: str, start: str, end: str):
    """Download OHLCV from Yahoo and flatten yfinance's MultiIndex columns."""
    df = yf.download(
        f"{symbol}.IS",
        start=start,
        end=end,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        return []

    # yfinance returns MultiIndex columns like ('Close', 'THYAO.IS').
    # Drop the ticker level so we're left with plain 'Close', 'Open', ...
    if isinstance(df.columns, __import__("pandas").MultiIndex):
        df.columns = df.columns.droplevel(1)

    rows = []
    for ts, r in df.iterrows():
        # Guard against Yahoo's occasional null rows on non-trading days.
        if r[["Open", "High", "Low", "Close"]].isnull().any():
            continue

        rows.append((
            ts.date(),
            float(r["Open"]),
            float(r["High"]),
            float(r["Low"]),
            float(r["Close"]),
            float(r["Adj Close"]),
            int(r["Volume"]) if r["Volume"] == r["Volume"] else 0,
        ))

    return rows


def ingest(symbol: str, start: str, end: str) -> int:
    conn = get_connection()
    run_id = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ingestion_runs (symbol) VALUES (%s) RETURNING id",
                (symbol,),
            )
            run_id = cur.fetchone()[0]
            conn.commit()

            instrument_id = upsert_instrument(cur, symbol)
            rows = fetch_ohlcv(symbol, start, end)

            payload = [(instrument_id, *row) for row in rows]

            # The upsert. Re-running this is safe: the composite PK
            # (instrument_id, ts) makes duplicates impossible.
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO ohlcv_daily
                    (instrument_id, ts, open, high, low, close, adj_close, volume)
                VALUES %s
                ON CONFLICT (instrument_id, ts) DO UPDATE SET
                    open      = EXCLUDED.open,
                    high      = EXCLUDED.high,
                    low       = EXCLUDED.low,
                    close     = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume    = EXCLUDED.volume
                """,
                payload,
            )

            cur.execute(
                """
                UPDATE ingestion_runs
                SET status = 'success',
                    finished_at = now(),
                    rows_upserted = %s
                WHERE id = %s
                """,
                (len(payload), run_id),
            )
            conn.commit()

        print(f"  {symbol}: {len(payload)} rows upserted")
        return len(payload)

    except Exception as exc:
        conn.rollback()
        if run_id is not None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ingestion_runs
                    SET status = 'failed', finished_at = now(), error = %s
                    WHERE id = %s
                    """,
                    (str(exc)[:500], run_id),
                )
                conn.commit()
        print(f"  {symbol}: FAILED - {exc}", file=sys.stderr)
        return 0

    finally:
        conn.close()


if __name__ == "__main__":
    start = "2020-01-01"
    end = date.today().isoformat()

    print(f"Ingesting {len(BIST30)} symbols from {start} to {end}\n")

    total = 0
    for symbol in BIST30:
        total += ingest(symbol, start, end)

    print(f"\nDone. {total} rows total.")