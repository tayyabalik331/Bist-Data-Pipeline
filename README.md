# BIST Market Data Pipeline

An end-to-end pipeline that ingests, stores, and serves historical market data for
Borsa İstanbul (BIST) equities.

`yfinance` → **Postgres** → **FastAPI**, with idempotent ingestion, run-level
observability, and a measured query-performance benchmark.

**Current dataset:** 10 BIST tickers · 16,355 daily bars · 2020-01-02 → present

---

## Why this exists

Serving historical market data quickly is a real problem in investment technology:
the tables are large, append-only, and almost always queried the same way —
*"give me instrument X between date A and date B."*

This project is a small, honest implementation of that path, built to be correct
under re-runs and fast under range queries, with both properties **measured rather
than assumed**.

---

## Architecture

```
  ┌────────────┐      ┌───────────────┐      ┌──────────────┐
  │  yfinance  │ ───► │  PostgreSQL   │ ───► │   FastAPI    │
  │  (ingest)  │      │   (store)     │      │   (serve)    │
  └────────────┘      └───────────────┘      └──────────────┘
       │                     │                      │
   ingest.py           docker-compose            api.py
                       db/schema.sql          OpenAPI 3.1 docs
```

| Layer | Choice |
|---|---|
| Ingest | Python 3.14, `yfinance` |
| Store | PostgreSQL 16 (Docker) |
| Serve | FastAPI + Uvicorn |
| Infra | Docker Compose |

---

## Schema

```sql
instruments      (id, symbol UNIQUE, name, exchange, currency, created_at)

ohlcv_daily      (instrument_id, ts, open, high, low, close, adj_close, volume)
                 PRIMARY KEY (instrument_id, ts)

ingestion_runs   (id, symbol, started_at, finished_at, status,
                  rows_upserted, error)
```

### Design decisions

**Prices are `NUMERIC(18,4)`, not `FLOAT`.**
Binary floating point cannot represent most decimal values exactly, so errors
accumulate under aggregation. Financial values are stored as exact decimals.

**The price table has a composite primary key, not a surrogate `id`.**
`(instrument_id, ts)` *is* the natural key — one bar per instrument per day — and
enforcing it in the database is what makes ingestion idempotent (below). It also
gives range queries their index for free.

**Every ingestion run is recorded.**
`ingestion_runs` logs start, finish, status, rows written, and error text. A failing
symbol is logged and skipped; it does not abort the run. The pipeline is designed to
be *operated*, not just executed.

---

## Idempotency

Ingestion upserts with `ON CONFLICT (instrument_id, ts) DO UPDATE`. Re-running the
job over the same window rewrites existing bars in place instead of duplicating them.

Verified empirically — `python ingest.py` run twice, then:

```sql
SELECT i.symbol, COUNT(*), MIN(ts), MAX(ts)
FROM ohlcv_daily o JOIN instruments i ON i.id = o.instrument_id
GROUP BY i.symbol;
```

Row counts are **identical across runs** (16,355 total). Backfills and retries are
therefore safe by construction, not by convention.

---

## Query performance

The access pattern that matters is *one instrument, one date range*. To show what the
composite primary key actually buys, the same query was run against the indexed table
and against an unindexed heap copy inflated to ~4.9M rows.

```sql
EXPLAIN ANALYZE
SELECT * FROM ohlcv_daily
WHERE instrument_id = 1 AND ts BETWEEN '2023-01-01' AND '2023-12-31';
```

| Table | Plan | Execution time |
|---|---|---|
| `ohlcv_daily` (composite PK) | Bitmap Index Scan on `ohlcv_daily_pkey` | **2.293 ms** |
| `ohlcv_noidx` (no index) | Seq Scan | **449.978 ms** |

**≈196× faster.** The decisive line in the sequential plan:

```
Rows Removed by Filter: 4922046
```

The unindexed scan read and discarded 4.9 million rows to return a few hundred. The
indexed scan touched 4 heap blocks.

---

## Data quality findings

Real market data is messier than it looks. Two issues surfaced during ingestion:

**1. Trading days are ragged across symbols.**
Over the same window, tickers returned different bar counts — 1,635 for most, 1,636
for EREGL and FROTO, 1,638 for BIMAS. Halts, suspensions, and gaps in upstream
coverage mean symbols do not share an identical calendar. Any code that assumes a
common trading-day index across instruments will silently misalign on joins.

**2. Yahoo's BIST *index* history is uncorrected.**
Borsa İstanbul removed two zeros from its index values in 2020. Yahoo Finance's
historical index series was never adjusted, so raw `XU100.IS` history mixes two
scales. Index series therefore need rescaling before the 2020 boundary — equity
series are unaffected.

Neither issue is visible without inspecting the data. Both were found by checking
rather than trusting.

---

## API

Interactive OpenAPI 3.1 docs at `/docs` (generated from type hints).

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /instruments` | All instruments with bar counts and date coverage |
| `GET /instruments/{symbol}/candles?start=&end=` | OHLCV bars for a date range |

Unknown symbols return `404`; inverted date ranges return `400`.

```bash
curl "http://localhost:8000/instruments/THYAO/candles?start=2024-01-01&end=2024-12-31"
```

---

## Running it

```bash
git clone https://github.com/tayyabalik331/bist-data-pipeline.git
cd bist-data-pipeline

python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows
python -m pip install -r requirements.txt

docker compose up -d                # Postgres on :5433, schema auto-applied
python ingest.py                    # backfill 10 BIST tickers from 2020
python -m uvicorn api:app --reload  # http://localhost:8000/docs
```

Create a `.env` alongside `docker-compose.yml`:

```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=bist
DB_USER=bist
DB_PASSWORD=bist
```

---

## Next

- Scheduled incremental ingestion (fetch only bars since `MAX(ts)`)
- Intraday bars and a partitioning strategy for the resulting volume
- Corporate-action handling (splits, dividends) beyond `adj_close`