# BIST Market Data & Sentiment Pipeline

An end-to-end pipeline for Borsa İstanbul (BIST) equities: it ingests historical
market data and public disclosures, scores each disclosure for market sentiment with
a local LLM, and serves everything through a single API.

Two layers, one platform:

1. **Market data** — daily OHLCV per instrument. `yfinance → PostgreSQL → FastAPI`
2. **Sentiment** — KAP disclosures, scored by a local model. `KAP → LLM → PostgreSQL → FastAPI`

**Current dataset:** 10 BIST tickers · 16,355 daily bars (2020→present) · 50 scored
disclosures (rolling 90-day window)

---

## Why this exists

Two problems that matter in investment technology, built small and honestly:

- **Serving historical market data fast** — large, append-only tables queried the same
  way every time (*instrument X, date A to date B*).
- **Turning unstructured filings into a signal** — reading company disclosures and
  distilling them into a per-company sentiment score, in the spirit of algorithmic
  fundamental-scoring products.

Both are implemented to be correct under re-runs, resilient to flaky external sources,
and *measured rather than assumed*.

---

## Architecture

```
  yfinance ───►┐                                    ┌───► /instruments/{s}/candles
               ├─►  PostgreSQL 16  ──►  FastAPI  ────┤
  KAP  ──► LLM ┘     (Docker)          (OpenAPI)     ├───► /instruments/{s}/disclosures
  (local Ollama)                                     └───► /instruments/{s}/sentiment
```

| Layer | Choice |
|---|---|
| Price ingest | Python, `yfinance` |
| Disclosure ingest | Python, `httpx` (direct KAP client) |
| Sentiment scoring | **Local** LLM via Ollama (`llama3.2:3b`), OpenAI-compatible API |
| Store | PostgreSQL 16 (Docker Compose) |
| Serve | FastAPI + Uvicorn |

---

## Data model

Six tables, grouped by concern.

**Market data**
```sql
instruments      (id, symbol UNIQUE, name, exchange, currency, created_at)
ohlcv_daily      (instrument_id, ts, open, high, low, close, adj_close, volume)
                 PRIMARY KEY (instrument_id, ts)
ingestion_runs   (id, symbol, started_at, finished_at, status, rows_upserted, error)
```

**Sentiment**
```sql
disclosures          (id, instrument_id, disclosure_index, publish_date,
                      subject, summary, related_stocks, fetched_at)
                     UNIQUE (instrument_id, disclosure_index)
disclosure_sentiment (disclosure_id PK, sentiment, score, reasoning, model, scored_at)
scoring_runs         (id, started_at, finished_at, status, scored, failed, error)
```

### Design decisions worth defending

**Prices are `NUMERIC(18,4)`, never `FLOAT`.** Binary floats can't represent decimals
exactly; the error accumulates under aggregation. Financial values are stored exact.

**Natural keys enforce idempotency.** `ohlcv_daily` uses a composite PK
`(instrument_id, ts)`; `disclosures` is unique on `(instrument_id, disclosure_index)`.
Re-running either ingester rewrites or skips existing rows instead of duplicating —
verified empirically (row counts identical across runs).

**`disclosure_sentiment.model` records provenance.** LLM output is non-deterministic,
so every score carries the name of the model that produced it. Re-score with a better
model later and you still know what came from where.

**`*_runs` tables make the pipeline observable.** Each ingest and scoring pass logs its
start, finish, status, counts, and errors — so "did last night's run work?" is a query,
not a guess.

---

## Layer 1 — Market data

Ingests daily OHLCV for the tracked tickers, 2020→present, upserting with
`ON CONFLICT (instrument_id, ts) DO UPDATE`. Backfills and retries are safe by
construction.

### Query performance

The access pattern is *one instrument, one date range*. To show what the composite PK
buys, the same query was run against the indexed table and an unindexed heap copy
inflated to ~4.9M rows.

```sql
EXPLAIN ANALYZE
SELECT * FROM ohlcv_daily
WHERE instrument_id = 1 AND ts BETWEEN '2023-01-01' AND '2023-12-31';
```

| Table | Plan | Execution time |
|---|---|---|
| `ohlcv_daily` (composite PK) | Bitmap Index Scan | **2.293 ms** |
| unindexed copy | Seq Scan | **449.978 ms** |

**≈196× faster.** The decisive line in the sequential plan — `Rows Removed by Filter:
4922046` — the unindexed scan read and discarded 4.9M rows to return a few hundred.

---

## Layer 2 — Disclosure sentiment

### Sourcing: when the library rotted

The initial plan used the `kap-client` library. It failed: KAP changed their
company-list endpoint, and the library's ticker→company lookup now returns empty for
all BIST tickers.

Rather than depend on a broken abstraction, the pipeline talks to KAP's disclosure
endpoint directly (`httpx`):

- `POST /tr/api/disclosure/members/byCriteria` with an **empty** member list returns
  *all* disclosures in a date window; each row carries a `relatedStocks` field, which is
  filtered client-side to the tracked tickers — no company lookup needed.
- **Session warm-up:** a GET to the query page first, to set the cookies KAP's firewall
  expects.
- **Windowing:** KAP caps responses at 2000 rows, so a 90-day backfill is walked in
  7-day windows.
- **Politeness:** honest User-Agent, ~2 requests/second.

### A data-quality filter

Many disclosures are exchange-wide bulk notices whose `relatedStocks` lists 30–40
companies — noise, not company news. The ingester drops any disclosure naming more than
three tickers, keeping only company-specific filings.

### Scoring: local, structured, resumable

Sentiment is produced by a **local** model (`llama3.2:3b`) via Ollama's
OpenAI-compatible endpoint — no API keys, quotas, or rate limits. The model reads a
disclosure's (Turkish) subject and returns a JSON verdict.

Design choices:

- **Structured output** — `response_format={"type": "json_object"}` forces valid JSON,
  turning the model into a function that returns data, not prose.
- **`temperature=0.0`** — deterministic scoring; the same subject scores the same way.
- **Typed retry** — transient errors (network/server) retry with exponential backoff;
  malformed output fails fast, because retrying a bad answer just wastes a call.
- **Resumable** — the scorer only touches disclosures with no sentiment row yet
  (`LEFT JOIN ... WHERE s.disclosure_id IS NULL`). A crash mid-run costs nothing; the
  re-run picks up exactly where it stopped. This proved its worth when a cloud model was
  deprecated mid-run — the switch to local inference cost no re-work.

### Prompt engineering: an iteration log

The scoring prompt was tuned over four runs, with the sentiment distribution as the
metric:

| Run | Change | Distribution (of 50) |
|---|---|---|
| 1 | Baseline prompt | 21 negative / 25 neutral / 4 positive |
| 2 | Added strict "default to neutral" rule | **50 neutral** (over-corrected) |
| 3 | Two-branch prompt: neutral for generic labels, *score* real events | 43 neutral / 7 positive |
| 4 | Added ambiguous labels to the neutral set | **38 neutral / 9 positive / 3 negative** |

Run 1 was trigger-happy negative — it read *"I lack information"* as *"bad news."* Run 2
over-corrected into uniform neutrality when given a single forceful rule (small models
over-obey the strongest instruction). Run 4 is the realistic spread: dividend / cash
payment disclosures correctly surface as positive, credit-rating actions as negative,
and routine procedural filings as neutral.

---

## Known limitations

Stated plainly, because they're real:

- **Subject-line only.** Scoring reads the disclosure subject, not the PDF body (KAP
  serves those as Java-serialized blobs needing OCR — out of scope). Direction-ambiguous
  subjects like *"Pay Alım Satım Bildirimi"* (a buy **or** sell notice) can't be
  resolved from the headline alone.
- **Small-model instruction-following is imperfect.** A 3B model treats prompt rules as
  strong suggestions; a few labels listed as neutral still occasionally score directional.
- **KAP's 2000-row cap** means very busy 7-day windows may truncate; narrowing the window
  on saturation would close this.

None of these is hidden by the code; each is a candidate for future work rather than a
silent bug.

---

## API

Interactive OpenAPI 3.1 docs at `/docs`.

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness check |
| `GET /instruments` | Instruments with bar counts and date coverage |
| `GET /instruments/{symbol}/candles?start=&end=` | OHLCV bars for a date range |
| `GET /instruments/{symbol}/disclosures?limit=` | Recent disclosures with their scores |
| `GET /instruments/{symbol}/sentiment` | Aggregate sentiment signal for a ticker |

Unknown symbols return `404`; inverted date ranges return `400`.

Example — `GET /instruments/BIMAS/sentiment`:

```json
{
  "symbol": "BIMAS",
  "signal": "bullish",
  "scored_disclosures": 9,
  "avg_score": 0.228,
  "positive": 5, "neutral": 4, "negative": 0,
  "latest_disclosure": "2026-06-19T17:11:16+00:00"
}
```

The `signal` collapses the average score into a label (`bullish` / `bearish` /
`neutral`) with a dead-zone around zero, so tiny averages aren't called directional.

---

## Running it

Requires Docker, Python 3.11+, and [Ollama](https://ollama.com).

```bash
git clone https://github.com/tayyabalik331/bist-data-pipeline.git
cd bist-data-pipeline

python -m venv .venv
.venv\Scripts\Activate.ps1                 # Windows
python -m pip install -r requirements.txt

docker compose up -d                        # Postgres on :5433, schema auto-applied
Get-Content db/002_sentiment.sql | docker exec -i bist_db psql -U bist -d bist

ollama pull llama3.2:3b                      # one-time model download

python ingest.py                             # backfill prices
python ingest_kap.py                         # backfill disclosures
python score.py                              # score disclosures locally
python -m uvicorn api:app --reload           # http://localhost:8000/docs
```

`.env` (alongside `docker-compose.yml`):

```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=bist
DB_USER=bist
DB_PASSWORD=bist
```

---

## Next

- Scheduled incremental ingestion (prices and disclosures since last run)
- Narrow KAP windows on 2000-row saturation to avoid truncation
- Parse disclosure bodies (OCR) for signal the subject line omits
- Join sentiment against forward price moves to test whether the score predicts anything
