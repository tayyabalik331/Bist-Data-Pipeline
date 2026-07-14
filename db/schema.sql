CREATE TABLE instruments (
    id         SERIAL PRIMARY KEY,
    symbol     TEXT NOT NULL UNIQUE,
    name       TEXT,
    exchange   TEXT NOT NULL DEFAULT 'BIST',
    currency   TEXT NOT NULL DEFAULT 'TRY',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ohlcv_daily (
    instrument_id INT  NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    ts            DATE NOT NULL,
    open          NUMERIC(18,4) NOT NULL,
    high          NUMERIC(18,4) NOT NULL,
    low           NUMERIC(18,4) NOT NULL,
    close         NUMERIC(18,4) NOT NULL,
    adj_close     NUMERIC(18,4),
    volume        BIGINT,
    PRIMARY KEY (instrument_id, ts)
);

CREATE INDEX idx_ohlcv_ts ON ohlcv_daily (ts);

CREATE TABLE ingestion_runs (
    id            SERIAL PRIMARY KEY,
    symbol        TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running',
    rows_upserted INT DEFAULT 0,
    error         TEXT
);
-- 1. unindexed copy
CREATE TABLE ohlcv_noidx AS SELECT * FROM ohlcv_daily;

-- 2. inflate it to ~5M rows
INSERT INTO ohlcv_noidx
SELECT instrument_id, ts - (g * 400), open, high, low, close, adj_close, volume
FROM ohlcv_daily, generate_series(1, 300) g;

-- 3. time the query on the indexed table
EXPLAIN ANALYZE
SELECT * FROM ohlcv_daily
WHERE instrument_id = 1 AND ts BETWEEN '2023-01-01' AND '2023-12-31';

-- 4. time it on the unindexed one
EXPLAIN ANALYZE
SELECT * FROM ohlcv_noidx
WHERE instrument_id = 1 AND ts BETWEEN '2023-01-01' AND '2023-12-31';

-- 5. bin the junk
DROP TABLE ohlcv_noidx;