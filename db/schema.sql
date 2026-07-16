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