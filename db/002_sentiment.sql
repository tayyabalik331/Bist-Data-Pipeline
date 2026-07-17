-- Disclosures pulled from KAP, associated to an instrument.
CREATE TABLE disclosures (
    id               SERIAL PRIMARY KEY,
    instrument_id    INT    NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
    disclosure_index BIGINT NOT NULL,
    publish_date     TIMESTAMPTZ,
    subject          TEXT,
    summary          TEXT,
    related_stocks   TEXT,
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (instrument_id, disclosure_index)
);

-- One sentiment score per disclosure. Provenance is first-class.
CREATE TABLE disclosure_sentiment (
    disclosure_id INT PRIMARY KEY REFERENCES disclosures(id) ON DELETE CASCADE,
    sentiment     TEXT          NOT NULL,   -- positive / neutral / negative
    score         NUMERIC(4,3)  NOT NULL,   -- -1.000 .. 1.000
    reasoning     TEXT,
    model         TEXT          NOT NULL,   -- which model produced this
    scored_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Observability for scoring runs (twin of ingestion_runs).
CREATE TABLE scoring_runs (
    id          SERIAL PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status      TEXT NOT NULL DEFAULT 'running',
    scored      INT DEFAULT 0,
    failed      INT DEFAULT 0,
    error       TEXT
);