-- Analytics layer: one fact table plus four KPI marts.
--
-- Full-refresh semantics throughout. The source is a single static extract,
-- so every run rebuilds the layer from scratch. Incremental loading would be
-- premature complexity here — and full refresh is idempotent for free.

CREATE TABLE IF NOT EXISTS fct_flights (
    batch_id                TEXT          NOT NULL,
    raw_row_num             INTEGER       NOT NULL,

    airline                 TEXT          NOT NULL,
    source_code             TEXT          NOT NULL,
    source_name             TEXT          NOT NULL,
    destination_code        TEXT          NOT NULL,
    destination_name        TEXT          NOT NULL,

    departure_at            TIMESTAMP     NOT NULL,
    arrival_at              TIMESTAMP     NOT NULL,
    duration_hrs            NUMERIC(8,4)  NOT NULL,
    stopovers               TEXT          NOT NULL,
    stopover_count          SMALLINT      NOT NULL,
    aircraft_type           TEXT          NOT NULL,
    travel_class            TEXT          NOT NULL,
    booking_source          TEXT          NOT NULL,

    base_fare_bdt           NUMERIC(12,2) NOT NULL,
    tax_surcharge_bdt       NUMERIC(12,2) NOT NULL,
    total_fare_reported_bdt NUMERIC(12,2) NOT NULL,
    total_fare_computed_bdt NUMERIC(12,2) NOT NULL,
    fare_variance_bdt       NUMERIC(12,2) NOT NULL,
    has_fare_discrepancy    BOOLEAN       NOT NULL,
    fare_variance_pct       NUMERIC(7,3)  NOT NULL,

    seasonality             TEXT          NOT NULL,
    is_peak_season          BOOLEAN       NOT NULL,
    days_before_departure   SMALLINT      NOT NULL,

    loaded_at               TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, raw_row_num)
);

CREATE INDEX IF NOT EXISTS idx_fct_airline ON fct_flights (airline);
CREATE INDEX IF NOT EXISTS idx_fct_route   ON fct_flights (source_code, destination_code);
CREATE INDEX IF NOT EXISTS idx_fct_season  ON fct_flights (seasonality);

-- The four KPI marts are NOT declared here. Each mart's own SQL file creates
-- it as part of an atomic swap (build a new table, then replace the live one
-- in a single transaction), so the file that builds a mart also owns its
-- shape. Declaring the schema in two places is what let the revenue_bdt rename
-- pass the DDL task and then fail the INSERT.
