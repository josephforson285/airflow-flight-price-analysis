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
    has_fare_markup         BOOLEAN       NOT NULL,
    markup_pct              NUMERIC(7,3)  NOT NULL,

    seasonality             TEXT          NOT NULL,
    is_peak_season          BOOLEAN       NOT NULL,
    days_before_departure   SMALLINT      NOT NULL,

    loaded_at               TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, raw_row_num)
);

CREATE INDEX IF NOT EXISTS idx_fct_airline ON fct_flights (airline);
CREATE INDEX IF NOT EXISTS idx_fct_route   ON fct_flights (source_code, destination_code);
CREATE INDEX IF NOT EXISTS idx_fct_season  ON fct_flights (seasonality);

-- ---------------------------------------------------------------------
-- KPI marts
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS kpi_fare_by_airline (
    airline            TEXT          NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    min_total_fare_bdt NUMERIC(12,2) NOT NULL,
    max_total_fare_bdt NUMERIC(12,2) NOT NULL,
    avg_base_fare_bdt  NUMERIC(12,2) NOT NULL,
    avg_tax_bdt        NUMERIC(12,2) NOT NULL,
    markup_row_count   BIGINT        NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (airline)
);

CREATE TABLE IF NOT EXISTS kpi_seasonal_variation (
    seasonality        TEXT          NOT NULL,
    is_peak_season     BOOLEAN       NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    -- premium of this season's mean fare over the 'Regular' baseline
    vs_regular_pct     NUMERIC(8,3)  NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (seasonality)
);

CREATE TABLE IF NOT EXISTS kpi_bookings_by_airline (
    airline           TEXT          NOT NULL,
    bookings          BIGINT        NOT NULL,
    share_pct         NUMERIC(8,3)  NOT NULL,
    routes_served     INTEGER       NOT NULL,
    revenue_bdt       NUMERIC(18,2) NOT NULL,
    built_at          TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (airline)
);

CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    rank_position      INTEGER       NOT NULL,
    source_code        TEXT          NOT NULL,
    source_name        TEXT          NOT NULL,
    destination_code   TEXT          NOT NULL,
    destination_name   TEXT          NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    airlines_serving   INTEGER       NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_code, destination_code)
);
