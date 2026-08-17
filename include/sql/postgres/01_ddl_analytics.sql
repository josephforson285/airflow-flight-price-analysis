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
--
-- DROP then CREATE, not CREATE IF NOT EXISTS.
--
-- Renaming revenue_bdt to total_fare_bdt exposed why: IF NOT EXISTS is a
-- no-op against an existing table, so the DDL silently kept the old shape and
-- the INSERT failed on a column that "should" have existed. Editing the DDL
-- appeared to work and changed nothing.
--
-- These four tables are derived, rebuilt in full every run, and have no
-- dependents, so dropping them is cheap and makes the DDL self-healing: the
-- file is the schema, always. fct_flights deliberately keeps IF NOT EXISTS
-- because it is the load target rather than a derivative — evolving ITS shape
-- needs a real migration, which this project does not yet have.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS kpi_fare_by_airline;
DROP TABLE IF EXISTS kpi_seasonal_variation;
DROP TABLE IF EXISTS kpi_bookings_by_airline;
DROP TABLE IF EXISTS kpi_popular_routes;
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

-- `bookings` is the brief's term for this measure and is kept for that reason,
-- but the data does not actually evidence a booking: all 57,000 rows are
-- distinct (airline, route, departure time) combinations, no flight appears
-- twice, and there is no booking id, passenger count or seat quantity — only
-- booking_source, which is a channel. Each row is one priced flight record.
--
-- total_fare_bdt was called revenue_bdt. That was wrong: revenue requires
-- seats sold, and summing advertised fares over unique flight records is not
-- that. Renamed rather than dropped, because the sum is still a useful
-- exposure measure as long as it is not called something it isn't.
CREATE TABLE IF NOT EXISTS kpi_bookings_by_airline (
    airline           TEXT          NOT NULL,
    bookings          BIGINT        NOT NULL,  -- flight records; see note above
    share_pct         NUMERIC(8,3)  NOT NULL,
    routes_served     INTEGER       NOT NULL,
    total_fare_bdt    NUMERIC(18,2) NOT NULL,  -- SUM of fares, NOT revenue
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
