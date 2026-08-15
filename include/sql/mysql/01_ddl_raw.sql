-- Landing zone. Every column is VARCHAR on purpose.
--
-- A malformed value ("N/A" in a fare, "not-a-timestamp" in a date) must be
-- able to LAND. If we typed these columns, a single bad cell would abort the
-- whole load with a driver-level error and no indication of which row did it.
-- Typed here = opaque failure. Typed later = a row we can look at.
--
-- Column names are snake_cased at this boundary because the source names
-- ("Tax & Surcharge (BDT)") are hostile to SQL. `class` becomes travel_class
-- to dodge the reserved-word question entirely.

CREATE TABLE IF NOT EXISTS raw_flight_prices (
    batch_id              VARCHAR(64)  NOT NULL,
    raw_row_num           INT UNSIGNED NOT NULL,   -- 1-based CSV data line, for tracing rejects
    ingested_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    airline               VARCHAR(255),
    source_code           VARCHAR(255),
    source_name           VARCHAR(255),
    destination_code      VARCHAR(255),
    destination_name      VARCHAR(255),
    departure_datetime    VARCHAR(255),
    arrival_datetime      VARCHAR(255),
    duration_hrs          VARCHAR(255),
    stopovers             VARCHAR(255),
    aircraft_type         VARCHAR(255),
    travel_class          VARCHAR(255),
    booking_source        VARCHAR(255),
    base_fare_bdt         VARCHAR(255),
    tax_surcharge_bdt     VARCHAR(255),
    total_fare_bdt        VARCHAR(255),
    seasonality           VARCHAR(255),
    days_before_departure VARCHAR(255),

    PRIMARY KEY (batch_id, raw_row_num)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
