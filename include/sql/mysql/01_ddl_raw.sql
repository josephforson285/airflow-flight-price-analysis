-- Landing zone. Every column is VARCHAR on purpose: a malformed value must be
-- able to land so it fails a validation we wrote, on a row we can inspect,
-- rather than aborting the load with a driver error naming no row.
--
-- Source names are snake_cased here ('Tax & Surcharge (BDT)' is hostile as an
-- identifier); `class` becomes travel_class to dodge the reserved word.

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
