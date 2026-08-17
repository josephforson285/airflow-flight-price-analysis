-- Cleaned, typed staging + the quarantine table.

-- Rejects: one row per (source row, reason), so all violations are recorded.
-- payload keeps the original values so a reject is diagnosable without the CSV.
CREATE TABLE IF NOT EXISTS rejects_flight_prices (
    reject_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id      VARCHAR(64)  NOT NULL,
    raw_row_num   INT UNSIGNED NOT NULL,
    reason_code   VARCHAR(64)  NOT NULL,
    reason_detail VARCHAR(512),
    payload       JSON,
    rejected_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (reject_id),
    KEY idx_rejects_batch (batch_id),
    KEY idx_rejects_row   (batch_id, raw_row_num)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Staging. Money is DECIMAL(12,2), never FLOAT -- binary floating point
-- cannot represent decimal currency exactly, so sums drift.
--
-- The fare columns keep both values for the x1.2 markup rows: whether that
-- is dirty data or an unstated rule cannot be settled from the data, so the
-- pipeline flags rather than picks.
CREATE TABLE IF NOT EXISTS stg_flights (
    batch_id                VARCHAR(64)  NOT NULL,
    raw_row_num             INT UNSIGNED NOT NULL,

    airline                 VARCHAR(128) NOT NULL,
    source_code             VARCHAR(8)   NOT NULL,
    source_name             VARCHAR(255) NOT NULL,
    destination_code        VARCHAR(8)   NOT NULL,
    destination_name        VARCHAR(255) NOT NULL,

    departure_at            DATETIME     NOT NULL,
    arrival_at              DATETIME     NOT NULL,
    duration_hrs            DECIMAL(8,4) NOT NULL,
    stopovers               VARCHAR(32)  NOT NULL,
    stopover_count          TINYINT      NOT NULL,   -- derived: Direct->0, 1 Stop->1, 2 Stops->2
    aircraft_type           VARCHAR(64)  NOT NULL,
    travel_class            VARCHAR(32)  NOT NULL,
    booking_source          VARCHAR(64)  NOT NULL,

    base_fare_bdt           DECIMAL(12,2) NOT NULL,
    tax_surcharge_bdt       DECIMAL(12,2) NOT NULL,
    total_fare_reported_bdt DECIMAL(12,2) NOT NULL,  -- as supplied; what the passenger paid
    total_fare_computed_bdt DECIMAL(12,2) NOT NULL,  -- base + tax
    fare_variance_bdt       DECIMAL(12,2) NOT NULL,  -- reported - computed
    has_fare_markup         TINYINT(1)    NOT NULL,  -- the exact x1.2 pattern
    markup_pct              DECIMAL(7,3)  NOT NULL,

    seasonality             VARCHAR(32)  NOT NULL,
    is_peak_season          TINYINT(1)   NOT NULL,   -- Eid / Hajj / Winter Holidays
    days_before_departure   SMALLINT     NOT NULL,

    loaded_at               TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, raw_row_num),
    KEY idx_stg_airline    (airline),
    KEY idx_stg_route      (source_code, destination_code),
    KEY idx_stg_season     (seasonality)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
