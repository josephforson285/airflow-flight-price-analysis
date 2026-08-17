-- KPI 1 -- Average fare by airline.
--
-- Uses total_fare_REPORTED: that is what the passenger paid, so it is the
-- honest basis for a price KPI. discrepancy_row_count keeps the caveat beside the
-- number instead of burying it.
--
-- Atomic swap: build into __new, then replace in one transaction. Postgres
-- has transactional DDL, so readers see the old mart until commit, a failure
-- rolls the whole rebuild back, and the schema comes from this file each run.

DROP TABLE IF EXISTS kpi_fare_by_airline__new;

CREATE TABLE kpi_fare_by_airline__new (
    airline            TEXT          NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    min_total_fare_bdt NUMERIC(12,2) NOT NULL,
    max_total_fare_bdt NUMERIC(12,2) NOT NULL,
    avg_base_fare_bdt  NUMERIC(12,2) NOT NULL,
    avg_tax_bdt        NUMERIC(12,2) NOT NULL,
    discrepancy_row_count   BIGINT        NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (airline)
);

INSERT INTO kpi_fare_by_airline__new (
    airline, bookings, avg_total_fare_bdt, min_total_fare_bdt, max_total_fare_bdt,
    avg_base_fare_bdt, avg_tax_bdt, discrepancy_row_count
)
SELECT
    airline,
    COUNT(*)                                        AS bookings,
    ROUND(AVG(total_fare_reported_bdt), 2)          AS avg_total_fare_bdt,
    MIN(total_fare_reported_bdt)                    AS min_total_fare_bdt,
    MAX(total_fare_reported_bdt)                    AS max_total_fare_bdt,
    ROUND(AVG(base_fare_bdt), 2)                    AS avg_base_fare_bdt,
    ROUND(AVG(tax_surcharge_bdt), 2)                AS avg_tax_bdt,
    COUNT(*) FILTER (WHERE has_fare_discrepancy)         AS discrepancy_row_count
FROM fct_flights
GROUP BY airline;

DROP TABLE IF EXISTS kpi_fare_by_airline;
ALTER TABLE kpi_fare_by_airline__new RENAME TO kpi_fare_by_airline;
