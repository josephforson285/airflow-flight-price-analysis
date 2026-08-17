-- KPI 1 — Average fare by airline.
--
-- Uses total_fare_REPORTED, not the recomputed value: the reported total is
-- what the passenger actually paid, so it is the honest basis for a price
-- KPI. markup_row_count carries the caveat alongside the number instead of
-- burying it — a reader can see how much of each airline's mean is affected
-- by the unexplained x1.2 rows.
--
-- ATOMIC SWAP. Build into a fresh table, then replace the live one. PostgreSQL
-- has transactional DDL and this file runs as a single transaction, so:
--   * readers see the previous mart right up until the commit — no window
--     where the table is missing or empty;
--   * a failure at any point rolls the whole thing back, leaving the previous
--     good data in place;
--   * the schema comes from this file every run, so it cannot drift from what
--     is checked in the way CREATE TABLE IF NOT EXISTS silently allowed.

DROP TABLE IF EXISTS kpi_fare_by_airline__new;

CREATE TABLE kpi_fare_by_airline__new (
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

INSERT INTO kpi_fare_by_airline__new (
    airline, bookings, avg_total_fare_bdt, min_total_fare_bdt, max_total_fare_bdt,
    avg_base_fare_bdt, avg_tax_bdt, markup_row_count
)
SELECT
    airline,
    COUNT(*)                                        AS bookings,
    ROUND(AVG(total_fare_reported_bdt), 2)          AS avg_total_fare_bdt,
    MIN(total_fare_reported_bdt)                    AS min_total_fare_bdt,
    MAX(total_fare_reported_bdt)                    AS max_total_fare_bdt,
    ROUND(AVG(base_fare_bdt), 2)                    AS avg_base_fare_bdt,
    ROUND(AVG(tax_surcharge_bdt), 2)                AS avg_tax_bdt,
    COUNT(*) FILTER (WHERE has_fare_markup)         AS markup_row_count
FROM fct_flights
GROUP BY airline;

DROP TABLE IF EXISTS kpi_fare_by_airline;
ALTER TABLE kpi_fare_by_airline__new RENAME TO kpi_fare_by_airline;
