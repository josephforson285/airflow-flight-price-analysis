-- KPI 1 — Average fare by airline.
--
-- Uses total_fare_REPORTED, not the recomputed value: the reported total is
-- what the passenger actually paid, so it is the honest basis for a price
-- KPI. markup_row_count carries the caveat alongside the number instead of
-- burying it — a reader can see how much of each airline's mean is affected
-- by the unexplained x1.2 rows.
--
-- DELETE + INSERT run inside one transaction, so the table is never
-- observed empty by a concurrent reader.

DELETE FROM kpi_fare_by_airline;

INSERT INTO kpi_fare_by_airline (
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
