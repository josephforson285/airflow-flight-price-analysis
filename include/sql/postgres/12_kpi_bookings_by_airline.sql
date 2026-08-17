-- KPI 3 — Booking count by airline.
--
-- share_pct and routes_served turn a bare count into something answerable:
-- "US-Bangla has the most bookings" is far less useful than "US-Bangla has
-- 7.9% of bookings across N routes".

-- On the word "bookings": it is the brief's term and is kept for traceability,
-- but the dataset does not support it literally. Every row is a distinct
-- (airline, route, departure time) and no flight recurs, so these are priced
-- flight records rather than confirmed bookings. Stated here so nobody reads
-- the column name as evidence of demand.

DELETE FROM kpi_bookings_by_airline;

INSERT INTO kpi_bookings_by_airline (
    airline, bookings, share_pct, routes_served, total_fare_bdt
)
WITH total AS (
    SELECT COUNT(*)::numeric AS all_bookings FROM fct_flights
)
SELECT
    f.airline,
    COUNT(*)                                                        AS bookings,
    CASE WHEN t.all_bookings = 0 THEN 0
         ELSE ROUND(100.0 * COUNT(*) / t.all_bookings, 3) END       AS share_pct,
    COUNT(DISTINCT (f.source_code, f.destination_code))              AS routes_served,
    SUM(f.total_fare_reported_bdt)                                   AS total_fare_bdt
FROM fct_flights f
CROSS JOIN total t
GROUP BY f.airline, t.all_bookings;
