-- KPI 3 — Booking count by airline.
--
-- share_pct and routes_served turn a bare count into something answerable:
-- "US-Bangla has the most bookings" is far less useful than "US-Bangla has
-- 7.9% of bookings across N routes".

DELETE FROM kpi_bookings_by_airline;

INSERT INTO kpi_bookings_by_airline (
    airline, bookings, share_pct, routes_served, revenue_bdt
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
    SUM(f.total_fare_reported_bdt)                                   AS revenue_bdt
FROM fct_flights f
CROSS JOIN total t
GROUP BY f.airline, t.all_bookings;
