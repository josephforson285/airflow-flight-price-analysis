-- KPI 3 -- Booking count by airline.
--
-- "bookings" is the brief's term, kept for traceability, but the data does
-- not evidence it: every row is a distinct (airline, route, departure time),
-- no flight recurs, and there is no booking id or seat count. Each row is one
-- priced flight record. total_fare_bdt is a SUM of fares, not revenue --
-- revenue would require seats sold.
--
-- Atomic swap -- see 10_kpi_fare_by_airline.sql.

DROP TABLE IF EXISTS kpi_bookings_by_airline__new;

CREATE TABLE kpi_bookings_by_airline__new (
    airline           TEXT          NOT NULL,
    bookings          BIGINT        NOT NULL,  -- flight records; see note above
    share_pct         NUMERIC(8,3)  NOT NULL,
    routes_served     INTEGER       NOT NULL,
    total_fare_bdt    NUMERIC(18,2) NOT NULL,  -- SUM of fares, NOT revenue
    built_at          TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (airline)
);

INSERT INTO kpi_bookings_by_airline__new (
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

DROP TABLE IF EXISTS kpi_bookings_by_airline;
ALTER TABLE kpi_bookings_by_airline__new RENAME TO kpi_bookings_by_airline;
