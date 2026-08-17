-- KPI 3 — Booking count by airline.
--
-- share_pct and routes_served turn a bare count into something answerable:
-- "US-Bangla has the most bookings" is far less useful than "US-Bangla has
-- 7.9% of bookings across N routes".
--
-- On the word "bookings": it is the brief's term and is kept for traceability,
-- but the dataset does not support it literally. All 57,000 rows are distinct
-- (airline, route, departure time) combinations, no flight recurs, and there
-- is no booking id, passenger count or seat quantity — booking_source records
-- a channel, not a transaction. Each row is one priced flight record.
--
-- total_fare_bdt was previously called revenue_bdt. That was wrong: revenue
-- requires seats sold, and a sum of advertised fares over unique flight
-- records is not that. Renamed rather than dropped — the sum is still a useful
-- exposure measure as long as it is not called something it isn't.
--
-- Built via atomic swap — see 10_kpi_fare_by_airline.sql for why.

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
