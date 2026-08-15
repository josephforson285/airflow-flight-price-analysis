-- KPI 4 — Most popular routes.
--
-- Every route is stored WITH its rank rather than storing only a top-N slice.
-- A pipeline that silently truncates to 10 rows reads, six months later, as
-- if the world only contains 10 routes. Ranking is cheap; "top 5" is then a
-- WHERE clause on the consumer's side:
--     SELECT * FROM kpi_popular_routes WHERE rank_position <= 5;
--
-- Ties break on route code so the ranking is deterministic across runs —
-- otherwise re-running the DAG shuffles equal-count routes and the table
-- appears to change when nothing did.

DELETE FROM kpi_popular_routes;

INSERT INTO kpi_popular_routes (
    rank_position, source_code, source_name, destination_code, destination_name,
    bookings, avg_total_fare_bdt, airlines_serving
)
WITH routes AS (
    SELECT
        source_code,
        MIN(source_name)                        AS source_name,
        destination_code,
        MIN(destination_name)                   AS destination_name,
        COUNT(*)                                AS bookings,
        ROUND(AVG(total_fare_reported_bdt), 2)  AS avg_total_fare_bdt,
        COUNT(DISTINCT airline)                 AS airlines_serving
    FROM fct_flights
    GROUP BY source_code, destination_code
)
SELECT
    ROW_NUMBER() OVER (ORDER BY bookings DESC, source_code, destination_code)::int,
    source_code, source_name, destination_code, destination_name,
    bookings, avg_total_fare_bdt, airlines_serving
FROM routes;
