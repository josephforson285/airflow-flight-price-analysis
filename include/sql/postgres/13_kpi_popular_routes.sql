-- KPI 4 -- Most popular routes.
--
-- Every route stored WITH its rank, not a top-N slice: a table truncated to
-- 10 reads later as though the world contains 10 routes. "Top 5" is the
-- consumer's WHERE clause. Ties break on route code so re-runs are stable.
--
-- Atomic swap -- see 10_kpi_fare_by_airline.sql.

DROP TABLE IF EXISTS kpi_popular_routes__new;

CREATE TABLE kpi_popular_routes__new (
    rank_position      INTEGER       NOT NULL,
    source_code        TEXT          NOT NULL,
    source_name        TEXT          NOT NULL,
    destination_code   TEXT          NOT NULL,
    destination_name   TEXT          NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    airlines_serving   INTEGER       NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_code, destination_code)
);

INSERT INTO kpi_popular_routes__new (
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

DROP TABLE IF EXISTS kpi_popular_routes;
ALTER TABLE kpi_popular_routes__new RENAME TO kpi_popular_routes;
