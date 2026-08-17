-- KPI 2 — Seasonal fare variation (peak vs non-peak).
--
-- The brief asks for "Eid, Winter holidays" style peak comparison and warns
-- this needs a holiday calendar. It does not: profiling showed the source
-- ships a Seasonality column with exactly four values —
-- Regular (44,525) / Winter Holidays (10,930) / Hajj (942) / Eid (603).
-- Peak is simply everything that is not 'Regular'.
--
-- vs_regular_pct expresses each season's mean fare as a premium over the
-- Regular baseline, which is the number a pricing analyst actually wants.
--
-- Built via atomic swap — see 10_kpi_fare_by_airline.sql for why.

DROP TABLE IF EXISTS kpi_seasonal_variation__new;

CREATE TABLE kpi_seasonal_variation__new (
    seasonality        TEXT          NOT NULL,
    is_peak_season     BOOLEAN       NOT NULL,
    bookings           BIGINT        NOT NULL,
    avg_total_fare_bdt NUMERIC(12,2) NOT NULL,
    -- premium of this season's mean fare over the 'Regular' baseline
    vs_regular_pct     NUMERIC(8,3)  NOT NULL,
    built_at           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (seasonality)
);

INSERT INTO kpi_seasonal_variation__new (
    seasonality, is_peak_season, bookings, avg_total_fare_bdt, vs_regular_pct
)
WITH baseline AS (
    SELECT AVG(total_fare_reported_bdt) AS regular_avg
    FROM fct_flights
    WHERE seasonality = 'Regular'
)
SELECT
    f.seasonality,
    BOOL_OR(f.is_peak_season)                        AS is_peak_season,
    COUNT(*)                                         AS bookings,
    ROUND(AVG(f.total_fare_reported_bdt), 2)         AS avg_total_fare_bdt,
    -- guard the divisor: if no Regular rows exist the premium is undefined,
    -- and 0 is a safer answer than a division-by-zero crash mid-DAG
    CASE
        WHEN b.regular_avg IS NULL OR b.regular_avg = 0 THEN 0
        ELSE ROUND(100.0 * (AVG(f.total_fare_reported_bdt) - b.regular_avg) / b.regular_avg, 3)
    END                                              AS vs_regular_pct
FROM fct_flights f
CROSS JOIN baseline b
GROUP BY f.seasonality, b.regular_avg;

DROP TABLE IF EXISTS kpi_seasonal_variation;
ALTER TABLE kpi_seasonal_variation__new RENAME TO kpi_seasonal_variation;
