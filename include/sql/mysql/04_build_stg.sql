-- Build clean staging from raw, excluding everything quarantined in step 03.
--
-- This is where casting finally happens. By now every value has been proven
-- castable, so a failure here is a genuine bug in our rules rather than a
-- surprise from the data.

DELETE FROM stg_flights;

INSERT INTO stg_flights (
    batch_id, raw_row_num,
    airline, source_code, source_name, destination_code, destination_name,
    departure_at, arrival_at, duration_hrs, stopovers, stopover_count,
    aircraft_type, travel_class, booking_source,
    base_fare_bdt, tax_surcharge_bdt,
    total_fare_reported_bdt, total_fare_computed_bdt, fare_variance_bdt,
    has_fare_markup, markup_pct,
    seasonality, is_peak_season, days_before_departure
)
SELECT
    r.batch_id,
    r.raw_row_num,
    TRIM(r.airline),
    UPPER(TRIM(r.source_code)),
    TRIM(r.source_name),
    UPPER(TRIM(r.destination_code)),
    TRIM(r.destination_name),
    -- Guarded exactly as in 03: strict-mode STR_TO_DATE raises on junk input
    -- rather than returning NULL, and the optimiser is free to evaluate this
    -- projection before the anti-join that filters rejected rows out.
    STR_TO_DATE(IF(r.departure_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', r.departure_datetime, NULL), '%Y-%m-%d %H:%i:%s'),
    STR_TO_DATE(IF(r.arrival_datetime   REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', r.arrival_datetime,   NULL), '%Y-%m-%d %H:%i:%s'),
    CAST(r.duration_hrs AS DECIMAL(8,4)),
    TRIM(r.stopovers),
    CASE TRIM(r.stopovers)
        WHEN 'Direct'  THEN 0
        WHEN '1 Stop'  THEN 1
        WHEN '2 Stops' THEN 2
    END,
    TRIM(r.aircraft_type),
    TRIM(r.travel_class),
    TRIM(r.booking_source),

    -- Money: rounded to 2dp exactly once, here. The source carries 15
    -- decimal places of float noise; carrying that downstream means every
    -- aggregate has to decide how to round, and they will disagree.
    ROUND(CAST(r.base_fare_bdt     AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.total_fare_bdt    AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.base_fare_bdt AS DECIMAL(20,10))
          + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.total_fare_bdt AS DECIMAL(20,10))
          - (CAST(r.base_fare_bdt AS DECIMAL(20,10))
             + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10))), 2),

    -- The x1.2 pattern found in profiling: flagged, never silently altered.
    CASE WHEN ABS(CAST(r.total_fare_bdt AS DECIMAL(20,10))
                  - 1.2 * (CAST(r.base_fare_bdt AS DECIMAL(20,10))
                           + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10))))
              <= {{ params.fare_tolerance }}
         THEN 1 ELSE 0 END,
    CASE WHEN CAST(r.base_fare_bdt AS DECIMAL(20,10))
              + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)) > 0
         THEN ROUND(100.0 * (CAST(r.total_fare_bdt AS DECIMAL(20,10))
                             - (CAST(r.base_fare_bdt AS DECIMAL(20,10))
                                + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10))))
                    / (CAST(r.base_fare_bdt AS DECIMAL(20,10))
                       + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10))), 3)
         ELSE 0 END,

    TRIM(r.seasonality),
    -- Peak season comes free with the data: Seasonality ships as one of
    -- Regular / Winter Holidays / Hajj / Eid, so no lunar holiday calendar
    -- is needed. Anything not 'Regular' is peak.
    CASE WHEN TRIM(r.seasonality) = 'Regular' THEN 0 ELSE 1 END,
    CAST(r.days_before_departure AS SIGNED)

FROM raw_flight_prices r
LEFT JOIN (
    SELECT DISTINCT batch_id, raw_row_num
    FROM rejects_flight_prices
    WHERE batch_id = '{{ run_id }}'
) q
  ON q.batch_id = r.batch_id AND q.raw_row_num = r.raw_row_num
WHERE r.batch_id = '{{ run_id }}'
  AND q.raw_row_num IS NULL;
