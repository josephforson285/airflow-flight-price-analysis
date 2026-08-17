-- Build clean staging from raw, excluding everything quarantined in 03.
-- Casting happens here, on rows already proven castable, so a failure at this
-- point is a bug in our rules rather than a surprise from the data.

DELETE FROM stg_flights;

INSERT INTO stg_flights (
    batch_id, raw_row_num,
    airline, source_code, source_name, destination_code, destination_name,
    departure_at, arrival_at, duration_hrs, stopovers, stopover_count,
    aircraft_type, travel_class, booking_source,
    base_fare_bdt, tax_surcharge_bdt,
    total_fare_reported_bdt, total_fare_computed_bdt, fare_variance_bdt,
    has_fare_discrepancy, fare_variance_pct,
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
    -- Regex-guarded as in 03: strict-mode STR_TO_DATE raises on junk, and the
    -- optimiser may evaluate this projection before the anti-join.
    STR_TO_DATE(IF(r.departure_datetime REGEXP '{{ dt_regex }}', r.departure_datetime, NULL), '{{ dt_format }}'),
    STR_TO_DATE(IF(r.arrival_datetime   REGEXP '{{ dt_regex }}', r.arrival_datetime,   NULL), '{{ dt_format }}'),
    CAST(r.duration_hrs AS DECIMAL(8,4)),
    TRIM(r.stopovers),
    -- LEFT JOIN, not INNER: stopover_count is NOT NULL, so an unseeded
    -- reference table fails loudly here instead of dropping every row.
    sv.numeric_equivalent,
    TRIM(r.aircraft_type),
    TRIM(r.travel_class),
    TRIM(r.booking_source),

    -- Money rounded to 2dp exactly once, here. Carrying 15 decimals of float
    -- noise downstream makes every aggregate decide rounding, and disagree.
    ROUND(CAST(r.base_fare_bdt     AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.total_fare_bdt    AS DECIMAL(20,10)), 2),
    -- Sum the ROUNDED components: ROUND(a+b) and ROUND(a)+ROUND(b) differ by a
    -- paisa when both halves round up, leaving a derived column that disagrees
    -- with its own formula.
    ROUND(CAST(r.base_fare_bdt AS DECIMAL(20,10)), 2)
      + ROUND(CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)), 2),
    ROUND(CAST(r.total_fare_bdt AS DECIMAL(20,10)), 2)
      - (ROUND(CAST(r.base_fare_bdt AS DECIMAL(20,10)), 2)
         + ROUND(CAST(r.tax_surcharge_bdt AS DECIMAL(20,10)), 2)),

    -- Flag any real disagreement. No fitted constant: the SIZE of it is
    -- recorded in fare_variance_pct, so a pattern is observed, not assumed.
    CASE WHEN ABS(CAST(r.total_fare_bdt AS DECIMAL(20,10))
                  - (CAST(r.base_fare_bdt AS DECIMAL(20,10))
                     + CAST(r.tax_surcharge_bdt AS DECIMAL(20,10))))
              > {{ params.fare_tolerance }}
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
    -- Peak season needs no holiday calendar; Seasonality ships in the source.
    CASE WHEN TRIM(r.seasonality) = '{{ regular_season }}' THEN 0 ELSE 1 END,
    CAST(r.days_before_departure AS SIGNED)

FROM raw_flight_prices r
LEFT JOIN ref_allowed_values sv
       ON sv.field_name = 'stopovers' AND sv.allowed_value = TRIM(r.stopovers)
LEFT JOIN (
    SELECT DISTINCT batch_id, raw_row_num
    FROM rejects_flight_prices
    WHERE batch_id = '{{ run_id }}'
) q
  ON q.batch_id = r.batch_id AND q.raw_row_num = r.raw_row_num
WHERE r.batch_id = '{{ run_id }}'
  AND q.raw_row_num IS NULL;
