-- Quarantine pass: one reject row per (source row, rule broken), so a row
-- breaking several rules reports all of them.
--
-- Marks only -- 04 does the excluding. Separating them means you can see
-- what would be dropped before anything is.

-- Full refresh: clear before re-deciding, so the task is safe to re-run.
DELETE FROM rejects_flight_prices;

-- 1. Required fields missing / blank.
-- Covers every column NOT NULL in stg_flights: a blank aircraft_type would
-- otherwise pass here and fail the staging INSERT on a constraint error.
-- The blank list is computed once and reused as the filter.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, reason_code, blanks, payload
FROM (
    SELECT batch_id, raw_row_num, 'MISSING_REQUIRED' AS reason_code,
           CONCAT_WS(',',
             CASE WHEN TRIM(COALESCE(airline,''))               = '' THEN 'airline'               END,
             CASE WHEN TRIM(COALESCE(source_code,''))           = '' THEN 'source_code'           END,
             CASE WHEN TRIM(COALESCE(source_name,''))           = '' THEN 'source_name'           END,
             CASE WHEN TRIM(COALESCE(destination_code,''))      = '' THEN 'destination_code'      END,
             CASE WHEN TRIM(COALESCE(destination_name,''))      = '' THEN 'destination_name'      END,
             CASE WHEN TRIM(COALESCE(departure_datetime,''))    = '' THEN 'departure_datetime'    END,
             CASE WHEN TRIM(COALESCE(arrival_datetime,''))      = '' THEN 'arrival_datetime'      END,
             CASE WHEN TRIM(COALESCE(duration_hrs,''))          = '' THEN 'duration_hrs'          END,
             CASE WHEN TRIM(COALESCE(stopovers,''))             = '' THEN 'stopovers'             END,
             CASE WHEN TRIM(COALESCE(aircraft_type,''))         = '' THEN 'aircraft_type'         END,
             CASE WHEN TRIM(COALESCE(travel_class,''))          = '' THEN 'travel_class'          END,
             CASE WHEN TRIM(COALESCE(booking_source,''))        = '' THEN 'booking_source'        END,
             CASE WHEN TRIM(COALESCE(base_fare_bdt,''))         = '' THEN 'base_fare_bdt'         END,
             CASE WHEN TRIM(COALESCE(tax_surcharge_bdt,''))     = '' THEN 'tax_surcharge_bdt'     END,
             CASE WHEN TRIM(COALESCE(total_fare_bdt,''))        = '' THEN 'total_fare_bdt'        END,
             CASE WHEN TRIM(COALESCE(seasonality,''))           = '' THEN 'seasonality'           END,
             CASE WHEN TRIM(COALESCE(days_before_departure,'')) = '' THEN 'days_before_departure' END
           ) AS blanks,
           JSON_OBJECT('airline', airline, 'source', source_code,
                       'destination', destination_code, 'base', base_fare_bdt,
                       'tax', tax_surcharge_bdt, 'total', total_fare_bdt) AS payload
    FROM raw_flight_prices
    WHERE batch_id = '{{ run_id }}'
) candidates
WHERE blanks <> '';

-- ---------------------------------------------------------------------
-- 2. Fare columns that are not numeric at all ("N/A", "", "abc")
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NON_NUMERIC_FARE',
       CONCAT_WS(',',
         CASE WHEN COALESCE(base_fare_bdt,'')     NOT REGEXP '{{ num_regex }}' THEN 'base_fare_bdt'     END,
         CASE WHEN COALESCE(tax_surcharge_bdt,'') NOT REGEXP '{{ num_regex }}' THEN 'tax_surcharge_bdt' END,
         CASE WHEN COALESCE(total_fare_bdt,'')    NOT REGEXP '{{ num_regex }}' THEN 'total_fare_bdt'    END),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   COALESCE(base_fare_bdt,'')     NOT REGEXP '{{ num_regex }}'
       OR COALESCE(tax_surcharge_bdt,'') NOT REGEXP '{{ num_regex }}'
       OR COALESCE(total_fare_bdt,'')    NOT REGEXP '{{ num_regex }}');

-- ---------------------------------------------------------------------
-- 3. Numeric but negative
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NEGATIVE_FARE',
       CONCAT('base=', base_fare_bdt, ' tax=', tax_surcharge_bdt, ' total=', total_fare_bdt),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND base_fare_bdt     REGEXP '{{ num_regex }}'
  AND tax_surcharge_bdt REGEXP '{{ num_regex }}'
  AND total_fare_bdt    REGEXP '{{ num_regex }}'
  AND (   CAST(base_fare_bdt     AS DECIMAL(20,10)) < 0
       OR CAST(tax_surcharge_bdt AS DECIMAL(20,10)) < 0
       OR CAST(total_fare_bdt    AS DECIMAL(20,10)) < 0);

-- 4. Unparseable timestamps.
-- STR_TO_DATE is regex-guarded at EVERY call site: under MySQL 8 strict
-- sql_mode it raises error 1411 on junk instead of returning NULL, so the
-- naive IS NULL test dies on the input it exists to detect. SQL does not
-- promise WHERE runs before the projection, so the guard cannot be hoisted.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'UNPARSEABLE_DATE',
       CONCAT('departure=', COALESCE(departure_datetime,'<null>'),
              ' arrival=',  COALESCE(arrival_datetime,'<null>')),
       JSON_OBJECT('departure', departure_datetime, 'arrival', arrival_datetime)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   STR_TO_DATE(IF(departure_datetime REGEXP '{{ dt_regex }}', departure_datetime, NULL), '{{ dt_format }}') IS NULL
       OR STR_TO_DATE(IF(arrival_datetime   REGEXP '{{ dt_regex }}', arrival_datetime,   NULL), '{{ dt_format }}') IS NULL);

-- ---------------------------------------------------------------------
-- 5. Arrival at or before departure
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'ARRIVAL_BEFORE_DEPARTURE',
       CONCAT(departure_datetime, ' -> ', arrival_datetime),
       JSON_OBJECT('departure', departure_datetime, 'arrival', arrival_datetime)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND STR_TO_DATE(IF(departure_datetime REGEXP '{{ dt_regex }}', departure_datetime, NULL), '{{ dt_format }}') IS NOT NULL
  AND STR_TO_DATE(IF(arrival_datetime   REGEXP '{{ dt_regex }}', arrival_datetime,   NULL), '{{ dt_format }}') IS NOT NULL
  AND STR_TO_DATE(IF(arrival_datetime   REGEXP '{{ dt_regex }}', arrival_datetime,   NULL), '{{ dt_format }}')
      <= STR_TO_DATE(IF(departure_datetime REGEXP '{{ dt_regex }}', departure_datetime, NULL), '{{ dt_format }}');

-- ---------------------------------------------------------------------
-- 6. Source == destination
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'SELF_ROUTE',
       CONCAT(source_code, ' -> ', destination_code),
       JSON_OBJECT('source', source_code, 'destination', destination_code)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND TRIM(COALESCE(source_code,'')) <> ''
  AND UPPER(TRIM(source_code)) = UPPER(TRIM(destination_code));

-- 7. Category outside the known domain.
-- Membership in ref_allowed_values, not literals here, so correcting a
-- domain is an UPDATE rather than a code change.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT r.batch_id, r.raw_row_num, 'UNKNOWN_CATEGORY',
       CONCAT_WS(',',
         CASE WHEN c.allowed_value IS NULL THEN CONCAT('class=',       r.travel_class) END,
         CASE WHEN s.allowed_value IS NULL THEN CONCAT('stopovers=',   r.stopovers)    END,
         CASE WHEN z.allowed_value IS NULL THEN CONCAT('seasonality=', r.seasonality)  END),
       JSON_OBJECT('class', r.travel_class, 'stopovers', r.stopovers,
                   'seasonality', r.seasonality)
FROM raw_flight_prices r
LEFT JOIN ref_allowed_values c
       ON c.field_name = 'travel_class' AND c.allowed_value = TRIM(r.travel_class)
LEFT JOIN ref_allowed_values s
       ON s.field_name = 'stopovers'    AND s.allowed_value = TRIM(r.stopovers)
LEFT JOIN ref_allowed_values z
       ON z.field_name = 'seasonality'  AND z.allowed_value = TRIM(r.seasonality)
WHERE r.batch_id = '{{ run_id }}'
  AND (c.allowed_value IS NULL OR s.allowed_value IS NULL OR z.allowed_value IS NULL);

-- ---------------------------------------------------------------------
-- 8. Negative lead time
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NEGATIVE_LEAD_TIME',
       CONCAT('days_before_departure=', days_before_departure),
       JSON_OBJECT('days_before_departure', days_before_departure)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND days_before_departure REGEXP '{{ num_regex }}'
  AND CAST(days_before_departure AS DECIMAL(20,10)) < 0;

-- 9. Implausible fare arithmetic.
-- A disagreement alone is not an error -- those are flagged in staging. This
-- rejects only totals that cannot be a surcharge: below their own components,
-- or beyond max_fare_ratio times them.
--
-- Relative, not absolute, because no absolute bound separates them: legitimate
-- discrepancies span 445-93,165 BDT and a genuinely broken row sits at 16,105,
-- inside that range. Expressed as multiplication so there is no division by
-- zero to guard.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'FARE_ARITHMETIC',
       CONCAT('base+tax=', CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10)),
              ' total=',   CAST(total_fare_bdt AS DECIMAL(20,10))),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND base_fare_bdt     REGEXP '{{ num_regex }}'
  AND tax_surcharge_bdt REGEXP '{{ num_regex }}'
  AND total_fare_bdt    REGEXP '{{ num_regex }}'
  -- differs by real money ...
  AND ABS(CAST(total_fare_bdt AS DECIMAL(20,10))
          - (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))))
      > {{ params.fare_tolerance }}
  -- ... and is implausible
  AND (   (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))) <= 0
       OR CAST(total_fare_bdt AS DECIMAL(20,10))
            < (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10)))
       OR CAST(total_fare_bdt AS DECIMAL(20,10))
            > {{ max_fare_ratio }} * (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))));

-- 10. Airport code outside the known domain -- the brief's "invalid city
-- names". Blank codes are skipped; MISSING_REQUIRED already reports them.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT r.batch_id, r.raw_row_num, 'UNKNOWN_AIRPORT',
       CONCAT_WS(',',
         CASE WHEN s.airport_code IS NULL THEN CONCAT('source=',      r.source_code)      END,
         CASE WHEN d.airport_code IS NULL THEN CONCAT('destination=', r.destination_code) END),
       JSON_OBJECT('source', r.source_code, 'destination', r.destination_code)
FROM raw_flight_prices r
LEFT JOIN ref_airports s ON s.airport_code = UPPER(TRIM(r.source_code))
LEFT JOIN ref_airports d ON d.airport_code = UPPER(TRIM(r.destination_code))
WHERE r.batch_id = '{{ run_id }}'
  AND TRIM(COALESCE(r.source_code,''))      <> ''
  AND TRIM(COALESCE(r.destination_code,'')) <> ''
  AND (s.airport_code IS NULL OR d.airport_code IS NULL);

-- ---------------------------------------------------------------------
-- 11. Exact duplicate rows — keep the first occurrence, reject the rest
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
WITH ranked AS (
    SELECT batch_id, raw_row_num,
           ROW_NUMBER() OVER (
               -- All 17 source columns: fewer makes this a business-key match,
               -- not the exact-duplicate check its name claims.
               -- CHAR(31) separator and CHAR(30) null marker so field
               -- boundaries cannot be forged by data: with no separator
               -- 'US'+'BD' would collide with 'USB'+'D'.
               PARTITION BY MD5(CONCAT_WS(CHAR(31),
                   COALESCE(airline,                 CHAR(30)),
                   COALESCE(source_code,             CHAR(30)),
                   COALESCE(source_name,             CHAR(30)),
                   COALESCE(destination_code,        CHAR(30)),
                   COALESCE(destination_name,        CHAR(30)),
                   COALESCE(departure_datetime,      CHAR(30)),
                   COALESCE(arrival_datetime,        CHAR(30)),
                   COALESCE(duration_hrs,            CHAR(30)),
                   COALESCE(stopovers,               CHAR(30)),
                   COALESCE(aircraft_type,           CHAR(30)),
                   COALESCE(travel_class,            CHAR(30)),
                   COALESCE(booking_source,          CHAR(30)),
                   COALESCE(base_fare_bdt,           CHAR(30)),
                   COALESCE(tax_surcharge_bdt,       CHAR(30)),
                   COALESCE(total_fare_bdt,          CHAR(30)),
                   COALESCE(seasonality,             CHAR(30)),
                   COALESCE(days_before_departure,   CHAR(30))))
               ORDER BY raw_row_num
           ) AS occurrence
    FROM raw_flight_prices
    WHERE batch_id = '{{ run_id }}'
)
SELECT batch_id, raw_row_num, 'DUPLICATE_ROW',
       CONCAT('occurrence #', occurrence, ' of an identical row'),
       NULL
FROM ranked
WHERE occurrence > 1;

-- 12. Non-numeric measures.
-- Both are cast in 04 and land in NOT NULL columns, so junk here would
-- otherwise surface as a cast warning or a constraint error.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NON_NUMERIC_MEASURE',
       CONCAT_WS(',',
         CASE WHEN COALESCE(duration_hrs,'')          NOT REGEXP '{{ num_regex }}' THEN CONCAT('duration_hrs=', duration_hrs)                   END,
         CASE WHEN COALESCE(days_before_departure,'') NOT REGEXP '{{ num_regex }}' THEN CONCAT('days_before_departure=', days_before_departure) END),
       JSON_OBJECT('duration_hrs', duration_hrs,
                   'days_before_departure', days_before_departure)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   COALESCE(duration_hrs,'')          NOT REGEXP '{{ num_regex }}'
       OR COALESCE(days_before_departure,'') NOT REGEXP '{{ num_regex }}');

-- 13. Implausible duration.
-- validate_stg_flights also asserts duration > 0, but that runs AFTER the
-- build: it would fail the whole run rather than quarantine one row.
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'IMPLAUSIBLE_DURATION',
       CONCAT('duration_hrs=', duration_hrs,
              ' (expected > 0 and <= {{ max_duration_hrs }})'),
       JSON_OBJECT('duration_hrs', duration_hrs)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND duration_hrs REGEXP '{{ num_regex }}'
  AND (   CAST(duration_hrs AS DECIMAL(20,10)) <= 0
       OR CAST(duration_hrs AS DECIMAL(20,10)) > {{ max_duration_hrs }});
