-- Quarantine pass. Reads the raw landing table, writes one row per
-- (source row, rule violated) into rejects_flight_prices.
--
-- One row PER REASON, deliberately: a row with a null destination AND a
-- negative fare produces two rejects. Recording only the first violation
-- hit would hide the second one from whoever has to fix the source.
--
-- Nothing is deleted here. Quarantine marks; the staging build (04) is what
-- excludes. Keeping those separate means you can inspect exactly what would
-- be dropped before anything is.

-- Re-runnable: clear previous verdicts before re-deciding. Full refresh —
-- the source is one static extract, so each run rebuilds every layer. This
-- is what makes the task safe to clear and re-run: counts come out identical
-- rather than doubling, which a bare INSERT would do silently.
DELETE FROM rejects_flight_prices;

-- ---------------------------------------------------------------------
-- 1. Required fields missing / blank
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'MISSING_REQUIRED',
       CONCAT_WS(',',
         CASE WHEN TRIM(COALESCE(airline,''))            = '' THEN 'airline'            END,
         CASE WHEN TRIM(COALESCE(source_code,''))        = '' THEN 'source_code'        END,
         CASE WHEN TRIM(COALESCE(destination_code,''))   = '' THEN 'destination_code'   END,
         CASE WHEN TRIM(COALESCE(base_fare_bdt,''))      = '' THEN 'base_fare_bdt'      END,
         CASE WHEN TRIM(COALESCE(tax_surcharge_bdt,''))  = '' THEN 'tax_surcharge_bdt'  END,
         CASE WHEN TRIM(COALESCE(total_fare_bdt,''))     = '' THEN 'total_fare_bdt'     END,
         CASE WHEN TRIM(COALESCE(seasonality,''))        = '' THEN 'seasonality'        END),
       JSON_OBJECT('airline', airline, 'source', source_code, 'destination', destination_code,
                   'base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   TRIM(COALESCE(airline,''))           = ''
       OR TRIM(COALESCE(source_code,''))       = ''
       OR TRIM(COALESCE(destination_code,''))  = ''
       OR TRIM(COALESCE(base_fare_bdt,''))     = ''
       OR TRIM(COALESCE(tax_surcharge_bdt,'')) = ''
       OR TRIM(COALESCE(total_fare_bdt,''))    = ''
       OR TRIM(COALESCE(seasonality,''))       = '');

-- ---------------------------------------------------------------------
-- 2. Fare columns that are not numeric at all ("N/A", "", "abc")
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NON_NUMERIC_FARE',
       CONCAT_WS(',',
         CASE WHEN COALESCE(base_fare_bdt,'')     NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$' THEN 'base_fare_bdt'     END,
         CASE WHEN COALESCE(tax_surcharge_bdt,'') NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$' THEN 'tax_surcharge_bdt' END,
         CASE WHEN COALESCE(total_fare_bdt,'')    NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$' THEN 'total_fare_bdt'    END),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   COALESCE(base_fare_bdt,'')     NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
       OR COALESCE(tax_surcharge_bdt,'') NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
       OR COALESCE(total_fare_bdt,'')    NOT REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$');

-- ---------------------------------------------------------------------
-- 3. Numeric but negative
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NEGATIVE_FARE',
       CONCAT('base=', base_fare_bdt, ' tax=', tax_surcharge_bdt, ' total=', total_fare_bdt),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND base_fare_bdt     REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND tax_surcharge_bdt REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND total_fare_bdt    REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND (   CAST(base_fare_bdt     AS DECIMAL(20,10)) < 0
       OR CAST(tax_surcharge_bdt AS DECIMAL(20,10)) < 0
       OR CAST(total_fare_bdt    AS DECIMAL(20,10)) < 0);

-- ---------------------------------------------------------------------
-- 4. Unparseable timestamps
--
-- Every STR_TO_DATE call in this file is wrapped in a format regex first.
-- Under MySQL 8's default strict sql_mode, STR_TO_DATE does NOT return NULL
-- for junk input — it raises error 1411 and aborts the statement. So the
-- naive `STR_TO_DATE(col, fmt) IS NULL` test cannot detect a bad date; it
-- dies on one. Feeding it NULL (via the IF guard) is safe and does return
-- NULL, which is what we can actually test.
--
-- Relying on evaluation order to protect a later STR_TO_DATE would be a bug
-- waiting to happen: SQL makes no promise that a WHERE predicate is applied
-- before a projection, so the guard travels with every call site.
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'UNPARSEABLE_DATE',
       CONCAT('departure=', COALESCE(departure_datetime,'<null>'),
              ' arrival=',  COALESCE(arrival_datetime,'<null>')),
       JSON_OBJECT('departure', departure_datetime, 'arrival', arrival_datetime)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   STR_TO_DATE(IF(departure_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', departure_datetime, NULL), '%Y-%m-%d %H:%i:%s') IS NULL
       OR STR_TO_DATE(IF(arrival_datetime   REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', arrival_datetime,   NULL), '%Y-%m-%d %H:%i:%s') IS NULL);

-- ---------------------------------------------------------------------
-- 5. Arrival at or before departure
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'ARRIVAL_BEFORE_DEPARTURE',
       CONCAT(departure_datetime, ' -> ', arrival_datetime),
       JSON_OBJECT('departure', departure_datetime, 'arrival', arrival_datetime)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND STR_TO_DATE(IF(departure_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', departure_datetime, NULL), '%Y-%m-%d %H:%i:%s') IS NOT NULL
  AND STR_TO_DATE(IF(arrival_datetime   REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', arrival_datetime,   NULL), '%Y-%m-%d %H:%i:%s') IS NOT NULL
  AND STR_TO_DATE(IF(arrival_datetime   REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', arrival_datetime,   NULL), '%Y-%m-%d %H:%i:%s')
      <= STR_TO_DATE(IF(departure_datetime REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}$', departure_datetime, NULL), '%Y-%m-%d %H:%i:%s');

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

-- ---------------------------------------------------------------------
-- 7. Category not in the known domain (profiled: exactly 3 classes)
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'UNKNOWN_CATEGORY',
       CONCAT_WS(',',
         CASE WHEN TRIM(COALESCE(travel_class,''))   NOT IN ('Economy','Business','First Class')      THEN CONCAT('class=',      travel_class)   END,
         CASE WHEN TRIM(COALESCE(stopovers,''))      NOT IN ('Direct','1 Stop','2 Stops')             THEN CONCAT('stopovers=',  stopovers)      END,
         CASE WHEN TRIM(COALESCE(seasonality,''))    NOT IN ('Regular','Winter Holidays','Hajj','Eid') THEN CONCAT('seasonality=', seasonality)   END),
       JSON_OBJECT('class', travel_class, 'stopovers', stopovers, 'seasonality', seasonality)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND (   TRIM(COALESCE(travel_class,'')) NOT IN ('Economy','Business','First Class')
       OR TRIM(COALESCE(stopovers,''))    NOT IN ('Direct','1 Stop','2 Stops')
       OR TRIM(COALESCE(seasonality,''))  NOT IN ('Regular','Winter Holidays','Hajj','Eid'));

-- ---------------------------------------------------------------------
-- 8. Negative lead time
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'NEGATIVE_LEAD_TIME',
       CONCAT('days_before_departure=', days_before_departure),
       JSON_OBJECT('days_before_departure', days_before_departure)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND days_before_departure REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND CAST(days_before_departure AS DECIMAL(20,10)) < 0;

-- ---------------------------------------------------------------------
-- 9. Fare arithmetic violation.
--
-- THIS IS THE SUBTLE ONE. Profiling found 2,522 rows (4.42%) where
-- total = (base + tax) * 1.2 EXACTLY — min, median and max discrepancy all
-- 16.6667%, zero rows under. That is a deterministic rule, not corruption,
-- so it is FLAGGED in staging rather than rejected. Rejecting it would
-- discard 4.42% of a clean dataset over a pattern we do not understand.
--
-- Only arithmetic that is broken in some OTHER way lands here.
-- ---------------------------------------------------------------------
INSERT INTO rejects_flight_prices (batch_id, raw_row_num, reason_code, reason_detail, payload)
SELECT batch_id, raw_row_num, 'FARE_ARITHMETIC',
       CONCAT('base+tax=', CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10)),
              ' total=',   CAST(total_fare_bdt AS DECIMAL(20,10))),
       JSON_OBJECT('base', base_fare_bdt, 'tax', tax_surcharge_bdt, 'total', total_fare_bdt)
FROM raw_flight_prices
WHERE batch_id = '{{ run_id }}'
  AND base_fare_bdt     REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND tax_surcharge_bdt REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  AND total_fare_bdt    REGEXP '^[+-]?[0-9]+(\\.[0-9]+)?$'
  -- differs from base + tax ...
  AND ABS(CAST(total_fare_bdt AS DECIMAL(20,10))
          - (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))))
      > {{ params.fare_tolerance }}
  -- ... and is NOT the known x1.2 markup
  AND ABS(CAST(total_fare_bdt AS DECIMAL(20,10))
          - 1.2 * (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))))
      > {{ params.fare_tolerance }};

-- ---------------------------------------------------------------------
-- 10. Airport code not in the known domain — the brief's "invalid city
--     names". Membership in ref_airports, not a pattern match: a code can
--     be perfectly well-formed ("XXX") and still not be a real airport.
--
--     Rows with a blank code are skipped here because MISSING_REQUIRED has
--     already reported them; flagging both adds noise without information.
-- ---------------------------------------------------------------------
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
               PARTITION BY MD5(CONCAT_WS('',
                   COALESCE(airline,'~'), COALESCE(source_code,'~'), COALESCE(destination_code,'~'),
                   COALESCE(departure_datetime,'~'), COALESCE(arrival_datetime,'~'),
                   COALESCE(travel_class,'~'), COALESCE(booking_source,'~'),
                   COALESCE(base_fare_bdt,'~'), COALESCE(tax_surcharge_bdt,'~'),
                   COALESCE(total_fare_bdt,'~'), COALESCE(seasonality,'~'),
                   COALESCE(days_before_departure,'~')))
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
