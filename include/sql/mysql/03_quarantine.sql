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
--
-- Covers EVERY column that is NOT NULL in stg_flights, not just the six the
-- brief names. The earlier version checked seven; staging declares twenty-odd
-- NOT NULL. A blank aircraft_type or booking_source therefore passed
-- quarantine and then blew up on the INSERT into stg_flights with a driver
-- constraint error naming no row — exactly the opaque failure the landing
-- zone exists to prevent.
--
-- The blank list is computed once in a derived table and reused as the
-- filter, rather than restating seventeen conditions in the WHERE clause.
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 7. Category not in the known domain.
--
-- Membership in ref_allowed_values, not literals in this file. The domains
-- were previously hardcoded here as NOT IN (...) lists, written out twice
-- each, which contradicted the argument made for airports one rule below:
-- validity is data, so correcting it should be an UPDATE rather than a code
-- change and a redeploy.
-- ---------------------------------------------------------------------
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
  AND base_fare_bdt     REGEXP '{{ num_regex }}'
  AND tax_surcharge_bdt REGEXP '{{ num_regex }}'
  AND total_fare_bdt    REGEXP '{{ num_regex }}'
  -- differs from base + tax ...
  AND ABS(CAST(total_fare_bdt AS DECIMAL(20,10))
          - (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))))
      > {{ params.fare_tolerance }}
  -- ... and is NOT the known x1.2 markup
  AND ABS(CAST(total_fare_bdt AS DECIMAL(20,10))
          - {{ markup_factor }} * (CAST(base_fare_bdt AS DECIMAL(20,10)) + CAST(tax_surcharge_bdt AS DECIMAL(20,10))))
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
               -- ALL 17 source columns. The fingerprint previously covered
               -- 12, omitting source_name, destination_name, duration_hrs,
               -- stopovers and aircraft_type -- so two rows differing only in,
               -- say, aircraft type hashed identically and the second was
               -- quarantined as an "identical row" it was not. The rule was
               -- called DUPLICATE_ROW and reported "occurrence #N of an
               -- identical row" while actually testing a partial business key.
               --
               -- Separator is CHAR(31) (ASCII unit separator), not ''. With an
               -- empty separator the fingerprint is ambiguous: airline 'US'
               -- plus source 'BD' concatenates to the same string as 'USB'
               -- plus 'D', so different rows could collide. CHAR(30) stands in
               -- for NULL for the same reason -- a literal '~' in the data
               -- could otherwise impersonate a null.
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

-- ---------------------------------------------------------------------
-- 12. Non-numeric measures.
--
-- duration_hrs had NO validation at all, and days_before_departure was only
-- checked for being negative — a check that silently skipped any value that
-- was not a number in the first place. Both are cast in the staging build and
-- both land in NOT NULL columns, so junk here produced a cast warning or a
-- constraint error at INSERT time rather than an inspectable reject.
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 13. Implausible duration.
--
-- A flight cannot take zero or negative time, and a value beyond the
-- configured bound is far more likely a unit error (minutes recorded as
-- hours) than a real itinerary. Profiled range is 0.5–15.83 hours.
--
-- validate_stg_flights already asserts duration_hrs > 0, but that runs AFTER
-- the staging build: it would fail the run rather than quarantine the row,
-- which is the wrong response to one bad record among 57,000.
-- ---------------------------------------------------------------------
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
