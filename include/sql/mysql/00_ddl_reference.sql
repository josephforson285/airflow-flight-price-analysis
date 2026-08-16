-- Reference data: the known airport domain.
--
-- This exists because "invalid city names" cannot be validated with a
-- regex — validity is membership in a set, and that set is DATA, not code.
-- Hardcoding 20 codes into a WHERE clause means every correction is a code
-- change, a review and a deploy. In a table it is an UPDATE.
--
-- Seeded from include/data/fixtures/ref_airports.csv, which is tracked in
-- git precisely because it is reference data rather than source data.
--
-- is_origin / is_destination are recorded (the source data has 8 origins but
-- 20 destinations) so a stricter directional rule can be added later without
-- re-deriving anything. The current check is membership only: rejecting a
-- legitimately-new origin would be worse than missing a reversed route.

CREATE TABLE IF NOT EXISTS ref_airports (
    airport_code   VARCHAR(8)   NOT NULL,
    airport_name   VARCHAR(255) NOT NULL,
    is_origin      TINYINT(1)   NOT NULL DEFAULT 0,
    is_destination TINYINT(1)   NOT NULL DEFAULT 0,
    PRIMARY KEY (airport_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Allowed values for the low-cardinality categorical fields.
--
-- These were previously string literals inside the quarantine SQL —
--     NOT IN ('Economy','Business','First Class')
-- repeated twice per field, in two places each. That was inconsistent with
-- how airports are handled one table above, and a reviewer would rightly ask
-- why one domain is data and three are code. There is no good answer, so
-- they are all data now.
--
-- numeric_equivalent carries a derived value alongside the domain. It is what
-- retires the CASE statement that mapped 'Direct' -> 0, '1 Stop' -> 1 in the
-- staging build: the mapping is a property of the domain, so it belongs
-- beside the domain rather than duplicated in transformation logic.
CREATE TABLE IF NOT EXISTS ref_allowed_values (
    field_name        VARCHAR(64)  NOT NULL,
    allowed_value     VARCHAR(128) NOT NULL,
    numeric_equivalent SMALLINT    NULL,
    PRIMARY KEY (field_name, allowed_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
