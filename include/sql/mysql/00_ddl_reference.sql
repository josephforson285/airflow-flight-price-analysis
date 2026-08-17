-- Reference data: the domains validation checks membership against.
-- Validity is a set, not a pattern ('XXX' is well-formed and not an airport),
-- so it lives in a table where a correction is an UPDATE, not a deploy.

CREATE TABLE IF NOT EXISTS ref_airports (
    airport_code   VARCHAR(8)   NOT NULL,
    airport_name   VARCHAR(255) NOT NULL,
    is_origin      TINYINT(1)   NOT NULL DEFAULT 0,
    is_destination TINYINT(1)   NOT NULL DEFAULT 0,
    PRIMARY KEY (airport_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Allowed values for the low-cardinality categorical fields.
-- numeric_equivalent carries a derived value with its domain: it is what
-- maps 'Direct' -> 0 without a CASE in the staging build.
CREATE TABLE IF NOT EXISTS ref_allowed_values (
    field_name        VARCHAR(64)  NOT NULL,
    allowed_value     VARCHAR(128) NOT NULL,
    numeric_equivalent SMALLINT    NULL,
    PRIMARY KEY (field_name, allowed_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
