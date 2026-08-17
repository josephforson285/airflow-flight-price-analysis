"""The schema contract: DDL must agree with schema.py.

The fact-table column list is consumed by three things — the MySQL staging
DDL, the PostgreSQL analytics DDL, and the transfer task. Previously all three
declared it independently, so adding a column meant remembering three files
and a mistake surfaced only as a runtime column-count error mid-transfer.

Rather than generate DDL from Python (which would make the SQL less readable
for the sake of a problem a test can solve), these tests parse the checked-in
DDL and assert it matches the declaration. Drift fails in seconds.

No Airflow import anywhere in this file — it is a pure-Python contract check.
"""

from __future__ import annotations

import re

import pytest

from flight_pipeline.config import get_config
from flight_pipeline.schema import (
    DB_MANAGED_COLUMNS,
    FACT_COLUMNS,
    REQUIRED_SOURCE_COLUMNS,
    SOURCE_TO_LANDING,
    assert_live_fact_schema,
    fact_column_list,
)

SQL_DIR = get_config().paths.sql

_SKIP = re.compile(r"^(PRIMARY\s+KEY|KEY|UNIQUE|INDEX|CONSTRAINT|FOREIGN)", re.I)
_COLUMN = re.compile(r"^([a-z_][a-z0-9_]*)\s+[A-Za-z]")


def ddl_columns(sql_text: str, table: str) -> list[str]:
    """Extract column names from a CREATE TABLE block."""
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS\s+{table}\s*\((.*?)\n\)", sql_text, re.S
    )
    if not match:
        raise AssertionError(f"no CREATE TABLE block found for {table}")

    columns = []
    for raw in match.group(1).splitlines():
        line = raw.strip()
        if not line or line.startswith("--") or _SKIP.match(line):
            continue
        found = _COLUMN.match(line)
        if found:
            columns.append(found.group(1))
    return columns


@pytest.fixture(scope="module")
def staging_ddl() -> str:
    return (SQL_DIR / "mysql" / "02_ddl_staging.sql").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def analytics_ddl() -> str:
    return (SQL_DIR / "postgres" / "01_ddl_analytics.sql").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def landing_ddl() -> str:
    return (SQL_DIR / "mysql" / "01_ddl_raw.sql").read_text(encoding="utf-8")


def test_staging_ddl_matches_declared_schema(staging_ddl):
    actual = ddl_columns(staging_ddl, "stg_flights")
    expected = list(FACT_COLUMNS) + list(DB_MANAGED_COLUMNS)
    assert actual == expected, (
        f"stg_flights DDL drifted.\n  missing: {set(expected) - set(actual)}"
        f"\n  extra:   {set(actual) - set(expected)}"
    )


def test_analytics_ddl_matches_declared_schema(analytics_ddl):
    actual = ddl_columns(analytics_ddl, "fct_flights")
    expected = list(FACT_COLUMNS) + list(DB_MANAGED_COLUMNS)
    assert actual == expected, (
        f"fct_flights DDL drifted.\n  missing: {set(expected) - set(actual)}"
        f"\n  extra:   {set(actual) - set(expected)}"
    )


def test_both_engines_agree_column_for_column(staging_ddl, analytics_ddl):
    """The transfer is a positional COPY, so order must match, not just names."""
    assert ddl_columns(staging_ddl, "stg_flights") == ddl_columns(
        analytics_ddl, "fct_flights"
    )


def test_landing_table_covers_every_mapped_column(landing_ddl):
    """Every column the ingest writes must exist in the landing DDL."""
    landing = set(ddl_columns(landing_ddl, "raw_flight_prices"))
    missing = set(SOURCE_TO_LANDING.values()) - landing
    assert not missing, f"landing DDL is missing mapped columns: {missing}"


def test_fact_column_list_is_sql_safe():
    rendered = fact_column_list()
    assert rendered.count(",") == len(FACT_COLUMNS) - 1
    assert "--" not in rendered and ";" not in rendered


def test_required_columns_are_a_subset_of_the_mapping():
    missing = set(REQUIRED_SOURCE_COLUMNS) - set(SOURCE_TO_LANDING)
    assert not missing, f"required columns absent from the mapping: {missing}"


def test_no_duplicate_declarations():
    assert len(FACT_COLUMNS) == len(set(FACT_COLUMNS))
    assert len(set(SOURCE_TO_LANDING.values())) == len(SOURCE_TO_LANDING)
    assert not set(FACT_COLUMNS) & set(DB_MANAGED_COLUMNS)


# ---------------------------------------------------------------------
# Live-database drift guard
# ---------------------------------------------------------------------
def _live(*, drop=None, add=None, reorder=False):
    cols = list(FACT_COLUMNS) + list(DB_MANAGED_COLUMNS)
    if drop:
        cols = [c for c in cols if c != drop]
    if add:
        cols.append(add)
    if reorder:
        cols[0], cols[1] = cols[1], cols[0]
    return cols


def test_matching_live_schema_passes():
    # must not raise
    assert_live_fact_schema(_live())


def test_missing_column_is_named_in_the_error():
    with pytest.raises(ValueError, match=r"missing from the database.*airline"):
        assert_live_fact_schema(_live(drop="airline"))


def test_undeclared_column_is_named_in_the_error():
    """The revenue_bdt case: DDL edited, database still holding the old shape."""
    with pytest.raises(ValueError, match=r"not declared.*revenue_bdt"):
        assert_live_fact_schema(_live(add="revenue_bdt"))


def test_column_order_difference_is_caught():
    """Order matters: the transfer is a positional COPY, so same-names-wrong-
    order would load every value into the wrong column without erroring."""
    with pytest.raises(ValueError, match="ORDER differs"):
        assert_live_fact_schema(_live(reorder=True))


def test_error_explains_why_the_ddl_edit_did_not_apply():
    with pytest.raises(ValueError, match="CREATE TABLE IF NOT EXISTS cannot alter"):
        assert_live_fact_schema(_live(drop="markup_pct"))
