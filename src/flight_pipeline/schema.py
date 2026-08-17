"""The pipeline's column contracts, declared once.

Before this module the fact-table column list existed in three places — the
MySQL staging DDL, the PostgreSQL analytics DDL, and a Python list inside the
transfer task. Adding a column meant editing three files, and nothing caught a
mismatch until a transfer failed at runtime with a column-count error.

SQL DDL is not generated from these tuples; hand-written DDL stays readable
and reviewable, and generating it would trade one problem for a worse one.
Instead `tests/test_schema_contract.py` parses both DDL files and asserts they
agree with what is declared here, so drift fails a test in seconds rather than
a pipeline run in production.
"""

from __future__ import annotations

# ---------------------------------------------------------------------
# Source CSV -> landing table
#
# The source names carry spaces, ampersands and parentheses ("Tax &
# Surcharge (BDT)"), which are hostile as SQL identifiers. They are
# normalised exactly once, here, at the ingest boundary. `Class` becomes
# travel_class to sidestep the reserved-word question across engines.
# ---------------------------------------------------------------------
SOURCE_TO_LANDING: dict[str, str] = {
    "Airline": "airline",
    "Source": "source_code",
    "Source Name": "source_name",
    "Destination": "destination_code",
    "Destination Name": "destination_name",
    "Departure Date & Time": "departure_datetime",
    "Arrival Date & Time": "arrival_datetime",
    "Duration (hrs)": "duration_hrs",
    "Stopovers": "stopovers",
    "Aircraft Type": "aircraft_type",
    "Class": "travel_class",
    "Booking Source": "booking_source",
    "Base Fare (BDT)": "base_fare_bdt",
    "Tax & Surcharge (BDT)": "tax_surcharge_bdt",
    "Total Fare (BDT)": "total_fare_bdt",
    "Seasonality": "seasonality",
    "Days Before Departure": "days_before_departure",
}

# The brief names these as the must-exist set. It writes them without the
# "(BDT)" suffix the file actually uses — recorded as a deviation in the
# project report.
REQUIRED_SOURCE_COLUMNS: tuple[str, ...] = (
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
)

# ---------------------------------------------------------------------
# stg_flights -> fct_flights
#
# Order is significant: it is the column order used by both the SELECT out of
# MySQL and the COPY into PostgreSQL, so the two cannot disagree.
# ---------------------------------------------------------------------
FACT_COLUMNS: tuple[str, ...] = (
    "batch_id",
    "raw_row_num",
    "airline",
    "source_code",
    "source_name",
    "destination_code",
    "destination_name",
    "departure_at",
    "arrival_at",
    "duration_hrs",
    "stopovers",
    "stopover_count",
    "aircraft_type",
    "travel_class",
    "booking_source",
    "base_fare_bdt",
    "tax_surcharge_bdt",
    "total_fare_reported_bdt",
    "total_fare_computed_bdt",
    "fare_variance_bdt",
    "has_fare_markup",
    "markup_pct",
    "seasonality",
    "is_peak_season",
    "days_before_departure",
)

# Columns the database populates itself, so they are declared in DDL but never
# transferred. Kept separate so the contract test can tell "missing" apart
# from "deliberately database-managed".
DB_MANAGED_COLUMNS: tuple[str, ...] = ("loaded_at",)


def fact_column_list() -> str:
    """Comma-separated column list for SELECT / COPY statements."""
    return ", ".join(FACT_COLUMNS)


def assert_live_fact_schema(actual_columns: list[str]) -> None:
    """Raise if the live fct_flights does not match this contract.

    fct_flights is created with CREATE TABLE IF NOT EXISTS, which is a no-op
    against an existing table: editing the DDL changes the file and not the
    database. The KPI marts avoid this by rebuilding themselves through an
    atomic swap, but the fact table is a load target, not a derivative — so it
    gets an explicit check instead.

    Without it, a schema change surfaces as a COPY failing on an unknown
    column, which reads like a bug in the transfer. With it, the error names
    exactly what drifted and what to do about it.
    """
    expected = list(FACT_COLUMNS) + list(DB_MANAGED_COLUMNS)
    if list(actual_columns) == expected:
        return

    missing = [c for c in expected if c not in actual_columns]
    extra = [c for c in actual_columns if c not in expected]
    detail = []
    if missing:
        detail.append(f"missing from the database: {missing}")
    if extra:
        detail.append(f"present in the database but not declared: {extra}")
    if not detail:
        detail.append(f"column ORDER differs: {actual_columns} != {expected}")

    raise ValueError(
        "fct_flights has drifted from the declared schema — "
        + "; ".join(detail)
        + ". CREATE TABLE IF NOT EXISTS cannot alter an existing table, so the "
        "DDL edit did not reach the database. Migrate it, or drop the table "
        "and re-run (all data is regenerated from staging)."
    )
