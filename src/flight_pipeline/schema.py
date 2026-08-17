"""Column contracts, declared once.

DDL is hand-written, not generated from these tuples; tests/unit/
test_schema_contract.py parses it and fails if the two disagree.
"""

from __future__ import annotations

# Source CSV -> landing table. Source names carry spaces, ampersands and
# parentheses, so they are normalised exactly once, here. `Class` becomes
# travel_class to dodge the reserved word.
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

# The brief's must-exist set (written there without the "(BDT)" suffix the
# file actually uses).
REQUIRED_SOURCE_COLUMNS: tuple[str, ...] = (
    "Airline",
    "Source",
    "Destination",
    "Base Fare (BDT)",
    "Tax & Surcharge (BDT)",
    "Total Fare (BDT)",
)

# stg_flights -> fct_flights. Order is significant: it drives both the SELECT
# and the COPY, which are positional.
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

# Declared in DDL, populated by the database, never transferred.
DB_MANAGED_COLUMNS: tuple[str, ...] = ("loaded_at",)


def fact_column_list() -> str:
    """Comma-separated column list for SELECT / COPY."""
    return ", ".join(FACT_COLUMNS)


def assert_live_fact_schema(actual_columns: list[str]) -> None:
    """Raise if the live fct_flights has drifted from this contract.

    fct_flights uses CREATE TABLE IF NOT EXISTS, which cannot alter an existing
    table, so a DDL edit can silently fail to reach the database.
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
