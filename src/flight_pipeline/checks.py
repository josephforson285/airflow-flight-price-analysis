"""The pipeline's gates.

Each gate is split in two: a pure decision function that takes numbers and
raises, and a thin wrapper that fetches those numbers from a database. The
decision logic is where the bugs live and where the edge cases are — zero
rows, a rate exactly on the threshold, a KPI that fails to reconcile — so it
is separated out and unit-tested directly, with no database involved.
"""

from __future__ import annotations

import logging

from flight_pipeline.db import Connect, read_cursor

log = logging.getLogger(__name__)

KPI_TABLES = (
    "fct_flights",
    "kpi_fare_by_airline",
    "kpi_seasonal_variation",
    "kpi_bookings_by_airline",
    "kpi_popular_routes",
)


# ---------------------------------------------------------------------
# Pure decision logic
# ---------------------------------------------------------------------
def evaluate_reject_rate(
    ingested: int,
    rejected: int,
    threshold: float,
    breakdown: dict[str, int] | None = None,
) -> float:
    """Return the reject rate, raising if the run should not proceed.

    An empty ingest is treated as a failure rather than a 0% reject rate. A
    run that loaded nothing has not validated anything, and letting it through
    would publish empty KPI tables over good ones.
    """
    if ingested <= 0:
        raise ValueError("no rows were ingested — refusing to continue")
    if rejected < 0:
        raise ValueError(f"negative reject count: {rejected}")
    if rejected > ingested:
        raise ValueError(f"more rejects ({rejected}) than ingested rows ({ingested})")

    rate = rejected / ingested
    if rate > threshold:
        raise ValueError(
            f"reject rate {rate:.4f} exceeds threshold {threshold:.4f}. "
            f"Breakdown: {breakdown or {}}"
        )
    return rate


def evaluate_kpi_counts(
    counts: dict[str, int],
    expected_rows: int,
    booked: int,
) -> None:
    """Raise unless every KPI table is populated and reconciles.

    The reconciliation is the part that matters. Non-empty only proves
    something was written; equal to the fact table proves a GROUP BY did not
    quietly drop rows.
    """
    for table, count in counts.items():
        if count == 0:
            raise ValueError(f"{table} is empty after a successful run")

    actual = counts.get("fct_flights")
    if actual != expected_rows:
        raise ValueError(
            f"fct_flights has {actual} rows, expected {expected_rows} "
            f"from the transfer step"
        )
    if booked != expected_rows:
        raise ValueError(
            f"kpi_bookings_by_airline sums to {booked}, expected {expected_rows}"
        )


# ---------------------------------------------------------------------
# Database-backed wrappers
# ---------------------------------------------------------------------
def _scalar(cursor, sql: str):
    cursor.execute(sql)
    return cursor.fetchone()[0]


def check_reject_rate(connect: Connect, ingested: int, threshold: float) -> dict:
    with read_cursor(connect) as cursor:
        rejected = int(
            _scalar(
                cursor,
                "SELECT COUNT(DISTINCT raw_row_num) FROM rejects_flight_prices",
            )
        )
        cursor.execute(
            "SELECT reason_code, COUNT(*) FROM rejects_flight_prices "
            "GROUP BY reason_code ORDER BY 2 DESC"
        )
        breakdown = {reason: int(count) for reason, count in cursor.fetchall()}

    log.info("rejected %s of %s rows", rejected, ingested)
    for reason, count in breakdown.items():
        log.info("  %-26s %s", reason, count)

    rate = evaluate_reject_rate(ingested, rejected, threshold, breakdown)
    return {
        "rows_ingested": ingested,
        "rows_rejected": rejected,
        "reject_rate": round(rate, 6),
        "by_reason": breakdown,
    }


def check_kpis_populated(connect: Connect, expected_rows: int) -> dict:
    with read_cursor(connect) as cursor:
        counts = {
            table: int(_scalar(cursor, f"SELECT COUNT(*) FROM {table}"))
            for table in KPI_TABLES
        }
        booked = int(_scalar(cursor, "SELECT SUM(bookings) FROM kpi_bookings_by_airline"))

    for table, count in counts.items():
        log.info("%-26s %s rows", table, count)

    evaluate_kpi_counts(counts, expected_rows, booked)
    log.info("all KPI tables populated and reconciled against fct_flights")
    return {"table_counts": counts}
