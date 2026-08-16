"""Unit tests for the gate decision logic.

These are the tests that were impossible before the package split: the
reject-rate and reconciliation rules used to live inside @task functions, so
exercising them meant running a DAG against two live databases. Extracted as
pure functions, their edge cases can be checked directly — no Airflow, no
containers, no fixtures.

Edge cases matter here more than the happy path. The gate is the thing
standing between bad data and published KPIs.
"""

from __future__ import annotations

import pytest

from flight_pipeline.checks import evaluate_kpi_counts, evaluate_reject_rate


# ---------------------------------------------------------------------
# evaluate_reject_rate
# ---------------------------------------------------------------------
def test_clean_run_passes():
    assert evaluate_reject_rate(ingested=57_000, rejected=0, threshold=0.05) == 0.0


def test_rate_below_threshold_passes():
    rate = evaluate_reject_rate(ingested=1000, rejected=40, threshold=0.05)
    assert rate == pytest.approx(0.04)


def test_rate_exactly_at_threshold_passes():
    """Boundary: the threshold is the highest ACCEPTABLE rate, not the lowest
    failing one. Off-by-one here would fail runs that are within tolerance."""
    assert evaluate_reject_rate(ingested=100, rejected=5, threshold=0.05) == 0.05


def test_rate_above_threshold_raises():
    with pytest.raises(ValueError, match="exceeds threshold"):
        evaluate_reject_rate(ingested=100, rejected=6, threshold=0.05)


def test_empty_ingest_is_a_failure_not_a_zero_percent_pass():
    """A run that loaded nothing has validated nothing.

    Naively this is 0 rejects out of 0 rows — a perfect score — and it would
    sail through the gate and publish empty KPI tables over good ones.
    """
    with pytest.raises(ValueError, match="no rows were ingested"):
        evaluate_reject_rate(ingested=0, rejected=0, threshold=0.05)


def test_more_rejects_than_rows_is_incoherent():
    """Guards against counting reject ROWS as reject RECORDS.

    A row can break several rules and gets one reject record per rule, so a
    naive COUNT(*) can exceed the ingested count. The query uses COUNT(DISTINCT
    raw_row_num); this test fails loudly if someone changes it.
    """
    with pytest.raises(ValueError, match="more rejects"):
        evaluate_reject_rate(ingested=27, rejected=31, threshold=0.9)


def test_negative_reject_count_rejected():
    with pytest.raises(ValueError, match="negative reject count"):
        evaluate_reject_rate(ingested=100, rejected=-1, threshold=0.05)


def test_the_real_fixture_scenario_fails_the_default_gate():
    """13 of 27 rows rejected against a 5% threshold — the documented case."""
    with pytest.raises(ValueError, match="exceeds threshold"):
        evaluate_reject_rate(ingested=27, rejected=13, threshold=0.05)


def test_the_real_dataset_scenario_passes():
    evaluate_reject_rate(ingested=57_000, rejected=0, threshold=0.05)


# ---------------------------------------------------------------------
# evaluate_kpi_counts
# ---------------------------------------------------------------------
def _counts(fct=57_000, **over):
    base = {
        "fct_flights": fct,
        "kpi_fare_by_airline": 24,
        "kpi_seasonal_variation": 4,
        "kpi_bookings_by_airline": 24,
        "kpi_popular_routes": 152,
    }
    base.update(over)
    return base


def test_reconciled_run_passes():
    evaluate_kpi_counts(_counts(), expected_rows=57_000, booked=57_000)


def test_empty_kpi_table_raises():
    with pytest.raises(ValueError, match="kpi_popular_routes is empty"):
        evaluate_kpi_counts(
            _counts(kpi_popular_routes=0), expected_rows=57_000, booked=57_000
        )


def test_fact_row_count_mismatch_raises():
    """Catches a transfer that silently moved fewer rows than it reported."""
    with pytest.raises(ValueError, match="expected 57000"):
        evaluate_kpi_counts(_counts(fct=56_999), expected_rows=57_000, booked=57_000)


def test_bookings_that_do_not_reconcile_raise():
    """The important one.

    Every table can be non-empty and the fact count correct while a GROUP BY
    quietly drops rows — for instance if a join introduced a NULL key. Only
    reconciling the KPI total against the fact table catches that.
    """
    with pytest.raises(ValueError, match="sums to 56000"):
        evaluate_kpi_counts(_counts(), expected_rows=57_000, booked=56_000)
