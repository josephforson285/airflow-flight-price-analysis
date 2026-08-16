"""DAG integrity tests.

The cheapest test in any Airflow project is "does the DAG import at all".
A syntax error or a bad import does not fail loudly — the scheduler simply
stops seeing the DAG, and it silently disappears from the UI. These tests
turn that into a red build instead of a mystery.
"""

from __future__ import annotations

import pytest

# Airflow 3 moved DagBag out of airflow.models and dropped include_examples —
# example loading is now controlled by core.load_examples, which the Compose
# stack sets to false.
from airflow.dag_processing.dagbag import DagBag

from flight_pipeline.config import PROJECT_ROOT

DAG_ID = "flight_price_pipeline"

EXPECTED_TASKS = {
    "create_staging_tables",
    "create_analytics_tables",
    "seed_reference_data",
    "ingest_csv_to_mysql",
    "quarantine_invalid_rows",
    "assert_reject_rate",
    "build_stg_flights",
    "validate_stg_flights",
    "transfer_to_postgres",
    "kpi_fare_by_airline",
    "kpi_seasonal_variation",
    "kpi_bookings_by_airline",
    "kpi_popular_routes",
    "assert_kpis_populated",
}

KPI_TASKS = {
    "kpi_fare_by_airline",
    "kpi_seasonal_variation",
    "kpi_bookings_by_airline",
    "kpi_popular_routes",
}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    """Parse the DAGs in memory.

    Reads dagbag.dags rather than dagbag.get_dag(): in Airflow 3 get_dag()
    goes to the metadata database, so structure tests would need a migrated
    database just to inspect a task graph that was already parsed in memory.
    That made CI fail with a SQLAlchemy OperationalError on a missing `dag`
    table -- a real failure reported as entirely the wrong problem.
    """
    # Derived from the project root, not hardcoded to /opt/airflow: that path
    # exists only inside the container, so a CI runner (which checks out to
    # /home/runner/work/...) would build an empty DagBag and every assertion
    # below would fail for the wrong reason.
    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"))


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_dag_is_registered(dagbag):
    assert DAG_ID in dagbag.dags, f"parsed DAGs: {list(dagbag.dags)}"


def test_task_set_is_exactly_as_expected(dagbag):
    actual = set(dagbag.dags[DAG_ID].task_ids)
    assert actual == EXPECTED_TASKS, (
        f"missing: {EXPECTED_TASKS - actual}, unexpected: {actual - EXPECTED_TASKS}"
    )


def test_kpi_tasks_run_in_parallel(dagbag):
    """No KPI mart may depend on another.

    They aggregate the same fact table independently, so a dependency between
    them would serialise work for no reason — and would quietly mean someone
    misread "runs after" for "needs the result of".
    """
    dag = dagbag.dags[DAG_ID]
    for task_id in KPI_TASKS:
        task = dag.get_task(task_id)
        assert not (set(task.upstream_task_ids) & KPI_TASKS), (
            f"{task_id} depends on another KPI task: "
            f"{set(task.upstream_task_ids) & KPI_TASKS}"
        )
        assert not (set(task.downstream_task_ids) & KPI_TASKS)


def test_reject_gate_blocks_the_staging_build(dagbag):
    """assert_reject_rate must sit BETWEEN quarantine and build_stg_flights.

    If the gate were parallel to the build rather than upstream of it, a run
    that exceeded the reject threshold could still publish data — which
    defeats the entire point of having a threshold.
    """
    dag = dagbag.dags[DAG_ID]
    gate = dag.get_task("assert_reject_rate")
    assert "quarantine_invalid_rows" in gate.upstream_task_ids
    assert "build_stg_flights" in gate.downstream_task_ids


def test_pipeline_ends_in_an_assertion(dagbag):
    """The DAG must not end on a write.

    A pipeline whose final task is a load can go green having written
    nothing. assert_kpis_populated is what makes "success" meaningful, so it
    must genuinely be the leaf.
    """
    dag = dagbag.dags[DAG_ID]
    leaves = {t.task_id for t in dag.tasks if not t.downstream_task_ids}
    assert leaves == {"assert_kpis_populated"}, f"unexpected leaf tasks: {leaves}"


def test_ingest_and_seed_are_independent(dagbag):
    """Reference seeding and CSV ingest have no data dependency."""
    dag = dagbag.dags[DAG_ID]
    ingest = dag.get_task("ingest_csv_to_mysql")
    seed = dag.get_task("seed_reference_data")
    assert "seed_reference_data" not in ingest.upstream_task_ids
    assert "ingest_csv_to_mysql" not in seed.upstream_task_ids


def test_tasks_have_retries(dagbag):
    """Transient database blips should not fail a whole run."""
    dag = dagbag.dags[DAG_ID]
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} has no retries configured"


def test_only_one_active_run_allowed(dagbag):
    """Full-refresh tables plus concurrent runs would corrupt each other."""
    assert dagbag.dags[DAG_ID].max_active_runs == 1
