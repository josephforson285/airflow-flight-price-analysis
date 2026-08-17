"""Flight Price Analysis — CSV -> MySQL staging -> PostgreSQL analytics.

Wiring only; the work lives in src/flight_pipeline/, which imports no Airflow.

Airflow moves control, the databases move data: XCom carries row counts, never
DataFrames. Every task is a full refresh, so re-running one cannot double a
count.
"""

from __future__ import annotations

from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task

from flight_pipeline import checks, ingest, transfer
from flight_pipeline.config import get_config

# Everything tunable comes from include/config/pipeline.yml. No magic numbers
# here, and no path hardcoded to /opt/airflow.
CONFIG = get_config()

MYSQL_CONN_ID = CONFIG.connections.mysql
POSTGRES_CONN_ID = CONFIG.connections.postgres


def mysql_connect():
    return MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_conn()


def postgres_connect():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


@dag(
    dag_id="flight_price_pipeline",
    description="Ingest, validate and aggregate Bangladesh flight price data",
    schedule=None,  # static source file; triggered on demand
    catchup=False,
    max_active_runs=1,  # full-refresh tables — concurrent runs would fight
    tags=["flights", "etl", "kpi"],
    template_searchpath=str(CONFIG.paths.sql),
    default_args={
        "retries": 2,
        "retry_exponential_backoff": True,
    },
    # Constants the SQL needs but which are not per-run tunable. Macros rather
    # than params: params are the trigger-time override surface, and a regex
    # does not belong in a UI text box.
    user_defined_macros={
        "dt_regex": CONFIG.validation.datetime_regex,
        "dt_format": CONFIG.validation.datetime_format,
        "num_regex": CONFIG.validation.numeric_regex,
        "max_duration_hrs": CONFIG.validation.max_duration_hrs,
        "regular_season": CONFIG.business_rules.regular_season_label,
    },
    params={
        # Trigger-time overrides. source_csv_path is what lets the same DAG run
        # against the corrupted fixture with no code change.
        "source_csv_path": Param(str(CONFIG.paths.source_csv), type="string"),
        "fare_tolerance": Param(
            CONFIG.validation.fare_tolerance_bdt, type="number", minimum=0
        ),
        "reject_rate_threshold": Param(
            CONFIG.validation.reject_rate_threshold,
            type="number",
            minimum=0,
            maximum=1,
        ),
    },
)
def flight_price_pipeline():
    # DDL. The two branches are independent, so they build in parallel and
    # converge only at the transfer.
    create_staging_tables = SQLExecuteQueryOperator(
        task_id="create_staging_tables",
        conn_id=MYSQL_CONN_ID,
        sql=[
            "mysql/00_ddl_reference.sql",
            "mysql/01_ddl_raw.sql",
            "mysql/02_ddl_staging.sql",
        ],
        split_statements=True,
        return_last=False,
    )

    create_analytics_tables = SQLExecuteQueryOperator(
        task_id="create_analytics_tables",
        conn_id=POSTGRES_CONN_ID,
        sql="postgres/01_ddl_analytics.sql",
        split_statements=True,
        return_last=False,
    )

    # Ingest — independent of each other, so they run side by side.
    @task
    def ingest_csv_to_mysql(**context) -> dict:
        run_id = context["run_id"]
        csv_path = context["params"]["source_csv_path"]
        rows = ingest.load_csv_to_landing(
            csv_path=csv_path,
            batch_id=run_id,
            connect=mysql_connect,
            batch_size=CONFIG.ingest_batch_size,
        )
        # Small scalars only — this is what XCom is for.
        return {"rows_ingested": rows, "batch_id": run_id, "source": str(csv_path)}

    @task
    def seed_reference_data() -> dict:
        return ingest.seed_reference_tables(
            airports_path=CONFIG.paths.reference_airports,
            allowed_values_path=CONFIG.paths.reference_allowed_values,
            connect=mysql_connect,
        )

    # Validation
    quarantine_invalid_rows = SQLExecuteQueryOperator(
        task_id="quarantine_invalid_rows",
        conn_id=MYSQL_CONN_ID,
        sql="mysql/03_quarantine.sql",
        split_statements=True,
        return_last=False,
    )

    @task
    def assert_reject_rate(ingest_result: dict, **context) -> dict:
        """Gate the run on the quarantine rate.

        Its own task so the UI shows which gate failed and it can be re-run
        alone after adjusting the threshold.
        """
        return checks.check_reject_rate(
            connect=mysql_connect,
            ingested=int(ingest_result["rows_ingested"]),
            threshold=float(context["params"]["reject_rate_threshold"]),
        )

    build_stg_flights = SQLExecuteQueryOperator(
        task_id="build_stg_flights",
        conn_id=MYSQL_CONN_ID,
        sql="mysql/04_build_stg.sql",
        split_statements=True,
        return_last=False,
    )

    # Declarative checks on the TYPED table: run against the VARCHAR landing
    # table they would compare strings and quietly pass.
    validate_stg_flights = SQLColumnCheckOperator(
        task_id="validate_stg_flights",
        conn_id=MYSQL_CONN_ID,
        table="stg_flights",
        column_mapping={
            "base_fare_bdt": {"min": {"geq_to": 0}, "null_check": {"equal_to": 0}},
            "tax_surcharge_bdt": {"min": {"geq_to": 0}},
            "total_fare_reported_bdt": {"min": {"greater_than": 0}},
            "days_before_departure": {"min": {"geq_to": 0}},
            "duration_hrs": {"min": {"greater_than": 0}},
            "stopover_count": {"min": {"geq_to": 0}, "max": {"leq_to": 2}},
        },
    )

    # Cross-engine transfer
    @task
    def transfer_to_postgres() -> dict:
        # SSCursor is server-side. MySQLdb's default buffers the entire result
        # set client-side, which would make the chunked read a walk over memory
        # that is already fully allocated.
        from MySQLdb.cursors import SSCursor

        moved = transfer.copy_staging_to_fact(
            mysql_connect=mysql_connect,
            postgres_connect=postgres_connect,
            batch_size=CONFIG.ingest_batch_size,
            mysql_cursor_factory=SSCursor,
        )
        return {"rows_transferred": moved}

    # KPI marts — mutually independent, so they fan out.
    kpi_tasks = [
        SQLExecuteQueryOperator(
            task_id=task_id,
            conn_id=POSTGRES_CONN_ID,
            sql=sql_file,
            split_statements=True,
            return_last=False,
        )
        for task_id, sql_file in [
            ("kpi_fare_by_airline", "postgres/10_kpi_fare_by_airline.sql"),
            ("kpi_seasonal_variation", "postgres/11_kpi_seasonal_variation.sql"),
            ("kpi_bookings_by_airline", "postgres/12_kpi_bookings_by_airline.sql"),
            ("kpi_popular_routes", "postgres/13_kpi_popular_routes.sql"),
        ]
    ]

    @task
    def assert_kpis_populated(transfer_result: dict) -> dict:
        """Final gate: a DAG ending on a write can go green having written
        nothing."""
        return checks.check_kpis_populated(
            connect=postgres_connect,
            expected_rows=int(transfer_result["rows_transferred"]),
        )

    # Dependencies
    ingested = ingest_csv_to_mysql()
    seeded = seed_reference_data()
    reject_gate = assert_reject_rate(ingested)
    transferred = transfer_to_postgres()

    # Ingest and reference seeding are independent of each other; both must
    # land before the quarantine pass can check domain membership.
    create_staging_tables >> [ingested, seeded]
    [ingested, seeded] >> quarantine_invalid_rows >> reject_gate >> build_stg_flights
    build_stg_flights >> validate_stg_flights >> transferred

    # The analytics schema is independent of all staging work — it only has
    # to exist before the transfer.
    create_analytics_tables >> transferred

    transferred >> kpi_tasks >> assert_kpis_populated(transferred)


flight_price_pipeline()
