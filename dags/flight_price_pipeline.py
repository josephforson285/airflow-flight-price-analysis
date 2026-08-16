"""Flight Price Analysis — CSV -> MySQL staging -> PostgreSQL analytics.

Orchestration only. Airflow moves *control*; the databases move *data*.
XCom carries row counts and batch ids — never DataFrames. XCom is backed by
Airflow's own metadata database, so pushing a 57k-row payload through it
turns the orchestrator's bookkeeping store into a data warehouse.

Re-runnability: every task is a full refresh (delete-then-load), so clearing
and re-running any task leaves identical row counts. A bare INSERT would
silently double every booking count on the second run.
"""

from __future__ import annotations

import csv
import logging
import os

from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, task

from flight_pipeline.config import get_config
from flight_pipeline.schema import (
    REQUIRED_SOURCE_COLUMNS,
    SOURCE_TO_LANDING,
    fact_column_list,
)

log = logging.getLogger(__name__)

# Everything tunable comes from config/pipeline.yml. Nothing in this file is
# a magic number, and no path is hardcoded to /opt/airflow — the config module
# resolves them from the project root, so the same code runs in the container,
# in CI and in a bare checkout.
CONFIG = get_config()

MYSQL_CONN_ID = CONFIG.connections.mysql
POSTGRES_CONN_ID = CONFIG.connections.postgres
INSERT_BATCH_SIZE = CONFIG.ingest_batch_size


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
    # Constants the SQL needs but which are NOT per-run tunable. Macros rather
    # than params, because params are the trigger-time override surface and
    # putting a regex there invites someone to edit it in the UI.
    #
    # dt_regex in particular was previously copy-pasted at eight call sites
    # across two files. A typo in one copy would have silently disabled that
    # one date check while every test still passed.
    user_defined_macros={
        "dt_regex": CONFIG.validation.datetime_regex,
        "dt_format": CONFIG.validation.datetime_format,
        "markup_factor": CONFIG.business_rules.fare_markup_factor,
        "regular_season": CONFIG.business_rules.regular_season_label,
    },
    params={
        # The trigger-time override surface. source_csv_path is what lets the
        # same DAG run against the corrupted fixture with no code change:
        #   {"source_csv_path": ".../fixtures/corrupted_sample.csv"}
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
    # -----------------------------------------------------------------
    # DDL. The two branches are independent — the analytics schema has no
    # dependency on the staging work — so they build in parallel and only
    # converge at the transfer.
    # -----------------------------------------------------------------
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

    # -----------------------------------------------------------------
    # Ingest
    # -----------------------------------------------------------------
    @task
    def ingest_csv_to_mysql(**context) -> dict:
        """Load the CSV into the landing table as TEXT.

        Two things this deliberately does not do:

        1. It does not cast. Every column lands as VARCHAR so a malformed
           value fails a validation *we* wrote, on a row we can inspect,
           instead of aborting the load with a driver error naming no row.
        2. It does not split on commas. Airport names contain them
           ("...International Airport, Kolkata"), so naive splitting yields
           17, 18 or 19 fields per row. csv.reader is RFC 4180 aware.
        """
        params = context["params"]
        run_id = context["run_id"]
        csv_path = params["source_csv_path"]

        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"source CSV not found: {csv_path}")

        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            header = reader.fieldnames or []

            missing = [c for c in REQUIRED_SOURCE_COLUMNS if c not in header]
            if missing:
                raise ValueError(
                    f"CSV is missing required columns: {missing}. Found: {header}"
                )
            unmapped = [c for c in header if c not in SOURCE_TO_LANDING]
            if unmapped:
                raise ValueError(
                    f"CSV has columns this pipeline does not know how to map: "
                    f"{unmapped}. Update SOURCE_TO_LANDING in "
                    f"plugins/flight_pipeline/schema.py before ingesting."
                )

            target_cols = ["batch_id", "raw_row_num"] + [
                SOURCE_TO_LANDING[c] for c in header
            ]
            placeholders = ", ".join(["%s"] * len(target_cols))
            insert_sql = (
                f"INSERT INTO raw_flight_prices ({', '.join(target_cols)}) "
                f"VALUES ({placeholders})"
            )

            hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            conn = hook.get_conn()
            conn.autocommit(False)
            cursor = conn.cursor()

            # Idempotency: clear before loading. Re-running this task must
            # not append a second copy.
            cursor.execute("DELETE FROM raw_flight_prices")

            buffer: list[tuple] = []
            total = 0
            for line_no, row in enumerate(reader, start=1):
                buffer.append(
                    (run_id, line_no, *[row[c] for c in header])
                )
                if len(buffer) >= INSERT_BATCH_SIZE:
                    cursor.executemany(insert_sql, buffer)
                    total += len(buffer)
                    buffer.clear()
            if buffer:
                cursor.executemany(insert_sql, buffer)
                total += len(buffer)

            conn.commit()
            cursor.close()
            conn.close()

        log.info("ingested %s rows from %s as batch %s", total, csv_path, run_id)
        # Small scalars only — this is what XCom is for.
        return {"rows_ingested": total, "batch_id": run_id, "source": csv_path}

    @task
    def seed_reference_data() -> dict:
        """Load the known-airport domain used by the UNKNOWN_AIRPORT rule.

        Independent of the CSV ingest, so it runs alongside it rather than
        after it.
        """
        ref_path = CONFIG.paths.reference_airports
        if not ref_path.is_file():
            raise FileNotFoundError(f"reference data missing: {ref_path}")

        with open(ref_path, newline="", encoding="utf-8") as fh:
            rows = [
                (
                    r["airport_code"].strip().upper(),
                    r["airport_name"].strip(),
                    int(r["is_origin"]),
                    int(r["is_destination"]),
                )
                for r in csv.DictReader(fh)
            ]
        if not rows:
            raise ValueError("reference airport file is empty")

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit(False)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM ref_airports")
        cursor.executemany(
            "INSERT INTO ref_airports "
            "(airport_code, airport_name, is_origin, is_destination) "
            "VALUES (%s, %s, %s, %s)",
            rows,
        )
        conn.commit()
        cursor.close()
        conn.close()

        log.info("seeded %s reference airports", len(rows))
        return {"airports": len(rows)}

    # -----------------------------------------------------------------
    # Validation
    # -----------------------------------------------------------------
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

        Its own task on purpose: the UI then shows precisely which gate
        failed, and you can clear and re-run just this one after adjusting
        the threshold rather than re-ingesting.
        """
        threshold = float(context["params"]["reject_rate_threshold"])
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)

        ingested = int(ingest_result["rows_ingested"])
        distinct_bad = hook.get_first(
            "SELECT COUNT(DISTINCT raw_row_num) FROM rejects_flight_prices"
        )[0]
        by_reason = hook.get_records(
            "SELECT reason_code, COUNT(*) FROM rejects_flight_prices "
            "GROUP BY reason_code ORDER BY 2 DESC"
        )

        rate = (distinct_bad / ingested) if ingested else 0.0
        log.info("rejected %s of %s rows (%.4f)", distinct_bad, ingested, rate)
        for reason, count in by_reason:
            log.info("  %-26s %s", reason, count)

        if ingested == 0:
            raise ValueError("no rows were ingested — refusing to continue")
        if rate > threshold:
            raise ValueError(
                f"reject rate {rate:.4f} exceeds threshold {threshold:.4f}. "
                f"Breakdown: {dict(by_reason)}"
            )

        return {
            "rows_ingested": ingested,
            "rows_rejected": int(distinct_bad),
            "reject_rate": round(rate, 6),
            "by_reason": {r: int(c) for r, c in by_reason},
        }

    build_stg_flights = SQLExecuteQueryOperator(
        task_id="build_stg_flights",
        conn_id=MYSQL_CONN_ID,
        sql="mysql/04_build_stg.sql",
        split_statements=True,
        return_last=False,
    )

    # Declarative checks on the *typed* table, where they can be trusted.
    # Running these against the raw VARCHAR landing table would compare
    # strings and quietly pass.
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

    # -----------------------------------------------------------------
    # Cross-engine transfer — MySQL -> PostgreSQL
    # -----------------------------------------------------------------
    @task
    def transfer_to_postgres(**context) -> dict:
        """Stream staging rows into the analytics fact table.

        Read in server-side chunks and written with execute_values so neither
        side has to hold 57k rows in memory at once. At this size it would
        fit — but a pipeline that only works because the data is small is a
        pipeline with an undocumented expiry date.
        """
        col_list = fact_column_list()

        my_conn = MySqlHook(mysql_conn_id=MYSQL_CONN_ID).get_conn()
        my_cur = my_conn.cursor()
        my_cur.execute(f"SELECT {col_list} FROM stg_flights ORDER BY raw_row_num")

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_conn = pg_hook.get_conn()
        pg_cur = pg_conn.cursor()
        pg_cur.execute("DELETE FROM fct_flights")

        # COPY FROM STDIN, not INSERT. Postgres' bulk path skips per-row
        # statement parsing and planning entirely — for a load of this shape
        # it is the difference between seconds and minutes.
        #
        # Note this is psycopg *3* (provider 7.x ships it). psycopg2 is also
        # present as a transitive dependency, so psycopg2.extras.execute_values
        # imports happily and then fails at runtime on a psycopg3 connection.
        moved = 0
        with pg_cur.copy(f"COPY fct_flights ({col_list}) FROM STDIN") as copy:
            while True:
                chunk = my_cur.fetchmany(INSERT_BATCH_SIZE)
                if not chunk:
                    break
                for row in chunk:
                    copy.write_row(row)
                moved += len(chunk)

        pg_conn.commit()
        pg_cur.close()
        pg_conn.close()
        my_cur.close()
        my_conn.close()

        log.info("transferred %s rows into fct_flights", moved)
        return {"rows_transferred": moved}

    # -----------------------------------------------------------------
    # KPI marts — mutually independent, so they fan out
    # -----------------------------------------------------------------
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
        """Final gate.

        A DAG that ends without asserting anything about its output can go
        green while writing empty tables. This is the task that makes
        "succeeded" mean something.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        expected = int(transfer_result["rows_transferred"])

        counts = {
            table: hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            for table in (
                "fct_flights",
                "kpi_fare_by_airline",
                "kpi_seasonal_variation",
                "kpi_bookings_by_airline",
                "kpi_popular_routes",
            )
        }
        for table, count in counts.items():
            log.info("%-26s %s rows", table, count)
            if count == 0:
                raise ValueError(f"{table} is empty after a successful run")

        if counts["fct_flights"] != expected:
            raise ValueError(
                f"fct_flights has {counts['fct_flights']} rows, "
                f"expected {expected} from the transfer step"
            )

        # Bookings must reconcile to the fact table — catches a GROUP BY that
        # silently drops rows.
        booked = hook.get_first("SELECT SUM(bookings) FROM kpi_bookings_by_airline")[0]
        if int(booked) != expected:
            raise ValueError(
                f"kpi_bookings_by_airline sums to {booked}, expected {expected}"
            )

        log.info("all KPI tables populated and reconciled against fct_flights")
        return {"table_counts": {k: int(v) for k, v in counts.items()}}

    # -----------------------------------------------------------------
    # Dependencies
    # -----------------------------------------------------------------
    ingested = ingest_csv_to_mysql()
    seeded = seed_reference_data()
    reject_gate = assert_reject_rate(ingested)
    transferred = transfer_to_postgres()

    # Ingest and reference seeding are independent of each other; both must
    # land before the quarantine pass can check airport membership.
    create_staging_tables >> [ingested, seeded]
    [ingested, seeded] >> quarantine_invalid_rows >> reject_gate >> build_stg_flights
    build_stg_flights >> validate_stg_flights >> transferred

    # The analytics schema is independent of all staging work — it only has
    # to exist before the transfer.
    create_analytics_tables >> transferred

    transferred >> kpi_tasks >> assert_kpis_populated(transferred)


flight_price_pipeline()
