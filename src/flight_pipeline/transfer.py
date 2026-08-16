"""The cross-engine hop: MySQL staging -> PostgreSQL analytics.

Airflow-free, like the rest of the package. Both connections arrive as
callables supplied by the DAG.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from flight_pipeline.schema import fact_column_list

log = logging.getLogger(__name__)

Connect = Callable[[], object]


def copy_staging_to_fact(
    mysql_connect: Connect,
    postgres_connect: Connect,
    batch_size: int,
) -> int:
    """Stream stg_flights into fct_flights. Returns rows transferred.

    Read in chunks and written with PostgreSQL's COPY, so neither side holds
    the whole dataset in memory. At 57,000 rows it would fit — but a pipeline
    that only works because the data is small has an undocumented expiry date.

    COPY rather than INSERT: it is PostgreSQL's bulk path and skips per-row
    statement parsing and planning entirely.

    Note this is psycopg *3*, which the Postgres provider 7.x returns.
    psycopg2 is present as a transitive dependency, so
    psycopg2.extras.execute_values imports without complaint and then fails at
    runtime on a psycopg3 connection with a missing-attribute error.
    """
    col_list = fact_column_list()

    my_conn = mysql_connect()
    my_cur = my_conn.cursor()
    my_cur.execute(f"SELECT {col_list} FROM stg_flights ORDER BY raw_row_num")

    pg_conn = postgres_connect()
    pg_cur = pg_conn.cursor()
    # Full refresh, so the task is safe to clear and re-run.
    pg_cur.execute("DELETE FROM fct_flights")

    moved = 0
    with pg_cur.copy(f"COPY fct_flights ({col_list}) FROM STDIN") as copy:
        while True:
            chunk = my_cur.fetchmany(batch_size)
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
    return moved
