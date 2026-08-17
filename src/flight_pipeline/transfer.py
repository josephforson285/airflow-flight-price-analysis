"""The cross-engine hop: MySQL staging -> PostgreSQL analytics.

Airflow-free, like the rest of the package. Both connections arrive as
callables supplied by the DAG.
"""

from __future__ import annotations

import logging

from flight_pipeline.db import Connect, read_cursor, transaction
from flight_pipeline.schema import assert_live_fact_schema, fact_column_list

log = logging.getLogger(__name__)


def copy_staging_to_fact(
    mysql_connect: Connect,
    postgres_connect: Connect,
    batch_size: int,
    mysql_cursor_factory=None,
) -> int:
    """Stream stg_flights into fct_flights. Returns rows transferred.

    Read in chunks and written with PostgreSQL's COPY, so neither side holds
    the whole dataset in memory. At 57,000 rows it would fit — but a pipeline
    that only works because the data is small has an undocumented expiry date.

    mysql_cursor_factory must be a SERVER-SIDE cursor class for that claim to
    hold. MySQLdb's default cursor buffers the entire result set client-side
    during execute(), which quietly made the chunked read a chunked walk over
    memory that was already fully allocated. The DAG passes
    MySQLdb.cursors.SSCursor; it is injected rather than imported here so this
    module stays importable without a MySQL driver present.

    COPY rather than INSERT: it is PostgreSQL's bulk path and skips per-row
    statement parsing and planning entirely.

    Note this is psycopg *3*, which the Postgres provider 7.x returns.
    psycopg2 is present as a transitive dependency, so
    psycopg2.extras.execute_values imports without complaint and then fails at
    runtime on a psycopg3 connection with a missing-attribute error.
    """
    col_list = fact_column_list()

    moved = 0
    # The DELETE and the COPY share one transaction. If the COPY fails partway
    # the delete is rolled back too, so a failed transfer leaves the previous
    # good fact table intact rather than an empty or half-filled one. Both
    # connections close on any exit path.
    with (
        read_cursor(mysql_connect, mysql_cursor_factory) as my_cur,
        transaction(postgres_connect) as pg_cur,
    ):
        # Check the live table matches the contract BEFORE deleting anything.
        # Otherwise a drifted schema is discovered mid-COPY, after the DELETE.
        pg_cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fct_flights' ORDER BY ordinal_position"
        )
        assert_live_fact_schema([row[0] for row in pg_cur.fetchall()])

        my_cur.execute(f"SELECT {col_list} FROM stg_flights ORDER BY raw_row_num")

        # Full refresh, so the task is safe to clear and re-run.
        pg_cur.execute("DELETE FROM fct_flights")

        with pg_cur.copy(f"COPY fct_flights ({col_list}) FROM STDIN") as copy:
            while True:
                chunk = my_cur.fetchmany(batch_size)
                if not chunk:
                    break
                for row in chunk:
                    copy.write_row(row)
                moved += len(chunk)

    log.info("transferred %s rows into fct_flights", moved)
    return moved
