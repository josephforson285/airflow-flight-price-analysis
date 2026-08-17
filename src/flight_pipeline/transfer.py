"""MySQL staging -> PostgreSQL analytics."""

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

    COPY rather than INSERT: it skips per-row parsing and planning.
    mysql_cursor_factory must be a server-side cursor class or the chunked read
    is not actually chunked (see db.read_cursor).

    Note this is psycopg *3*. psycopg2 is present as a transitive dependency,
    so its execute_values imports fine and then fails at runtime.
    """
    col_list = fact_column_list()
    moved = 0

    # DELETE and COPY share one transaction, so a failed transfer leaves the
    # previous fact table intact rather than an empty one.
    with (
        read_cursor(mysql_connect, mysql_cursor_factory) as my_cur,
        transaction(postgres_connect) as pg_cur,
    ):
        # Checked before the DELETE: otherwise drift is found mid-COPY.
        pg_cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fct_flights' ORDER BY ordinal_position"
        )
        assert_live_fact_schema([row[0] for row in pg_cur.fetchall()])

        my_cur.execute(f"SELECT {col_list} FROM stg_flights ORDER BY raw_row_num")
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
