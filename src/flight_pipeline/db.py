"""Connection type and transaction handling."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress

log = logging.getLogger(__name__)

# Returns a DB-API connection. The DAG supplies these from Airflow hooks.
Connect = Callable[[], object]


def _disable_autocommit(conn) -> None:
    # MySQLdb exposes autocommit as a method, psycopg3 as a settable property.
    # Getting it wrong leaves autocommit on and makes rollback a no-op.
    attr = getattr(conn, "autocommit", None)
    if callable(attr):
        attr(False)
    elif attr is not None:
        with suppress(Exception):
            conn.autocommit = False


@contextmanager
def transaction(connect: Connect) -> Iterator:
    """Yield a cursor; commit on success, roll back on error, always close."""
    conn = connect()
    cursor = None
    try:
        _disable_autocommit(conn)
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except BaseException:
        # BaseException, not Exception: a SIGTERM'd task must also roll back
        # rather than leave a half-applied batch.
        with suppress(Exception):
            conn.rollback()
            log.warning("transaction rolled back")
        raise
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            conn.close()


@contextmanager
def read_cursor(connect: Connect, cursor_factory=None) -> Iterator:
    """Yield a read-only cursor, always closing it.

    Pass a server-side cursor class to stream: MySQLdb's default buffers the
    whole result set client-side during execute(), so a fetchmany() loop over
    it walks memory that is already fully allocated.
    """
    conn = connect()
    cursor = None
    try:
        cursor = conn.cursor(cursor_factory) if cursor_factory else conn.cursor()
        yield cursor
    finally:
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            conn.close()
