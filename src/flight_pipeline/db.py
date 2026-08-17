"""Database plumbing: the connection type, and transaction handling.

Two problems this solves.

The `Connect` alias was declared independently in ingest.py, transfer.py and
checks.py — three copies of one definition, which is how they drift.

More seriously, none of those modules had any failure handling. They opened a
connection, worked, committed and closed. If anything raised in between, the
commit never ran, the rollback never ran, and the connection was never closed:
it leaked until garbage collection, holding server-side locks and a slot in
the connection limit. On MySQL a half-finished multi-statement load would sit
in an open transaction until the server timed it out.

`transaction()` makes the failure path explicit — commit on success, rollback
on any exception, close either way — and, being a context manager, it cannot
be forgotten at a new call site the way a stray `conn.close()` can.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress

log = logging.getLogger(__name__)

# A callable returning a DB-API connection. The DAG supplies these from Airflow
# hooks; tests can supply anything with the same shape.
Connect = Callable[[], object]


def _disable_autocommit(conn) -> None:
    """Turn autocommit off across both drivers.

    MySQLdb exposes autocommit as a *method*; psycopg3 as a settable
    *property*. Getting this wrong silently leaves autocommit on, which would
    make rollback a no-op — the failure mode this module exists to prevent.
    """
    attr = getattr(conn, "autocommit", None)
    if callable(attr):
        attr(False)
    elif attr is not None:
        with suppress(Exception):
            conn.autocommit = False


@contextmanager
def transaction(connect: Connect) -> Iterator:
    """Yield a cursor inside a transaction. Commit, rollback and close."""
    conn = connect()
    cursor = None
    try:
        _disable_autocommit(conn)
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except BaseException:
        # BaseException, not Exception: a task killed by SIGTERM (Airflow
        # marking it up_for_retry, a pod eviction) must also roll back rather
        # than leave a half-applied batch behind.
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
    """Yield a cursor for read-only work, always closing it.

    No commit: nothing was written. Kept separate from transaction() so a
    reader cannot accidentally commit and so the intent is visible at the
    call site.

    cursor_factory exists for streaming. MySQLdb's default cursor is
    CLIENT-SIDE buffered: it pulls the entire result set into the client during
    execute(), so a fetchmany() loop over it iterates memory that is already
    fully allocated. Measured on this pipeline, `cursor.rowcount` returned
    57,000 immediately after execute() and before a single fetch — proof the
    rows were already here. Passing a server-side cursor class makes the
    chunked read genuinely chunked.

    The parameter is a factory rather than a hard-coded class so this module
    stays driver-agnostic and importable without MySQLdb installed.
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
