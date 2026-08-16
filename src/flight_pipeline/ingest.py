"""CSV ingestion and reference seeding.

No Airflow import anywhere in this module. Database access arrives as a
`connect` callable returning a DB-API connection, which the DAG supplies from
a hook. That inversion is what keeps this logic testable without a scheduler,
a metadata database or a 3 GB image — and it is why these functions read as
plain Python rather than as Airflow internals.
"""

from __future__ import annotations

import csv
import logging
from collections.abc import Callable
from pathlib import Path

from flight_pipeline.schema import REQUIRED_SOURCE_COLUMNS, SOURCE_TO_LANDING

log = logging.getLogger(__name__)

Connect = Callable[[], object]


def validate_header(header: list[str]) -> None:
    """Fail fast on a source file whose shape we do not recognise.

    Two distinct failures, reported separately because the fixes differ: a
    missing required column is a broken extract, whereas an unmapped column is
    a schema change nobody told us about.
    """
    missing = [c for c in REQUIRED_SOURCE_COLUMNS if c not in header]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}. Found: {header}")
    unmapped = [c for c in header if c not in SOURCE_TO_LANDING]
    if unmapped:
        raise ValueError(
            f"CSV has columns this pipeline does not know how to map: {unmapped}. "
            f"Update SOURCE_TO_LANDING in src/flight_pipeline/schema.py."
        )


def load_csv_to_landing(
    csv_path: str | Path,
    batch_id: str,
    connect: Connect,
    batch_size: int,
) -> int:
    """Load the CSV into raw_flight_prices as text. Returns rows loaded.

    Deliberately does not cast: every column lands as VARCHAR so a malformed
    value fails a validation we wrote, on a row we can inspect, instead of
    aborting the load with a driver error naming no row.

    Deliberately does not split on commas: airport names contain them
    ("...International Airport, Kolkata"), which yields 17, 18 or 19 fields
    per row under naive splitting. csv.DictReader is RFC 4180 aware.
    """
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"source CSV not found: {path}")

    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        validate_header(header)

        target_cols = ["batch_id", "raw_row_num"] + [SOURCE_TO_LANDING[c] for c in header]
        placeholders = ", ".join(["%s"] * len(target_cols))
        insert_sql = (
            f"INSERT INTO raw_flight_prices ({', '.join(target_cols)}) "
            f"VALUES ({placeholders})"
        )

        conn = connect()
        conn.autocommit(False)
        cursor = conn.cursor()
        # Idempotency: clear before loading. Re-running must not append a
        # second copy — a bare INSERT would double every downstream count.
        cursor.execute("DELETE FROM raw_flight_prices")

        buffer: list[tuple] = []
        total = 0
        for line_no, row in enumerate(reader, start=1):
            buffer.append((batch_id, line_no, *[row[c] for c in header]))
            if len(buffer) >= batch_size:
                cursor.executemany(insert_sql, buffer)
                total += len(buffer)
                buffer.clear()
        if buffer:
            cursor.executemany(insert_sql, buffer)
            total += len(buffer)

        conn.commit()
        cursor.close()
        conn.close()

    log.info("ingested %s rows from %s as batch %s", total, path, batch_id)
    return total


def _read_reference(path: Path, build: Callable[[dict], tuple]) -> list[tuple]:
    if not path.is_file():
        raise FileNotFoundError(f"reference data missing: {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        rows = [build(r) for r in csv.DictReader(fh)]
    if not rows:
        raise ValueError(f"reference file is empty: {path}")
    return rows


def seed_reference_tables(
    airports_path: Path,
    allowed_values_path: Path,
    connect: Connect,
) -> dict[str, int]:
    """Seed the domains the validation rules check membership against."""
    airports = _read_reference(
        airports_path,
        lambda r: (
            r["airport_code"].strip().upper(),
            r["airport_name"].strip(),
            int(r["is_origin"]),
            int(r["is_destination"]),
        ),
    )
    allowed = _read_reference(
        allowed_values_path,
        lambda r: (
            r["field_name"].strip(),
            r["allowed_value"].strip(),
            # blank means this domain has no derived numeric value
            int(r["numeric_equivalent"]) if r["numeric_equivalent"].strip() else None,
        ),
    )

    conn = connect()
    conn.autocommit(False)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM ref_airports")
    cursor.executemany(
        "INSERT INTO ref_airports "
        "(airport_code, airport_name, is_origin, is_destination) "
        "VALUES (%s, %s, %s, %s)",
        airports,
    )
    cursor.execute("DELETE FROM ref_allowed_values")
    cursor.executemany(
        "INSERT INTO ref_allowed_values "
        "(field_name, allowed_value, numeric_equivalent) VALUES (%s, %s, %s)",
        allowed,
    )

    conn.commit()
    cursor.close()
    conn.close()

    log.info("seeded %s airports, %s allowed values", len(airports), len(allowed))
    return {"airports": len(airports), "allowed_values": len(allowed)}
