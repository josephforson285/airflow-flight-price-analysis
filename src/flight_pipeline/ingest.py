"""CSV ingestion and reference seeding."""

from __future__ import annotations

import csv
import logging
from collections.abc import Callable
from pathlib import Path

from flight_pipeline.db import Connect, transaction
from flight_pipeline.schema import REQUIRED_SOURCE_COLUMNS, SOURCE_TO_LANDING

log = logging.getLogger(__name__)


def validate_header(header: list[str]) -> None:
    """Fail fast on an unrecognised source shape.

    Reported separately because the fixes differ: a missing required column is
    a broken extract, an unmapped one is an untold schema change.
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

    Does not cast, so a malformed value fails an inspectable validation rather
    than aborting the load. Does not split on commas either -- airport names
    contain them, so naive splitting yields 17, 18 or 19 fields per row.
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

        total = 0
        # One transaction for the whole load: a failure halfway through must
        # not leave a partial batch behind for the quarantine pass to validate.
        with transaction(connect) as cursor:
            # Idempotency: clear before loading. Re-running must not append a
            # second copy — a bare INSERT would double every downstream count.
            cursor.execute("DELETE FROM raw_flight_prices")

            buffer: list[tuple] = []
            for line_no, row in enumerate(reader, start=1):
                buffer.append((batch_id, line_no, *[row[c] for c in header]))
                if len(buffer) >= batch_size:
                    cursor.executemany(insert_sql, buffer)
                    total += len(buffer)
                    buffer.clear()
            if buffer:
                cursor.executemany(insert_sql, buffer)
                total += len(buffer)

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

    # Both tables in ONE transaction. Seeding airports but failing on allowed
    # values would leave the validation rules half-armed — UNKNOWN_AIRPORT
    # working, UNKNOWN_CATEGORY silently passing everything.
    with transaction(connect) as cursor:
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

    log.info("seeded %s airports, %s allowed values", len(airports), len(allowed))
    return {"airports": len(airports), "allowed_values": len(allowed)}
