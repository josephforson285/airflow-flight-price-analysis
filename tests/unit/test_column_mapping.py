"""Tests for the source-to-landing column contract.

Schema drift in the source CSV is the failure this project is most likely to
meet in the wild: a column gets renamed upstream, the ingest maps it to
nothing, and the pipeline either crashes obscurely or — worse — loads NULLs
and reports confidently on incomplete data.

The ingest task raises on unmapped columns at runtime. These tests catch the
same problem at build time, against the tracked fixture.
"""

from __future__ import annotations

import csv

import pytest

from flight_pipeline.config import get_config
from flight_pipeline.schema import REQUIRED_SOURCE_COLUMNS, SOURCE_TO_LANDING

_CFG = get_config()
FIXTURE = _CFG.paths.corrupted_fixture
REF_AIRPORTS = _CFG.paths.reference_airports


@pytest.fixture(scope="module")
def fixture_header() -> list[str]:
    with open(FIXTURE, newline="", encoding="utf-8") as fh:
        return csv.DictReader(fh).fieldnames or []


def test_every_source_column_is_mapped(fixture_header):
    unmapped = [c for c in fixture_header if c not in SOURCE_TO_LANDING]
    assert not unmapped, f"columns with no landing target: {unmapped}"


def test_map_has_no_stale_entries(fixture_header):
    """A mapping for a column that no longer exists is dead weight."""
    stale = [c for c in SOURCE_TO_LANDING if c not in fixture_header]
    assert not stale, f"SOURCE_TO_LANDING references columns not in the source: {stale}"


def test_required_columns_are_mapped():
    missing = [c for c in REQUIRED_SOURCE_COLUMNS if c not in SOURCE_TO_LANDING]
    assert not missing, f"required columns absent from SOURCE_TO_LANDING: {missing}"


def test_landing_names_are_unique():
    """Two source columns mapping to one landing column would silently
    overwrite each other."""
    targets = list(SOURCE_TO_LANDING.values())
    assert len(targets) == len(set(targets)), "duplicate landing column names"


def test_landing_names_are_sql_safe():
    """The whole point of the mapping is escaping hostile identifiers."""
    for source, target in SOURCE_TO_LANDING.items():
        assert target.replace("_", "").isalnum(), f"{source} -> {target} not SQL-safe"
        assert target.islower(), f"{target} should be lower case"
        assert target != "class", "'class' is ambiguous across engines"


def test_fixture_actually_contains_defects(fixture_header):
    """Guards the guard.

    If someone regenerates the fixture from clean data, every quarantine rule
    would pass against it and the error handling would go untested again —
    the exact trap this fixture exists to avoid.
    """
    with open(FIXTURE, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    blank_required = sum(
        1 for r in rows if any(not (r[c] or "").strip() for c in REQUIRED_SOURCE_COLUMNS)
    )
    non_numeric = sum(
        1
        for r in rows
        if not (r["Base Fare (BDT)"] or "").replace("-", "").replace(".", "").isdigit()
    )
    assert blank_required > 0, "fixture has no missing-value rows"
    assert non_numeric >= 0
    assert len(rows) > len({tuple(r.values()) for r in rows}), (
        "fixture no longer contains a duplicate row"
    )


def test_reference_airports_cover_the_fixture_domain():
    """Every airport in the fixture's CLEAN rows must be known.

    The fixture deliberately contains one bogus code; anything beyond that
    means the reference table has drifted from the data.
    """
    with open(REF_AIRPORTS, newline="", encoding="utf-8") as fh:
        known = {r["airport_code"].strip().upper() for r in csv.DictReader(fh)}
    assert len(known) == 20, f"expected 20 reference airports, found {len(known)}"

    with open(FIXTURE, newline="", encoding="utf-8") as fh:
        used = set()
        for r in csv.DictReader(fh):
            used.add((r["Source"] or "").strip().upper())
            used.add((r["Destination"] or "").strip().upper())
    unknown = {c for c in used if c and c not in known}
    assert unknown == {"XXX"}, f"unexpected unknown airport codes: {unknown}"
