"""Make the pipeline package importable without booting Airflow.

Airflow adds `plugins/` to sys.path only once Airflow itself has been
imported, and pytest's `pythonpath` ini setting is not reliable here because
`pyproject.toml` is not among the paths mounted into the container. A
conftest is the portable answer: pytest always loads it, wherever the suite is
run from.

Paths are derived from this file's location, so the same conftest works inside
the container (`/opt/airflow/tests`) and in a bare checkout.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

for folder in ("plugins", "dags"):
    path = str(PROJECT_ROOT / folder)
    if path not in sys.path:
        sys.path.insert(0, path)
