"""Reusable pipeline logic, kept out of the DAG file.

Airflow puts `plugins/` on sys.path (verified: `dags/` is not), so this
package is importable both from the DAG and from tests that never boot
Airflow at all.

The split matters for testability. Checking a column mapping is a pure-Python
question, and it should not require building a 3 GB image and starting a
scheduler to answer it.
"""
