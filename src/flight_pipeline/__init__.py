"""Reusable pipeline logic, kept out of the DAG file.

A real installable package under src/, not a directory that happens to sit on
sys.path. It was previously in plugins/ — which worked, because Airflow puts
that folder on the path, but plugins/ exists for Airflow's plugin system
(AirflowPlugin subclasses, custom operators), not as a library location.
Relying on it meant the import worked by side effect, and tests needed a
conftest sys.path hack to reproduce that side effect.

Installed with `pip install -e .`, this imports the same way everywhere: in
the container, in CI, in a bare checkout, and in an editor.

The split matters for testability. Checking a column mapping is a pure-Python
question, and it should not require building a 3 GB image and starting a
scheduler to answer it.
"""
