ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY requirements.txt /tmp/requirements.txt

# Constraints are not optional: without them pip will happily upgrade Airflow
# itself and leave an unbootable image. Python version derived at build time so
# this survives a base-image bump.
RUN PYV="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"

# Installed at the same path compose mounts ./src to, so edits are live.
# --chown: an editable install writes egg-info into the source tree, and COPY
# lands files as root. --no-deps: Airflow's tree is already resolved above.
COPY --chown=airflow:0 pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:0 src /opt/airflow/src
RUN pip install --no-cache-dir --no-deps -e /opt/airflow && \
    python -c "import flight_pipeline"
