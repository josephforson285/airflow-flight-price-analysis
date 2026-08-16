# Extended Airflow image.
#
# Why a Dockerfile instead of _PIP_ADDITIONAL_REQUIREMENTS: that shortcut
# re-installs every package on every container start, so a `docker compose
# restart` costs minutes and needs the network. The official docs flag it as
# quick-check-only. Building once is slower today and correct forever.
ARG AIRFLOW_VERSION=3.3.1
FROM apache/airflow:${AIRFLOW_VERSION}
ARG AIRFLOW_VERSION

COPY requirements.txt /tmp/requirements.txt

# Airflow pins its whole dependency tree. Installing providers WITHOUT the
# matching constraints file lets pip silently upgrade Airflow itself and
# leave you with an unbootable image. The python version is derived at build
# time so this keeps working if the base image bumps its interpreter.
RUN PYV="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    echo "building against Airflow ${AIRFLOW_VERSION} / Python ${PYV}" && \
    pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYV}.txt"

# Install the pipeline package properly rather than relying on a directory
# that happens to be on sys.path. Editable, and installed at the same path the
# compose file mounts ./src to, so edits are live without a rebuild.
#
# --no-deps because Airflow's dependency tree is already resolved against its
# constraints file above; letting pip resolve again here is how a working
# image quietly becomes an unbootable one.
# --chown matters: COPY lands files as root, and an editable install writes
# flight_pipeline.egg-info back into the source tree. Without it the build
# fails with "could not create 'src/flight_pipeline.egg-info': Permission
# denied", which reads like a pip problem and is actually a file-ownership one.
COPY --chown=airflow:0 pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:0 src /opt/airflow/src
RUN pip install --no-cache-dir --no-deps -e /opt/airflow && \
    python -c "import flight_pipeline; print('flight_pipeline importable:', flight_pipeline.__file__)"
