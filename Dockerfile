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

# dbt goes in an isolated virtualenv, NOT the Airflow environment.
# Installing dbt-core alongside Airflow downgrades more-itertools and
# pathspec (measured, see requirements-dbt.txt). Cosmos is pointed at this
# interpreter via DBT_EXECUTABLE_PATH, so the two never share a dependency
# tree and Airflow's pins stay exactly as its constraints file intends.
ENV DBT_VENV=/home/airflow/dbt-venv
ENV DBT_EXECUTABLE_PATH=${DBT_VENV}/bin/dbt

COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv "${DBT_VENV}" && \
    "${DBT_VENV}/bin/pip" install --no-cache-dir --upgrade pip && \
    "${DBT_VENV}/bin/pip" install --no-cache-dir -r /tmp/requirements-dbt.txt && \
    "${DBT_EXECUTABLE_PATH}" --version
