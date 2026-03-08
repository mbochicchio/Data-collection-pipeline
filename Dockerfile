# ---------------------------------------------------------------------------
# Data-collection-pipeline — Airflow 3.x image
#
# Build:  docker compose build
# ---------------------------------------------------------------------------

FROM apache/airflow:3.1.7

# Switch to root for system-level installs
USER root

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------

# Install Java 22 via Adoptium/Temurin repository
RUN apt-get update && apt-get install -y --no-install-recommends wget gnupg \
    && wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor > /etc/apt/trusted.gpg.d/adoptium.gpg \
    && echo "deb https://packages.adoptium.net/artifactory/deb bookworm main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update && apt-get install -y --no-install-recommends \
        temurin-22-jre \
        git \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Designite tool directory
# ---------------------------------------------------------------------------

RUN mkdir -p /opt/designite
# Mount actual binaries via docker-compose volume:
#   - ./tools/DesigniteJava.jar  → /opt/designite/DesigniteJava.jar
#   - ./tools/DesigniteP.py      → /opt/designite/DesigniteP.py

# ---------------------------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------------------------

USER airflow

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Use the official Airflow constraint file to avoid dependency conflicts
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.7/constraints-3.12.txt" \
    -r /opt/airflow/requirements.txt

# ---------------------------------------------------------------------------
# Application code
# ---------------------------------------------------------------------------

COPY --chown=airflow:root . /opt/airflow/pipeline/

ENV PYTHONPATH="/opt/airflow/pipeline:${PYTHONPATH}"

# ---------------------------------------------------------------------------
# Data & workspace directories
# ---------------------------------------------------------------------------

RUN mkdir -p /opt/airflow/data /opt/airflow/workspace

ENV DATA_DIR=/opt/airflow/data
ENV WORKSPACE_DIR=/opt/airflow/workspace
ENV DUCKDB_PATH=/opt/airflow/data/pipeline.duckdb