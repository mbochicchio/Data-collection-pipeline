# ---------------------------------------------------------------------------
# GitHub Empirical Studies Pipeline — Airflow image
#
# Build:  docker build -t github-pipeline:latest .
# ---------------------------------------------------------------------------

# Pin to the same Airflow version used in requirements.txt
FROM apache/airflow:2.10.5-python3.11

# Switch to root for system-level installs, then drop back to airflow user
USER root

# ---------------------------------------------------------------------------
# System dependencies
# ---------------------------------------------------------------------------

RUN apt-get update && apt-get install -y --no-install-recommends \
        # Java runtime for DesigniteJava.jar
        default-jre-headless \
        # Git is required to clone repositories
        git \
        # curl/wget used in health-checks and optional downloads
        curl \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Designite tool installation
# ---------------------------------------------------------------------------

# Create a dedicated directory for Designite binaries.
# Mount or COPY the actual .jar / .py files here at build time or via volume.
RUN mkdir -p /opt/designite
# COPY tools/DesigniteJava.jar  /opt/designite/DesigniteJava.jar
# COPY tools/DesigniteP.py      /opt/designite/DesigniteP.py

# ---------------------------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------------------------

USER airflow

# Copy only the requirements file first to leverage Docker layer cache
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# ---------------------------------------------------------------------------
# Application code
# ---------------------------------------------------------------------------

COPY --chown=airflow:root common /opt/airflow/pipeline/

# Make the pipeline package importable from DAGs and plugins
ENV PYTHONPATH="/opt/airflow/pipeline:${PYTHONPATH}"

# ---------------------------------------------------------------------------
# Data & workspace directories
# ---------------------------------------------------------------------------

# These are overridden by the Docker Compose volume mounts in production,
# but we create them so the image works standalone for quick testing.
RUN mkdir -p /opt/airflow/data /opt/airflow/workspace

ENV DATA_DIR=/opt/airflow/data
ENV WORKSPACE_DIR=/opt/airflow/workspace
ENV DUCKDB_PATH=/opt/airflow/data/pipeline.duckdb
