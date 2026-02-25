"""
Central configuration for the Data Pipeline.

All settings are loaded from environment variables with sensible defaults.
Sensitive values (tokens, paths) must be provided via environment or .env file.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Base paths
# ---------------------------------------------------------------------------

# Root directory of the project (one level above config/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Directory where the DuckDB database file is stored (mounted volume in Docker)
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))

# Directory used as working space for cloning repos and running tools
WORKSPACE_DIR = Path(os.getenv("WORKSPACE_DIR", BASE_DIR / "workspace"))

# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------

# Full path to the DuckDB database file
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", DATA_DIR / "pipeline.duckdb"))

# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------

# Personal Access Token for GitHub REST API (required to avoid rate limits)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")

# GitHub API base URL (can be overridden for GitHub Enterprise)
GITHUB_API_BASE = os.getenv("GITHUB_API_BASE", "https://api.github.com")

# Maximum number of retries on transient HTTP errors
GITHUB_API_MAX_RETRIES = int(os.getenv("GITHUB_API_MAX_RETRIES", "5"))

# Seconds to wait between retry attempts (exponential backoff base)
GITHUB_API_RETRY_BACKOFF = float(os.getenv("GITHUB_API_RETRY_BACKOFF", "2.0"))

# ---------------------------------------------------------------------------
# Designite — Java edition
# ---------------------------------------------------------------------------

# Absolute path to the DesigniteJava.jar file inside the container
DESIGNITE_JAVA_JAR = Path(os.getenv("DESIGNITE_JAVA_JAR", "/opt/designite/DesigniteJava.jar"))

# Java executable (can be overridden if java is not on PATH)
JAVA_EXECUTABLE = os.getenv("JAVA_EXECUTABLE", "java")

# ---------------------------------------------------------------------------
# Designite — Python edition
# ---------------------------------------------------------------------------

# Absolute path to the DesigniteP.py entry-point script
DESIGNITE_PYTHON_SCRIPT = Path(
    os.getenv("DESIGNITE_PYTHON_SCRIPT", "/opt/designite/DesigniteP.py")
)

# Python executable used to run DesigniteP.py (e.g. "python3")
PYTHON_EXECUTABLE = os.getenv("DESIGNITE_PYTHON_EXECUTABLE", "python3")

# ---------------------------------------------------------------------------
# Airflow DAG defaults
# ---------------------------------------------------------------------------

# Cron schedule for the ingestion DAG (default: daily at midnight UTC)
INGESTION_SCHEDULE = os.getenv("INGESTION_SCHEDULE", "@daily")

# Polling interval in minutes for the execution DAG
# The execution DAG reschedules itself this often to look for pending versions
EXECUTION_POLL_MINUTES = int(os.getenv("EXECUTION_POLL_MINUTES", "30"))

# Maximum number of concurrent Designite analyses (limits parallelism)
EXECUTION_MAX_ACTIVE_TASKS = int(os.getenv("EXECUTION_MAX_ACTIVE_TASKS", "4"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
