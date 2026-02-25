"""
Ingestion DAG — Data-collection-pipeline
=========================================

Runs daily and fetches the latest commit for every active project in the
``projects`` table, persisting results to the ``versions`` table in DuckDB.

Schedule: configurable via the ``INGESTION_SCHEDULE`` environment variable
          (default: ``@daily``).

Tasks
-----
check_active_projects
    Short-circuit operator: skips the rest of the DAG if there are no active
    projects in the database, avoiding unnecessary API calls.

ingest_github_metadata
    Core task: iterates over all active projects, calls the GitHub API, updates
    language information, and inserts new versions.  A single project failure
    does not abort the run — errors are logged and reported via XCom.

log_ingestion_summary
    Reads the XCom summary from the previous task and logs human-readable
    statistics (processed, new versions, skipped, errors).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from common.db import get_active_projects
from config.settings import DUCKDB_PATH, INGESTION_SCHEDULE
from plugins.operators.github_operator import GitHubIngestionOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # Do not run catch-up DAG runs for past dates on first deploy
    "depends_on_past": False,
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _has_active_projects() -> bool:
    """Return True if there is at least one active project in the database."""
    projects = get_active_projects(db_path=DUCKDB_PATH)
    if not projects:
        logger.warning("No active projects found in the database — skipping ingestion run.")
        return False
    logger.info("%d active project(s) found — proceeding with ingestion.", len(projects))
    return True


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


@dag(
    dag_id="ingestion",
    description="Daily ingestion of GitHub repository metadata and latest commits.",
    schedule=INGESTION_SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "github"],
    doc_md=__doc__,
)
def ingestion_dag():

    # ------------------------------------------------------------------
    # Task 1 — short-circuit if no projects are configured
    # ------------------------------------------------------------------
    check_projects = ShortCircuitOperator(
        task_id="check_active_projects",
        python_callable=_has_active_projects,
        doc_md=(
            "Skip the entire DAG run if there are no active projects in the "
            "``projects`` table.  This avoids noisy empty runs."
        ),
    )

    # ------------------------------------------------------------------
    # Task 2 — fetch GitHub data and persist new versions
    # ------------------------------------------------------------------
    ingest = GitHubIngestionOperator(
        task_id="ingest_github_metadata",
        db_path=str(DUCKDB_PATH),
        doc_md=(
            "For each active project: fetch repository metadata from the GitHub "
            "API, update the language field if needed, fetch the HEAD commit of "
            "the default branch, and insert a new row in ``versions`` if the "
            "commit SHA has not been seen before."
        ),
    )

    # ------------------------------------------------------------------
    # Task 3 — log a human-readable summary
    # ------------------------------------------------------------------
    @task(task_id="log_ingestion_summary")
    def log_summary(**context) -> None:
        """
        Pull the XCom summary from the ingest task and log it.

        This task never fails — it is purely informational.
        """
        summary: dict = context["ti"].xcom_pull(task_ids="ingest_github_metadata")

        if not summary:
            logger.warning("No summary received from ingest task.")
            return

        logger.info("=" * 50)
        logger.info("INGESTION SUMMARY")
        logger.info("  Projects processed : %d", summary.get("processed", 0))
        logger.info("  New versions found : %d", summary.get("new_versions", 0))
        logger.info("  Already up-to-date : %d", summary.get("skipped", 0))
        logger.info("  Errors             : %d", len(summary.get("errors", [])))

        for err in summary.get("errors", []):
            logger.error("  [ERROR] %s — %s", err["project"], err["error"])

        logger.info("=" * 50)

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    check_projects >> ingest >> log_summary()


# Instantiate the DAG
ingestion_dag()