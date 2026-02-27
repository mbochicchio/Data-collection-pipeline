""""
Ingestion DAG — Data-collection-pipeline
=========================================

Runs daily and fetches the latest commit for every active project in the
``projects`` table, persisting results to the ``versions`` table in PostgreSQL.

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

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.db import get_active_projects
from config.settings import INGESTION_SCHEDULE
from plugins.operators.github_operator import GitHubIngestionOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-pipeline",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "depends_on_past": False,
}


def _has_active_projects() -> bool:
    """Return True if there is at least one active project in the database."""
    projects = get_active_projects()
    if not projects:
        logger.warning("No active projects found in the database — skipping ingestion run.")
        return False
    logger.info("%d active project(s) found — proceeding with ingestion.", len(projects))
    return True


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

    check_projects = ShortCircuitOperator(
        task_id="check_active_projects",
        python_callable=_has_active_projects,
    )

    ingest = GitHubIngestionOperator(
        task_id="ingest_github_metadata",
    )

    @task(task_id="log_ingestion_summary")
    def log_summary(**context) -> None:
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

    check_projects >> ingest >> log_summary()


ingestion_dag()