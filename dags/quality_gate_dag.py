"""
Quality Gate DAG — Data-collection-pipeline
============================================

Runs RepoQuester on all active projects that have not yet been evaluated.
Projects that score > 5 out of 9 quality dimensions will be eligible
for Designite static analysis in the execution DAG.

The 9 quality dimensions evaluated by RepoQuester:
    community, continuous_integration, documentation, history, management,
    license, unit_test, pull, releases

Schedule: @weekly by default — quality characteristics change slowly,
          so running once a week is sufficient.

Tasks
-----
check_pending_projects
    Short-circuit: skips the run if all active projects already have a
    quality gate result.

run_quality_gate
    Core task: writes project list, runs RepoQuester, imports results
    into DuckDB quality_gates table.

log_quality_gate_summary
    Reads XCom summary and logs pass/fail statistics.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import ShortCircuitOperator

from common.db import get_projects_without_quality_gate
from config.settings import REPOQUESTER_DIR
from plugins.operators.repoquester_operator import RepoQuesterOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
}

QUALITY_GATE_SCHEDULE = "@weekly"


def _has_pending_projects() -> bool:
    """Return True if there are projects without a quality gate result."""
    pending = get_projects_without_quality_gate()
    if not pending:
        logger.info("All projects already have a quality gate result — skipping.")
        return False
    logger.info("%d project(s) pending quality gate evaluation.", len(pending))
    return True


@dag(
    dag_id="quality_gate",
    description="Run RepoQuester quality gate on all pending active projects.",
    schedule=QUALITY_GATE_SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["quality_gate", "repoquester"],
    doc_md=__doc__,
)
def quality_gate_dag():

    # ------------------------------------------------------------------
    # Task 1 — short-circuit if nothing to evaluate
    # ------------------------------------------------------------------
    check_pending = ShortCircuitOperator(
        task_id="check_pending_projects",
        python_callable=_has_pending_projects,
    )

    # ------------------------------------------------------------------
    # Task 2 — run RepoQuester on all pending projects
    # ------------------------------------------------------------------
    run_gate = RepoQuesterOperator(
        task_id="run_quality_gate",
        repoquester_dir=REPOQUESTER_DIR,
        retries=1,
    )

    # ------------------------------------------------------------------
    # Task 3 — log summary
    # ------------------------------------------------------------------
    @task
    def log_quality_gate_summary(**context):
        summary = context["ti"].xcom_pull(task_ids="run_quality_gate")
        if not summary:
            logger.info("No summary available.")
            return
        logger.info(
            "Quality gate summary — "
            "total: %d | passed: %d | failed: %d | errors: %d",
            summary.get("total", 0),
            summary.get("passed", 0),
            summary.get("failed", 0),
            summary.get("errors", 0),
        )

    check_pending >> run_gate >> log_quality_gate_summary()


quality_gate_dag()
