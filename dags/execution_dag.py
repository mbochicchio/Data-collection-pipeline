"""
Execution DAG — Data-collection-pipeline
=========================================

Runs Designite static analysis on project versions that:
  1. Have not yet been analysed.
  2. Belong to a project that passed the RepoQuester quality gate.
  3. Have a supported language (Java or Python).

Schedule: @daily — picks up new versions discovered by the ingestion DAG.

Tasks
-----
check_pending_versions
    Short-circuit: skips the run if there are no versions pending analysis.

run_designite
    Core task: for each eligible version, clones the repository at the
    specific commit SHA and runs Designite. Results are stored in the
    ``analyses`` table.

log_execution_summary
    Logs a human-readable summary of the run (success, skipped, failed).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import dag, task

from common.db import (
    get_connection,
    analysis_exists_for_version,
)
from common.models import ProjectLanguage, Version
from config.settings import WORKSPACE_DIR, EXECUTION_SCHEDULE
from plugins.operators.designite_operator import DesigniteOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_eligible_versions(limit: int | None = None) -> list[dict]:
    """
    Return versions that:
    - Have no analysis record yet (or only pending/running — retryable)
    - Belong to a project that passed the quality gate
    - Have a supported language (Java or Python)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    v.id,
                    v.project_id,
                    v.commit_sha,
                    v.commit_message,
                    v.committed_at,
                    v.discovered_at,
                    v.default_branch,
                    v.metadata_json,
                    p.full_name,
                    p.language
                FROM versions v
                JOIN projects p ON p.id = v.project_id
                JOIN quality_gates qg ON qg.project_id = v.project_id
                LEFT JOIN analyses a ON a.version_id = v.id
                WHERE a.id IS NULL
                  AND qg.passed = TRUE
                  AND p.language IN ('Java', 'Python')
                  AND p.is_active = TRUE
                ORDER BY v.discovered_at
                {limit_clause}
            """.format(limit_clause=f"LIMIT {limit}" if limit else ""))
            rows = cur.fetchall()

    return [
        {
            "version": Version(
                id=r[0],
                project_id=r[1],
                commit_sha=r[2],
                commit_message=r[3],
                committed_at=r[4],
                discovered_at=r[5],
                default_branch=r[6],
                metadata=r[7] or {},
            ),
            "full_name": r[8],
            "language": ProjectLanguage(r[9]),
        }
        for r in rows
    ]


def _has_pending_versions() -> bool:
    """Return True if there is at least one version eligible for analysis."""
    versions = _get_eligible_versions(limit=1)
    if not versions:
        logger.info("No versions pending analysis — skipping execution run.")
        return False
    logger.info("Versions pending analysis found — proceeding.")
    return True


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


@dag(
    dag_id="execution",
    description="Run Designite static analysis on eligible project versions.",
    schedule=EXECUTION_SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["execution", "designite"],
    doc_md=__doc__,
)
def execution_dag():

    # ------------------------------------------------------------------
    # Task 1 — short-circuit if nothing to analyse
    # ------------------------------------------------------------------
    check_versions = ShortCircuitOperator(
        task_id="check_pending_versions",
        python_callable=_has_pending_versions,
    )

    # ------------------------------------------------------------------
    # Task 2 — run Designite on eligible versions
    # ------------------------------------------------------------------
    @task(task_id="run_designite")
    def run_designite(**context) -> dict:
        """
        Run Designite on up to 5 eligible versions (testing limit).

        For each version:
        - Skip if already analysed (idempotent).
        - Skip if language is unsupported.
        - Clone repo, run Designite, parse CSV output, save results.
        """
        eligible = _get_eligible_versions(limit=None)
        logger.info("%d version(s) eligible for analysis.", len(eligible))

        summary = {"processed": 0, "success": 0, "skipped": 0, "failed": 0, "errors": []}

        for item in eligible:
            version: Version = item["version"]
            full_name: str = item["full_name"]
            language: ProjectLanguage = item["language"]

            logger.info(
                "Analysing version %s of '%s' (language=%s) …",
                version.commit_sha[:7],
                full_name,
                language.value,
            )

            # Guard: skip if already analysed
            if analysis_exists_for_version(version.id):
                logger.info("Version %s already analysed — skipping.", version.commit_sha[:7])
                summary["skipped"] += 1
                continue

            # Guard: skip unknown language
            if language == ProjectLanguage.UNKNOWN:
                logger.warning("Unknown language for '%s' — skipping.", full_name)
                summary["skipped"] += 1
                continue

            try:
                op = DesigniteOperator(
                    task_id=f"designite_{version.id}",
                    version=version,
                    language=language,
                    repo_full_name=full_name,
                    workspace_dir=WORKSPACE_DIR,
                )
                result = op.execute(context)
                summary["processed"] += 1
                if result.get("status") == "skipped":
                    summary["skipped"] += 1
                else:
                    summary["success"] += 1
                logger.info(
                    "Version %s of '%s' — status: %s",
                    version.commit_sha[:7],
                    full_name,
                    result.get("status"),
                )
            except Exception as exc:
                logger.error(
                    "Failed to analyse version %s of '%s': %s",
                    version.commit_sha[:7],
                    full_name,
                    exc,
                    exc_info=True,
                )
                summary["failed"] += 1
                summary["errors"].append({"project": full_name, "error": str(exc)})

        return summary

    # ------------------------------------------------------------------
    # Task 3 — log summary
    # ------------------------------------------------------------------
    @task(task_id="log_execution_summary")
    def log_summary(**context) -> None:
        summary = context["ti"].xcom_pull(task_ids="run_designite")
        if not summary:
            logger.warning("No summary received from run_designite task.")
            return
        logger.info("=" * 50)
        logger.info("EXECUTION SUMMARY")
        logger.info("  Processed : %d", summary.get("processed", 0))
        logger.info("  Success   : %d", summary.get("success", 0))
        logger.info("  Skipped   : %d", summary.get("skipped", 0))
        logger.info("  Failed    : %d", summary.get("failed", 0))
        for err in summary.get("errors", []):
            logger.error("  [ERROR] %s — %s", err["project"], err["error"])
        logger.info("=" * 50)

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    check_versions >> run_designite() >> log_summary()


execution_dag()
