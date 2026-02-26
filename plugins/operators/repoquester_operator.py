"""
Airflow operator for running RepoQuester quality gate analysis.

RepoQuester evaluates GitHub projects on 9 quality dimensions:
community, continuous_integration, documentation, history, management,
license, unit_test, pull, releases.

A project passes the quality gate if at least 5 of the 9 dimensions
have a score > 0. Only projects that pass will be analysed by Designite.

Workflow
--------
1. Write all pending project names to RepoQuester's ``repo_urls`` file.
2. Inject the GitHub token into RepoQuester's ``tokens.py``.
3. Run ``initialize.sh`` to set up RepoQuester's SQLite database.
4. Run ``run.sh`` to execute the analysis.
5. Read results from ``repo_quester.db`` (SQLite).
6. Upsert results into the pipeline's DuckDB ``quality_gates`` table.
7. Clean up temporary files produced by RepoQuester.
"""

from __future__ import annotations

import logging
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

from airflow.sdk import BaseOperator

from common.db import get_projects_without_quality_gate, upsert_quality_gate
from config.settings import DUCKDB_PATH, GITHUB_TOKEN, GITHUB_TOKENS, REPOQUESTER_DIR

logger = logging.getLogger(__name__)

# The 9 quality metric columns in repoquester_results
METRIC_COLUMNS = [
    "community",
    "continuous_integration",
    "documentation",
    "history",
    "management",
    "license",
    "unit_test",
    "pull",
    "releases",
]


class RepoQuesterOperator(BaseOperator):
    """
    Run RepoQuester on all active projects that have not yet been evaluated.

    This operator is designed to run as a single task in the quality_gate DAG.
    It processes all pending projects in one batch run.

    Parameters
    ----------
    repoquester_dir : Path
        Path to the RepoQuester installation directory inside the container.
        Default: ``REPOQUESTER_DIR`` from settings.
    db_path : Path
        Path to the pipeline DuckDB database file.
    """

    def __init__(
        self,
        *,
        repoquester_dir: Path = REPOQUESTER_DIR,
        db_path: Path = DUCKDB_PATH,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.repoquester_dir = Path(repoquester_dir)
        self.db_path = Path(db_path)

    # ------------------------------------------------------------------
    # Airflow entry point
    # ------------------------------------------------------------------

    def execute(self, context: dict) -> dict:
        """
        Run RepoQuester on all projects pending quality gate evaluation.

        Returns a summary dict stored as XCom:
        {total, passed, failed, errors}
        """
        # --- Get projects that need quality gate evaluation -----------
        pending = get_projects_without_quality_gate(db_path=self.db_path)

        if not pending:
            logger.info("No projects pending quality gate evaluation — skipping.")
            return {"total": 0, "passed": 0, "failed": 0, "errors": 0}

        logger.info(
            "%d project(s) pending quality gate evaluation.", len(pending)
        )

        # --- Resolve GitHub token ------------------------------------
        token = self._resolve_token()

        # --- Run RepoQuester in a temp working directory -------------
        # We copy RepoQuester to a temp dir to avoid polluting the
        # source installation with run artifacts.
        with tempfile.TemporaryDirectory(prefix="repoquester_run_") as tmp_dir:
            work_dir = Path(tmp_dir) / "repoquester"
            shutil.copytree(str(self.repoquester_dir), str(work_dir))

            try:
                self._write_repo_urls(work_dir, pending)
                self._write_token(work_dir, token)
                self._run_initialize(work_dir)
                self._run_analysis(work_dir)
                results = self._read_results(work_dir)
            except Exception as exc:
                logger.error("RepoQuester run failed: %s", exc, exc_info=True)
                raise

        # --- Import results into DuckDB ------------------------------
        summary = {"total": len(pending), "passed": 0, "failed": 0, "errors": 0}
        full_name_to_id = {p["full_name"]: p["id"] for p in pending}

        for full_name, metrics in results.items():
            project_id = full_name_to_id.get(full_name)
            if project_id is None:
                logger.warning("RepoQuester returned unknown project: %s", full_name)
                continue
            try:
                upsert_quality_gate(project_id, metrics, db_path=self.db_path)
                score = sum(1 for k in METRIC_COLUMNS if (metrics.get(k) or 0) > 0)
                if score >= 5:
                    summary["passed"] += 1
                else:
                    summary["failed"] += 1
            except Exception as exc:
                logger.error(
                    "Failed to save quality gate for %s: %s", full_name, exc
                )
                summary["errors"] += 1

        logger.info(
            "Quality gate complete — total: %d, passed: %d, failed: %d, errors: %d",
            summary["total"], summary["passed"], summary["failed"], summary["errors"],
        )
        return summary

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _write_repo_urls(work_dir: Path, pending: list[dict]) -> None:
        """Write project names to RepoQuester's repo_urls file."""
        repo_urls_path = work_dir / "repo_urls"
        with open(repo_urls_path, "w") as f:
            for project in pending:
                f.write(project["full_name"] + "\n")
        logger.info(
            "Wrote %d project(s) to %s.", len(pending), repo_urls_path
        )

    def _write_token(self, work_dir: Path, token: str) -> None:
        """Inject the GitHub token into RepoQuester's tokens.py."""
        tokens_path = work_dir / "tokens.py"
        tokens_path.write_text(
            f'import os\n'
            f'root_directory = os.getcwd()\n'
            f'git_tokens = {{"{token}": "pipeline"}}\n'
        )
        logger.debug("GitHub token injected into tokens.py.")

    # ------------------------------------------------------------------
    # Execution helpers
    # ------------------------------------------------------------------

    def _run_initialize(self, work_dir: Path) -> None:
        """Run RepoQuester's initialize.sh to set up the SQLite database."""
        logger.info("Running RepoQuester initialize.sh …")
        # Fix line endings (Windows CRLF → Unix LF) before running
        self._run_command(
            ["sed", "-i", "-e", "s/\\r$//", "initialize.sh"],
            cwd=work_dir,
        )
        self._run_command(["chmod", "+x", "initialize.sh"], cwd=work_dir)
        self._run_command(["bash", "initialize.sh"], cwd=work_dir)

    def _run_analysis(self, work_dir: Path) -> None:
        """Run RepoQuester's run.sh to execute the quality analysis."""
        logger.info("Running RepoQuester run.sh …")
        self._run_command(
            ["sed", "-i", "-e", "s/\\r$//", "run.sh"],
            cwd=work_dir,
        )
        self._run_command(["chmod", "+x", "run.sh"], cwd=work_dir)
        self._run_command(["bash", "run.sh"], cwd=work_dir)

    # ------------------------------------------------------------------
    # Result parsing
    # ------------------------------------------------------------------

    def _read_results(self, work_dir: Path) -> dict[str, dict]:
        """
        Read analysis results from RepoQuester's SQLite database.

        Returns a dict mapping full_name → metrics dict.
        """
        db_path = work_dir / "repo_quester.db"
        if not db_path.exists():
            raise FileNotFoundError(
                f"RepoQuester did not produce a database at {db_path}. "
                "Check RepoQuester logs for errors."
            )

        results = {}
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(
                f"""
                SELECT repository, {', '.join(METRIC_COLUMNS)}
                FROM repoquester_results
                """
            )
            for row in cursor:
                full_name = row["repository"]
                metrics = {col: row[col] for col in METRIC_COLUMNS}
                results[full_name] = metrics
        finally:
            conn.close()

        logger.info("Read %d result(s) from RepoQuester database.", len(results))
        return results

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_token() -> str:
        """Pick the first available GitHub token."""
        multi = GITHUB_TOKENS
        if multi:
            tokens = [t.strip() for t in multi.split(",") if t.strip()]
            if tokens:
                return tokens[0]
        if GITHUB_TOKEN:
            return GITHUB_TOKEN
        raise RuntimeError(
            "No GitHub token available. Set GITHUB_TOKEN or GITHUB_TOKENS in .env."
        )

    @staticmethod
    def _run_command(cmd: list[str], cwd: Path) -> None:
        """Run a shell command and raise on non-zero exit code."""
        logger.debug("Running: %s (cwd=%s)", " ".join(cmd), cwd)
        result = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
        )
        if result.stdout:
            logger.debug("stdout: %s", result.stdout[-2000:])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[-2000:])
        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"stderr: {result.stderr[-1000:]}"
            )
