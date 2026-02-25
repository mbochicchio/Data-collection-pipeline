"""
Airflow operator for running Designite static analysis.

Supports two flavours of Designite:
- **Java**:  ``java -jar DesigniteJava.jar -i <input> -o <output>``
- **Python**: ``./DPy analyze -i <input> -o <output>``

The operator:
1. Clones the repository at the specific commit SHA into a temporary workspace.
2. Runs the appropriate Designite tool based on the project language.
3. Parses all CSV output files produced by Designite.
4. Persists the parsed results as JSON into the ``analyses`` table in DuckDB.
5. Cleans up the cloned repository and Designite output from disk.
"""

from __future__ import annotations

import csv
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from airflow.sdk import BaseOperator

from common.db import (
    analysis_exists_for_version,
    create_analysis,
    update_analysis_failed,
    update_analysis_started,
    update_analysis_success,
)
from common.models import ProjectLanguage, Version
from config.settings import (
    DESIGNITE_JAVA_JAR,
    DESIGNITE_PYTHON_EXECUTABLE,
    DUCKDB_PATH,
    JAVA_EXECUTABLE,
    WORKSPACE_DIR,
)

logger = logging.getLogger(__name__)


class DesigniteOperator(BaseOperator):
    """
    Run Designite static analysis on a single project version.

    This operator is designed to be instantiated once per version inside a
    dynamic task mapping in the execution DAG.

    Parameters
    ----------
    version : Version
        The version dataclass instance to analyse.
    language : ProjectLanguage
        The programming language of the project (JAVA or PYTHON).
    repo_full_name : str
        The GitHub repository full name (``owner/repo``), used to clone.
    db_path : Path
        Path to the DuckDB database file.

    Example usage in a DAG::

        op = DesigniteOperator(
            task_id=f"analyse_{version.id}",
            version=version,
            language=ProjectLanguage.JAVA,
            repo_full_name="apache/spark",
        )
    """

    template_fields = ("db_path",)

    def __init__(
        self,
        *,
        version: Version,
        language: ProjectLanguage,
        repo_full_name: str,
        db_path: Path = DUCKDB_PATH,
        workspace_dir: Path = WORKSPACE_DIR,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.version = version
        self.language = language
        self.repo_full_name = repo_full_name
        self.db_path = Path(db_path)
        self.workspace_dir = Path(workspace_dir)

    # ------------------------------------------------------------------
    # Airflow entry point
    # ------------------------------------------------------------------

    def execute(self, context: dict) -> dict:
        """
        Main execution method called by the Airflow scheduler.

        Returns a summary dict stored as XCom.
        """
        version = self.version

        # --- Guard: skip if already analysed ---------------------------
        if analysis_exists_for_version(version.id, db_path=self.db_path):
            logger.info(
                "Version %s (id=%s) already analysed — skipping.",
                version.commit_sha[:7],
                version.id,
            )
            return {"status": "skipped", "version_id": version.id}

        # --- Guard: skip unsupported languages -------------------------
        if self.language == ProjectLanguage.UNKNOWN:
            logger.warning(
                "Project '%s' has unknown language — skipping analysis.",
                self.repo_full_name,
            )
            return {"status": "skipped", "reason": "unknown language", "version_id": version.id}

        # --- Create analysis record in DB ------------------------------
        analysis_id = create_analysis(
            version_id=version.id,
            project_id=version.project_id,
            db_path=self.db_path,
        )
        update_analysis_started(analysis_id, db_path=self.db_path)
        logger.info(
            "Starting analysis id=%s for version %s of '%s'.",
            analysis_id,
            version.commit_sha[:7],
            self.repo_full_name,
        )

        # Use a temporary directory as the working area for this analysis
        with tempfile.TemporaryDirectory(
            prefix=f"designite_{version.id}_",
            dir=self.workspace_dir,
        ) as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_path = tmp_path / "repo"
            output_path = tmp_path / "output"
            output_path.mkdir()

            try:
                # --- Step 1: clone repo at specific commit -------------
                self._clone_repo(repo_path)

                # --- Step 2: run Designite -----------------------------
                self._run_designite(repo_path, output_path)

                # --- Step 3: parse CSV output --------------------------
                results = self._parse_output(output_path)

                # --- Step 4: persist results ---------------------------
                update_analysis_success(analysis_id, results, db_path=self.db_path)
                logger.info(
                    "Analysis id=%s completed successfully. Parsed %d output file(s).",
                    analysis_id,
                    len(results),
                )
                return {"status": "success", "analysis_id": analysis_id, "files": list(results.keys())}

            except Exception as exc:
                error_msg = str(exc)
                logger.error(
                    "Analysis id=%s failed: %s", analysis_id, error_msg, exc_info=True
                )
                update_analysis_failed(analysis_id, error_msg, db_path=self.db_path)
                raise

    # ------------------------------------------------------------------
    # Clone
    # ------------------------------------------------------------------

    def _clone_repo(self, repo_path: Path) -> None:
        """
        Clone the repository and checkout the exact commit SHA.

        Uses a shallow clone (--depth=1) targeting the specific commit to
        minimise disk usage and clone time.
        """
        clone_url = f"https://github.com/{self.repo_full_name}.git"
        sha = self.version.commit_sha

        logger.info("Cloning %s at %s …", self.repo_full_name, sha[:7])

        # Git does not support shallow clone of an arbitrary SHA directly.
        # We clone the default branch shallowly first, then fetch the
        # specific commit if it is not already present.
        self._run_command(
            ["git", "clone", "--depth=1", clone_url, str(repo_path)],
            cwd=repo_path.parent,
        )

        # Fetch the specific commit (in case it differs from HEAD)
        self._run_command(
            ["git", "fetch", "--depth=1", "origin", sha],
            cwd=repo_path,
        )

        # Checkout the exact commit
        self._run_command(
            ["git", "checkout", sha],
            cwd=repo_path,
        )

        logger.info("Repository cloned and checked out at %s.", sha[:7])

    # ------------------------------------------------------------------
    # Run Designite
    # ------------------------------------------------------------------

    def _run_designite(self, repo_path: Path, output_path: Path) -> None:
        """Dispatch to the correct Designite flavour based on language."""
        if self.language == ProjectLanguage.JAVA:
            self._run_designite_java(repo_path, output_path)
        elif self.language == ProjectLanguage.PYTHON:
            self._run_designite_python(repo_path, output_path)
        else:
            raise ValueError(f"Unsupported language for Designite: {self.language}")

    def _run_designite_java(self, repo_path: Path, output_path: Path) -> None:
        """Run DesigniteJava.jar on the cloned repository."""
        logger.info("Running DesigniteJava on %s …", self.repo_full_name)
        self._run_command([
            JAVA_EXECUTABLE,
            "-jar", str(DESIGNITE_JAVA_JAR),
            "-i", str(repo_path),
            "-o", str(output_path),
        ])

    def _run_designite_python(self, repo_path: Path, output_path: Path) -> None:
        """Run DPy analyze on the cloned repository."""
        logger.info("Running DPy on %s …", self.repo_full_name)
        self._run_command([
            str(DESIGNITE_PYTHON_EXECUTABLE),
            "analyze",
            "-i", str(repo_path),
            "-o", str(output_path),
        ])

    # ------------------------------------------------------------------
    # Parse output
    # ------------------------------------------------------------------

    def _parse_output(self, output_path: Path) -> dict[str, list[dict[str, Any]]]:
        """
        Parse all CSV files produced by Designite into a dict of lists.

        Returns a dict where:
        - key   = CSV filename without extension (e.g. ``"MethodMetrics"``)
        - value = list of dicts, one dict per CSV row

        Empty CSV files (header only) are included as empty lists.
        """
        results: dict[str, list[dict[str, Any]]] = {}
        csv_files = list(output_path.rglob("*.csv"))

        if not csv_files:
            logger.warning("No CSV output files found in %s.", output_path)
            return results

        for csv_file in csv_files:
            key = csv_file.stem  # filename without .csv extension
            try:
                rows = self._parse_csv(csv_file)
                results[key] = rows
                logger.debug("  Parsed %s: %d rows.", csv_file.name, len(rows))
            except Exception as exc:
                logger.warning("  Failed to parse %s: %s", csv_file.name, exc)
                results[key] = []

        logger.info(
            "Parsed %d CSV file(s): %s",
            len(results),
            ", ".join(results.keys()),
        )
        return results

    @staticmethod
    def _parse_csv(csv_file: Path) -> list[dict[str, Any]]:
        """Read a single CSV file and return a list of row dicts."""
        rows = []
        with open(csv_file, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Strip whitespace from keys and values
                clean_row = {k.strip(): v.strip() for k, v in row.items() if k}
                rows.append(clean_row)
        return rows

    # ------------------------------------------------------------------
    # Subprocess helper
    # ------------------------------------------------------------------

    @staticmethod
    def _run_command(cmd: list[str], cwd: Path | None = None) -> None:
        """
        Run a shell command and raise on non-zero exit code.

        Stdout and stderr are captured and logged at DEBUG level to avoid
        cluttering Airflow task logs with Designite's verbose output.
        """
        logger.debug("Running command: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
        )

        if result.stdout:
            logger.debug("stdout: %s", result.stdout[-2000:])  # last 2000 chars
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[-2000:])

        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed with exit code {result.returncode}.\n"
                f"Command: {' '.join(cmd)}\n"
                f"stderr: {result.stderr[-1000:]}"
            )