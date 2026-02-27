"""
Airflow operator for running Designite static analysis.

Supports two flavours of Designite:
- **Java**:  ``java -jar DesigniteJava.jar -i <input> -o <o>``
- **Python**: ``./DPy analyze -i <input> -o <o>``

The operator:
1. Clones the repository at the specific commit SHA into a temporary workspace.
2. Runs the appropriate Designite tool based on the project language.
3. Parses all CSV output files produced by Designite.
4. Persists the parsed results as JSON into the ``analyses`` table in PostgreSQL.
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
    JAVA_EXECUTABLE,
    WORKSPACE_DIR,
)

logger = logging.getLogger(__name__)


class DesigniteOperator(BaseOperator):
    """
    Run Designite static analysis on a single project version.

    Parameters
    ----------
    version : Version
        The version dataclass instance to analyse.
    language : ProjectLanguage
        The programming language of the project (JAVA or PYTHON).
    repo_full_name : str
        The GitHub repository full name (``owner/repo``), used to clone.
    workspace_dir : Path
        Directory used as working space for cloning and analysis output.
    """

    def __init__(
        self,
        *,
        version: Version,
        language: ProjectLanguage,
        repo_full_name: str,
        workspace_dir: Path = WORKSPACE_DIR,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.version = version
        self.language = language
        self.repo_full_name = repo_full_name
        self.workspace_dir = Path(workspace_dir)

    def execute(self, context: dict) -> dict:
        version = self.version

        if analysis_exists_for_version(version.id):
            logger.info(
                "Version %s (id=%s) already analysed — skipping.",
                version.commit_sha[:7],
                version.id,
            )
            return {"status": "skipped", "version_id": version.id}

        if self.language == ProjectLanguage.UNKNOWN:
            logger.warning(
                "Project '%s' has unknown language — skipping analysis.",
                self.repo_full_name,
            )
            return {"status": "skipped", "reason": "unknown language", "version_id": version.id}

        analysis_id = create_analysis(
            version_id=version.id,
            project_id=version.project_id,
        )
        update_analysis_started(analysis_id)
        logger.info(
            "Starting analysis id=%s for version %s of '%s'.",
            analysis_id,
            version.commit_sha[:7],
            self.repo_full_name,
        )

        with tempfile.TemporaryDirectory(
            prefix=f"designite_{version.id}_",
            dir=self.workspace_dir,
        ) as tmp_dir:
            tmp_path = Path(tmp_dir)
            repo_path = tmp_path / "repo"
            output_path = tmp_path / "output"
            output_path.mkdir()

            try:
                self._clone_repo(repo_path)
                self._run_designite(repo_path, output_path)
                results = self._parse_output(output_path)
                update_analysis_success(analysis_id, results)
                logger.info(
                    "Analysis id=%s completed successfully. Parsed %d output file(s).",
                    analysis_id,
                    len(results),
                )
                return {"status": "success", "analysis_id": analysis_id, "files": list(results.keys())}

            except Exception as exc:
                error_msg = str(exc)
                logger.error("Analysis id=%s failed: %s", analysis_id, error_msg, exc_info=True)
                update_analysis_failed(analysis_id, error_msg)
                raise

    def _clone_repo(self, repo_path: Path) -> None:
        clone_url = f"https://github.com/{self.repo_full_name}.git"
        sha = self.version.commit_sha
        logger.info("Cloning %s at %s …", self.repo_full_name, sha[:7])
        self._run_command(["git", "clone", "--depth=1", clone_url, str(repo_path)], cwd=repo_path.parent)
        self._run_command(["git", "fetch", "--depth=1", "origin", sha], cwd=repo_path)
        self._run_command(["git", "checkout", sha], cwd=repo_path)
        logger.info("Repository cloned and checked out at %s.", sha[:7])

    def _run_designite(self, repo_path: Path, output_path: Path) -> None:
        if self.language == ProjectLanguage.JAVA:
            self._run_designite_java(repo_path, output_path)
        elif self.language == ProjectLanguage.PYTHON:
            self._run_designite_python(repo_path, output_path)
        else:
            raise ValueError(f"Unsupported language for Designite: {self.language}")

    def _run_designite_java(self, repo_path: Path, output_path: Path) -> None:
        logger.info("Running DesigniteJava on %s …", self.repo_full_name)
        self._run_command([JAVA_EXECUTABLE, "-jar", str(DESIGNITE_JAVA_JAR), "-i", str(repo_path), "-o", str(output_path)])

    def _run_designite_python(self, repo_path: Path, output_path: Path) -> None:
        logger.info("Running DPy on %s …", self.repo_full_name)
        self._run_command([str(DESIGNITE_PYTHON_EXECUTABLE), "analyze", "-i", str(repo_path), "-o", str(output_path)])

    def _parse_output(self, output_path: Path) -> dict[str, list[dict[str, Any]]]:
        results: dict[str, list[dict[str, Any]]] = {}
        csv_files = list(output_path.rglob("*.csv"))
        if not csv_files:
            logger.warning("No CSV output files found in %s.", output_path)
            return results
        for csv_file in csv_files:
            key = csv_file.stem
            try:
                results[key] = self._parse_csv(csv_file)
            except Exception as exc:
                logger.warning("Failed to parse %s: %s", csv_file.name, exc)
                results[key] = []
        logger.info("Parsed %d CSV file(s): %s", len(results), ", ".join(results.keys()))
        return results

    @staticmethod
    def _parse_csv(csv_file: Path) -> list[dict[str, Any]]:
        rows = []
        with open(csv_file, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append({k.strip(): v.strip() for k, v in row.items() if k})
        return rows

    @staticmethod
    def _run_command(cmd: list[str], cwd: Path | None = None) -> None:
        logger.debug("Running command: %s", " ".join(cmd))
        result = subprocess.run(cmd, cwd=str(cwd) if cwd else None, capture_output=True, text=True)
        if result.stdout:
            logger.debug("stdout: %s", result.stdout[-2000:])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[-2000:])
        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed with exit code {result.returncode}.\n"
                f"Command: {' '.join(cmd)}\n"
                f"stderr: {result.stderr[-1000:]}"
            )