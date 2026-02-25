"""
Airflow operator for fetching GitHub repository data.

This operator wraps :class:`~common.github_client.GitHubClient` and writes
results directly to the DuckDB database via the ``common.db`` layer.

It performs two operations in sequence for each project:
1. Fetch repository metadata → update ``language`` in the ``projects`` table.
2. Fetch the latest commit on the default branch → insert into ``versions``
   (skipped silently if the commit was already recorded).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import BaseOperator

from common.db import (
    get_active_projects,
    insert_version_if_new,
    upsert_project,
)
from common.github_client import GitHubClient
from common.models import Project, ProjectLanguage, Version
from config.settings import DUCKDB_PATH

logger = logging.getLogger(__name__)


class GitHubIngestionOperator(BaseOperator):
    """
    Fetch the latest commit for every active project and persist it to DuckDB.

    For each active project in the ``projects`` table the operator will:

    - Call the GitHub API to retrieve repository metadata (language, default
      branch, stars, …).
    - Update the project's ``language`` field in the database.
    - Fetch the HEAD commit of the default branch.
    - Insert a new row in ``versions`` if the commit SHA has not been seen
      before; skip it otherwise (idempotent).

    Parameters
    ----------
    db_path:
        Path to the DuckDB file.  Defaults to :data:`config.settings.DUCKDB_PATH`.
    github_token:
        GitHub Personal Access Token.  Falls back to the value in
        ``config.settings`` (which reads from the ``GITHUB_TOKEN`` env var).

    Example usage in a DAG::

        ingest = GitHubIngestionOperator(
            task_id="ingest_github_metadata",
        )
    """

    # Airflow renders these fields as Jinja templates if needed
    template_fields = ("db_path",)

    def __init__(
        self,
        *,
        db_path: str | Path = DUCKDB_PATH,
        github_token: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.db_path = Path(db_path)
        self._github_token = github_token  # None → client falls back to settings

    # ------------------------------------------------------------------
    # Airflow entry point
    # ------------------------------------------------------------------

    def execute(self, context: dict) -> dict:
        """
        Main execution method called by the Airflow scheduler.

        Returns a summary dict that Airflow stores as XCom for observability.
        """
        client = GitHubClient(
            **({"token": self._github_token} if self._github_token else {})
        )

        projects = get_active_projects(db_path=self.db_path)
        logger.info("Found %d active project(s) to ingest.", len(projects))

        summary = {"processed": 0, "new_versions": 0, "skipped": 0, "errors": []}

        for project in projects:
            try:
                new_version_id = self._process_project(project, client)
                summary["processed"] += 1
                if new_version_id is not None:
                    summary["new_versions"] += 1
                else:
                    summary["skipped"] += 1
            except Exception as exc:
                # Log and continue — one failing project must not abort the rest
                logger.error(
                    "Failed to ingest project '%s': %s", project.full_name, exc, exc_info=True
                )
                summary["errors"].append({"project": project.full_name, "error": str(exc)})

        logger.info(
            "Ingestion complete. processed=%d  new_versions=%d  skipped=%d  errors=%d",
            summary["processed"],
            summary["new_versions"],
            summary["skipped"],
            len(summary["errors"]),
        )
        return summary

    # ------------------------------------------------------------------
    # Per-project logic
    # ------------------------------------------------------------------

    def _process_project(self, project: Project, client: GitHubClient) -> int | None:
        """
        Fetch metadata and latest commit for a single project.

        Returns the new ``versions.id`` if a new version was inserted,
        or ``None`` if the commit was already recorded.
        """
        logger.info("Processing '%s' …", project.full_name)

        # --- Step 1: fetch repo metadata and update language ---------------
        meta = client.get_repo_metadata(project.full_name)
        detected_language = ProjectLanguage.from_github(meta.language)

        if detected_language != project.language:
            logger.info(
                "  Updating language for '%s': %s → %s",
                project.full_name,
                project.language.value,
                detected_language.value,
            )
            updated_project = Project(
                id=project.id,
                full_name=project.full_name,
                owner=project.owner,
                repo_name=project.repo_name,
                language=detected_language,
                is_active=project.is_active,
                added_at=project.added_at,
            )
            upsert_project(updated_project, db_path=self.db_path)

        # --- Step 2: fetch latest commit and insert version if new ---------
        commit = client.get_latest_commit(
            branch=meta.default_branch,
            owner=project.owner,
            repo_name=project.repo_name,
        )

        logger.info(
            "  Latest commit: %s ('%s')", commit.sha[:7], commit.message[:60]
        )

        version = Version(
            project_id=project.id,
            commit_sha=commit.sha,
            commit_message=commit.message,
            committed_at=commit.committed_at,
            discovered_at=datetime.now(timezone.utc),
            default_branch=meta.default_branch,
            metadata={
                "description": meta.description,
                "stars": meta.stars,
                "forks": meta.forks,
                "open_issues": meta.open_issues,
                "is_fork": meta.is_fork,
                "is_archived": meta.is_archived,
                "updated_at": meta.updated_at.isoformat(),
            },
        )

        return insert_version_if_new(version, db_path=self.db_path)
