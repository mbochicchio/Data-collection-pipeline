"""
Domain models for the GitHub Empirical Studies Pipeline.

Each dataclass maps 1-to-1 to a DuckDB table.  They are intentionally plain
Python dataclasses (no ORM) to keep the dependency footprint minimal and to
make serialization straightforward.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ProjectLanguage(str, Enum):
    """Supported programming languages for static analysis."""

    JAVA = "Java"
    PYTHON = "Python"
    UNKNOWN = "unknown"

    @classmethod
    def from_github(cls, value: str | None) -> "ProjectLanguage":
        """Map the raw GitHub API 'language' field to a known enum value."""
        if value is None:
            return cls.UNKNOWN
        mapping = {
            "java": cls.JAVA,
            "python": cls.PYTHON,
        }
        return mapping.get(value.lower(), cls.UNKNOWN)


class AnalysisStatus(str, Enum):
    """Lifecycle states of a Designite analysis run."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"   # e.g., unsupported language


# ---------------------------------------------------------------------------
# Table: projects
# ---------------------------------------------------------------------------


@dataclass
class Project:
    """
    A GitHub repository that the pipeline should monitor.

    This is the *input* table — rows are added manually or via a seed script.
    The pipeline never deletes rows; it only deactivates them.
    """

    full_name: str          # "owner/repo_name" as stored in GitHub
    owner: str
    repo_name: str
    language: ProjectLanguage = ProjectLanguage.UNKNOWN
    is_active: bool = True
    added_at: datetime = field(default_factory=datetime.now(timezone.utc))
    id: int | None = None   # auto-assigned by DuckDB SEQUENCE

    @classmethod
    def from_full_name(cls, full_name: str) -> "Project":
        """Construct a Project from an 'owner/repo' string."""
        parts = full_name.strip().split("/", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Invalid full_name '{full_name}'. Expected format: 'owner/repo_name'."
            )
        owner, repo_name = parts
        return cls(full_name=full_name, owner=owner, repo_name=repo_name)


# ---------------------------------------------------------------------------
# Table: versions
# ---------------------------------------------------------------------------


@dataclass
class Version:
    """
    A snapshot of a project as seen at a specific point in time.

    One row is inserted each day per project, representing the HEAD commit of
    the default branch at the time the ingestion DAG ran.  Historical rows are
    never updated, which allows trend analysis over time.
    """

    project_id: int
    commit_sha: str          # Full 40-character SHA
    commit_message: str
    committed_at: datetime   # Author date of the commit
    discovered_at: datetime  # When the ingestion DAG recorded this row
    default_branch: str
    metadata: dict[str, Any] = field(default_factory=dict)
    id: int | None = None

    @property
    def metadata_json(self) -> str:
        """Serialize metadata dict to a JSON string for DB storage."""
        return json.dumps(self.metadata, default=str)

    @classmethod
    def from_db_row(cls, row: tuple) -> "Version":
        """
        Reconstruct a Version from a raw DuckDB row.

        Expected column order matches the SELECT in db.py:
        id, project_id, commit_sha, commit_message, committed_at,
        discovered_at, default_branch, metadata_json
        """
        (
            id_,
            project_id,
            commit_sha,
            commit_message,
            committed_at,
            discovered_at,
            default_branch,
            metadata_json,
        ) = row
        return cls(
            id=id_,
            project_id=project_id,
            commit_sha=commit_sha,
            commit_message=commit_message,
            committed_at=committed_at,
            discovered_at=discovered_at,
            default_branch=default_branch,
            metadata=json.loads(metadata_json) if metadata_json else {},
        )


# ---------------------------------------------------------------------------
# Table: analyses
# ---------------------------------------------------------------------------


@dataclass
class Analysis:
    """
    The result of running Designite on a specific Version of a project.

    Results from all Designite CSV output files are stored as JSON blobs in
    dedicated columns so that every metric is queryable via DuckDB's JSON
    functions without needing separate tables.
    """

    version_id: int
    project_id: int
    status: AnalysisStatus = AnalysisStatus.PENDING
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None

    # Each key corresponds to a Designite output CSV file name (without .csv).
    # Values are lists of dicts, one dict per CSV row.
    # Example keys for Java: "MethodMetrics", "ClassMetrics", "DesignSmells", ...
    results: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    id: int | None = None

    @property
    def results_json(self) -> str:
        """Serialize results to a JSON string for DB storage."""
        return json.dumps(self.results, default=str)
