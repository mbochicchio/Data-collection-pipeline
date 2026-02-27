"""
Database access layer for the GitHub Empirical Studies Pipeline.

All SQL is written explicitly here — no ORM — to keep the code auditable.

Uses PostgreSQL via psycopg2. Connection parameters are read from environment
variables (see config/settings.py).

Thread-safety note: each Airflow task runs in a separate process and opens
its own short-lived connection. We use a context-manager pattern and never
share connections across tasks.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

import psycopg2
import psycopg2.extras

from common.models import AnalysisStatus, Project, ProjectLanguage, Version
from config.settings import (
    PIPELINE_DB_HOST,
    PIPELINE_DB_PORT,
    PIPELINE_DB_NAME,
    PIPELINE_DB_USER,
    PIPELINE_DB_PASSWORD,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Context manager that yields an open PostgreSQL connection and closes it on exit.

    Commits on success, rolls back on exception.

    Usage::

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
    """
    conn = psycopg2.connect(
        host=PIPELINE_DB_HOST,
        port=PIPELINE_DB_PORT,
        dbname=PIPELINE_DB_NAME,
        user=PIPELINE_DB_USER,
        password=PIPELINE_DB_PASSWORD,
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

DDL_PROJECTS = """
CREATE TABLE IF NOT EXISTS projects (
    id           SERIAL PRIMARY KEY,
    full_name    TEXT NOT NULL UNIQUE,
    owner        TEXT NOT NULL,
    repo_name    TEXT NOT NULL,
    language     TEXT NOT NULL DEFAULT 'unknown',
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    added_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);
"""

DDL_VERSIONS = """
CREATE TABLE IF NOT EXISTS versions (
    id             SERIAL PRIMARY KEY,
    project_id     INTEGER NOT NULL REFERENCES projects(id),
    commit_sha     TEXT NOT NULL,
    commit_message TEXT,
    committed_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    discovered_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    default_branch TEXT NOT NULL,
    metadata_json  JSONB,

    UNIQUE (project_id, commit_sha)
);
"""

DDL_ANALYSES = """
CREATE TABLE IF NOT EXISTS analyses (
    id            SERIAL PRIMARY KEY,
    version_id    INTEGER NOT NULL REFERENCES versions(id),
    project_id    INTEGER NOT NULL REFERENCES projects(id),
    status        TEXT NOT NULL DEFAULT 'pending',
    started_at    TIMESTAMP WITH TIME ZONE,
    finished_at   TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    results_json  JSONB,

    UNIQUE (version_id)
);
"""

DDL_QUALITY_GATES = """
CREATE TABLE IF NOT EXISTS quality_gates (
    id                      SERIAL PRIMARY KEY,
    project_id              INTEGER NOT NULL REFERENCES projects(id),
    run_at                  TIMESTAMP WITH TIME ZONE NOT NULL,

    community               DOUBLE PRECISION,
    continuous_integration  DOUBLE PRECISION,
    documentation           DOUBLE PRECISION,
    history                 DOUBLE PRECISION,
    management              DOUBLE PRECISION,
    license                 DOUBLE PRECISION,
    unit_test               DOUBLE PRECISION,
    pull                    DOUBLE PRECISION,
    releases                DOUBLE PRECISION,

    score                   INTEGER NOT NULL,
    passed                  BOOLEAN NOT NULL,

    UNIQUE (project_id)
);
"""


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------


def init_schema() -> None:
    """
    Create all tables if they do not already exist.

    Safe to call on every startup — all statements use IF NOT EXISTS.
    """
    logger.info(
        "Initialising database schema on %s:%s/%s",
        PIPELINE_DB_HOST, PIPELINE_DB_PORT, PIPELINE_DB_NAME,
    )
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_PROJECTS)
            cur.execute(DDL_VERSIONS)
            cur.execute(DDL_ANALYSES)
            cur.execute(DDL_QUALITY_GATES)
    logger.info("Schema initialisation complete.")


# ---------------------------------------------------------------------------
# Project operations
# ---------------------------------------------------------------------------


def upsert_project(project: Project) -> int:
    """
    Insert a project row, or update language/is_active if it already exists.

    Returns the project's database id.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO projects (full_name, owner, repo_name, language, is_active, added_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (full_name) DO UPDATE SET
                    language  = EXCLUDED.language,
                    is_active = EXCLUDED.is_active
                RETURNING id
                """,
                (
                    project.full_name,
                    project.owner,
                    project.repo_name,
                    project.language.value,
                    project.is_active,
                    project.added_at,
                ),
            )
            row = cur.fetchone()
    return row[0]


def get_active_projects() -> list[Project]:
    """Return all projects where is_active = TRUE, ordered by full_name."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, full_name, owner, repo_name, language, is_active, added_at
                FROM projects
                WHERE is_active = TRUE
                ORDER BY full_name
                """
            )
            rows = cur.fetchall()

    return [
        Project(
            id=r[0],
            full_name=r[1],
            owner=r[2],
            repo_name=r[3],
            language=ProjectLanguage(r[4]),
            is_active=r[5],
            added_at=r[6],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Version operations
# ---------------------------------------------------------------------------


def insert_version_if_new(version: Version) -> int | None:
    """
    Insert a version row only if the (project_id, commit_sha) pair is new.

    Returns the new row's id, or None if the version already existed.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO versions
                    (project_id, commit_sha, commit_message, committed_at,
                     discovered_at, default_branch, metadata_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (project_id, commit_sha) DO NOTHING
                RETURNING id
                """,
                (
                    version.project_id,
                    version.commit_sha,
                    version.commit_message,
                    version.committed_at,
                    version.discovered_at,
                    version.default_branch,
                    json.dumps(version.metadata_json) if version.metadata_json else None,
                ),
            )
            row = cur.fetchone()

    if row is None:
        logger.debug(
            "Version %s for project_id=%s already exists — skipped.",
            version.commit_sha[:7],
            version.project_id,
        )
        return None

    logger.info(
        "Recorded new version %s for project_id=%s (versions.id=%s).",
        version.commit_sha[:7],
        version.project_id,
        row[0],
    )
    return row[0]


def get_versions_pending_analysis() -> list[Version]:
    """
    Return versions that have no associated analysis row yet.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT v.id, v.project_id, v.commit_sha, v.commit_message,
                       v.committed_at, v.discovered_at, v.default_branch, v.metadata_json
                FROM versions v
                LEFT JOIN analyses a ON a.version_id = v.id
                WHERE a.id IS NULL
                ORDER BY v.discovered_at
                """
            )
            rows = cur.fetchall()

    return [
        Version(
            id=r[0],
            project_id=r[1],
            commit_sha=r[2],
            commit_message=r[3],
            committed_at=r[4],
            discovered_at=r[5],
            default_branch=r[6],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Analysis operations
# ---------------------------------------------------------------------------


def create_analysis(version_id: int, project_id: int) -> int:
    """Create a new analysis row in PENDING status. Returns the new analysis id."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO analyses (version_id, project_id, status)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (version_id, project_id, AnalysisStatus.PENDING.value),
            )
            row = cur.fetchone()
    return row[0]


def update_analysis_started(analysis_id: int) -> None:
    """Mark an analysis as RUNNING and record the start timestamp."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analyses
                SET status = %s, started_at = %s
                WHERE id = %s
                """,
                (AnalysisStatus.RUNNING.value, datetime.now(timezone.utc), analysis_id),
            )


def update_analysis_success(analysis_id: int, results: dict) -> None:
    """Mark an analysis as SUCCESS and persist the parsed Designite results."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analyses
                SET status = %s, finished_at = %s, results_json = %s
                WHERE id = %s
                """,
                (
                    AnalysisStatus.SUCCESS.value,
                    datetime.now(timezone.utc),
                    json.dumps(results, default=str),
                    analysis_id,
                ),
            )


def update_analysis_failed(analysis_id: int, error_message: str) -> None:
    """Mark an analysis as FAILED and store the error message."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analyses
                SET status = %s, finished_at = %s, error_message = %s
                WHERE id = %s
                """,
                (
                    AnalysisStatus.FAILED.value,
                    datetime.now(timezone.utc),
                    error_message,
                    analysis_id,
                ),
            )


def analysis_exists_for_version(version_id: int) -> bool:
    """
    Return True if a completed (SUCCESS or FAILED) analysis already exists
    for the given version_id.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM analyses
                WHERE version_id = %s
                  AND status IN ('success', 'failed', 'skipped')
                """,
                (version_id,),
            )
            row = cur.fetchone()
    return row[0] > 0


# ---------------------------------------------------------------------------
# Quality gate operations
# ---------------------------------------------------------------------------


def get_projects_without_quality_gate() -> list:
    """Return active projects that have never been evaluated by RepoQuester."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.full_name
                FROM projects p
                LEFT JOIN quality_gates qg ON qg.project_id = p.id
                WHERE p.is_active = TRUE
                  AND qg.id IS NULL
                ORDER BY p.full_name
                """
            )
            rows = cur.fetchall()
    return [{"id": r[0], "full_name": r[1]} for r in rows]


def upsert_quality_gate(project_id: int, metrics: dict) -> None:
    """
    Insert or replace a quality gate result for a project.
    """
    metric_keys = [
        "community", "continuous_integration", "documentation",
        "history", "management", "license", "unit_test", "pull", "releases",
    ]
    score = sum(1 for k in metric_keys if (metrics.get(k) or 0) > 0)
    passed = score > 5
    run_at = datetime.now(timezone.utc)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quality_gates (
                    project_id, run_at,
                    community, continuous_integration, documentation,
                    history, management, license, unit_test, pull, releases,
                    score, passed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (project_id) DO UPDATE SET
                    run_at                 = EXCLUDED.run_at,
                    community              = EXCLUDED.community,
                    continuous_integration = EXCLUDED.continuous_integration,
                    documentation          = EXCLUDED.documentation,
                    history                = EXCLUDED.history,
                    management             = EXCLUDED.management,
                    license                = EXCLUDED.license,
                    unit_test              = EXCLUDED.unit_test,
                    pull                   = EXCLUDED.pull,
                    releases               = EXCLUDED.releases,
                    score                  = EXCLUDED.score,
                    passed                 = EXCLUDED.passed
                """,
                (
                    project_id, run_at,
                    metrics.get("community"),
                    metrics.get("continuous_integration"),
                    metrics.get("documentation"),
                    metrics.get("history"),
                    metrics.get("management"),
                    metrics.get("license"),
                    metrics.get("unit_test"),
                    metrics.get("pull"),
                    metrics.get("releases"),
                    score,
                    passed,
                ),
            )
    logger.debug(
        "Quality gate saved for project_id=%s — score=%d, passed=%s",
        project_id, score, passed,
    )


def project_passed_quality_gate(project_id: int) -> bool | None:
    """
    Check if a project has passed the quality gate.

    Returns True (passed), False (failed), or None (not yet evaluated).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT passed FROM quality_gates WHERE project_id = %s",
                (project_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return bool(row[0])