"""
Database access layer for the GitHub Empirical Studies Pipeline.

All SQL is written explicitly here — no ORM — to keep the code auditable and
to leverage DuckDB-specific features (e.g. JSON functions, SEQUENCE).

Thread-safety note: DuckDB supports a single write connection at a time.
Airflow tasks run in separate processes, so each process opens its own
connection.  We therefore use a context-manager pattern and never share a
connection across tasks.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import duckdb

from common.models import AnalysisStatus, Project, ProjectLanguage, Version
from config.settings import DUCKDB_PATH

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------


@contextmanager
def get_connection(db_path: Path = DUCKDB_PATH) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Context manager that yields an open DuckDB connection and closes it on exit.

    Usage::

        with get_connection() as conn:
            conn.execute("SELECT 1")

    The database file (and any parent directories) are created automatically
    if they do not exist yet.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

# DDL statements are kept here so that init_db.py and tests can import them.

DDL_PROJECTS = """
CREATE SEQUENCE IF NOT EXISTS seq_projects_id START 1;

CREATE TABLE IF NOT EXISTS projects (
    id           INTEGER PRIMARY KEY DEFAULT nextval('seq_projects_id'),
    full_name    VARCHAR NOT NULL UNIQUE,   -- "owner/repo_name"
    owner        VARCHAR NOT NULL,
    repo_name    VARCHAR NOT NULL,
    language     VARCHAR NOT NULL DEFAULT 'unknown',
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    added_at     TIMESTAMP NOT NULL DEFAULT current_timestamp
);
"""

DDL_VERSIONS = """
CREATE SEQUENCE IF NOT EXISTS seq_versions_id START 1;

CREATE TABLE IF NOT EXISTS versions (
    id             INTEGER PRIMARY KEY DEFAULT nextval('seq_versions_id'),
    project_id     INTEGER NOT NULL REFERENCES projects(id),
    commit_sha     VARCHAR NOT NULL,
    commit_message VARCHAR,
    committed_at   TIMESTAMP NOT NULL,
    discovered_at  TIMESTAMP NOT NULL DEFAULT current_timestamp,
    default_branch VARCHAR NOT NULL,
    metadata_json  JSON,

    -- A project cannot have the same commit recorded twice
    UNIQUE (project_id, commit_sha)
);
"""

DDL_ANALYSES = """
CREATE SEQUENCE IF NOT EXISTS seq_analyses_id START 1;

CREATE TABLE IF NOT EXISTS analyses (
    id            INTEGER PRIMARY KEY DEFAULT nextval('seq_analyses_id'),
    version_id    INTEGER NOT NULL REFERENCES versions(id),
    project_id    INTEGER NOT NULL REFERENCES projects(id),
    status        VARCHAR NOT NULL DEFAULT 'pending',
    started_at    TIMESTAMP,
    finished_at   TIMESTAMP,
    error_message VARCHAR,
    results_json  JSON,

    -- One analysis record per version (re-runs update the existing row)
    UNIQUE (version_id)
);
"""





# ---------------------------------------------------------------------------
# Project operations
# ---------------------------------------------------------------------------


def upsert_project(project: Project, db_path: Path = DUCKDB_PATH) -> int:
    """
    Insert a project row, or update language/is_active if it already exists.

    Returns the project's database id.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO projects (full_name, owner, repo_name, language, is_active, added_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (full_name) DO UPDATE SET
                language  = excluded.language,
                is_active = excluded.is_active
            RETURNING id
            """,
            [
                project.full_name,
                project.owner,
                project.repo_name,
                project.language.value,
                project.is_active,
                project.added_at,
            ],
        )
        row = conn.fetchone()
    return row[0]


def get_active_projects(db_path: Path = DUCKDB_PATH) -> list[Project]:
    """Return all projects where is_active = TRUE, ordered by full_name."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            SELECT id, full_name, owner, repo_name, language, is_active, added_at
            FROM projects
            WHERE is_active = TRUE
            ORDER BY full_name
            """
        )
        rows = conn.fetchall()

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


def insert_version_if_new(version: Version, db_path: Path = DUCKDB_PATH) -> int | None:
    """
    Insert a version row only if the (project_id, commit_sha) pair is new.

    Returns the new row's id, or None if the version already existed.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO versions
                (project_id, commit_sha, commit_message, committed_at,
                 discovered_at, default_branch, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (project_id, commit_sha) DO NOTHING
            RETURNING id
            """,
            [
                version.project_id,
                version.commit_sha,
                version.commit_message,
                version.committed_at,
                version.discovered_at,
                version.default_branch,
                version.metadata_json,
            ],
        )
        row = conn.fetchone()

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


def get_versions_pending_analysis(db_path: Path = DUCKDB_PATH) -> list[Version]:
    """
    Return versions that have no corresponding analysis row yet.

    These are the candidates for the execution DAG to process.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            SELECT
                v.id, v.project_id, v.commit_sha, v.commit_message,
                v.committed_at, v.discovered_at, v.default_branch, v.metadata_json
            FROM versions v
            LEFT JOIN analyses a ON a.version_id = v.id
            WHERE a.id IS NULL
            ORDER BY v.discovered_at ASC
            """
        )
        rows = conn.fetchall()

    return [Version.from_db_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Analysis operations
# ---------------------------------------------------------------------------


def create_analysis(version_id: int, project_id: int, db_path: Path = DUCKDB_PATH) -> int:
    """
    Create a new analysis row in PENDING status.

    Returns the new analysis id.
    Raises if an analysis for this version already exists (UNIQUE constraint).
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO analyses (version_id, project_id, status)
            VALUES (?, ?, ?)
            RETURNING id
            """,
            [version_id, project_id, AnalysisStatus.PENDING.value],
        )
        row = conn.fetchone()
    return row[0]


def update_analysis_started(analysis_id: int, db_path: Path = DUCKDB_PATH) -> None:
    """Mark an analysis as RUNNING and record the start timestamp."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE analyses
            SET status = ?, started_at = ?
            WHERE id = ?
            """,
            [AnalysisStatus.RUNNING.value, datetime.now(timezone.utc), analysis_id],
        )


def update_analysis_success(
    analysis_id: int,
    results: dict,
    db_path: Path = DUCKDB_PATH,
) -> None:
    """Mark an analysis as SUCCESS and persist the parsed Designite results."""
    import json

    with get_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE analyses
            SET status = ?, finished_at = ?, results_json = ?
            WHERE id = ?
            """,
            [
                AnalysisStatus.SUCCESS.value,
                datetime.now(timezone.utc),
                json.dumps(results, default=str),
                analysis_id,
            ],
        )


def update_analysis_failed(
    analysis_id: int,
    error_message: str,
    db_path: Path = DUCKDB_PATH,
) -> None:
    """Mark an analysis as FAILED and store the error message."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            UPDATE analyses
            SET status = ?, finished_at = ?, error_message = ?
            WHERE id = ?
            """,
            [AnalysisStatus.FAILED.value, datetime.now(timezone.utc), error_message, analysis_id],
        )


def analysis_exists_for_version(version_id: int, db_path: Path = DUCKDB_PATH) -> bool:
    """
    Return True if a completed (SUCCESS or FAILED) analysis already exists
    for the given version_id.

    PENDING/RUNNING rows are ignored so that crashed tasks can be retried.
    """
    with get_connection(db_path) as conn:
        conn.execute(
            """
            SELECT COUNT(*)
            FROM analyses
            WHERE version_id = ?
              AND status IN ('success', 'failed', 'skipped')
            """,
            [version_id],
        )
        row = conn.fetchone()
    return row[0] > 0


# ---------------------------------------------------------------------------
# Quality gate DDL
# ---------------------------------------------------------------------------

DDL_QUALITY_GATES = """
CREATE SEQUENCE IF NOT EXISTS seq_quality_gates_id START 1;

CREATE TABLE IF NOT EXISTS quality_gates (
    id                      INTEGER PRIMARY KEY DEFAULT nextval('seq_quality_gates_id'),
    project_id              INTEGER NOT NULL REFERENCES projects(id),
    run_at                  TIMESTAMP NOT NULL,

    -- RepoQuester metric scores (0 or 1 each)
    community               DOUBLE,
    continuous_integration  DOUBLE,
    documentation           DOUBLE,
    history                 DOUBLE,
    management              DOUBLE,
    license                 DOUBLE,
    unit_test               DOUBLE,
    pull                    DOUBLE,
    releases                DOUBLE,

    -- Derived fields
    score                   INTEGER NOT NULL,   -- how many metrics > 0
    passed                  BOOLEAN NOT NULL,   -- score >= 5

    -- One quality gate record per project (re-runs replace the existing row)
    UNIQUE (project_id)
);
"""


# ---------------------------------------------------------------------------
# Quality gate operations
# ---------------------------------------------------------------------------


def init_schema(db_path: Path = DUCKDB_PATH) -> None:
    """
    Create all tables and sequences if they do not already exist.

    Safe to call on every startup — all statements use IF NOT EXISTS.
    """
    logger.info("Initialising database schema at %s", db_path)
    with get_connection(db_path) as conn:
        conn.execute(DDL_PROJECTS)
        conn.execute(DDL_VERSIONS)
        conn.execute(DDL_ANALYSES)
        conn.execute(DDL_QUALITY_GATES)
    logger.info("Schema initialisation complete.")


def get_projects_without_quality_gate(db_path: Path = DUCKDB_PATH) -> list:
    """Return active projects that have never been evaluated by RepoQuester."""
    with get_connection(db_path) as conn:
        conn.execute(
            """
            SELECT p.id, p.full_name
            FROM projects p
            LEFT JOIN quality_gates qg ON qg.project_id = p.id
            WHERE p.is_active = TRUE
              AND qg.id IS NULL
            ORDER BY p.full_name
            """
        )
        rows = conn.fetchall()
    return [{"id": r[0], "full_name": r[1]} for r in rows]


def upsert_quality_gate(
    project_id: int,
    metrics: dict,
    db_path: Path = DUCKDB_PATH,
) -> None:
    """
    Insert or replace a quality gate result for a project.

    ``metrics`` must be a dict with keys matching the 9 RepoQuester columns:
    community, continuous_integration, documentation, history, management,
    license, unit_test, pull, releases.
    """
    metric_keys = [
        "community", "continuous_integration", "documentation",
        "history", "management", "license", "unit_test", "pull", "releases",
    ]
    score = sum(1 for k in metric_keys if (metrics.get(k) or 0) > 0)
    passed = score >= 5
    run_at = datetime.now(timezone.utc)

    with get_connection(db_path) as conn:
        conn.execute(
            """
            INSERT INTO quality_gates (
                project_id, run_at,
                community, continuous_integration, documentation,
                history, management, license, unit_test, pull, releases,
                score, passed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (project_id) DO UPDATE SET
                run_at                 = excluded.run_at,
                community              = excluded.community,
                continuous_integration = excluded.continuous_integration,
                documentation          = excluded.documentation,
                history                = excluded.history,
                management             = excluded.management,
                license                = excluded.license,
                unit_test              = excluded.unit_test,
                pull                   = excluded.pull,
                releases               = excluded.releases,
                score                  = excluded.score,
                passed                 = excluded.passed
            """,
            [
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
            ],
        )
    logger.debug(
        "Quality gate saved for project_id=%s — score=%d, passed=%s",
        project_id, score, passed,
    )


def project_passed_quality_gate(project_id: int, db_path: Path = DUCKDB_PATH) -> bool | None:
    """
    Check if a project has passed the quality gate.

    Returns:
        True  — project passed (score >= 5)
        False — project failed
        None  — quality gate not yet run for this project
    """
    with get_connection(db_path) as conn:
        conn.execute(
            "SELECT passed FROM quality_gates WHERE project_id = ?",
            [project_id],
        )
        row = conn.fetchone()
    if row is None:
        return None
    return bool(row[0])