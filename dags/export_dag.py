"""
Export DAG — Data-collection-pipeline
======================================

Exports the entire pipeline PostgreSQL database to a DuckDB file
in the local ``data/`` directory.

The export creates a DuckDB file at ``/opt/airflow/data/pipeline_export.duckdb``
which is mounted as a volume to ``./data/pipeline_export.duckdb`` on the host.

Tables exported:
    - projects
    - versions
    - analyses
    - quality_gates

Schedule: manual — run on demand before data analysis.

Tasks
-----
export_to_duckdb
    Reads all tables from PostgreSQL and writes them to a DuckDB file.

log_export_summary
    Logs row counts for each exported table.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task

from common.db import get_connection
from config.settings import EXPORT_SCHEDULE

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "pipeline",
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

TABLES = ["projects", "versions", "analyses", "quality_gates"]
EXPORT_PATH = Path("/opt/airflow/data/pipeline_export.duckdb")


@dag(
    dag_id="export",
    description="Export the pipeline PostgreSQL database to a local DuckDB file.",
    schedule=EXPORT_SCHEDULE,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["export", "duckdb"],
    doc_md=__doc__,
)
def export_dag():

    @task(task_id="export_to_duckdb")
    def export_to_duckdb() -> dict:
        import duckdb

        export_path = str(EXPORT_PATH)
        logger.info("Exporting pipeline database to %s …", export_path)

        # Remove existing export file to start fresh
        if Path(export_path).exists():
            Path(export_path).unlink()
            logger.info("Removed existing export file.")

        summary = {}

        with get_connection() as pg_conn:
            with pg_conn.cursor() as cur:
                duck = duckdb.connect(export_path)

                for table in TABLES:
                    logger.info("Exporting table '%s' …", table)

                    # Fetch column names
                    cur.execute(f"SELECT * FROM {table} LIMIT 0")
                    columns = [desc[0] for desc in cur.description]

                    # Fetch all rows
                    cur.execute(f"SELECT * FROM {table}")
                    rows = cur.fetchall()

                    # Create table in DuckDB and insert rows
                    duck.execute(f"DROP TABLE IF EXISTS {table}")
                    duck.execute(f"CREATE TABLE {table} AS SELECT * FROM (VALUES {','.join(['(' + ','.join(['?' for _ in columns]) + ')' for _ in rows])}) t({','.join(columns)})" if rows else f"CREATE TABLE {table} AS SELECT * FROM (VALUES (NULL)) t({','.join(columns)}) WHERE 1=0")

                    if rows:
                        placeholders = ", ".join(["?" for _ in columns])
                        duck.executemany(
                            f"INSERT INTO {table} VALUES ({placeholders})",
                            [list(row) for row in rows],
                        )

                    summary[table] = len(rows)
                    logger.info("  → %d rows exported.", len(rows))

                duck.close()

        logger.info("Export complete: %s", export_path)
        return summary

    @task(task_id="log_export_summary")
    def log_export_summary(summary: dict) -> None:
        if not summary:
            logger.warning("No summary received.")
            return
        logger.info("=" * 50)
        logger.info("EXPORT SUMMARY → %s", str(EXPORT_PATH))
        for table, count in summary.items():
            logger.info("  %-20s %d rows", table, count)
        logger.info("=" * 50)

    log_export_summary(export_to_duckdb())


export_dag()
