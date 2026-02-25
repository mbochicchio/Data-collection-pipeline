"""
Database initialization script.

Run this once before starting Airflow (or at every container startup —
it is idempotent thanks to IF NOT EXISTS guards in the DDL).

Usage::

    python scripts/init_db.py [--db-path /custom/path/pipeline.duckdb]
    python scripts/init_db.py --seed projects.txt # also seed projects
    python scripts/init_db.py --add "apache/spark" # add a single project

A seed file is a plain-text file with one 'owner/repo' per line.
Lines starting with '#' are treated as comments and ignored.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Make sure the project root is on sys.path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.db import init_schema, upsert_project
from common.models import Project
from config.settings import DUCKDB_PATH

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def seed_from_file(seed_file: Path, db_path: Path) -> None:
    """Insert every 'owner/repo' line from *seed_file* into the projects table."""
    if not seed_file.exists():
        logger.error("Seed file not found: %s", seed_file)
        sys.exit(1)

    lines = seed_file.read_text(encoding="utf-8").splitlines()
    entries = [line.strip() for line in lines if line.strip() and not line.startswith("#")]

    if not entries:
        logger.warning("Seed file is empty or contains only comments.")
        return

    for full_name in entries:
        try:
            project = Project.from_full_name(full_name)
            project_id = upsert_project(project, db_path=db_path)
            logger.info("  ✓  %s  (id=%s)", full_name, project_id)
        except ValueError as exc:
            logger.warning("  ✗  Skipping '%s': %s", full_name, exc)


def add_single_project(full_name: str, db_path: Path) -> None:
    """Insert or update a single project by its 'owner/repo' name."""
    project = Project.from_full_name(full_name)
    project_id = upsert_project(project, db_path=db_path)
    logger.info("Project '%s' upserted with id=%s.", full_name, project_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialise the pipeline DuckDB database.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DUCKDB_PATH,
        help=f"Path to the DuckDB file (default: {DUCKDB_PATH})",
    )
    parser.add_argument(
        "--seed",
        type=Path,
        metavar="FILE",
        help="Seed the projects table from a text file (one 'owner/repo' per line).",
    )
    parser.add_argument(
        "--add",
        metavar="OWNER/REPO",
        help="Add a single project to the projects table.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path: Path = args.db_path

    logger.info("Database path: %s", db_path)

    # Step 1 — always ensure the schema is up to date
    init_schema(db_path=db_path)

    # Step 2 — optional project seeding
    if args.seed:
        logger.info("Seeding projects from %s …", args.seed)
        seed_from_file(args.seed, db_path)

    if args.add:
        add_single_project(args.add, db_path)

    logger.info("Done.")


if __name__ == "__main__":
    main()
