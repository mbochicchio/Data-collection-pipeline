"""
Database initialisation script.

Run this once before starting Airflow (or at every container startup —
it is idempotent thanks to IF NOT EXISTS guards in the DDL).

Usage::

    python scripts/init_db.py
    python scripts/init_db.py --seed projects.txt   # also seed projects
    python scripts/init_db.py --add "apache/spark"  # add a single project

A seed file is a plain-text file with one 'owner/repo' per line.
Lines starting with '#' are treated as comments and ignored.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.db import init_schema, upsert_project
from common.models import Project

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def seed_from_file(seed_file: Path) -> None:
    """Insert every 'owner/repo' line from seed_file into the projects table."""
    if not seed_file.exists():
        logger.error("Seed file not found: %s", seed_file)
        sys.exit(1)

    lines = seed_file.read_text(encoding="utf-8").splitlines()
    entries = [l.strip() for l in lines if l.strip() and not l.startswith("#")]

    if not entries:
        logger.warning("Seed file is empty or contains only comments.")
        return

    for full_name in entries:
        try:
            project = Project.from_full_name(full_name)
            project_id = upsert_project(project)
            logger.info("  ✓  %s  (id=%s)", full_name, project_id)
        except ValueError as exc:
            logger.warning("  ✗  Skipping '%s': %s", full_name, exc)


def add_single_project(full_name: str) -> None:
    """Insert or update a single project by its 'owner/repo' name."""
    project = Project.from_full_name(full_name)
    project_id = upsert_project(project)
    logger.info("Project '%s' upserted with id=%s.", full_name, project_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialise the pipeline PostgreSQL database.",
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

    init_schema()

    if args.seed:
        logger.info("Seeding projects from %s …", args.seed)
        seed_from_file(args.seed)

    if args.add:
        add_single_project(args.add)

    logger.info("Done.")


if __name__ == "__main__":
    main()