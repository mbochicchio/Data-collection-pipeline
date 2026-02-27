"""
Database reset and reinitialisation utility — Data-collection-pipeline.

Available commands
------------------
status      Show current row counts for all tables (non-destructive).
reset       Drop all tables and recreate the schema from scratch.
            All data will be lost — requires explicit confirmation.
reseed      Re-insert projects from repo_urls.txt without touching
            versions or analyses (safe to run at any time).
full-reset  Drop everything AND re-seed projects from repo_urls.txt.

Usage
-----
    python scripts/reset_db.py status
    python scripts/reset_db.py reset
    python scripts/reset_db.py reseed
    python scripts/reset_db.py full-reset
    python scripts/reset_db.py reset --yes      # skip confirmation prompt
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from common.db import get_connection, init_schema, upsert_project
from common.models import Project
from config.settings import PIPELINE_DB_HOST, PIPELINE_DB_PORT, PIPELINE_DB_NAME

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SEED_FILE = Path(__file__).resolve().parent.parent / "data" / "repo_urls.txt"


def cmd_status() -> None:
    """Print row counts for every table without modifying anything."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            tables = ["projects", "versions", "analyses", "quality_gates"]
            print("\n── Database status ─────────────────────────")
            print(f"  Host: {PIPELINE_DB_HOST}:{PIPELINE_DB_PORT}/{PIPELINE_DB_NAME}\n")
            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    print(f"  {table:<20} {count:>6} rows")
                except Exception:
                    print(f"  {table:<20}   (table not found)")
            print("─" * 44 + "\n")


def cmd_reset(yes: bool) -> None:
    """Drop all tables and recreate the schema from scratch."""
    if not yes:
        confirm = input(
            f"\n⚠️  This will DELETE ALL DATA in {PIPELINE_DB_NAME}.\n"
            "Type 'yes' to confirm: "
        ).strip().lower()
        if confirm != "yes":
            logger.info("Reset cancelled.")
            return

    logger.info("Dropping all pipeline tables …")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS quality_gates CASCADE;
                DROP TABLE IF EXISTS analyses CASCADE;
                DROP TABLE IF EXISTS versions CASCADE;
                DROP TABLE IF EXISTS projects CASCADE;
            """)

    logger.info("Recreating schema …")
    init_schema()
    logger.info("Reset complete. Database is empty and ready.")


def cmd_reseed(seed_file: Path) -> None:
    """Re-insert projects from the seed file without touching versions or analyses."""
    if not seed_file.exists():
        logger.error("Seed file not found: %s", seed_file)
        sys.exit(1)

    lines = seed_file.read_text(encoding="utf-8").splitlines()
    entries = [l.strip() for l in lines if l.strip() and not l.startswith("#")]

    if not entries:
        logger.warning("Seed file is empty.")
        return

    logger.info("Reseeding %d project(s) from %s …", len(entries), seed_file)
    ok, skipped = 0, 0
    for full_name in entries:
        try:
            project = Project.from_full_name(full_name)
            pid = upsert_project(project)
            logger.info("  ✓  %-40s (id=%s)", full_name, pid)
            ok += 1
        except Exception as exc:
            logger.warning("  ✗  %-40s — %s", full_name, exc)
            skipped += 1

    logger.info("Reseed complete. inserted/updated=%d  skipped=%d", ok, skipped)


def cmd_full_reset(seed_file: Path, yes: bool) -> None:
    """Drop everything and re-seed projects."""
    cmd_reset(yes=yes)
    cmd_reseed(seed_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Database reset and reinitialisation utility.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "command",
        choices=["status", "reset", "reseed", "full-reset"],
        help="Action to perform.",
    )
    parser.add_argument(
        "--seed-file",
        type=Path,
        default=DEFAULT_SEED_FILE,
        help=f"Seed file (default: {DEFAULT_SEED_FILE})",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt for destructive operations.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dispatch = {
        "status":     cmd_status,
        "reset":      lambda: cmd_reset(yes=args.yes),
        "reseed":     lambda: cmd_reseed(args.seed_file),
        "full-reset": lambda: cmd_full_reset(args.seed_file, yes=args.yes),
    }
    dispatch[args.command]()


if __name__ == "__main__":
    main()