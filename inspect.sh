#!/bin/bash
# =============================================================================
# inspect.sh — Start pipeline-db locally for inspection (no Airflow)
#
# What it does:
#   1. Starts pipeline-db and restores the local backup
#   2. Prints connection info for scripts and notebook
#   3. Waits for Ctrl+C
#   4. On shutdown: saves a backup and stops the container
#
# Usage:
#   chmod +x inspect.sh
#   ./inspect.sh
#   # then in another terminal:
#   PIPELINE_DB_HOST=localhost PIPELINE_DB_PORT=5434 python scripts/reset_db.py status
# =============================================================================

set -e

BACKUP_FILE="./data/pipeline_backup.sql"
DB_USER="pipeline"
DB_NAME="pipeline"

shutdown() {
    echo ""
    echo "Saving backup to $BACKUP_FILE ..."
    mkdir -p ./data
    docker compose exec pipeline-db pg_dump -U "$DB_USER" "$DB_NAME" > "$BACKUP_FILE"
    echo "Backup saved."
    docker compose stop pipeline-db
    echo "pipeline-db stopped."
    exit 0
}

trap shutdown SIGINT SIGTERM

# --- Start pipeline-db -------------------------------------------------------
echo ""
echo "Starting pipeline-db..."
docker compose up -d pipeline-db

echo "Waiting for pipeline-db to be healthy..."
until docker compose exec pipeline-db pg_isready -U "$DB_USER" > /dev/null 2>&1; do
    sleep 2
done
echo "pipeline-db is ready."

# --- Restore backup ----------------------------------------------------------
if [ -f "$BACKUP_FILE" ]; then
    echo "Restoring backup from $BACKUP_FILE ..."
    docker compose exec -T pipeline-db psql -U "$DB_USER" -d "$DB_NAME" < "$BACKUP_FILE"
    echo "Backup restored."
else
    echo "WARNING: No backup found at $BACKUP_FILE — database is empty."
fi

# --- Print connection info ---------------------------------------------------
echo ""
echo "=============================================="
echo "  pipeline-db is running on localhost:5434"
echo ""
echo "  Scripts:"
echo "  PIPELINE_DB_HOST=localhost PIPELINE_DB_PORT=5434 \\"
echo "    python scripts/reset_db.py status"
echo ""
echo "  Notebook:"
echo "  Set PIPELINE_DB_PORT=5434 in .env or run"
echo "  the navigator notebook directly."
echo ""
echo "  Press Ctrl+C to save backup and stop."
echo "=============================================="
echo ""

while true; do
    sleep 5
done
