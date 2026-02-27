#!/bin/bash
# =============================================================================
# inspect.sh — Start pipeline-db locally for inspection (no Airflow)
#
# Usage:
#   ./inspect.sh
#   # then in another terminal:
#   ./db.sh status
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
    # Drop and recreate the database to avoid conflicts on re-import
    docker compose exec pipeline-db psql -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
    docker compose exec pipeline-db psql -U "$DB_USER" -d postgres -c "CREATE DATABASE $DB_NAME;"
    docker compose exec -T pipeline-db psql -U "$DB_USER" -d "$DB_NAME" < "$BACKUP_FILE"
    echo "Backup restored."
else
    echo "WARNING: No backup found at $BACKUP_FILE — database is empty."
fi

# --- Print connection info ---------------------------------------------------
echo ""
echo "=============================================="
echo "  pipeline-db is running on localhost:5440"
echo ""
echo "  ./db.sh status"
echo "  ./db.sh reset"
echo "  ./db.sh reseed"
echo ""
echo "  Press Ctrl+C to save backup and stop."
echo "=============================================="
echo ""

while true; do
    sleep 5
done