#!/bin/bash
# =============================================================================
# start.sh — Start the Airflow pipeline
#
# What it does:
#   1. Starts pipeline-db and restores the local backup
#   2. Starts all Airflow services
#   3. Waits for Ctrl+C or SIGTERM
#   4. On shutdown: saves a backup of pipeline-db to data/pipeline_backup.sql
#   5. Stops all containers
#
# Usage:
#   ./start.sh
# =============================================================================

set -e

BACKUP_FILE="./data/pipeline_backup.sql"
DB_USER="pipeline"
DB_NAME="pipeline"

# --- Shutdown handler -------------------------------------------------------
shutdown() {
    echo ""
    echo "=============================================="
    echo "  Shutting down..."
    echo "=============================================="

    echo ""
    echo "[1/2] Saving database backup to $BACKUP_FILE ..."
    mkdir -p ./data
    docker compose exec pipeline-db pg_dump -U "$DB_USER" "$DB_NAME" > "$BACKUP_FILE"
    echo "      Backup saved."

    echo ""
    echo "[2/2] Stopping containers..."
    docker compose down
    echo "      All containers stopped."

    echo ""
    echo "=============================================="
    echo "  Shutdown complete."
    echo "=============================================="
    echo ""
    exit 0
}

trap shutdown SIGINT SIGTERM

# --- Step 1: start pipeline-db and restore backup --------------------------
echo ""
echo "=============================================="
echo "  Pipeline Start"
echo "=============================================="

echo ""
echo "[1/2] Starting pipeline-db..."
docker compose up -d postgres pipeline-db
echo "      Waiting for pipeline-db to be healthy..."
until docker compose exec pipeline-db pg_isready -U "$DB_USER" > /dev/null 2>&1; do
    sleep 2
done
echo "      pipeline-db is ready."

if [ -f "$BACKUP_FILE" ]; then
    echo "      Restoring backup from $BACKUP_FILE ..."
    docker compose exec -T pipeline-db psql -U "$DB_USER" -d "$DB_NAME" < "$BACKUP_FILE"
    echo "      Backup restored."
else
    echo "      WARNING: No backup found at $BACKUP_FILE."
    echo "      Run ./init.sh first to initialize the database."
    docker compose down
    exit 1
fi

# --- Step 2: start all services --------------------------------------------
echo ""
echo "[2/2] Starting Airflow services..."
docker compose up -d
echo "      Services started. Airflow UI: http://localhost:8080"

echo ""
echo "=============================================="
echo "  Pipeline running. Press Ctrl+C to stop."
echo "=============================================="
echo ""

while true; do
    sleep 5
done