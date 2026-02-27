#!/bin/bash
# =============================================================================
# init.sh — Initialize the pipeline for the first time (or after a reset)
#
# What it does:
#   1. Starts postgres and pipeline-db
#   2. Restores the local backup into pipeline-db (if it exists)
#   3. Runs airflow-init (db migrate, create admin user, seed projects)
#   4. Saves a backup of pipeline-db to data/pipeline_backup.sql
#
# Usage:
#   chmod +x init.sh start.sh
#   ./init.sh
# =============================================================================

set -e

BACKUP_FILE="./data/pipeline_backup.sql"
DB_CONTAINER="data-collection-pipeline-pipeline-db-1"
DB_USER="pipeline"
DB_NAME="pipeline"

echo ""
echo "=============================================="
echo "  Pipeline Init"
echo "=============================================="

# --- Step 1: start databases -----------------------------------------------
echo ""
echo "[1/4] Starting databases..."
docker compose up -d postgres pipeline-db
echo "      Waiting for pipeline-db to be healthy..."
until docker compose exec pipeline-db pg_isready -U "$DB_USER" > /dev/null 2>&1; do
    sleep 2
done
echo "      pipeline-db is ready."

# --- Step 2: restore backup if exists --------------------------------------
echo ""
echo "[2/4] Checking for local backup..."
if [ -f "$BACKUP_FILE" ]; then
    echo "      Restoring backup from $BACKUP_FILE ..."
    docker compose exec -T pipeline-db psql -U "$DB_USER" -d "$DB_NAME" < "$BACKUP_FILE"
    echo "      Backup restored."
else
    echo "      No backup found — starting with empty database."
fi

# --- Step 3: run airflow-init ----------------------------------------------
echo ""
echo "[3/4] Running airflow-init..."
docker compose --profile init up airflow-init
echo "      airflow-init completed."

# --- Step 4: save backup ---------------------------------------------------
echo ""
echo "[4/4] Saving database backup to $BACKUP_FILE ..."
mkdir -p ./data
docker compose exec pipeline-db pg_dump -U "$DB_USER" "$DB_NAME" > "$BACKUP_FILE"
echo "      Backup saved."

echo ""
echo "=============================================="
echo "  Init complete. Run ./start.sh to start."
echo "=============================================="
echo ""
