#!/bin/bash
# =============================================================================
# init.sh — Initialize the pipeline for the first time (or after a reset)
#
# What it does:
#   1. Copies the local pipeline.duckdb into the Docker volume
#   2. Runs airflow-init (db migrate, create admin user, seed projects)
#   3. Copies the updated pipeline.duckdb back to local data/
#
# Usage:
#   chmod +x init.sh
#   ./init.sh
# =============================================================================

set -e  # Exit immediately on error

VOLUME_NAME="data-collection-pipeline_pipeline-data"
LOCAL_DB="./data/pipeline.duckdb"
CONTAINER_DB="/opt/airflow/data/pipeline.duckdb"

echo ""
echo "=============================================="
echo "  Pipeline Init"
echo "=============================================="

# --- Step 1: ensure volume exists -----------------------------------------
echo ""
echo "[1/4] Creating Docker volume if not exists..."
docker volume create "$VOLUME_NAME" > /dev/null
echo "      Volume '$VOLUME_NAME' ready."

# --- Step 2: copy local DB into volume ------------------------------------
echo ""
echo "[2/4] Copying local database into Docker volume..."
if [ -f "$LOCAL_DB" ]; then
    docker run --rm \
        -v "$(pwd)/data:/source" \
        -v "$VOLUME_NAME:/dest" \
        alpine sh -c "cp /source/pipeline.duckdb /dest/pipeline.duckdb && chmod -R 777 /dest"
    echo "      Copied $LOCAL_DB → volume."
else
    echo "      No local database found — will create a new one."
fi

# --- Step 3: run airflow-init ---------------------------------------------
echo ""
echo "[3/4] Running airflow-init..."
docker compose --profile init up airflow-init
echo "      airflow-init completed."

# --- Step 4: copy DB back to local ----------------------------------------
echo ""
echo "[4/4] Copying database from volume back to local data/..."
docker run --rm \
    -v "$VOLUME_NAME:/source" \
    -v "$(pwd)/data:/dest" \
    alpine cp /source/pipeline.duckdb /dest/pipeline.duckdb
echo "      Saved to $LOCAL_DB."

echo ""
echo "=============================================="
echo "  Init complete. Run ./start.sh to start."
echo "=============================================="
echo ""
