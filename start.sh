#!/bin/bash
# =============================================================================
# start.sh — Start the Airflow pipeline
#
# What it does:
#   1. Copies the local pipeline.duckdb into the Docker volume
#   2. Starts all Airflow services (docker compose up -d)
#   3. Waits for Ctrl+C or SIGTERM
#   4. On shutdown: copies the updated pipeline.duckdb back to local data/
#   5. Stops all containers
#
# Usage:
#   chmod +x start.sh
#   ./start.sh
# =============================================================================

set -e

VOLUME_NAME="data-collection-pipeline_pipeline-data"
LOCAL_DB="./data/pipeline.duckdb"

# --- Shutdown handler -------------------------------------------------------
shutdown() {
    echo ""
    echo "=============================================="
    echo "  Shutting down..."
    echo "=============================================="

    echo ""
    echo "[1/2] Copying database from volume back to local data/..."
    docker run --rm \
        -v "$VOLUME_NAME:/source" \
        -v "$(pwd)/data:/dest" \
        alpine cp /source/pipeline.duckdb /dest/pipeline.duckdb
    echo "      Saved to $LOCAL_DB."

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

# Trap Ctrl+C (SIGINT) and SIGTERM
trap shutdown SIGINT SIGTERM

# --- Step 1: copy local DB into volume -------------------------------------
echo ""
echo "=============================================="
echo "  Pipeline Start"
echo "=============================================="

echo ""
echo "[1/2] Copying local database into Docker volume..."
if [ -f "$LOCAL_DB" ]; then
    docker run --rm \
        -v "$(pwd)/data:/source" \
        -v "$VOLUME_NAME:/dest" \
        alpine sh -c "cp /source/pipeline.duckdb /dest/pipeline.duckdb && chmod -R 777 /dest"
    echo "      Copied $LOCAL_DB → volume."
else
    echo "      WARNING: No local database found at $LOCAL_DB."
    echo "      Run ./init.sh first to initialize the database."
    exit 1
fi

# --- Step 2: start services ------------------------------------------------
echo ""
echo "[2/2] Starting Airflow services..."
docker compose up -d
echo "      Services started. Airflow UI: http://localhost:8080"

echo ""
echo "=============================================="
echo "  Pipeline running. Press Ctrl+C to stop."
echo "=============================================="
echo ""

# Wait indefinitely until signal
while true; do
    sleep 5
done