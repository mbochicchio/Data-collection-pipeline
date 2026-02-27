#!/bin/bash
# =============================================================================
# db.sh — Run database management scripts against the local pipeline-db
#
# Usage:
#   ./db.sh status
#   ./db.sh reset
#   ./db.sh reseed
#   ./db.sh full-reset
# =============================================================================

PIPELINE_DB_HOST=localhost PIPELINE_DB_PORT=5440 python scripts/reset_db.py "$@"