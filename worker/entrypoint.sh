#!/usr/bin/env sh
# ---------------------------------------------------------------------------
# entrypoint.sh – container entry point for the ML worker
#
# Environment variables consumed here:
#   JOB_CONFIG_PATH  – path to the YAML job config (default: /app/config/job_config.yaml)
#   LOG_LEVEL        – Python logging level (default: INFO)
# ---------------------------------------------------------------------------
set -eu

CONFIG_PATH="${JOB_CONFIG_PATH:-/app/config/job_config.yaml}"

echo "[entrypoint] $(date -u '+%Y-%m-%dT%H:%M:%SZ') Starting ML worker"
echo "[entrypoint] Config path : ${CONFIG_PATH}"
echo "[entrypoint] Log level   : ${LOG_LEVEL:-INFO}"
echo "[entrypoint] Python      : $(python --version)"

# Validate that the config file is reachable before handing off to Python
if [ ! -f "${CONFIG_PATH}" ]; then
    echo "[entrypoint] ERROR: job config not found at '${CONFIG_PATH}'" >&2
    exit 1
fi

exec python /app/worker.py

