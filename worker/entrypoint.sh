#!/usr/bin/env sh
set -eu

CONFIG_PATH="/app/config/job_config.yaml"
mkdir -p "$(dirname "$CONFIG_PATH")"
printf '%s' "$JOB_CONFIG_YAML" > "$CONFIG_PATH"

exec python /app/worker.py
