#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# s3.helical.dev must be reachable — RustFS needs to be running before bucket provisioning
RUSTFS_ENDPOINT="http://s3.helical.dev"

cd "${REPO_ROOT}/terraform"

tofu init
# .tfvars holds the RustFS endpoint and credentials (non-secret, local dev only)
tofu apply -auto-approve --var-file=.tfvars