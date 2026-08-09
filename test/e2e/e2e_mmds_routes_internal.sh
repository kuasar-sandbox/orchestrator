#!/usr/bin/env bash
# Final MMDS route contract on a real guest through the internal proxy.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_ROUTES_E2E=1
export MMDS_SECRET_INITIAL_VALUE=MMDS_SECRET_INITIAL_GUEST_E2E
export REQ_MMDS_HEADER='{"secrets":{"e2e_secret":"MMDS_SECRET_INITIAL_GUEST_E2E"},"routes":[{"path":"/e2e/static","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret":"e2e_secret","content_type":"application/x-kuasar-e2e-secret"},{"path":"/e2e/unresolved","type":"secret","secret":"e2e_unresolved"},{"path":"/e2e/service","type":"service","service":"e2e_service"}]}'
exec bash "$SCRIPT_DIR/e2e_execute.sh" "$@"
