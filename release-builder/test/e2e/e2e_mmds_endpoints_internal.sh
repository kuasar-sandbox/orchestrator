#!/usr/bin/env bash
#
# MMDS endpoints E2E suite: standalone node, internal proxy mode.
#
# Current scenario -- declared static route, real guest:
#   1. Create a sandbox with X-Kuasar-Sandbox-MMDS declaring /e2e/static.
#   2. Boot a real microVM and obtain an MMDSv2 token from inside the guest.
#   3. GET the declared route through
#        169.254.169.254 -> vswitch mgmt-service -> internal MMDS server.
#   4. Assert the body/content type, security headers, undeclared-route 404,
#      and rejection of non-canonical paths without redirects.
#
# Keep additional standalone/internal MMDS scenarios in this suite. Extend the
# shared helper or add an opt-in scenario hook to e2e_execute.sh; create another
# top-level entry only when the required topology is different.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_STATIC_E2E=1

# Scenario: static route.
export REQ_MMDS_HEADER='{"version":1,"routes":[{"path":"/e2e/static","type":"static","content_type":"text/plain","data":"MMDS_STATIC_GUEST_E2E"}]}'
exec bash "$SCRIPT_DIR/e2e_execute.sh" "$@"
