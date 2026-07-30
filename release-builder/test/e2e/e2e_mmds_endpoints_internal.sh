#!/usr/bin/env bash
#
# MMDS endpoints E2E suite: standalone node, internal proxy mode.
#
# Current scenarios -- declared static, secret, and service routes, real guest:
#   1. Create a sandbox with X-Kuasar-Sandbox-MMDS declaring /e2e/static.
#   2. Boot a real microVM and obtain an MMDSv2 token from inside the guest.
#   3. GET the declared route through
#        169.254.169.254 -> vswitch mgmt-service -> internal MMDS server.
#   4. Assert the static exact-path contract.
#   5. Install, overwrite, and revoke a declared secret through the admin UDS;
#      assert every state from the guest and reject an undeclared secret name.
#   6. GET a declared service route backed by a real operator-registered UDS;
#      assert identity headers, response passthrough, timeout/size limits,
#      backend restart recovery, and continued access after pause/resume.
#
# Keep additional standalone/internal MMDS scenarios in this suite. Extend the
# shared helper or add an opt-in scenario hook to e2e_execute.sh; create another
# top-level entry only when the required topology is different.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_STATIC_E2E=1
export MMDS_SECRET_E2E=1
export MMDS_SERVICE_E2E=1

# Scenario declarations: static route, secret backend, and service backend.
export REQ_MMDS_HEADER='{"version":1,"secrets":[{"name":"e2e_secret"}],"services":[{"name":"e2e_service","target":"e2e_backend"}],"routes":[{"path":"/e2e/static","type":"static","content_type":"text/plain","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret_name":"e2e_secret"},{"path":"/e2e/service","type":"service","service_name":"e2e_service"}]}'
exec bash "$SCRIPT_DIR/e2e_execute.sh" "$@"
