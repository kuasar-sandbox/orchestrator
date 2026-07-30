#!/usr/bin/env bash
#
# MMDS endpoints E2E suite: standalone node, external proxy mode.
#
# Scenarios -- admission, declared static, secret, and service routes:
#   1. Reject malformed declarations before allocating a sandbox.
#   2. Create a sandbox with X-Kuasar-Sandbox-MMDS declaring /e2e/static.
#   3. Boot a real microVM and obtain an MMDSv2 token inside the guest.
#   4. GET the declared route through
#        169.254.169.254 -> vswitch mgmt-service -> external proxy worker
#        -> worker/master MMDS route lookup.
#   5. Assert the body/content type, security headers, undeclared-route 404,
#      and rejection of non-canonical paths without redirects.
#   6. Install and overwrite a declared secret through the admin UDS; restart
#      the conductor and an empty proxy, prove encrypted state + external route
#      snapshot preserve guest access, then update the recovered revision and revoke.
#   7. Restart the conductor with tenant routes disabled, reject a new create,
#      and prove the existing route remains available through the external proxy.
#   8. GET a declared service route through an external worker to a real UDS;
#      assert identity headers, response passthrough, timeout/size limits,
#      backend restart recovery, and continued access after pause/resume.
#
# Keep additional standalone/external MMDS scenarios in this suite. Extend the
# shared helper or add an opt-in scenario hook to e2e_orchestrator_proxy.sh;
# create another top-level entry only when the required topology is different.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_STATIC_E2E=1
export MMDS_INVALID_REJECT_E2E=1
export MMDS_SECRET_E2E=1
export MMDS_POLICY_REPLAY_E2E=1
export MMDS_SERVICE_E2E=1

# Scenario declarations: static route, secret backend, and service backend.
export REQ_MMDS_HEADER='{"version":1,"secrets":[{"name":"e2e_secret"}],"services":[{"name":"e2e_service","target":"e2e_backend"}],"routes":[{"path":"/e2e/static","type":"static","content_type":"text/plain","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret_name":"e2e_secret"},{"path":"/e2e/service","type":"service","service_name":"e2e_service"}]}'
exec bash "$SCRIPT_DIR/e2e_orchestrator_proxy.sh" "$@"
