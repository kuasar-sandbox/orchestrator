#!/usr/bin/env bash
#
# MMDS endpoints E2E suite: clustered control plane, real node and real guest.
#
# Current scenarios -- declared static, secret, and service routes:
#   1. Run both registry-n1 and registry-redirect cluster cases.
#   2. Create through router/placer with X-Kuasar-Sandbox-MMDS declaring
#      /e2e/static; propagate the declaration over node-link to the real node.
#   3. Boot a real microVM, obtain an MMDSv2 token inside the guest, and GET the
#      route through 169.254.169.254 -> vswitch mgmt-service -> node MMDS server.
#   4. Assert the static exact-path contract.
#   5. Install, overwrite, and revoke a declared secret through the node admin
#      UDS; assert node-link/route-sync propagation from the real guest.
#   6. GET a declared service route backed by the real node's registered UDS;
#      assert stable/local identities, response/error mapping, backend recovery,
#      and continued service access from the restored generation after pause/resume.
#   7. Retain pause/resume, SID data routing, and delete convergence checks.
#
# Keep additional cluster MMDS scenarios in this suite. Extend the shared
# helper or add an opt-in scenario hook to e2e_cluster_real.sh; create another
# top-level entry only when the required cluster topology is different.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_STATIC_E2E=1
export MMDS_SECRET_E2E=1
export MMDS_SERVICE_E2E=1

# Scenario declarations: static route, secret backend, and service backend.
export REQ_MMDS_HEADER='{"version":1,"secrets":[{"name":"e2e_secret"}],"services":[{"name":"e2e_service","target":"e2e_backend"}],"routes":[{"path":"/e2e/static","type":"static","content_type":"text/plain","data":"MMDS_STATIC_GUEST_E2E"},{"path":"/e2e/secret","type":"secret","secret_name":"e2e_secret"},{"path":"/e2e/service","type":"service","service_name":"e2e_service"}]}'
exec bash "$SCRIPT_DIR/e2e_cluster_real.sh" "$@"
