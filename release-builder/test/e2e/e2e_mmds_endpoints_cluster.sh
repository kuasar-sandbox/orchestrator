#!/usr/bin/env bash
#
# MMDS endpoints E2E suite: clustered control plane, real node and real guest.
#
# A declared kuasar-sandbox.mmds rides through the cluster control plane as
# opaque config, unexamined by registry; the node's own mmds.routes policy
# (ExtractMMDS, same mechanism standalone uses) is the only validation,
# applied at dispatch -- cluster has no registry-owned policy of its own, so
# every node in the cluster must run the same mmds.routes configuration.
# Scenarios -- declared static route end to end:
#   1. Run both registry-n1 and registry-redirect cluster cases.
#   2. Create through router/placer with X-Kuasar-Sandbox-MMDS declaring
#      /e2e/static; propagate the declaration over node-link to the real node.
#   3. Boot a real microVM, obtain an MMDSv2 token inside the guest, and GET the
#      route through 169.254.169.254 -> vswitch mgmt-service -> node MMDS server.
#   4. Assert the shared static-route/exact-path contract, then retain the base
#      cluster checks for pause/resume, SID data routing, and delete convergence.
#
# Keep additional cluster MMDS scenarios in this suite. Extend the shared
# helper or add an opt-in scenario hook to e2e_cluster_real.sh; create another
# top-level entry only when the required cluster topology is different.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export MMDS_STATIC_E2E=1

# Scenario: static route.
export REQ_MMDS_HEADER='{"version":1,"routes":[{"path":"/e2e/static","type":"static","content_type":"text/plain","data":"MMDS_STATIC_GUEST_E2E"}]}'
exec bash "$SCRIPT_DIR/e2e_cluster_real.sh" "$@"
