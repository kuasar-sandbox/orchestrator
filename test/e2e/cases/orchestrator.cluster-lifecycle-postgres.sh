#!/usr/bin/env bash
# e2e-requires: ["bpf-map","bpf-tc","cgroup-v2","docker","journald","kvm","netns","systemd","tun","userfaultfd"]
set -euo pipefail
export CLUSTER_STORAGE_BACKEND=postgres CLUSTER_USE_PLACER=0
. "${E2E_LIB:?}/orchestrator/cluster_direct_lifecycle.sh"
