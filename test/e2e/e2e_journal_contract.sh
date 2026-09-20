#!/usr/bin/env bash
# Run source regressions on the exact orchestrator module assembled by CI.
# Binary-only packages exercise native identities through the owner lifecycle cases.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$SCRIPT_DIR/lib" -p test_journal_identity.py -v
# A standalone binary-only journal check can omit Go. Required owner CI must
# execute the build regression; an explicit invalid distribution must also fail.
if [[ "${REQUIRE_PROXY:-0}" = 1 || -n "${GOROOT:-}" ]] || command -v go >/dev/null 2>&1; then
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$SCRIPT_DIR/lib" -p test_orchestrator_proxy_go.py -v
fi
# Source-wide checks are scheduled once by run_all.sh. This case only keeps
# the journal/proxy contracts that need the packaged E2E surface.
echo "Source-wide Go checks are owned by run_all.sh; native identity checks remain in the owner lifecycle suite."

echo "==> e2e_journal_contract: OK"
