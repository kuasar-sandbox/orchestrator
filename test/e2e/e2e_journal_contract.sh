#!/usr/bin/env bash
# Run source regressions on the exact orchestrator module assembled by CI.
# Binary-only packages exercise native identities through the owner lifecycle cases.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$SCRIPT_DIR/lib" -p test_journal_identity.py -v
SOURCE=""
if command -v go >/dev/null 2>&1; then
    SOURCE="$(GOPROXY=off GOSUMDB=off go list -m -f '{{.Dir}}' github.com/kuasar-sandbox/orchestrator 2>/dev/null || true)"
fi
if [[ -n "$SOURCE" && -f "$SOURCE/internal/orch/log_targets_test.go" ]]; then
    (
        cd "$SOURCE"
        echo "==> journal contract: full orchestrator source checks ($SOURCE)"
        go version
        go test -count=1 -timeout=5m ./...
        CGO_ENABLED=1 go test -race -count=1 -timeout=5m ./...
        go vet ./...
    )
else
    echo "Source-only Go checks unavailable; native identity checks remain in the owner lifecycle suite."
fi
echo "==> e2e_journal_contract: OK"
