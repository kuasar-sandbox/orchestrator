#!/usr/bin/env bash
# Run source regressions on the exact orchestrator module assembled by CI.
# Binary-only packages exercise native identities through the owner lifecycle cases.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s "$SCRIPT_DIR/../e2e/lib" -p test_journal_identity.py -v
# Go compiler selection and source regressions run in scripts/ci-source-checks.sh.
echo "==> journal source contract: OK"
