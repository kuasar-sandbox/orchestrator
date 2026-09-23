#!/usr/bin/env bash
# Execute the prepared owner tests against the selected platform products.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${BIN:?BIN must point to the assembled platform binary directory}"
: "${ORCH_CLI_TEST_BIN:?ORCH_CLI_TEST_BIN must point to the prepared orch-cli.test}"
exec python3 -B "$SCRIPT_DIR/lib/capture_cli.py" "$BIN" "$ORCH_CLI_TEST_BIN"
