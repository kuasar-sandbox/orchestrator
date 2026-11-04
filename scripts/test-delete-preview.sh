#!/usr/bin/env bash
set -euo pipefail
exec python3 -B "$(dirname "${BASH_SOURCE[0]}")/test-delete-preview.py"
