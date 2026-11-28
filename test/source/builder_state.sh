#!/usr/bin/env bash
# Standalone entrypoint for the required Builder policy/state contracts.
# CI validates these contracts in its existing ordinary Go test run instead.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
exec python3 -B test/source/builder_state.py
