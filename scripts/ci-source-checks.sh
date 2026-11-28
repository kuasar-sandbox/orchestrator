#!/usr/bin/env bash
# Required source/race/vet and privileged Collector regressions, separate from E2E.
# kuasar-source-stages: orchestrator-source-v2
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
# Older platforms invoke this without arguments and retain the complete gate.
source_gate_program="$(python3 -B test/source/source_gate.py "$@")"
eval "$source_gate_program"
