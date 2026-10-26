#!/usr/bin/env bash
# Release admission is implemented by the separately recorded trusted framework.
set -euo pipefail
[ "$#" -eq 4 ] || {
  echo "usage: validate-release-source.sh <source-ref> <source-sha> <tag> <unit>" >&2
  exit 2
}
[ -f trusted/platform/release/producer-inputs.py ] || {
  echo "release-source: missing trusted platform producer admission" >&2
  exit 1
}
exec python3 -B trusted/platform/release/producer-inputs.py admit \
  "$4" "$1" "$2" "$3" "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}" producer-source.tar
