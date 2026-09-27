#!/usr/bin/env bash
# Socket-bearing runtime paths must fit independently of the runner's case ID.
case_workspace_init() {
    : "${WORK:?WORK must be supplied by the public runner}"
    CASE_OUT="${OUT:-$WORK}"
    mkdir -p "$CASE_OUT"
    CASE_RUNTIME="$(mktemp -d /tmp/o-XXXXXX)"
    WORK="$CASE_RUNTIME"
}

case_workspace_cleanup() {
    [ -n "${CASE_RUNTIME:-}" ] || return 0
    mkdir -p "$CASE_OUT/runtime"
    find "$CASE_RUNTIME" -maxdepth 1 -type f \( -name '*.log' -o -name '*.out' \) \
        -exec cp {} "$CASE_OUT/runtime/" \;
    if [ -n "${E2E_KEEP:-}" ]; then
        echo "kept runtime directory: $CASE_RUNTIME"
    else
        rm -rf -- "$CASE_RUNTIME"
    fi
}
