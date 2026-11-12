#!/usr/bin/env bash
set -euo pipefail

root="${1:-$(dirname "${BASH_SOURCE[0]}")/../..}"
root="$(cd "$root" && pwd)"
consumer="$(mktemp -d)"
work=
cleanup() {
    rm -rf -- "$consumer"
    if [[ -n "$work" ]]; then rm -f -- "$work" "$work.sum"; fi
}
trap cleanup EXIT
selection="${GOWORK:-off}"
# Source checks may run go through a driver restricted to the product workspace.
# The external application owns its module and, for Preview, a private workspace.
go="${KUASAR_CONSUMER_GO:-}"
if [[ -z "$go" ]]; then go="$(go -C "$root" env GOROOT)/bin/go"; fi
cd "$consumer"
export GOWORK=off
"$go" mod init example.com/consumer
printf '%s\n' 'package consumer' 'import _ "github.com/kuasar-sandbox/orchestrator/app/telemetry"' > consumer.go

if [[ "$selection" == off ]]; then
    # Only the candidate comes from its checkout. All dependencies resolve as
    # they do for an application using a published Stable orchestrator.
    "$go" mod edit -require=github.com/kuasar-sandbox/orchestrator@v0.0.0 "-replace=github.com/kuasar-sandbox/orchestrator=$root"
    "$go" build -mod=mod .
else
    # Keep relative use/replace paths anchored to the original workfile's
    # directory. Never edit the workspace shared by CI or other builds.
    work="$(mktemp --tmpdir="$(dirname "$selection")" consumer-XXXXXXXX.work)"
    cp -- "$selection" "$work"
    if [[ -f "$selection.sum" ]]; then cp -- "$selection.sum" "$work.sum"; fi
    export GOWORK="$work"
    "$go" work use "$consumer"
    [[ "$("$go" list -m -f '{{.Dir}}' github.com/kuasar-sandbox/orchestrator)" == "$root" ]]
    "$go" build .
fi
