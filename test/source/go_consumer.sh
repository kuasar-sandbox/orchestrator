#!/usr/bin/env bash
set -euo pipefail

root="${1:-$(dirname "${BASH_SOURCE[0]}")/../..}"
root="$(cd "$root" && pwd -P)"
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
module=github.com/kuasar-sandbox/orchestrator
"$go" mod edit "-require=$module@v0.0.0" "-replace=$module=$root"
printf '%s\n' 'package consumer' 'import _ "github.com/kuasar-sandbox/orchestrator/app/telemetry"' > consumer.go

if [[ "$selection" == off ]]; then
    "$go" build -mod=mod .
    exit
fi

# Keep relative replace paths anchored to their original directory. Only the
# external app may remain a workspace main: Go ignores a dependency's go.mod
# replacements, and this regression must exercise exactly that behavior.
work="$(mktemp --tmpdir="$(dirname "$selection")" consumer-XXXXXXXX.work)"
cp -- "$selection" "$work"
if [[ -f "$selection.sum" ]]; then cp -- "$selection.sum" "$work.sum"; fi
uses="$("$go" work edit -json "$work" | jq -er '.Use[].DiskPath')"
selected_owner=
while IFS= read -r use; do
    source="$(cd "$(dirname "$work")" && cd -- "$use" && pwd -P)"
    path="$("$go" -C "$source" mod edit -json | jq -er '.Module.Path')"
    "$go" work edit "-dropuse=$use" "$work"
    if [[ "$path" == "$module" ]]; then
        selected_owner="$source"
    else
        # Ordinary source PRs select sibling module roots with "use". Preview
        # release workspaces already carry version-specific upstream mappings.
        "$go" work edit "-replace=$path=$source" "$work"
    fi
done <<< "$uses"
[[ "$selected_owner" == "$root" ]] || { echo 'workspace selects another orchestrator candidate' >&2; exit 1; }
export GOWORK="$work"
"$go" work use "$consumer"
"$go" build .
