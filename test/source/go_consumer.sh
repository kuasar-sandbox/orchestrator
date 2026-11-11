#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT
cd "$consumer"
export GOWORK=off

# Only the unpublished candidate comes from the checkout. Go must resolve its
# transitive requirements exactly as it would for an external application.
go mod init example.com/consumer
go mod edit -require=github.com/kuasar-sandbox/orchestrator@v0.0.0 \
    "-replace=github.com/kuasar-sandbox/orchestrator=$root"
printf '%s\n' 'package consumer' 'import _ "github.com/kuasar-sandbox/orchestrator/app/telemetry"' > consumer.go
go build -mod=mod .
