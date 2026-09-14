[English](README.md) | [简体中文](README_zh.md)

# Custom telemetry

Build the trusted statically linked App:

```sh
GOWORK=off CGO_ENABLED=0 go build -o /opt/kuasar/bin/custom-telemetry ./examples/custom-telemetry
```

Protect the executable against group/world writes. Root dispatch requires root
ownership; non-root dispatch accepts root or the service's UID. Set its absolute
path in `paths.telemetry_executable` of the independent `telemetry.yaml`, then run
`node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml`. Direct execution
is rejected. Node-ctl validates the file, creates sealed bootstrap, and replaces
itself in place; there is no fallback after a custom startup failure.

The same `proxy_netns` configuration reaches `Configure` as `Config.ProxyNetNS`.
It selects the namespace for both sandbox OTLP listeners; it does not move this
executable or its remote clients. Rename the early Preview `sandbox_netns` key:
strict bootstrap/config decoding rejects that old name. See the
[management network deployment](../../docs/telemetry.md#4-direct-sandbox-facing-otlp).

`Configure` runs once before stores/listeners. Config contains declarations;
Runtime contains nonserializable process-local bindings. The example retains
the selected primary storage, logs extension start/stop, and optionally provides
extra-exporter Authorization headers from `TELEMETRY_EXPORTER_AUTHORIZATION`.
When supplied, that provider replaces the YAML header map; failure never falls
back to YAML credentials. Do not log/upload secrets or diagnostic environments.
Production builds can replace it with a secret-file or credential service.

`collector.go` separately demonstrates the narrow `app/telemetry/otel` API: a
real Collector processor adds a deployment attribute and preserves the ingress
context. Core surrounds custom processors with identity enrichment and a final
guard. Do not detach the context, merge different sandbox identities into one
resource, replace trusted attributes, or insert receivers ahead of the guard.
Core rejects missing/stale identity and overwrites sandbox ID/StableID before
primary storage and every exporter; RunID is excluded.

Custom primary storage binds `Runtime.Storage` with `storage.type: custom` and
implements `extension.Storage` (canonical Collector writes, exact-SandboxID
Bounds/Query, Shutdown). It is not an extra exporter. `Runtime.StorageHeaders`
provides material for built-in Prometheus/ClickHouse primary backends. Extra
Collector exporters bind through the separate `otel.Components.Exporters` list
and do not make history queryable. The ordinary `extension` package has no OTel
types and no dynamic registry/DI container.

See the complete [telemetry specification](../../docs/telemetry.md) and
[extension contract](../../docs/extensions.md#telemetry-bootstrap-and-extension).
