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
It selects both sandbox OTLP listeners' namespace without moving the executable
or remote clients. The early Preview `sandbox_netns` key is rejected by strict
bootstrap/config decoding. See the [management deployment](../../docs/telemetry.md#4-direct-sandbox-facing-otlp).

`Configure` runs once before stores/listeners. Config contains declarations;
Runtime contains process-local bindings. The example logs lifecycle start/stop and
registers the `privatedeployment` processor factory. Native config providers supply
exporter credentials through component options, for example `${env:OTLP_TOKEN}`;
provider failure fails startup instead of selecting alternative credentials.
[telemetry.yaml](telemetry.yaml) is a complete write-only example using all three
native receivers, the custom processor, batch and an OTLP HTTP exporter. Set
`OTLP_ENDPOINT` to the destination. With no selected reader, `/metrics` remains
unavailable; native conductor stats continue to work.

Add `privatedeployment: {}` to `collector.processors` and include
`privatedeployment` in the desired pipeline's `processors` list. Merely registering
a factory does not insert it into every pipeline. `collector.go` demonstrates a
normal mutating Collector processor. Source identity is already accepted into
each pdata resource, so standard batch, queue and retry can lose the original
request context and combine multiple resources. Do not merge different sandbox
identities into a single resource. Pause/delete does not discard accepted history,
and user `application.run_id` is retained. Native static factories may add
receivers, processors, exporters, connectors, Collector extensions, config
providers and converters; instances and pipeline edges stay declarative.

[query.yaml](query.yaml) is a separate query-only configuration: it selects the
Prometheus Reader and the real HTTP handler in [query.go](query.go), without
Collector or local storage. Set the query endpoint and protected read credentials.
`/sandboxes/{SandboxID}/metrics?metric=task_temperature` returns raw generic
`Series` JSON with `X-Metrics-Contract: kuasar-example-series-v1`. Repeated `metric`
parameters select up to 64 exact backend names. This response is its own contract;
E2B SDKs must use `query.handler: e2b`. Conductor keeps opaque forwarding for both.
The handler's Reader is bound to the authorized exact SandboxID and cannot query
another sandbox through client parameters or a replacement context.

A custom Reader binds `Runtime.QueryBackend` with `query.backend: custom` and
implements `extension.QueryBackend`: Selection-based Bounds, generic Series Query
and Shutdown. No Write method or Collector graph is required. `Runtime.QueryHeaders`
provides read credentials for built-in Prometheus/ClickHouse backends; native
Collector config providers independently supply exporter credentials. A provider
error does not fall back to YAML or local DB. `local.enabled` explicitly creates
the optional TSDB, and `sandboxlocal` exports to it through Collector. The
`extension` package has no OTel types, dynamic registry or DI container.

See the complete [telemetry specification](../../docs/telemetry.md) and
[extension contract](../../docs/extensions.md#telemetry-bootstrap-and-extension).
