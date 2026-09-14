[English](telemetry.md) | [简体中文](telemetry_zh.md)

# Telemetry — sandbox metrics and history

## 1. Component and authority

Telemetry is a separate node process, alongside Conductor and Proxy:

```sh
node-ctl conductor serve --config /etc/node-ctl/conductor.yaml
node-ctl proxy serve --config /etc/node-ctl/proxy.yaml
node-ctl telemetry serve --config /etc/node-ctl/telemetry.yaml
```

Run each command in its own service. Conductor owns lifecycle, API authentication,
ownership and the Plugin registry. Proxy owns sandbox application data forwarding,
MMDS and instantaneous traffic observations. Telemetry owns its RouteEntry view,
envd scrape, sandbox-facing OTLP metrics ingress, trusted identity enrichment,
Collector pipeline, primary storage, exporters, history queries and E2B conversion.
It does not expand the resource controller or change checkpoint/lifecycle policy.

```text
Conductor Plugin Plane ── full RouteEntry stream ──► Telemetry view
                                                       │
envd /metrics over per-sandbox UDS ─► envd receiver ─────┤
guest ─► connector mgmt-extract ─► OTLP HTTP/gRPC ───────┤
                                                       ▼
                             Collector: trusted identity enrichment
                                      → custom processors (optional)
                                      → final identity guard
                                      ├─ primary storage exporter → DB
                                      └─ extra exporters → external collectors

GET /sandboxes/{SandboxID}/metrics
  → Conductor API auth + ownership → live registered query UDS
  → Telemetry E2B handler → primary Reader → same DB
```

Every primary write passes through a real OpenTelemetry Collector service graph
and exporter. Receivers have no storage handle. The built-in distribution links
only required Collector components, including OTLP/HTTP extra exporting; it does
not embed a complete contrib distribution or Prometheus server.

## 2. Plugin lease and query channel

The fixed Plugin ID is `telemetry`. Registration uses the existing authenticated
local `PUT /internal/plugin/telemetry/register` and routesync version 8:

```json
{
  "subscribe": {"kind": "route"},
  "telemetry": {"api": {"path": "/run/sandbox/telemetry.sock"}}
}
```

This is the registration capability body, not a separate target protocol.
Telemetry receives the complete existing `RouteEntry`. It keeps only SandboxID,
StableID, Profile, State, EnvdUDS, EnvdAccessToken and FloatingIP in its view; there
is no telemetry projection/permission system, lifecycle socket or Create/Resume
barrier. The long-lived Plugin connection owns registration, lease and route
stream. The registered API UDS is an independent HTTP channel, never routesync
frame multiplexing. Forwarding-only registration omits `telemetry.api`.

Same-ID replacement synchronously revokes the previous lease before publishing
the successor. Disconnect also revokes it. Late removal of an old registration
cannot delete its successor. Each forwarded query binds one lease and one local
UDS transport; it never pools across registrations. Replacement/disconnect cancel
in-flight work and new requests cannot use the old endpoint. Once response bytes
have streamed, cancellation terminates the stream rather than inventing a new
HTTP status. Query sockets are canonical absolute local paths, mode 0600, with
exclusive sidecar locking; live sockets and unrelated regular files are not
overwritten. The service account and protected Plugin socket remain the trust
boundary, just as for Proxy registration.

Conductor authenticates with the normal API key mechanism, performs exact
SandboxID lookup and ownership checking, then forwards method/path/raw query,
body, status and relevant headers unchanged. It filters hop-by-hop headers and
does not support protocol upgrades on this channel. It interprets no metric
names, DTOs, ranges, steps or storage results. Missing/non-owned sandbox is 404;
invalid authentication follows the existing API behavior. No live readable
telemetry endpoint, a failed UDS request or unavailable primary reader gives 503.
Client cancellation reaches the HTTP UDS request and backend query. Queries never
call Wake, Resume, import, reservation or guest control APIs; paused history is
read without touching the sandbox.

## 3. envd and metric identity

After full sync reaches Bookmark, only `profile=e2b`, `state=running`, nonempty
EnvdUDS targets are scraped. Trusted StableID must also be present. The receiver
does `GET /metrics` with `X-Access-Token: <EnvdAccessToken>`, default interval 5s
and timeout 1s. It uses guest observations, not host quota, `memory.current`,
balloon grants or image-diff size. The source contract follows
[upstream envd metrics](https://github.com/e2b-dev/infra/blob/main/packages/envd/internal/host/metrics.go).

| envd JSON | OTel gauge | Unit | E2B field |
|---|---|---|---|
| `cpu_count` | `sandbox.cpu.count` | `{cpu}` | `cpuCount` |
| `cpu_used_pct` | `sandbox.cpu.used` | `%` | `cpuUsedPct` |
| `mem_total` | `sandbox.memory.total` | `By` | `memTotal` |
| `mem_used` | `sandbox.memory.used` | `By` | `memUsed` |
| `mem_cache` | `sandbox.memory.cache` | `By` | `memCache` |
| `disk_total` | `sandbox.disk.total` | `By` | `diskTotal` |
| `disk_used` | `sandbox.disk.used` | `By` | `diskUsed` |

`ts` supplies the guest Unix-second timestamp. Missing, null, negative, malformed
or nonfinite fields reject that scrape; deprecated MiB fields are not substitutes.
CPU percentage keeps envd's original aggregate percentage, without host quota
normalization. There are no `kuasar.sandbox.*` built-in metric names.

Pause removes the scrape target; resume creates a fresh local generation; delete
removes it. Endpoint/token changes cancel the old request, and stale responses
are discarded even if a transport ignores cancellation. Disconnect invalidates
the entire view until a new full sync and Bookmark. A heap scheduler, bounded
worker/queue count and global idle pool avoid per-target timer goroutines.
Pool keys include UDS, local generation and SandboxID, so one sandbox's UDS
connection cannot be reused for another. There are at most `concurrency` active
scrapes and `concurrency` idle scrape connections.

## 4. Direct sandbox-facing OTLP

Telemetry itself binds both listeners in `proxy_netns`, using the same network
namespace primitives as Proxy/MMDS. Empty means the process's current namespace.
`proxy_netns` is the canonical YAML/JSON key and `ProxyNetNS` the Go field.
Configurations from the early Preview that used `sandbox_netns` must rename
that key; strict decoding rejects it, including when both keys are present.
Telemetry was not part of the existing Stable configuration contract.
An unknown namespace name/path, failed setns or failed bind aborts startup;
already-created listeners and application resources are closed. There is no
fallback to the host namespace. Only listener creation runs inside the selected
namespace; the calling thread returns to its original namespace. This setting
does not move the process or change conductor/query/envd UDS, remote exporters
or query clients. Those retain their normal process network environment.
The supported signal is metrics: OTLP/gRPC MetricsService on 4317 and OTLP/HTTP
`POST /v1/metrics` on 4318 (protobuf or JSON, optionally gzip). No OTLP token
identity scheme is added; application traces/logs are not accepted by this metrics
component. Do not route these listeners through the application Proxy or another
L7 reverse proxy.

Use the existing connector management path. For example, with Proxy/MMDS and
telemetry in the management namespace and loopback listeners, retain the existing
MMDS mapping and add these service mappings to the deployment's vswitch command:

```text
--mgmt-extract=sandbox-proxy:sw0m0:169.254.169.254/32
--mgmt-service=169.254.169.254:4317:127.0.0.1:4317
--mgmt-service=169.254.169.254:4318:127.0.0.1:4318
```

Set `proxy_netns: sandbox-proxy`, `grpc_listen: 127.0.0.1:4317` and
`http_listen: 127.0.0.1:4318`. Guest exporters use
`http://169.254.169.254:4318` (or the gRPC port). Connector's existing slot-derived
SNAT converts the shared guest inner IP to its assigned FloatingIP; service DNAT
selects telemetry's listener and the management return path restores the guest
tuple. This is packet-level delivery to telemetry, not a forwarded identity
header. Existing loopback management deployments need their management interface
`route_localnet=1`; a routable namespace interface listener instead uses its
address in the mappings and the existing FloatingIP return route. See the
[connector management network](https://github.com/kuasar-sandbox/connector/blob/main/docs/vswitch.md#23-packet-paths).
Use the actual management namespace/device names of the node, not a second switch.
The [deployment template](../deploy/telemetry.example.yaml) includes the matching
management-service mappings. Both protocols are exercised through that existing
path by the component's real guest E2E. The privileged Collector regression also
reserves the same ports on the host, sends HTTP and gRPC inside an isolated
namespace, verifies source identity and checks export to a host-only destination.
Unprivileged skips are not evidence for this network behavior.

The route view indexes SandboxID and FloatingIP and reuses MMDS's IPv4 parsing,
active-state and reverse-lookup consistency rules. Identity comes from the
accepted transport peer for both HTTP and gRPC; `Forwarded`, `X-Forwarded-For`
and guest resource attributes confer no authority. Unknown/paused/deleted peers
fail closed at TCP accept (the connection is closed before HTTP/gRPC decoding).
Clients that connect before full sync can reconnect after the view is ready;
an anonymous keep-alive connection never becomes trusted. Identity is pinned to the accepted
connection; remap, pause or stream invalidation closes that connection instead
of turning it into the successor sandbox's connection.

Core overwrites `sandbox.id` and `sandbox.stable_id` from the current route at
resource level and removes conflicting point/scope identity. The trusted
`sandbox.telemetry.source=envd|otlp` also cannot be guest-forged: guest OTLP gauges
named like built-in resource metrics cannot poison E2B envd history. Custom
processors are bracketed by enrichment and a final revalidation/overwrite guard.
Losing the private ingress context fails closed. RunID-like attributes are removed
at resource, scope and datapoint levels, including custom processor output.

Limits apply before transport decoding where possible: 256 connections per
listener, 32 concurrent requests globally, 4 MiB per encoded/decompressed HTTP
request and gRPC message by default. Resource/point attributes are limited to 32,
keys 128 bytes and values 256 bytes; batches allow 128 resources, 16,384 points
and 4,096 metrics per scope. Excess is rejected, not silently truncated.
The gRPC server's 10s deadline starts before reading the message body, so a
stalled client cannot indefinitely retain a global request slot.

## 5. Primary storage versus extra exporters

`telemetry.storage` selects exactly one primary with both Collector write and
`extension.Reader` history read. `telemetry.exporters` and custom Collector
exporters are additional write-only fan-out. An exporter never implicitly becomes
a primary reader. `storage.type: none` requires an exporter; telemetry still
collects/enriches/forwards but registers no query API, so public metrics is 503.

### Local (default)

The core `sandboxstorage` Collector exporter targets embedded Prometheus TSDB;
the `Local` backend provides both its writer and the reader of that same DB. No Prometheus web
server, scrape manager, rule manager or Alertmanager runs. Configuration:

```yaml
telemetry:
  storage:
    type: local
    path: /var/lib/sandbox/telemetry
    retention: 168h
    max_size: 10GiB
    max_series: 1000000
```

The DB owns its WAL, replay, compaction and retention. Writes transactionally
append a canonical batch, rollback on failure, and store millisecond timestamps
(sub-millisecond precision is truncated). Exact duplicate samples are idempotent;
conflicting values at the same timestamp fail rather than overwrite history.
Out-of-order data is accepted within min(5m, retention); older data fails. Data
older than wall-clock retention is omitted, and timestamps over one minute in
the future are rejected to protect the head. Paused/deleted sandbox history
continues aging even when no newer sample arrives. Background head expiration
and block expiration reclaim idle data; queries enforce retention immediately.

`max_size` is the Prometheus TSDB retention-size target, not a filesystem quota:
head/WAL and temporary compaction data can exceed it.
Provision disk headroom and monitor the filesystem. `max_series` bounds live head
series, including labels; startup rejects a replayed head above the configured
limit. Scrape/ingress/request limits bound per-operation growth. Storage health
errors, corruption/WAL recovery warnings and failed startup surface as component
failure rather than a silently usable partial reader. Preserve a failed DB for
offline diagnosis/backup recovery; do not automatically delete it. Process restart
replays WAL; this is not an extra power-loss durability guarantee.

Canonical mapping preserves UTF-8 OTel metric names. Trusted identity has direct
labels; other attributes use collision-free `resource.`, `scope.`, `point.` label
namespaces plus scope name/version, unit and metric kind. Sums retain explicit
temporality/monotonicity, not a silent delta-to-cumulative conversion. Histograms
and exponential histograms become count/sum/cumulative-bucket scalar series;
summaries become count/sum/quantile series. Expansion is bounded (160 histogram
bounds/quantiles and 65,536 scalar samples per write). Exemplars and histogram
min/max are not indexed by the scalar primary representation; extra OTLP
exporters retain the Collector pdata representation. Float64 storage has the
usual precision limit for integers above 2^53. E2B reads only trusted envd gauges.

### Prometheus-compatible readable primary

```yaml
telemetry:
  storage:
    type: prometheus
    retention: 168h
    prometheus:
      endpoint: https://metrics.example.com/prometheus
      headers: {Authorization: "Bearer REPLACE_WITH_PROTECTED_MATERIAL"}
```

The adapter writes standard Snappy/protobuf remote-write v1 to
`<endpoint>/api/v1/write` and negotiates `STREAMED_XOR_CHUNKS` from
`<endpoint>/api/v1/read`, then applies the same field-wise MAX behavior. Each
history-boundary lookup and data query uses one request, including empty or sparse
long-retention histories. Frames are checksum-verified and limited to 32 MiB;
only one frame is retained at a time, with a 10s HTTP deadline covering the body.
Complete edge chunks are filtered to the exact inclusive sample range. A backend
that only supports `SAMPLES` can fall back to a single Snappy response, with both
compressed and decoded sizes capped at 32 MiB; use streaming for larger histories. The
backend must enable both APIs and accept Prometheus 3 UTF-8 metric/label names;
a query-only server or write-only exporter is not sufficient. Remote read
preserves exact observations instead of PromQL lookback/step interpolation. See
the [remote read API](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
Configured retention defines the read lookback/write age policy; provision
backend retention, capacity and cardinality controls independently.

### ClickHouse readable primary

```yaml
telemetry:
  storage:
    type: clickhouse
    retention: 168h
    clickhouse:
      endpoint: https://clickhouse.example.com:8443
      database: default
      table: sandbox_metrics
      headers: {X-ClickHouse-User: telemetry, X-ClickHouse-Key: REPLACE_WITH_PROTECTED_MATERIAL}
```

The database must exist; telemetry creates its dedicated MergeTree table and
sets its TTL from retention. Give the service scoped CREATE/ALTER/INSERT/SELECT
permissions for that table, not unrelated tables. Changing retention updates its
TTL. JSONEachRow writes keep canonical labels and DateTime64(3, UTC) timestamps.
Small concurrent writes use bounded server-side async insert batching and wait
for flush completion, propagating errors/backpressure. Parameterized SQL filters
exact SandboxID and trusted envd source; integer epoch time buckets group each
metric independently with MAX. Duplicate/out-of-order rows preserve these MAX
results. Reads impose time, row, byte, thread and memory limits and cancel readonly
queries on HTTP client disconnect. HTTP-200 error bodies are not accepted as data.
See [ClickHouse HTTP](https://clickhouse.com/docs/interfaces/http) and
[async inserts](https://clickhouse.com/docs/optimize/asynchronous-inserts).

Both external clients bound pools and responses, propagate request cancellation,
reject redirects, and allow only operator-configured HTTP(S) destinations.
Credentials may instead come from Runtime providers. External storage owns its
disk-size/cardinality enforcement; local-only `max_size`/`max_series` do not
configure an external server. No external adapter is intentionally deferred.

### Extra exporters and custom primary

```yaml
telemetry:
  exporters:
    - name: observability
      type: otlphttp
      endpoint: https://collector.example.com
```

Built-in OTLP/HTTP exporters have bounded 16 MiB in-memory queues, two consumers,
10s request timeout and at most 30s retry elapsed time. These queues are not
durable history and do not make a failed primary query succeed. Transient
scrape/export failure drops or retries according to that component; telemetry
never pauses a sandbox to obtain delivery. Custom primary and Collector
integration use the [extension contract](extensions.md#telemetry-bootstrap-and-extension).

## 6. E2B history query

`GET /sandboxes/{SandboxID}/metrics?start=<unix-seconds>&end=<unix-seconds>`
uses the existing SandboxID exclusively. Missing lookup never falls back to
StableID, including cluster mode. Cluster Router's existing public-SandboxID to
current node SandboxID routing is unchanged; telemetry on a node always reads
that exact node SandboxID and does not join migration histories by StableID.
StableID is only external observability correlation. RunID remains runner/run-plane,
systemd/pidfile, MMDSv2 and logging/debug identity; it is never a telemetry label,
resource attribute, TSDB identity, query key or pause/resume series discriminator.

The telemetry handler parses nonnegative integral Unix-second boundaries
(through 2299-12-31 UTC), resolves omitted start/end from first/last retained
history, validates start <= end, calculates step, calls Reader, then takes MAX
independently for each of the seven fields in epoch-aligned buckets:

| Range | Step |
|---|---|
| `< 1h` | 5s |
| `< 6h` | 30s |
| `< 12h` | 1m |
| `< 24h` | 2m |
| `< 7d` | 5m |
| otherwise | 15m |

Bounds are inclusive. A bucket timestamp may precede an unaligned start while
its contributing samples still satisfy the requested bounds. Neither averaging
nor last-sample selection is used. Only complete resource buckets produce an
E2B object; missing fields are not fabricated as zero. Legal empty history/ranges
return `[]`. Invalid/repeated boundaries or reversed ranges return 400. As in
[E2B's range resolution](https://github.com/e2b-dev/infra/blob/87968fc1e1fa57d896378249ae14d09916382d75/packages/api/internal/clusters/resources_local.go),
validation follows omitted-boundary resolution: start-only after the last retained
sample, or end-only before the first, returns 400 when history exists. Supplying
both boundaries for a valid nonoverlapping range returns `[]`. Reader
failure returns sanitized 503. There is no interpolation, zero filling or Wake.
Queries have a 15s context budget, eight concurrent readers and 100,000 output
buckets maximum; exceeding query capacity returns 503 with Retry-After.

The nine required fields match the
[E2B API schema](https://github.com/e2b-dev/infra/blob/main/spec/openapi.yml):

```json
[{"timestamp":"2026-09-13T00:00:00Z","timestampUnix":1789257600,
  "cpuCount":2,"cpuUsedPct":12.5,"memTotal":1073741824,
  "memUsed":536870912,"memCache":134217728,
  "diskTotal":4294967296,"diskUsed":1073741824}]
```

The platform's instantaneous `/stats/resource` and `/stats/traffic` remain
separate read-only interfaces, not substitutes for this guest-metric history.

Native resource, traffic and usage reads are owned by conductor and remain available when telemetry stops. The trusted telemetry lease can consume selected sections through the existing config socket; it does not access sandbox ctl sockets or usage files. Paused saved usage does not require an active guest. See [Native usage and local reads](node-usage.md).

Native traffic uses the flat `state`, `maxInflight`, `inflight:{parking,connected}`, `idleSince`, `services`, `platform`, `transit` and `egress` shape. Platform and transit retain connector Mgmt/Transit counters from the sandbox viewpoint; `egress:{}` means no publishable statistics. They do not alter Proxy ingress idle or turn management monitoring packets into admitted application flows. The conductor batches current bindings per switch through connector's native Go API. See [native traffic](node-proxy.md#83-traffic-stats-and-the-unified-worker-stream).

## 7. Configuration, shutdown and verification

Use `node-ctl config telemetry --template`, or
`node-ctl config telemetry --config FILE [--resolve]`, to inspect declarations.
`--resolve` has no extra telemetry-specific effect. YAML is one strictly decoded
document, capped at 4 MiB; unknown/duplicate fields fail. Defaults are applied
once, not after custom hooks. `paths.telemetry_executable` selects a trusted
static App through the existing sealed bootstrap, not Go runtime plugins.
See [deploy YAML](../deploy/telemetry.example.yaml),
[service unit](../deploy/node-telemetry.service) and
[custom telemetry](../examples/custom-telemetry/README.md).

Shutdown first stops route subscription/revokes the query lease, drains query
and OTLP ingress, shuts down Collector receivers/processors/exporters, then the
extension and finally primary storage. Startup failures unwind the same owned
resources. Retained extension work is canceled even when startup fails. Keep the
storage path persistent and do not make conductor/Proxy systemd dependencies
require telemetry; telemetry failure must not change sandbox lifecycle.

Reproducible checks (from a source checkout with sibling dependencies):

```sh
GOWORK=off go test ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api ./app/telemetry ./config ./cmd/node-ctl
GOWORK=off go test -race ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api
GOWORK=off go test ./internal/telemetry -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x -benchmem
GOWORK=off go test ./internal/telemetry -run '^$' -bench 'Benchmark(Local|Scrape)' -benchtime=100x -benchmem
TELEMETRY_CLICKHOUSE_TEST_URL=http://127.0.0.1:8123 GOWORK=off go test ./internal/telemetry -run TestClickHouseIntegration -count=1
TELEMETRY_PROMETHEUS_TEST_URL=http://127.0.0.1:9090 GOWORK=off go test ./internal/telemetry -run TestPrometheusIntegration -count=1
make test vet build
make test-e2e # assembled project BIN and real KVM host required
```

Use disposable endpoints for optional live-engine tests. The ClickHouse test
creates/drops only a unique test table. The Prometheus test writes a uniquely
named sandbox series (removed by backend retention), requires remote write and
an out-of-order window of at least 5m, and verifies streaming, UTF-8 labels,
duplicate/out-of-order samples, field MAX and exact time bounds. Density
benchmarks measure real 5s periods for 1k/10k/50k
synthetic targets, scrape counts, peak goroutines/FDs (including fixture server),
allocations, TSDB batch writes and local query cost. Receiver and TSDB benchmarks
are separate diagnostics, not a claim of end-to-end production capacity. Timing
numbers are not ordinary CI gates; repeat on representative hosts/workloads.
The existing real Proxy E2E fixture also exercises envd → Collector → local DB,
guest OTLP through connector mgmt-extract, auth, paused queries, unavailable
telemetry, lease revocation and TSDB restart, without a second VM lifecycle.
