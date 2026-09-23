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
Collector pipelines, optional local TSDB, exporters, query backends and HTTP adapters.
It does not expand the resource controller or change checkpoint/lifecycle policy.

```text
Conductor Plugin Plane -> full RouteEntry -> Telemetry view
Conductor config_socket -> native stats -> sandboxstats receiver
Envd UDS -> envd receiver
Guest -> connector management -> sandboxotlp receiver
                |
      source acceptance + trusted pdata identity
                |
Collector native processors / connectors / pipelines
                |
       configured exporters -> selected destinations

GET /sandboxes/{SandboxID}/metrics
  -> Conductor auth + ownership -> registered HTTP query UDS
  -> Telemetry handler -> selected Reader
```

All collection writes pass through a native OpenTelemetry Collector service
and configured exporters. Receivers have no storage handle. Native Collector
configuration selects all pipeline edges and linked signals; the distribution
does not embed the whole contrib repository or a Prometheus server.

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
telemetry endpoint, a failed UDS request or unavailable query backend gives 503.
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
the entire view until a new full sync and Bookmark. Each `envd/name` instance has its own heap scheduler, bounded
worker/queue count and idle pool; these avoid per-target timer goroutines.
Pool keys include UDS, local generation and SandboxID, so one sandbox's UDS
connection cannot be reused for another. There are at most `concurrency` active
scrapes and `concurrency` idle scrape connections.

### Native Collector configuration

`collector` is an unmodified Collector configuration map, resolved with native
confmap providers and component validation. It supports `receivers`, `processors`,
`exporters`, `connectors`, `extensions`, `service.extensions`, `service.pipelines`
and `service.telemetry`. The resolved graph is the graph that runs. A factory
registers its type once; configuration defines instances such as `envd/fast` and
`envd/slow`, each with its own cadence and lifecycle. Unused or invalid component
options are not silently translated into a smaller routing language.

| Category | Included component types | Version |
|---|---|---|
| Receivers | `envd`, `sandboxstats`, `sandboxotlp` | This orchestrator source |
| Infrastructure receiver | `otlp` | Collector 0.145.0 |
| Processors | `batch`, `memory_limiter` | Collector 0.145.0 |
| Processors | `filter`, `transform` | contrib 0.145.0 |
| Exporters | `otlp`, `otlp_http`, `debug` | Collector 0.145.0 |
| Exporters | `prometheusremotewrite`, `clickhouse` | contrib 0.145.0 |
| Optional local exporter | `sandboxlocal` | This orchestrator source |
| Connector | `routing` | contrib 0.145.0 |
| Collector extension | `health_check` | contrib 0.145.0 |
| Config providers | `env`, `file`, `yaml` | confmap 1.51.0 |

Static builds can add every factory category, providers and converters through
`app/telemetry/otel.Components`. Native `${env:NAME}` and `${file:/path}` resolution
is supported inside Collector configuration. The selected Collector components
validate their own options. Standard infrastructure `otlp` and linked exporters
can carry logs/traces pipelines without SandboxID; only the sandbox business
receivers are metrics-specific. No logs/traces business query API is introduced.
`proxy_netns` applies only to each configured `sandboxotlp` instance.

The old `telemetry.scrape`, `telemetry.otlp` and list-style `telemetry.exporters`
are rejected. Move those settings into native receiver/exporter instances and
explicitly connect them in `service.pipelines`. Envd options are now
`collection_interval`, `timeout`, `concurrency`; sandboxotlp retains
`http_listen`, `grpc_listen`, `max_connections`, `max_requests`,
`max_request_bytes`, without an `enabled` switch. Include or omit the receiver
from a pipeline to select it. See the complete [local deployment](../deploy/telemetry.example.yaml)
and [two-sink configuration](../deploy/telemetry-fanout.example.yaml).

### Native sandboxstats receiver

`sandboxstats` discovers exact SandboxIDs from the same RouteEntry view and calls
conductor's existing `config_socket` batch adaptation. The current telemetry
lease and actual peer PID authorize that read; connecting to a UDS does not grant
ordinary tenant API permissions. It needs no tenant API-key collection and has
no sandbox ctl client, usage-file access or second port binding map. Conductor
organizes native sources and retains ownership/binding checks and failure rules.

The internal request is `POST /internal/plugin/telemetry/stats`, authorized by the ready, current, live registration with fixed Plugin ID `telemetry`, the actual `SO_PEERCRED` PID and the existing `plugin_pidfile` allowlist. Replacement or disconnection cancels in-flight reads; a write-only registration needs no query UDS to consume native stats. [Node local stats](node.md#native-stats-batches) owns the full request/response example, whole-batch failure, parameters, size/concurrency bounds and read/write budgets.

Configure `resource_interval` (default 5s), `traffic_interval` (10s),
`usage_interval` (1m), `timeout` (5s maximum), and `concurrency` (default 4, 1..8).
An interval of zero disables that section; enabled intervals are 1s..1h and at
least one must be enabled. Each instance issues at most 64 SandboxIDs per request,
with conductor's 4 MiB response limit. Rounds do not overlap for a section, and
completion starts the next interval. A due round waits for the route bookmark
while discovery is unsynchronized, including startup and reconnection, rather
than consuming an interval as an empty round. Cancellation bounds this wait,
pending requests and delivery. An unavailable configured source logs an error
and drops that read; it never exports zeros or a stale complete response. Resource selects current
starting/running objects; traffic and saved usage also include paused objects.
Once conductor accepts the object data, later route changes do not revoke it.

All projections below are Gauges with trusted `sandbox.telemetry.source=sandboxstats`.
Their values describe current counters or native cumulative endpoints, not
never-reset counters. Native HTTP/usage JSON remains lossless; float64 projection
can round uint64 values above 2^53 and 128-bit integrals. Telemetry scalar history
cannot reconstruct the native usage ledger.

| Native section | Metric prefix and suffixes | Observation semantics |
|---|---|---|
| resource | `sandbox.resource.cpu.capacity`, `cpu.allocatable`; `memory.capacity`, `memory.headroom`, `memory.reserved`, `memory.used`; `cpu.seconds` | Core counts, bytes, seconds. Configuration/reservation are current reads; host Used/CPU retain native `timestampUnix`. Missing values are omitted independently; valid zero is retained. Headroom is the balloon controller's effective allocatable memory, not node reservation; CPU allocatable is relative weight, not quota |
| traffic | `sandbox.traffic.state`, `max_inflight`, `idle_since`, `inflight.parking`, `inflight.connected`; `service.parking`, `service.connected`, `service.max_inflight`, `service.idle_since` | Current Proxy ingress read, with state/service attributes. Zero admission limit means unlimited. Optional idle timestamps remain optional and refer only to admitted ingress, never whole-sandbox idle |
| traffic | `sandbox.traffic.platform` / `sandbox.traffic.transit` + `.rx.packets`, `.rx.bytes`, `.tx.packets`, `.tx.bytes` | Current connector counter read, sandbox direction; never summed across observation points. Missing planes publish no counters; `egress:{}` produces no fake egress series |
| usage | `sandbox.usage.cpu.seconds`, `memory.integral`, `memory.span`, `memory.covered` | Saved native endpoint only. CPU uses native known nanoseconds / 1e9; integrals use byte-nanoseconds / 1e9, span/coverage nanoseconds / 1e9. `usage.name`, `usage.status`, and CPU `usage.complete` preserve the existing category/validity. Source identities and run_epoch remain native metadata, never labels |
| usage | `sandbox.usage.saved.available`, `enabled`, `saving`, `unknown_tail`, `save_error`, `read_error` | Separate Boolean Gauges of current read status. They do not redate saved quantities or convert an uncertain tail into reliable zero consumption |

Usage collection requests `view=saved`. Each cumulative quantity retains its
Record `saved_utc_ns`; repeated reads publish the same endpoint, never add totals
or reintegrate memory. Unobserved quantities are omitted. Pausing or replacing a
source invalidates its current position without erasing already saved totals;
known contributions and coverage remain published with the original record time.
`usage.source_known`, `usage.position_known`, and `usage.value_known` distinguish
that saved contribution from a current source observation. Live data
can be ahead of saved; crash/failed save can lose that unsaved tail. The receiver
does not force a sample, save, fsync or export acknowledgement and does not change
native usage policy. Current/live/history, precise integers, raw counters,
128-bit integrals, coverage and detailed errors remain available through
[`stats/usage`](node.md#native-usage). Enabling telemetry does not enable usage.

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
The `sandboxotlp` receiver supports metrics: OTLP/gRPC MetricsService on 4317 and OTLP/HTTP
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

Set outer `proxy_netns: sandbox-proxy` and the `sandboxotlp` receiver
`grpc_listen: 127.0.0.1:4317`, `http_listen: 127.0.0.1:4318`. Guest exporters use
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

At source acceptance, envd validates the current target and sandbox OTLP validates
the pinned connection. Core overwrites `sandbox.id`, `sandbox.stable_id` and
`sandbox.telemetry.source` on each pdata resource and removes conflicting
point/scope identity. Spellings that normalize to the same standard exporter
labels (for example `sandbox_id` or `sandbox-telemetry-source`) are reserved too
and removed before stamping, so Guest attributes cannot merge into trusted
SID, StableID or source values during export. EnvD uses source `envd`, guest OTLP uses `otlp`; sources are
not globally restricted to those two names. Guest metrics cannot forge envd
source even when they copy its metric names. The exact obsolete platform keys
`sandbox.run_id`, `run_id`, `runId` and `RunID` are removed at ingress; user
attributes such as `application.run_id` are retained.

Accepted resources carry their own identity through standard batch, queue and
retry. A batch may contain multiple SandboxIDs. Subsequent pause/deletion or
loss of the original request context does not revoke accepted history. No view
lock is held while calling downstream processors or exporters. An unfinished
envd fetch or unaccepted OTLP request still fails when its target is invalidated.
Ordinary infrastructure receivers use their own configuration and need no
SandboxID. Trusted deployment processors and static extensions remain in the
existing trust domain and can transform pdata; there is no final sandbox-only
guard across asynchronous pipeline boundaries.

Limits apply before transport decoding where possible: 256 connections per
listener, 32 concurrent requests globally, 4 MiB per encoded/decompressed HTTP
request and gRPC message by default. Resource/point attributes are limited to 32,
keys 128 bytes and values 256 bytes; batches allow 128 resources, 16,384 points
and 4,096 metrics per scope. Excess is rejected, not silently truncated.
The gRPC server's 10s deadline starts before reading the message body, so a
stalled client cannot indefinitely retain a global request slot.

## 5. Independent writes and queries

`collector` is the native write graph. `query.backend` independently selects
`none`, `local`, `prometheus`, `clickhouse` or `custom`; `query.handler` selects
`none`, `e2b` or `custom`. A usable Reader and handler are both needed to register
the query UDS. With no handler, public `/metrics` returns 503. Conductor's three
native stats APIs keep their original sources in every mode.

| Deployment | Configuration |
|---|---|
| Remote write and read | Standard remote exporter plus matching `query.backend`/endpoint/schema/labels |
| Write-only | Collector exporters, `query: {backend: none, handler: none}`; no query UDS |
| Query-only | Select a backend and handler; omit `collector` entirely; no dummy receiver |
| Local write and read | `local.enabled: true`, `sandboxlocal` exporter, `query.backend: local` |
| Local plus remote fan-out | Enable local and reference `sandboxlocal` plus remote exporters; explicitly select one query backend |
| Custom query contract | Statically bind `Runtime.MetricsHandler` with `query.handler: custom` |

The old unreleased `telemetry.storage`, combined `extension.Storage`,
`Runtime.Storage` and `Runtime.StorageHeaders` contracts are replaced directly.
Use `local`, `query`, `Runtime.QueryBackend` and `Runtime.QueryHeaders`; the local
Collector exporter is now `sandboxlocal`. There is no parsing alias or second
runtime path. The write exporter and read backend are configured and validated
separately: an OTLP destination is not automatically a query backend. Remote
errors never select a local database or become successful empty history.

### Local TSDB

Local storage is created only with `local.enabled: true`. Merely selecting a
remote query backend or starting Collector creates no local DB. The core
`sandboxlocal` exporter writes the existing embedded Prometheus TSDB; there is no
Prometheus server, scrape manager, rule manager or Alertmanager. For local reads,
`query.backend: local` also requires explicit local enablement. See the complete
[local deployment](../deploy/telemetry.example.yaml).

```yaml
query: {backend: local, handler: e2b}
local:
  enabled: true
  path: /var/lib/sandbox/telemetry
  retention: 168h
  max_size: 10GiB
  max_series: 1000000
```

The TSDB owns its existing WAL, replay, compaction and retention. Its WAL contract
is independent of native usage's no-sync contract. Each Collector delivery is a
transaction; failures roll back. Millisecond timestamps truncate sub-millisecond
precision. Exact duplicate samples are idempotent; conflicting values at the
same timestamp fail. Out-of-order samples are accepted within min(5m, retention).
Older samples fail. Wall-clock retention excludes expired observations and
reclaims idle head/block data even after all sandboxes pause. Writes more than
one minute in the future fail. Cached observations are never given a new time.

`max_size` is a retention target, not a filesystem quota; head/WAL and temporary
compaction data may exceed it. `max_series` bounds live head series and checks
replay. Background errors and corruption/WAL recovery warnings stop the component
and revoke its query lease. Startup does not delete a damaged DB. Restart replays
WAL without adding a new power-loss durability guarantee.

The scalar representation preserves OTel metric names. Accepted identity labels
are `sandbox.id`, `sandbox.stable_id` and `sandbox.telemetry.source`. Other
attributes use `resource.`, `scope.` and `point.` prefixes; scope name/version,
unit and kind use `otel.*`. Sums retain temporality/monotonicity. Histogram and
exponential histogram count/sum/cumulative buckets use the original metric name
with `otel.part`/`otel.bound`, and summaries use count/sum/quantile parts. Optional
histogram sums remain absent when not recorded. Expansion is bounded at 160
bounds/quantiles and 65,536 scalar samples per write. Exemplars and histogram
min/max are not part of this scalar query representation. Other Collector
exporters retain their supported pdata representation. Float64 cannot preserve
all integers above 2^53 or replace the lossless native usage ledger.

### Prometheus remote read and standard remote write

The [Prometheus deployment](../deploy/telemetry-prometheus.example.yaml) selects
`prometheusremotewrite` independently of the `prometheus` query backend. The
exporter uses its native queue, retry, batching and resource conversion options.
The Reader only calls `<query.prometheus.endpoint>/api/v1/read`; it does not
implement Write. A [query-only deployment](../deploy/telemetry-query-only.example.yaml)
can use an existing remote database without starting Collector.

The linked exporter replaces punctuation in names/labels with underscores.
`resource_to_telemetry_conversion.enabled: true` carries accepted identity onto
each metric. Default query label mapping translates logical `sandbox.id`,
`sandbox.stable_id` and `sandbox.telemetry.source` to physical `sandbox_id`,
`sandbox_stable_id` and `sandbox_telemetry_source`. `query.prometheus.labels` can
supply another explicit mapping, including identity mappings for a backend that
stores original UTF-8 names. Duplicate/overlapping mappings fail validation.
Result attributes use the configured logical names; other remote labels remain
as stored, including point labels. Physical identity aliases cannot replace the
mandatory exact SandboxID match.

Metric names in generic queries are the physical backend names. The example
sets `add_metric_suffixes: false` and maps the seven E2B fields to names such as
`sandbox_memory_used`. With suffixes enabled, standard unit/type conversion may
produce names such as `task_payload_bytes` or cumulative Sum names ending in
`_total`; update the E2B mapping if those metrics supply the compatibility API.
Query code does not guess a writer's namespace, normalization or unit suffixes.
Configure server retention, remote-write reception and out-of-order policy on
the server separately. Query credentials need remote-read access independently
of exporter credentials.

The Reader negotiates `STREAMED_XOR_CHUNKS`, verifies each frame's CRC32C, bounds
frames at 32 MiB and retains one frame at a time. A server offering `SAMPLES`
uses a single Snappy response with both compressed and decoded sizes capped at
32 MiB. One request obtains Bounds and one obtains Query, including empty and
sparse long histories. The 10s HTTP deadline covers the body. Complete edge
chunks are filtered to the exact inclusive raw-sample range before aggregation.
PromQL lookback, interpolation and last-value extension never enter this API.
See the [remote read API](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
`query.lookback` bounds the remote read window, defaults to 168h and must be at
least one minute; it does not change exporter writes or server retention.

Native histogram chunks and histogram samples are decoded through the linked
Prometheus model. They retain the physical metric name and expose `otel.part`
values `count`, `sum` and `native_bucket`; `otel.bound` records each native
bucket's actual open/closed interval. Native bucket counts are absolute, not
cumulative. These derived attributes can be selected at the Reader boundary.
Classic histogram `_bucket`/`_sum`/`_count` series and summary quantile labels
retain the standard exporter's physical representation. Stale markers do not
become observations or extend a previous value. Both unit-suffix settings and
all five metric kinds are verified against the real server.

### ClickHouse standard schema and read-only queries

The [ClickHouse deployment](../deploy/telemetry-clickhouse.example.yaml) uses the
linked standard `clickhouse` exporter and its five native metrics tables:
`otel_metrics_gauge`, `otel_metrics_sum`, `otel_metrics_summary`,
`otel_metrics_histogram` and `otel_metrics_exponential_histogram`. The exporter
controls database creation, `create_schema`, TTL, queue/retry and write grants.
The Reader uses only SELECT through its separately configured HTTP(S) endpoint,
credentials, database and `query.clickhouse.tables`. It never creates a table,
changes TTL or requires Write. Old `sandbox_metrics` canonical tables are not this
schema. A missing/mismatched table or backend error is an error, not empty data.

The Reader matches `ResourceAttributes['sandbox.id']` before reading rows. It
uses `MetricName`, `TimeUnix`, `Value`, resource/scope/point maps and metric-kind
columns from the standard exporter schema. `Flags.NoRecordedValue` rows are
excluded. Gauge/Sum values keep the original metric name/unit, and attributes
use the local scalar namespace. Summary parts include count, sum and quantiles;
explicit Histogram parts include count and cumulative buckets. The standard
schema does not retain Histogram `HasSum`, so its ambiguous sum column is not
published as an observed zero. It also omits exponential `ZeroThreshold`:
exponential results publish count/zero_count and stored positive/negative bucket
counts, with `otel.scale` and the original bucket index in `otel.bound`, rather
than inventing numeric bounds. These schema limits do not remove data from the
Collector or other exporters; they define this backend's scalar read mapping.

SQL parameters carry SandboxID, metric names, equality attributes, time and
step. Client input never supplies SQL. Time is projected to milliseconds before
inclusive filtering, matching local/Prometheus precision. Raw queries preserve
observed times. MAX filters raw rows first, then groups independently by complete
attributes and epoch-aligned bucket. Duplicate identical observations deduplicate;
conflicting raw values at one millisecond fail instead of being arbitrarily
selected. Out-of-order/duplicate inserts do not alter MAX. No missing Gauge is
extended into later buckets.

Read requests enforce nine seconds of server execution, two threads, 256 MiB
server memory, 700,000 rows and 32 MiB response limits, cancel readonly queries
when HTTP clients disconnect, and reject HTTP-200 error bodies. See
[ClickHouse HTTP](https://clickhouse.com/docs/interfaces/http). Both remote clients
bound pools, propagate cancellation, reject redirects and use only protected
operator-configured destinations. `Runtime.QueryHeaders` replaces read credentials;
exporter credentials use native Collector config providers. Neither read errors
nor authentication errors fall back to a local DB. External size/cardinality
controls remain the external server's responsibility.

### Fan-out and static extensions

Add factories once by type; declare each instance and edge in native Collector
configuration. The [fan-out example](../deploy/telemetry-fanout.example.yaml)
uses batch/filter/transform/routing and two OTLP HTTP sinks with queue/retry.
Adding `local.enabled` and a `sandboxlocal` destination creates optional local
fan-out; `query.backend` still explicitly selects the Reader. Queue durability
comes only from the configured component's own contract. Export failures do not
pause or wake sandboxes. See [static extensions](extensions.md#telemetry-bootstrap-and-extension).

`extension.Reader` accepts `Selection{SandboxID, Metrics, Attributes}` and
`Query{Selection, Start, End, Step, Aggregation}`, returning `[]Series` with metric
name, complete attributes and timestamp/value points. Metrics are exact names,
attributes require an existing key with an equal value (empty does not match
missing), and an empty metric list selects all names
within that exact sandbox. There is no seven-field enum, source allowlist or
public SQL/PromQL input. Omitted aggregation is normalized to `Raw` before
dispatch to any backend. `Raw` requires zero Step;
`Max` requires a positive whole-millisecond Step. Queries accept 1970–2299,
up to 64 metric names, 110 equality attributes, 100,000 series and 700,000 points.
Absent points stay absent; negative Gauges are legal. Bounds applies the same
selection to retained observations.

`Runtime.QueryBackend` constructs a `Reader` plus Shutdown without Write.
`Runtime.MetricsHandler` supplies a standard `http.Handler` per `QueryScope`.
The scope Reader is permanently bound to conductor's exact SandboxID and rejects
attempts to replace it, including attribute selectors or a different context.
Deployments and static extensions remain trusted. A custom JSON contract is not
E2B compatibility; use the built-in E2B handler for E2B SDKs. The buildable
[custom handler](../examples/custom-telemetry/query.go) publishes arbitrary raw
series, and its [query-only config](../examples/custom-telemetry/query.yaml)
starts neither Collector nor local storage.

## 6. E2B history query

`GET /sandboxes/{SandboxID}/metrics?start=<unix-seconds>&end=<unix-seconds>`
uses the existing SandboxID exclusively. Missing lookup never falls back to
StableID, including cluster mode. Cluster Router's existing public-SandboxID to
current node SandboxID routing is unchanged; telemetry on a node always reads
that exact node SandboxID and does not join migration histories by StableID.
StableID is only external observability correlation. RunID remains runner/run-plane,
systemd/pidfile, MMDSv2 and logging/debug identity; it is never a telemetry label,
resource attribute, TSDB identity, query key or pause/resume series discriminator.

With `query.handler: e2b`, the independent E2B adapter uses the seven
`query.e2b.metrics` mappings and `query.e2b.source` (default `envd`). Deployments
can explicitly select another source with the same observation semantics. Generic
Readers do not impose either mapping or source. This handler parses nonnegative integral Unix-second boundaries
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

Native resource, traffic and usage reads are owned by conductor and remain available when telemetry stops. The trusted telemetry lease can consume selected sections through the existing config socket; it does not access sandbox ctl sockets or usage files. Paused saved usage does not require an active guest. See [Native usage and local reads](node.md#native-usage).

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
extension and finally the query backend and optional local TSDB. Startup failures unwind the same owned
resources. Retained extension work is canceled even when startup fails. Keep the
storage path persistent and do not make conductor/Proxy systemd dependencies
require telemetry; telemetry failure must not change sandbox lifecycle.

Reproducible checks (from a source checkout with sibling dependencies):

```sh
GOWORK=off go test ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api ./app/telemetry ./config ./cmd/node-ctl
GOWORK=off go test -race ./internal/telemetry ./internal/telemetryapp ./internal/configsock ./internal/api
GOWORK=off go test ./internal/telemetry -run '^$' -bench BenchmarkEnvdDensity -benchtime=2x -benchmem
GOWORK=off go test ./internal/telemetry -run '^$' -bench 'Benchmark(Local|Scrape)' -benchtime=100x -benchmem
bash test/e2e/e2e_telemetry_backends.sh # source: Go + Docker; installed package: BIN + Docker
make test vet build
make test-e2e # assembled project BIN and real KVM host required
```

The component-owned backend case creates disposable Prometheus 3.5.0 and
ClickHouse 25.8 containers from pinned manifest digests, publishes loopback-only
ports, records actual versions and image identities, and owns a private Docker
network. Containers, volumes and that network are removed on exit, including
partial startup failure; the shared default bridge is not required or modified.
Both engines and every named case are required; missing
prerequisites, skips and failures are errors. Uncached images use platform's
existing public Docker Hub mirror with a bounded pull and the same pinned
manifest digest; no tag or backend version is substituted. Source CI locates the exact sibling
checkout from the assembled `BIN` directory; missing sources remain an error.
Set `TELEMETRY_SOURCE_ROOT` only for another source layout. The same case runs
from `test/e2e/run_all.sh` in source and exact-assets validation. Exact-assets uses
the shipped `BIN/node-ctl`, without Go or rebuilding component sources.
`TELEMETRY_BACKEND_OUT_DIR` optionally selects retained configurations, JSON test
events, executable hashes and engine logs; CI defaults to its uploaded metadata
directory. Fixture tables and Prometheus samples exist only in disposable
containers, whose volumes are removed on exit.

Both layouts run the installed node-ctl with a trusted infrastructure OTLP
fixture, native batch/queue, the standard remote exporter and the matching Reader
through the private E2B query UDS. Assertions cover exact SID/source isolation,
StableID as a label only, independent MAX, inclusive boundaries, incomplete
buckets, no Gauge extension, no local DB and clean shutdown. This backend case
does not replace the real guest/conductor authentication and namespace cases.

Coverage includes actual native deployment startup, name/unit/resource-label
conversion, all metric kinds, duplicate/out-of-order samples, raw/MAX edges,
gaps, exact SID, local/remote fan-out and accepted batches after route deletion.
Source validation additionally builds and executes node-ctl and custom-telemetry from the exact sources,
passes sealed bootstrap, queries a non-E2B metric from real Prometheus through
the actual Plugin lease/query UDS, and verifies query-only cleanup with no
Collector graph or local DB. Ordinary unit invocations may skip these external
fixtures; those skips are never backend acceptance. Density
benchmarks measure real 5s periods for 1k/10k/50k
synthetic targets, scrape counts, peak goroutines/FDs (including fixture server),
allocations, TSDB batch writes and local query cost. Receiver and TSDB benchmarks
are separate diagnostics, not a claim of end-to-end production capacity. Timing
numbers are not ordinary CI gates; repeat on representative hosts/workloads.

The real guest fixtures additionally export conductor resource/traffic observations
through sandboxstats, native batch and an HTTP sink on the host network. A paused
sandbox exports its actual saved usage through the same lease-authorized reader;
assertions compare CPU/memory cumulative values and the original saved timestamp
with the public native API and confirm the sandbox stays paused.

The existing real Proxy E2E fixture also exercises envd → Collector → local DB,
guest OTLP through connector mgmt-extract, auth, paused queries, unavailable
telemetry, lease revocation and TSDB restart, without a second VM lifecycle.
