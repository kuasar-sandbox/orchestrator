[English](node-usage.md) | [简体中文](node-usage_zh.md)

# Native sandbox usage and local stats reads

## 1. Overview

Conductor publishes `GET /sandboxes/{SandboxID}/stats/usage` alongside
`/stats/resource` and `/stats/traffic`. Usage reads sandboxer's existing native
lifecycle accounting. Resource stats describe current effective specification
and VMM observations; traffic stats describe the existing Proxy ingress
observations. These are independent of telemetry and its history query
at `/sandboxes/{SandboxID}/metrics`.

Every public read authenticates the API key and checks the exact SandboxID's
ownership. StableID is a correlation label, never a lookup alias. Conductor
resolves the current object and calls sandboxer's shared
[`pkg/usagereader.Read`](https://github.com/kuasar-sandbox/sandboxer/blob/main/pkg/usagereader/read.go),
also used by `sandbox-ctl usage`. It does not duplicate the native codec,
recovery, locks or history algorithm. Telemetry obtains native sections only
through conductor, without opening sandbox `ctl.sock` or `.usage` files.

## 2. CLI and HTTP

```sh
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=saved"
curl -H "X-API-Key: $API_KEY" "$NODE_API/sandboxes/$SANDBOX_ID/stats/usage?view=history&cursor=0&limit=10"
```

| Parameter | Contract |
|---|---|
| `view` | `current` (default), `saved`, or `history` |
| `cursor` | History only; nonnegative decimal byte position, default `0`. Preserve the returned `next_cursor` string without floating-point conversion |
| `limit` | History only; integer `1`–`100`, default `10`. Reduce it if the native 1 MiB page bound is exceeded |

Unknown, repeated, empty or malformed parameters are rejected with 400.
There is no `/usage` alias. The response has `Cache-Control: no-store`.
Normal authentication errors remain unchanged; a missing or non-owned
SandboxID returns 404. Missing files, live-writer lock conflicts, unavailable
owners, invalid owner identity, failed reads, timeouts and concurrent runtime
replacement return 503. Native `read_error`/`save_error` fields in a valid view
remain part of that view, rather than being discarded or converted to zeros.

`current` returns the native View; `saved` returns that View with `live`
omitted. For example, a disabled owner with no saved record can return:

```json
{"enabled":false,"saved_end":"0","saving":false,"unknown_tail":false}
```

This is neither a measured zero-usage record nor proof that historical usage
does not exist elsewhere. `history` returns native Records and `next_cursor`;
an empty validated file returns `{"records":[],"next_cursor":"0"}`.
Native counters, sizes, timestamps, positions and 128-bit integrals retain
their decimal-string representations. Coverage, completeness, status,
`live`, `saved`, `saving`, `unknown_tail`, errors and native `run_epoch`
metadata are preserved. No orchestrator RunID is added to the stats model.

## 3. Node configuration

```yaml
sandbox:
  usage:
    enabled: true
    sample_interval: 1s
    flush_interval: 5m
```

Defaults are false, `1s` and `5m`. Durations must be positive Go durations,
flush must be at least sample, and twice sample must fit the native signed
duration range. Validation also applies when disabled. Explicit null, wrong
types, unknown/duplicate keys and YAML merge keys are rejected. Clone,
diagnostic output, external conductor bootstrap and final Configure-hook
validation preserve the same policy.

Conductor passes this node policy to image cold start, `run --from` and
`run --restore`. Usage is host policy and is absent from portable E/S
artifacts; restore uses the current node's policy. Enabling telemetry does
not enable usage, and stopping telemetry does not stop native accounting.
`flush_interval` schedules native appends, not fsync or device-cache flushes.
See the parsed [conductor deployment example](../deploy/conductor.example.yaml).

## 4. Online, saved and offline ownership

The running owner is authoritative for live state, its adopted saved baseline
and confirmed history. Only an absent/refused owner socket permits offline
fallback. After a connection succeeds, protocol errors, EOF, owner errors,
cancellation and timeout do not fall back to an apparently readable file.
Every response proves the exact owner SandboxID, including empty/disabled
views and empty history. Reader and sandboxer owner must use compatible
revisions of that ctl envelope.

A paused object can be read without waking it. Offline reads take the native
nonblocking shared lock, validate a regular file, then use Recover and
ReadHistory. An active writer prevents bypassing its adopted saved boundary.
Offline recovery finds complete surviving records; it does not prove that
the former writer confirmed an append or that data survived a power failure.

Reads do not sample, integrate, append, flush, recover by writing, resize or
call the guest. They neither save unsaved live values nor resolve an uncertain
tail. Native memory integral coverage and CPU source resets retain their
existing semantics. Repeated reads of a cumulative record are the same
total, not new interval consumption. A floating-point telemetry projection
cannot reconstruct the lossless native ledger or certify an incomplete tail.
There is no new usage persistence, sync, ACK, finalizer, retention-after-delete
or lifecycle behavior.

## 5. Trusted conductor reading surface

Statically linked conductor extensions use `Host.Stats().ReadStats(ctx,
StatsRequest)`. The existing `paths.config_socket` carries the corresponding
internal `POST /internal/plugin/telemetry/stats`, selecting explicit native
sections for SandboxIDs already discovered from the full RouteEntry stream:

```json
{"sandboxIDs":["exact-sid"],"sections":["usage"],"usage":{"view":"saved"}}
```

The ordered response contains `sandboxID`, optional `stableID` and each
selected section's unchanged native body. No credentials, paths or RunID are
returned. Missing or failed selected sections fail the entire batch; no old
sample or zero completes a failed read. A paused object may supply usage
without an active guest; requesting an unavailable resource section can
instead fail that batch.

UDS connectivity alone does not grant this operation. The server verifies
actual SO_PEERCRED PID, the existing optional `plugin_pidfile` allowlist and
the ready, current, live registration with fixed Plugin ID `telemetry`.
The stats connection must belong to that registered process. Replacing or
disconnecting the lease cancels in-flight reads. Registration need not expose
a query UDS, so write-only telemetry can consume native stats. With no
allowlist, mode 0600 retains the established trusted local plugin boundary;
ordinary API requests still require their API authentication and ownership.
No tenant API-key collection, new permission model or socket is introduced.

## 6. Reliability and performance

Each batch contains 1–64 distinct nonempty SandboxIDs and 1–3 distinct
sections from `resource`, `traffic`, `usage`. Usage options are accepted only
when usage is selected. Request bodies are limited to 64 KiB, responses to
4 MiB and total time to 5 seconds; underlying native readers retain their
smaller limits. At most eight batch objects are read concurrently across
batch calls. Parent cancellation and lease revocation reach source reads;
slots and connections are released. Invalid requests return 400, missing
objects 404, conflicting lifecycle states 409 and unavailable reads 503,
using the same domain errors as the public APIs.

These values are fetched on demand, outside the RouteEntry subscription.
Public reads, local batches and in-process extensions share conductor's
domain implementation and final current-binding checks. Neither stats nor
telemetry creates a Create/Resume barrier. The density benchmark measures
bounded reads and FD/goroutine behavior without a machine-specific gate;
native sampling and history append algorithms remain unchanged.

```sh
go test ./internal/orch -run '^$' -bench '^BenchmarkNativeUsageBatch$' -benchmem -count=3
```

This benchmark uses real SQLite object reads and native saved files at
1/16/64 objects per batch. It reports allocations and retained FD/goroutine
deltas; it does not represent guest sampling or remote export throughput.

## 7. See Also

- [Node API and local planes](node.md)
- [Node resources](node-resource.md)
- [Telemetry](telemetry.md)
- [Conductor extensions](extensions.md)
- [Native sandboxer usage](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/usage.md)
