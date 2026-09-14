[English](README.md) | [简体中文](README_zh.md)

# Custom conductor

Build the example with:

```sh
go build -o /opt/kuasar/bin/xconductor ./examples/custom-conductor
```

Own the binary with root or the non-root node-ctl service UID, protect it from
group/world writes, and set the absolute path in
`paths.conductor_executable`, and keep starting the service through
`node-ctl conductor serve --config ...`. The binary is not a standalone CLI.

`Configure` runs once before any conductor listener, durable store, systemd
unit, or worker starts. Declarative overrides belong in `Config`; logger and
material providers belong in `Runtime` and are never serialized.

The example also binds one trusted, statically linked runtime Extension. Its
`Start` method saves the conductor `Host` and starts a Sandbox Watch that exits
with the process context. `Get` and Watch views are deep-copied, non-secret
projections. Watch is generation-based eventual convergence, not a durable
audit stream; discard every generation that does not reach `sync_end`.
`SandboxView.ID` is node-local; `SandboxView.StableID` remains unchanged when
an identity-preserving import or cluster re-place changes that local ID.

The same object also implements the optional `APIWrapper` and `SandboxHook`.
The wrapper adds `/private/extension/health` and calls the canonical handler for
every unmatched request. The Hook adds example metadata and raises a short
Create timeout before the core performs final normalization. The private route
has no production authentication; it is deliberately only a wiring example.
Wrappers may override canonical routes, so production code owns the resulting
URL and authentication contract. Mandatory cleanup never depends on a Hook.
The wrapped handler is served by both the public API listener and config-socket
API fallback; sandbox data and CONNECT ingress belong only to the independent
Proxy and never enter this App.

There is no dynamic plugin loader or extension registry. A nil Extension keeps
the built-in behavior and creates no observation hub or watcher goroutine. See
[`docs/extensions.md`](../../docs/extensions.md) for the trust and consistency
contracts.

`config.DecodeConductor` is an environment-independent declarative parser: a
third-party document need not repeat the local dispatch executable or contain
encryption material. Preserve the immutable bootstrap executable when replacing
the Config, then bind encryption material through Runtime before startup.

`cfg.Sandbox.Resources.Allocatable.SetMemory("512MiB")` makes memory explicit;
direct pointer assignment is equivalent. `InheritMemory()` restores the
omitted `256MiB` default and its capacity-clamp behavior. Config snapshots and
`Clone` preserve this distinction.

Native accounting stays in `cfg.Sandbox.Usage`; for example, declare
`enabled: true`, `sample_interval: 1s`, `flush_interval: 5m` under
`sandbox.usage` in the conductor YAML. The public config carries that policy
through bootstrap and all three native launch paths. The final Configure-hook
validation rejects invalid intervals even when usage is disabled.

Trusted private tasks can read `e.host.Stats()` after Start. Until core startup
and provider wiring complete, reads return unavailable immediately; retry with
the task context instead of waiting inside Start. Use the exact
`SandboxView.ID`, with a cancellable context, outside the Watch callback:

```go
rows, err := e.host.Stats().ReadStats(ctx, conductor.StatsRequest{
    SandboxIDs: []string{sandboxID},
    Sections: []string{"usage"},
    Usage: conductor.UsageQuery{View: "saved"},
})
// On success, rows[0].Usage is lossless native JSON, including for a paused VM.
```

This calls the same conductor domain reader as the public stats API and the
trusted telemetry plugin. The fixed request/concurrency/time/size limits and
whole-batch errors also apply in process. StableID cannot replace SandboxID.
Keep the raw JSON or decode the native integer types; do not round through a
generic float64 map or expose this trusted reader through an unauthenticated
wrapper. See [native usage](../../docs/node-usage.md).
