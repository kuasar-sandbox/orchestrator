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
