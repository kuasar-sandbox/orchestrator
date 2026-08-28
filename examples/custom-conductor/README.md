# Custom conductor

Build the example with:

```sh
go build -o /opt/kuasar/bin/xconductor ./examples/custom-conductor
```

Own the binary with the node-ctl service UID, protect it from group/world
writes, and set the absolute path in
`paths.conductor_executable`, and keep starting the service through
`node-ctl conductor serve --config ...`. The binary is not a standalone CLI.

`Configure` runs once before any conductor listener, durable store, systemd
unit, or worker starts. Declarative overrides belong in `Config`; logger and
material providers belong in `Runtime` and are never serialized.

`cfg.Sandbox.Resources.Allocatable.SetMemory("512MiB")` makes memory explicit;
direct pointer assignment is equivalent. `InheritMemory()` restores the
omitted `256MiB` default and its capacity-clamp behavior. Config snapshots and
`Clone` preserve this distinction.
