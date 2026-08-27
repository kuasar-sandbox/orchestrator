# Custom conductor

Build the example with:

```sh
go build -o /opt/kuasar/bin/xconductor ./examples/custom-conductor
```

Protect the binary from group/world writes, set the absolute path in
`paths.conductor_executable`, and keep starting the service through
`node-ctl conductor serve --config ...`. The binary is not a standalone CLI.

`Configure` runs once before any conductor listener, durable store, systemd
unit, or worker starts. Declarative overrides belong in `Config`; logger and
material providers belong in `Runtime` and are never serialized.
