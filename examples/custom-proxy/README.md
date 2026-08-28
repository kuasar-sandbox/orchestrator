# Custom external proxy

Build the example with:

```sh
go build -o /opt/kuasar/bin/xproxy ./examples/custom-proxy
```

Own the binary with root or the non-root node-ctl service UID, protect it from
group/world writes, set its absolute path in `paths.proxy_executable`, and keep starting the
service through `node-ctl proxy serve --config ...`. It is not a standalone CLI.

`Configure` runs exactly once in the master. `BindRuntime` runs in the master
and every worker epoch, so process-local logger and TLS/HSM handles are reopened
after re-exec. Workers receive the frozen effective config over a sealed memfd;
they never read `proxy.yaml`.

This public App customizes only `proxy.mode=external`; the conductor's internal
proxy has no customization entry point. Runtime providers are authoritative and
fail closed, while TLS versions, ALPN, and client-auth policy remain core-owned.
V1 has no configuration or material hot reload. Deploy `xproxy` and `node-ctl`
from compatible orchestrator versions.
