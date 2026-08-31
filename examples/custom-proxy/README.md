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

The example binds one trusted `MasterExtension` only for `RoleMaster`. Its
`Start` method launches a convergent route Watch, and its management wrapper
adds `GET /private/route?sandbox_id=...` on the existing 0600 stats socket while
passing every unmatched request to the built-in handler. The route response is
the public non-secret projection; raw route credentials are not copied into
ordinary Watch events.

This is process organization for statically linked, same-UID trusted code, not
a security sandbox or dynamic plugin system. The private route and its lack of
authentication are deliberately minimal demonstration choices, not
production-grade authorization. A real deployment must define and enforce its
own local management authentication policy.

This public App customizes only `proxy.mode=external`; the conductor's internal
proxy has no customization entry point. Runtime providers are authoritative and
fail closed, while TLS versions, ALPN, and client-auth policy remain core-owned.
V1 has no configuration or material hot reload. Deploy `xproxy` and `node-ctl`
from compatible orchestrator versions.
