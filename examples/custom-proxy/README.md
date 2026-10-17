[English](README.md) | [简体中文](README_zh.md)

# Custom independent Proxy

Build the example with:

```sh
go build -o /opt/kuasar/bin/xproxy ./examples/custom-proxy
```

Own the binary with root or the non-root node-ctl service UID, protect it from
group/world writes, set its absolute path in `paths.proxy_executable`, and keep starting the
service through `node-ctl proxy serve --config ...`. It is not a standalone CLI.

`Configure` runs once per master incarnation from the accepted original input. `BindRuntime` runs in the master
and every worker epoch, so process-local logger and TLS/HSM handles are reopened
after re-exec. Workers receive the frozen effective config over a sealed memfd;
they never read `proxy.yaml`.

The example binds one trusted `MasterExtension` for `RoleMaster` and creates a
fresh trusted `WorkerExtension` for every `RoleWorker` epoch. The master
`Start` method launches a convergent route Watch, and its management wrapper
adds `GET /private/route?sandbox_id=...` on the existing 0600 stats socket while
passing every unmatched request to the built-in handler. The route response is
the public non-secret projection; raw route credentials are not copied into
ordinary Watch events.
After business drains, the core cancels the extension context and its optional
`Shutdown` waits for Watch to exit. The worker's Shutdown returns immediately
because it has no retained background task. Both extensions therefore satisfy
InplaceRestart's completion preflight, including with `inplace_restart: false`.
In each route projection, `SandboxID` is the node-local lookup key and
`StableID` is the identity preserved across node-local replacements.

The worker wrapper sees raw requests before Kuasar parses the canonical
`E2b-Sandbox-*` host/header contract. It demonstrates two private shapes:

- `X-Sandbox-Id` plus `X-Sandbox-Port`; or
- `/private/sandboxes/{sandbox-id}/{port}/{guest-path...}`.

`X-Private-Authorization: example-only` is intentionally a non-production
placeholder. Real code must authenticate and bind its own credential to the
requested sandbox and target. After that placeholder check, the example reads
a point-in-time local route registration, remembers its revision, and calls
`ForwardAuthorized`. The activation-time `Revalidate` callback fences that
revision, while `Rewrite` removes private headers and optionally maps the
private URL to a guest path. Unmatched requests call `next`, retaining the
built-in parser, `X-Access-Token`, parking, native-exec KAT/CEL, and error
behavior.

`ForwardAuthorized` is handler-style: it owns the response on success and
failure, does not return a normal error, and does not verify Kuasar's
`X-Access-Token`. It supports the current ordinary HTTP and CONNECT transport;
it also relays negotiated HTTP/1.1 WebSocket upgrades through the shared core
transport. HTTP/2 extended CONNECT and arbitrary Upgrade protocols are not
supported; see [transport boundaries](../../docs/node-proxy.md#51-http11-websocket-forwarding). The wrapped handler serves the node's
`data_listen` sandbox ingress, while MMDS remains outside the wrapper.

This is process organization for statically linked, same-UID trusted code, not
a security sandbox or dynamic plugin system. The private route and its lack of
authentication are deliberately minimal demonstration choices, not
production-grade authorization. A real deployment must define and enforce its
own local management authentication policy.

This public App customizes the independent Proxy; conductor has no data-plane
customization entry point. Runtime providers are authoritative and fail closed,
while TLS versions, ALPN, and client-auth policy remain core-owned. Requests
reach the wrapper only through the Proxy `data_listen`; conductor is API-only.
Cluster ingress still has no Extension and must canonicalize requests at its
outer boundary before they reach this node-local wrapper. V1 has no configuration
or material hot reload, worker route Watch, namespace, or dynamic plugin registry.
Deploy `xproxy` and `node-ctl` from compatible orchestrator versions.

The example uses `App.Run()`: SIGINT/SIGTERM request complete service shutdown;
`inplace_restart: true` additionally registers SIGUSR2 only in the main
coordinator and requires a positive `shutdown_timeout`. The default false leaves
SIGUSR2 unregistered. With a positive timeout, embedding code can call
`app.InplaceRestart(ctx)` directly after `WaitReady` in either case. An omitted
or zero timeout makes InplaceRestart unavailable and automatic termination
immediate. Explicit `app.Shutdown(ctx)` still drains gracefully at zero, without
an implicit deadline; canceling its context ends only the caller's wait.
The call returns an accepted operation ID; inspect `app.Status()` or
`GET /status` for completion, since the Go caller stack cannot survive exec.
For custom signal assembly use `RunContext`, keeping triggers in the main
coordinator. B and workers reject nested operations.

The core reserves `/readyz` and `/status` on `stats_socket`; `/private/route`
keeps its existing behavior. Ready depends on initial Bookmark and actual
worker acceptance, including MMDS when enabled. `Runtime.ListenerFiles` can
deliver named listeners from master BindRuntime; successful binding transfers
ownership to App. Ordinary node-ctl → xproxy activation is received by the final
master automatically. See [Proxy deployment and bounded shutdown](../../docs/node-proxy.md#92-business-readiness-and-service-status)
for FD names, systemd units, atomic target replacement, deadlines and recovery.
