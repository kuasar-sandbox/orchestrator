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

The example binds one trusted `MasterExtension` for `RoleMaster` and creates a
fresh trusted `WorkerExtension` for every `RoleWorker` epoch. The master
`Start` method launches a convergent route Watch, and its management wrapper
adds `GET /private/route?sandbox_id=...` on the existing 0600 stats socket while
passing every unmatched request to the built-in handler. The route response is
the public non-secret projection; raw route credentials are not copied into
ordinary Watch events.

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
it does not add WebSocket support. The same wrapped handler serves the data and
`proxy_socket` listeners, while MMDS remains outside the wrapper.

This is process organization for statically linked, same-UID trusted code, not
a security sandbox or dynamic plugin system. The private route and its lack of
authentication are deliberately minimal demonstration choices, not
production-grade authorization. A real deployment must define and enforce its
own local management authentication policy.

This public App customizes only `proxy.mode=external`; the conductor's internal
proxy has no customization entry point. Runtime providers are authoritative and
fail closed, while TLS versions, ALPN, and client-auth policy remain core-owned.
The conductor external fallback forwards non-canonical private header/path
requests unchanged to a worker; canonical requests retain SID affinity. Cluster
ingress still has no Extension and must canonicalize requests at its outer
boundary before they reach this node-local wrapper. V1 has no configuration or
material hot reload, worker route Watch, namespace, or dynamic plugin registry.
Deploy `xproxy` and `node-ctl` from compatible orchestrator versions.
