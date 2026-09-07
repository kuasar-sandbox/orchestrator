# Proxy worker lifetime and transport cancellation

[简体中文](proxy-worker-lifetime_zh.md)

A Proxy worker is a dedicated one-shot subprocess. The built-in and custom App
entry points must exit that process after `Run` returns; they are not an API for
restarting workers inside an otherwise long-lived process.

Successful route SHM and admission arena mappings belong to the worker process.
`PreparedWorker.Close` closes inherited descriptors but does not unmap either
mapping. Existing handlers, extensions and traffic GC may still hold references
when `Run` returns. No ownership flag, reference count, graceful-drain protocol or
`http.Server.Shutdown` join is needed to reclaim process-owned mappings. The
kernel reclaims them when the subprocess exits. Mapping constructors still clean
up their own unsuccessful operations; low-level mapping tests can explicitly
unmap only after every reader has stopped. Worker lifecycle tests use child
processes instead of requiring production teardown to serve test-only reuse.

This does not change normal request cleanup. A finished request or complete tunnel
still closes its backend and releases its inflight contribution exactly once.
The supervisor must still wait for process exit before clearing that worker's
shared counters and stats contribution: unmapping a process does not zero shared
memory used by surviving processes. Master route apply and Create barriers do not
wait for worker health or handler shutdown.

Traffic limits decide whether a new flow is admitted, not how an admitted flow
is transported. Ordinary HTTP cancellation and HTTP/2 CONNECT stream cancellation
close the associated backend with or without an effective limit. HTTP/1 hijacked
CONNECT retains half-close semantics; its original HTTP request context is not a
universal tunnel-close signal. Native exec keeps its existing KAT / first-frame /
CEL / traffic-admission ordering.

No worker graceful draining, connection pool, global limiter, public configuration
field or internal wire/layout change is introduced by this correction.
