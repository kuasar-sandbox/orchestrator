# Deterministic Pause cancellation barrier

This branch changes only the existing `e2e_execute.sh` sandbox-ctl wrapper. For the one all-unset local Pause regression, the wrapper blocks the exact sandbox's `snapshot` invocation after the orchestrator has accepted the Pause operation and before the real snapshot command runs. The test waits for that barrier, verifies the HTTP client is still pending, terminates that client, verifies SIGTERM status 143, then releases the wrapper and retains the existing durable Pause, cleanup, Connect and exec assertions.

The barrier is scoped by a per-run temporary file containing the exact sandbox ID. It is absent for every ordinary snapshot and is released from the E2E cleanup path. This is test-only behavior in the existing E2E wrapper; it adds no product API, configuration field, timeout extension, retry, or production hook.
