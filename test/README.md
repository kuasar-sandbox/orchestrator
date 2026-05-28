# Cross-repo e2e / perf harness

System-level integration + performance suites for the kuasar-sandbox platform
(shell-driven; they build the binaries, boot real microVMs, and drive the
full stack end to end). This is their home because they span every sub-repo.

## Status after the monorepo → multi-repo split

The scripts were authored against the single-repo layout: they call
`make build` / `make cloud-hypervisor` / `make vmlinux` at one root and expect
all binaries under one `bin/`. Under the org layout each binary is produced by
its own repo, so the harness's **build + binary-discovery steps need
adaptation**. Two equivalent ways to supply binaries:

1. Build the aggregated bundle and point the harness at it:

   ```bash
   ../scripts/release.sh v0.1.0 --with-natives
   export SANDBOX_BIN=../dist/kuasar-sandbox-v0.1.0-linux-$(uname -m)/bin
   ```

2. Build each repo (`GOWORK=off make build` in sandbox-accelerator / -runtime /
   -sentinel / -builder / -vswitch, and `make` targets in sandbox-deps) and
   collect the binaries into one directory.

The test bodies themselves are unchanged; the inline node-ctl client in
`e2e/e2e_node_ctl.sh` already imports the relocated protocol
(`sandbox-runtime/pkg/resource`, aliased `nodectl`).

## Suites

- `e2e/` — cold boot, tapfd networking, snapshot/restore, node-ctl protocol, density.
- `perf/` — density system load harness + dedup report.
- `scripts/`, `results/` — helpers and recorded baselines.
