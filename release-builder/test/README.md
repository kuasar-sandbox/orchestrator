# Cross-repo e2e / perf harness

System-level integration and performance suites for the kuasar-sandbox
platform. This directory is the umbrella test home under
`orchestrator/release-builder` because these cases span multiple sibling repos
and consume the aggregated `bin/<arch>/` artifact set.

## Entry Points

From `orchestrator/release-builder`:

```bash
make build
make test-e2e
make test-e2e-sandbox-cold
make perf
make demo
```

From the org root:

```bash
make -C orchestrator/release-builder build
make -C orchestrator/release-builder test-e2e
make -C orchestrator/release-builder release
```

`make build` drives `guest-runtime/native-deps`, `accelerator`, `sandboxer`,
`guest-runtime`, `connector`, and `orchestrator`, then collects their artifacts
into `orchestrator/release-builder/bin/<arch>/`. Individual scripts default to
`SCRIPT_DIR/../../bin`, so release-package layout and source-tree layout use the
same binary discovery convention. `BIN=/path/to/bin` can override it.

## Suites

- `e2e/` — cold boot, tapfd networking, snapshot/restore, manifest/cache/store,
  node orchestration, e2b-compatible execution, and cluster stub coverage.
- `perf/` — sandbox latency, manifest path latency, and density harnesses.
- `demo/` — e2b SDK walkthrough driven by `demo_prep.sh` and `demo_e2b.sh`.

See `QUICKSTART.md` for release-package usage and prerequisite details.
