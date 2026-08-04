# Working-set snapshot performance evidence (2026-08-04)

This directory preserves the canonical performance evidence used to close
[`sandboxer#54`](https://github.com/kuasar-sandbox/sandboxer/issues/54) and
[`sandboxer#41`](https://github.com/kuasar-sandbox/sandboxer/issues/41).

- Workflow: [`30907661187`](https://github.com/kuasar-sandbox/orchestrator/actions/runs/30907661187)
- Orchestrator candidate: `d5c77bacaaf303852047ddb627e1b1ccc86cad24`
- Actions artifact: `ci-metadata-30907661187-1`
- Matrix: `A/off`, `B/off`, `C/off`, `D/off`, and `D/memory`, with 30
  independent restores per scenario (150 samples total)
- Statistics: nearest-rank p50/p95/p99, with no performance pass/fail threshold

Files:

- [`report.md`](report.md) is the generated human-readable report;
- [`environment.json`](environment.json) records the exact five-repository
  revision set, binary and kernel digests, host, image, and test inputs;
- [`samples.jsonl`](samples.jsonl) contains the original structured samples.

The report was regenerated from the committed environment and samples with
`test/perf/working_set_report.py` and compared byte-for-byte with the CI
artifact. The report validator also requires a complete, balanced matrix and
the exact five Cloud Hypervisor backend roles for every row. Per-iteration
service logs and stats snapshots remain in the named Actions artifact; the
structured source samples required to reproduce every table percentile are
committed here.

SHA-256:

```text
73989738f3ef2e5414a640cb94bd93faceaf19aa180d4a20eac058ca333edef6  environment.json
ac535aaa744602350607282d01143a0a72b3f827dc29a29a8feae88491a35cb9  samples.jsonl
c5bb0a37872e03ab5fe924995206226808f59dae37b0de7c64e39ad6ecfd5270  report.md
```
