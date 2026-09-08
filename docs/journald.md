[English](journald.md) | [简体中文](journald_zh.md)

# Journal identities for sandboxes and Builds

This guide supplements the logging section of [the node reference](node.md).
The runtime target syntax is specified by [sandboxer's journal guide](https://github.com/kuasar-sandbox/sandboxer/blob/main/docs/journald.md).

## Ownership and output targets

Orchestrator owns the meaning and selection of business identity fields.
Sandboxer receives independent `journald=TAG[,FIELD=VALUE...]` output targets;
it neither reads these fields from environment variables nor infers them from
unit names, directories, templates or snapshots. There is no process-global
`--journal-field` option and no inheritance between outputs.

For each managed sandbox attempt, Conductor creates four complete targets:

| Argument | Identifier | Identity fields |
| --- | --- | --- |
| `--log-to` | `sandbox-ctl` | `KUASAR_STABLE_ID`, `KUASAR_SANDBOX_ID`, `KUASAR_RUN_ID` |
| `--stdout-to` | `sandbox` | The same three explicitly supplied fields |
| `--stderr-to` | `sandbox` | The same three explicitly supplied fields |
| `--console` | `console` | The same three explicitly supplied fields |

The values come directly from `Sandbox.StableID()`, `Sandbox.ID` and the current
`Sandbox.RunID`. Ordinary standalone sandboxes therefore also have a StableID,
falling back to their local ID. A separately supplied StableID or one preserved
by import does not require cluster ownership. A new sandbox using the same
artifact receives its own identity; artifact contents are not a log-field source.

A same-node resume retains logical identity and uses its newly assigned RunID.
Identity-preserving import/migration can retain StableID while changing local
SandboxID and RunID. StableID is an aggregation key, not a unique node-local
lookup key: an identity-preserving copy may leave multiple instances alive.

## Build output

Every Build phase `run` target explicitly carries `KUASAR_BUILD_ID` and
`KUASAR_RUN_ID`, from the current `BuildSpec`. Its component identifier is
`sandbox-ctl`, application identifier is `build`, and console identifier is
`console`. Build outputs do not synthesize a sandbox StableID or reuse a phase
sandbox ID as the Build identity.

The four journal-routed flatten/export/referrer `exec` call sites also receive
complete `build` targets. Artifact/file destinations and captured stderr remain
unchanged; `exec` process diagnostics remain available to the existing error-tail
capture. The SDK continues to consume the `build` stream, not console or runtime
component diagnostics. Run-builder's own milestones and Envd RUN replay retain
their existing native Build fields.

## Encoding and queries

The formatter sorts keys and uses Go `url.PathEscape` for each opaque value.
The runtime splits fields before applying `url.PathUnescape` exactly once.
Commas and percent signs cannot inject a field, `+` remains a literal plus, and
an explicit empty value is not an instruction to read an environment variable.
For example, the value `worker,42%2C+east` is encoded as
`worker%2C42%252C+east`.

```bash
# All sandbox native streams for a stable logical identity on this host.
journalctl KUASAR_STABLE_ID=worker-42

# One local execution, including component diagnostics.
journalctl KUASAR_SANDBOX_ID=worker-42-g1 KUASAR_RUN_ID=run-17

# Application-only or component-only views.
journalctl SYSLOG_IDENTIFIER=sandbox KUASAR_STABLE_ID=worker-42
journalctl SYSLOG_IDENTIFIER=sandbox-ctl KUASAR_STABLE_ID=worker-42

# SDK-visible Build stream.
journalctl SYSLOG_IDENTIFIER=build KUASAR_BUILD_ID=build-17
```

Queries operate on the journal being read; cross-node collection is the
platform operator's responsibility. This change does not add an exporter or a
central log store.

## Boundaries and verification

No guest environment, StdioSpec/MUX wire, public API, metadata/header, snapshot
format or authentication identity is changed. `MANIFEST_KEY` stays on the
existing secret delivery path and is never included in output targets.
`--log-to` only selects runtime component diagnostics: it does not replace
process descriptors or capture Cloud Hypervisor stderr, runtime panics,
node-ctl pre-exec messages or systemd events.

Native sends are synchronous and best-effort. Successful sends are not copied
to stderr; failed sends fall back to the original sink. No exactly-once durable
logging or zero-blocking guarantee is implied.

Tests cover generated runner/Build arguments, fallback and independent StableID,
resume/import/new-object isolation, escaping and file/capture preservation.
The owner E2E runner brackets its real sandbox, cluster and Build cases with
journal cursors and filters by the exact sandbox-ctl executable, then validates
native JSON fields across application, console and component outputs. Source
workspaces also run the complete module unit tests, race tests and vet; binary
packages do not require a Go toolchain. Sandboxer's companion suite exercises
cold/restore/exec native output, partial tails, independent same-tag fields and
startup failure without deliberate stderr duplication.
