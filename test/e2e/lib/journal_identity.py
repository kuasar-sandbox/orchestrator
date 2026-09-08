#!/usr/bin/env python3
"""Validate identities on native records from real owner lifecycle cases."""
import json
import sys
from collections import Counter, defaultdict


def validate(kind, rows):
    if kind not in ("sandbox", "cluster", "build"):
        raise ValueError("unknown journal case: " + kind)
    sandbox_tags = defaultdict(set)
    build_tags = defaultdict(set)
    stable_by_run = {}
    observed = Counter()
    count = 0
    for row in rows:
        tag = row.get("SYSLOG_IDENTIFIER")
        observed[(str(row.get("_TRANSPORT")), str(tag))] += 1
        if row.get("_TRANSPORT") != "journal" or tag not in (
            "sandbox", "build", "console", "sandbox-ctl"
        ):
            continue
        count += 1
        run = row.get("KUASAR_RUN_ID")
        assert isinstance(run, str) and run, (kind, tag, "missing RunID")
        if "KUASAR_BUILD_ID" in row:
            build = row["KUASAR_BUILD_ID"]
            assert isinstance(build, str) and build, (kind, tag, "missing BuildID")
            assert "KUASAR_SANDBOX_ID" not in row and "KUASAR_STABLE_ID" not in row, (
                kind, tag, "sandbox identity leaked into Build"
            )
            assert tag != "sandbox", (kind, "Build output used sandbox tag")
            build_tags[(build, run)].add(tag)
        else:
            sid, stable = row.get("KUASAR_SANDBOX_ID"), row.get("KUASAR_STABLE_ID")
            assert isinstance(sid, str) and sid, (kind, tag, "missing SandboxID")
            assert isinstance(stable, str) and stable, (kind, tag, "missing StableID")
            assert tag != "build", (kind, "sandbox output used Build tag")
            assert stable_by_run.setdefault((sid, run), stable) == stable, (
                kind, "one sandbox attempt has conflicting StableIDs"
            )
            sandbox_tags[(sid, stable, run)].add(tag)

    # Print counts and identities, not guest messages or complete journal rows.
    print(f"Journal coverage ({kind}): transports/tags={dict(observed)}", flush=True)
    for identity, tags in sorted(sandbox_tags.items()):
        print(f"  Sandbox (local, stable, run)={identity}: {sorted(tags)}", flush=True)
    for identity, tags in sorted(build_tags.items()):
        print(f"  Build (build, run)={identity}: {sorted(tags)}", flush=True)
    assert count, (kind, "no native sandbox-ctl journal records in this invocation")
    if kind == "build":
        assert any({"build", "console", "sandbox-ctl"} <= tags for tags in build_tags.values()), (
            kind, "no Build attempt has application, console and component records"
        )
    elif kind == "cluster":
        # This fixture deliberately writes through the primary guest pipes.
        assert any(sid != stable and {"sandbox", "console", "sandbox-ctl"} <= tags
                   for (sid, stable, _), tags in sandbox_tags.items()), (
            kind, "no distinct stable/local identity has all three native streams"
        )
    else:
        # e2e_execute uses quiet envd and captures exec output separately.
        # Silence is valid. Emitting application streams are mandatory in the
        # cluster fixture above and sandboxer's cold/restore/exec native suite.
        # Still require live native streams proving the standalone fallback.
        assert any(sid == stable and {"console", "sandbox-ctl"} <= tags
                   for (sid, stable, _), tags in sandbox_tags.items()), (
            kind, "no standalone attempt proves default StableID on native streams"
        )
    print(f"PASS: {kind} native journal identities; {count} records, "
          f"{len(sandbox_tags)} sandbox attempts, {len(build_tags)} Build attempts")


if __name__ == "__main__":
    with open(sys.argv[2], encoding="utf-8") as source:
        validate(sys.argv[1], (json.loads(line) for line in source))
