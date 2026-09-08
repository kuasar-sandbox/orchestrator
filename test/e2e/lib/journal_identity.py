#!/usr/bin/env python3
"""Check native sandbox-ctl records captured around one real owner E2E case."""
import json
import sys
from collections import defaultdict

kind, filename = sys.argv[1:]
sandbox_tags = defaultdict(set)
build_tags = defaultdict(set)
independent_stable = False
count = 0
with open(filename, encoding="utf-8") as source:
    for line in source:
        row = json.loads(line)
        tag = row.get("SYSLOG_IDENTIFIER")
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
            sandbox_tags[(sid, stable, run)].add(tag)
            independent_stable |= sid != stable
assert count, (kind, "no native sandbox-ctl journal records in this invocation")
if kind == "build":
    assert any({"build", "console", "sandbox-ctl"} <= tags for tags in build_tags.values()), (
        kind, "no Build attempt has application, console and component records"
    )
else:
    assert any({"sandbox", "console", "sandbox-ctl"} <= tags for tags in sandbox_tags.values()), (
        kind, "no sandbox attempt has application, console and component records"
    )
    if kind == "cluster":
        assert independent_stable, "cluster did not exercise distinct stable and node-local identities"
print(f"PASS: {kind} native journal identities; {count} records, "
      f"{len(sandbox_tags)} sandbox attempts, {len(build_tags)} Build attempts")
