#!/usr/bin/env python3
"""Observe legacy Snapshot rejection through the actual public cluster path."""
import base64
import json
import os
import pathlib
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import urllib.parse

from runtime_variant import identity


def main(diagnostic):
    work = pathlib.Path(os.environ["LEGACY_TEST_WORK"])
    out = pathlib.Path(os.environ["LEGACY_TEST_OUT"])
    template = os.environ["LEGACY_TEST_TEMPLATE"]
    assert template.startswith("e2b-snp-")
    encoded = template.removeprefix("e2b-snp-")
    reference = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)).decode()
    assert "@runtime:" not in reference
    headers = {
        "Host": os.environ["LEGACY_TEST_HOST"],
        "X-API-Key": os.environ["LEGACY_TEST_API_KEY"],
        "X-Kuasar-Sandbox-Group": os.environ["LEGACY_TEST_GROUP"],
        "X-Kuasar-Route-Key": os.environ["LEGACY_TEST_ROUTE"],
        "Content-Type": "application/json",
    }
    # Prove the product's startup inventory reader accepted the valid variant.
    # A malformed bundle would be omitted and cannot satisfy this query.
    diagnostic["phase"] = "variant-identity"
    _, _, _, _, digest = identity(work / "incompatible-runtime/runtime-v2.bundle")
    annotated = reference + "@runtime:runtime-v2.bundle:digest:" + digest
    diagnostic_template = "e2b-snp-" + base64.urlsafe_b64encode(annotated.encode()).decode().rstrip("=")
    query = urllib.parse.urlencode({"group": os.environ["LEGACY_TEST_GROUP"],
        "node_id": os.environ["LEGACY_TEST_NODE_ID"], "template_id": diagnostic_template})
    diagnostic["phase"] = "inventory"
    status_request = urllib.request.Request(os.environ["LEGACY_TEST_REGISTRY"] +
        "/route-link/v2/group-status?" + query, headers={"X-API-Key": os.environ["LEGACY_TEST_API_KEY"]})
    with urllib.request.urlopen(status_request, timeout=10) as response:
        observed = json.load(response)
    diagnostic.update(inventory_published=bool(observed.get("published")),
                      inventory_runtime_match=observed.get("runtime_match") is True)
    assert observed.get("published") and observed.get("runtime_match") is True, "product inventory rejected the valid runtime variant"
    diagnostic["phase"] = "public-create"
    start = time.time()
    request = urllib.request.Request(
        os.environ["LEGACY_TEST_ROUTER"] + "/sandboxes",
        headers=headers,
        data=json.dumps({"templateID": template, "timeout": 60}).encode(),
        method="POST",
    )
    try:
        response = urllib.request.urlopen(request, timeout=90)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        status = response.code
        raw = response.read().decode(errors="replace")
    diagnostic.update(phase="private-response", public_status=status)
    # Failed responses can contain capabilities or local paths; keep them private.
    response_path = work / "legacy-create-response.private"
    response_path.write_text(raw)
    response_path.chmod(0o600)
    # Router retries may see a retained dead slot. Status alone proves nothing.
    diagnostic["phase"] = "public-status"
    assert status in (409, 500, 503, 504), ("unexpected public response", status)
    diagnostic["phase"] = "node-rows"
    with sqlite3.connect(work / "cl/node-ctl.db") as db:
        db.row_factory = sqlite3.Row
        rows = [dict(row) for row in db.execute(
            "select id,stable_id,run_id,dead_run_id,state from sandboxes"
        )]
    diagnostic["node_row_count"] = len(rows)
    assert rows, "the selected Node never accepted the Create"
    diagnostic["phase"] = "node-log"
    node_log = (work / "cluster-node.log").read_text(errors="replace")
    # Read only this fixture's sandbox and exact runner-unit journals.
    diagnostic.update(phase="journal", journal_reads=0)
    journal = ""
    for row in rows:
        filters = [["KUASAR_SANDBOX_ID=" + row["id"]]]
        run_id = row["run_id"] or row["dead_run_id"]
        if run_id:
            filters.append(["-u", "sandbox-runner@" + run_id + ".service"])
        for match in filters:
            journal += subprocess.run(
                ["journalctl", *match, "--since", "@" + str(int(start) - 2),
                 "--no-pager", "-o", "cat"],
                text=True, capture_output=True, check=True,
            ).stdout
            diagnostic["journal_reads"] += 1
    needles = ("no compatible runtime bundle", "identity mismatch")
    lines = [line for line in (journal + "\n" + node_log).splitlines()
             if any(needle in line for needle in needles)]
    evidence = "\n".join(lines)
    diagnostic.update(phase="runtime-evidence",
                      runtime_rejection_present="no compatible runtime bundle" in evidence,
                      digest_mismatch_present="identity mismatch" in evidence,
                      captured_runtime_present="runtime-v1.bundle" in evidence)
    assert "no compatible runtime bundle" in evidence, "missing Node runtime rejection"
    assert "identity mismatch" in evidence, "missing wrong-digest evidence"
    assert "runtime-v1.bundle" in evidence, "missing captured runtime fallback evidence"
    diagnostic.update(phase="fallback",
                      fallback_absent=not (work / "incompatible-runtime/runtime-v1.bundle").exists())
    assert diagnostic["fallback_absent"]
    diagnostic.update(phase="node-state",
                      all_nodes_nonrunning=all(row["state"] != "running" for row in rows))
    assert diagnostic["all_nodes_nonrunning"], rows
    diagnostic["phase"] = "receipt"
    receipt = {
        "case": "C02-public-cluster-legacy-runtime",
        "public_status": status,
        "legacy_reference_unannotated": True,
        "capture_provenance": "current real VM capture; external runtime annotation omitted",
        "variant_inventory_verified": True,
        "explicit_template": True,
        "selected_node": "legacy-incompatible-node",
        "node_runtime_rejected": True,
        "captured_runtime": "runtime-v1.bundle",
        "configured_runtime": "runtime-v2.bundle",
        "fallback_absent": True,
        "node_rows": rows,
        "runtime_rejection_lines": len(lines),
        "scope": "actual Router, Registry/FDB, NodeLink, Node SQLite and existing "
                 "runtime preflight; expected rejection before a restored VM becomes ready",
    }
    (out / "legacy-runtime-results.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt), file=sys.stderr)


if __name__ == "__main__":
    # Failure output must contain only fixed field names, fixed categories,
    # booleans and counts. Never include exception text, response bodies,
    # references, sandbox identities or journal lines in the public diagnostic.
    diagnostic = {"case": "C02-public-cluster-legacy-runtime", "phase": "reference"}
    try:
        main(diagnostic)
    except Exception as error:
        category = "unexpected"
        for kind, name in (
            (AssertionError, "assertion"),
            (urllib.error.HTTPError, "http"),
            (urllib.error.URLError, "transport"),
            (sqlite3.Error, "database"),
            (subprocess.CalledProcessError, "journal"),
            (OSError, "io"),
            (ValueError, "decode"),
        ):
            if isinstance(error, kind):
                category = name
                break
        diagnostic["error"] = category
        frame = error.__traceback__
        while frame is not None:
            if (frame.tb_frame.f_code.co_filename == __file__
                    and frame.tb_frame.f_code.co_name == "main"):
                diagnostic["line"] = frame.tb_lineno
                break
            frame = frame.tb_next
        try:
            print("FAIL: cluster legacy runtime " + json.dumps(diagnostic, sort_keys=True),
                  flush=True)
        except Exception:
            pass  # A closed public output stream must not replace the real failure.
        try:
            path = pathlib.Path(os.environ["LEGACY_TEST_OUT"]) / "legacy-runtime-failure.json"
            path.write_text(json.dumps(diagnostic, sort_keys=True) + "\n")
        except Exception:
            pass  # Preserve the original failure even when diagnostics cannot be written.
        raise
