#!/usr/bin/env python3
"""Build through an empty direct Group, then CAS-install its canonical default."""
import json
from pathlib import Path
import urllib.parse
import urllib.request

import build_client as build
from group_readiness import wait_for_group


def _report_failure(error):
    # Public output contains only code-owned phases, bounded HTTP status codes
    # and exception categories. Response bodies and traceback stay private.
    phase, line = "setup", 0
    observed_status, expected_status = None, None
    frame = error.__traceback__
    while frame is not None:
        code = frame.tb_frame.f_code
        values = frame.tb_frame.f_locals
        if code.co_filename == __file__ and code.co_name == "main":
            phase = values.get("phase", "setup")
            line = frame.tb_lineno
        if code.co_filename == build.__file__ and code.co_name == "require":
            observed, expected = values.get("code"), values.get("want")
            if type(observed) is int and 100 <= observed <= 599:
                observed_status = observed
            if type(expected) is int and 100 <= expected <= 599:
                expected_status = expected
        frame = frame.tb_next
    kind = type(error).__name__
    if kind not in {"AssertionError", "TimeoutError", "TimeoutExpired",
                    "CalledProcessError", "HTTPError", "URLError",
                    "RemoteDisconnected", "ConnectionError", "ConnectionResetError",
                    "OSError", "FileNotFoundError", "PermissionError",
                    "JSONDecodeError", "OperationalError", "ValueError", "KeyError"}:
        kind = "Exception"
    status = ""
    if observed_status is not None:
        status += f" http_status={observed_status}"
    if expected_status is not None:
        status += f" expected_status={expected_status}"
    print(f"FAIL: cluster direct Build phase={phase} exception={kind} "
          f"source=cluster_direct_build.py line={line}{status}", flush=True)


def main():
    phase = "setup"
    args = build.configure()
    group_url = args.node_url.rstrip("/") + "/groups?" + urllib.parse.urlencode({"group": args.group})

    def group_request(method, body=None):
        headers = {"Host": args.host, "X-API-KEY": build.key}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(group_url, method=method, headers=headers,
                                         data=None if body is None else json.dumps(body).encode())
        with build.opener.open(request, timeout=10) as response:
            assert response.status == 200, "direct Group request failed"
            return json.load(response)

    phase = "group-publication"
    published = wait_for_group(args.node_url, args.group, registry_urls=args.registry_url,
                               api_key=build.key, node_host=args.host, node_id=args.node_id)
    phase = "group-read"
    original = group_request("GET")
    assert original["version"] == published["version"] and not original["context"].get("templateID"), \
        "onboarding fixture must start with a published Group without a default template"
    phase = "register-build"
    tid, bid = build.register("direct-onboarding")
    phase = "start-build"
    build.require("POST", f"/v2/templates/{tid}/builds/{bid}", 202,
                  {"fromImage": args.source, "steps": [{"type": "RUN", "args": ["echo DIRECT_ROUTED_BUILD"]}]})
    phase = "build-completion"
    ready = build.wait_for("empty-Group routed Build completes", lambda: build.check_terminal(tid, bid, "ready"), 300)
    phase = "guest-run-evidence"
    build.wait_for("routed Build RUN reached the guest", lambda: "DIRECT_ROUTED_BUILD" in build.command(
        "journalctl", "KUASAR_BUILD_ID=" + bid, "--no-pager", "--output=cat").splitlines(), 30)
    phase = "artifact-persistence"
    canonical = ready["templateID"]
    assert canonical.startswith("e2b-img-"), "routed Build did not return a canonical image"
    stored = build.row(bid)
    assert stored and stored["persist_id"] == canonical and stored["execution_claimed"] == 0, \
        "routed Build did not persist its final artifact and release execution"
    phase = "default-cas"
    updated = group_request("PUT", {"expected_version": original["version"],
                                    "context": {**original["context"], "templateID": canonical}})
    assert updated["version"] > original["version"], "setting the default did not advance Group revision"
    phase = "default-publication"
    wait_for_group(args.node_url, args.group, require_template=True, registry_urls=args.registry_url,
                   api_key=build.key, node_host=args.host, node_id=args.node_id)
    phase = "delete-transient"
    build.require("DELETE", "/templates/" + tid, 204)
    assert build.row(bid) is None, "deleted transient Build record remains locally"
    phase = "receipt"
    Path(args.evidence).write_text(json.dumps({"canonical": canonical,
        "empty_group_build": True, "default_set_by_cas": True, "transient_deleted": True}) + "\n")
    print("PASS: empty direct Group -> Router Build -> canonical default CAS -> published Group")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        try:
            _report_failure(error)
        except Exception:
            pass
        raise
