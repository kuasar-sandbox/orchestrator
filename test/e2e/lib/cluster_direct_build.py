#!/usr/bin/env python3
"""Build through an empty direct Group, then CAS-install its canonical default."""
import json
from pathlib import Path
import urllib.parse
import urllib.request

import build_client as build
from group_readiness import wait_for_group


def main():
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

    published = wait_for_group(args.node_url, args.group, registry_urls=args.registry_url,
                               api_key=build.key, node_host=args.host, node_id=args.node_id)
    original = group_request("GET")
    assert original["version"] == published["version"] and not original["context"].get("templateID"), \
        "onboarding fixture must start with a published Group without a default template"
    tid, bid = build.register("direct-onboarding")
    build.require("POST", f"/v2/templates/{tid}/builds/{bid}", 202,
                  {"fromImage": args.source, "steps": [{"type": "RUN", "args": ["echo DIRECT_ROUTED_BUILD"]}]})
    ready = build.wait_for("empty-Group routed Build completes", lambda: build.check_terminal(tid, bid, "ready"), 300)
    canonical = ready["templateID"]
    assert canonical.startswith("e2b-img-"), "routed Build did not return a canonical image"
    stored = build.row(bid)
    assert stored and stored["persist_id"] == canonical and stored["execution_claimed"] == 0, \
        "routed Build did not persist its final artifact and release execution"
    updated = group_request("PUT", {"expected_version": original["version"],
                                    "context": {**original["context"], "templateID": canonical}})
    assert updated["version"] > original["version"], "setting the default did not advance Group revision"
    wait_for_group(args.node_url, args.group, require_template=True, registry_urls=args.registry_url,
                   api_key=build.key, node_host=args.host, node_id=args.node_id)
    build.require("DELETE", "/templates/" + tid, 204)
    assert build.row(bid) is None, "deleted transient Build record remains locally"
    Path(args.evidence).write_text(json.dumps({"canonical": canonical,
        "empty_group_build": True, "default_set_by_cas": True, "transient_deleted": True}) + "\n")
    print("PASS: empty direct Group -> Router Build -> canonical default CAS -> published Group")


if __name__ == "__main__":
    main()
