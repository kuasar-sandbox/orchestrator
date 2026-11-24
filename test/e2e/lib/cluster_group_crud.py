#!/usr/bin/env python3
"""Versioned Group CRUD through a real Node; private contexts are never printed."""
import argparse
import json
import os
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-url", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--group", required=True)
    args = parser.parse_args()
    url = args.node_url.rstrip("/") + "/groups?" + urllib.parse.urlencode({"group": args.group})
    headers = {"Host": args.host, "X-API-KEY": os.environ["GROUP_API_KEY"], "Content-Type": "application/json"}
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(method, expected, body=None, suffix=""):
        data = None if body is None else json.dumps(body).encode()
        try:
            response = opener.open(urllib.request.Request(url + suffix, data=data, headers=headers, method=method), timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            if response.status != expected:
                raise AssertionError("Group " + method + " HTTP " + str(response.status) + ", expected " + str(expected))
            return json.load(response)

    original = request("GET", 200)
    changed = {**original["context"], "metadata": {"fixture": "direct-update"}}
    updated = request("PUT", 200, {"expected_version": original["version"], "context": changed})
    assert updated["version"] > original["version"], "Group revision did not increase"
    request("PUT", 409, {"expected_version": original["version"], "context": original["context"]})
    current = request("GET", 200)
    assert current["version"] == updated["version"] and current["context"] == changed, "stale writer changed Group"
    tombstone = request("DELETE", 200, suffix="&expected_version=" + str(updated["version"]))
    absent = request("GET", 404)
    assert absent["version"] == tombstone["version"] > updated["version"], "Group tombstone revision missing"
    request("PUT", 409, {"expected_version": updated["version"], "context": original["context"]})
    recreated = request("PUT", 200, {"expected_version": tombstone["version"], "context": original["context"]})
    assert recreated["version"] > tombstone["version"], "Group recreation reused a revision"
    print("PASS: direct Group read/update/stale-write/delete/tombstone/recreate")


if __name__ == "__main__":
    main()
