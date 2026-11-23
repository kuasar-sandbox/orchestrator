#!/usr/bin/env python3
"""Wait for fixture Group import and Registry authentication after reconnect."""

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def wait_for_group(node_url, group, timeout=60, *, require_template=False,
                   registry_urls=(), api_key=None, node_host=None, opener=None,
                   monotonic=time.monotonic, sleep=time.sleep):
    """Read existing state only. Admission and capacity are checked by the write."""
    if not registry_urls or not api_key:
        raise ValueError("Group readiness requires Registry URLs and the fixture API key")
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
    query = urllib.parse.urlencode({"group": group})
    deadline = monotonic() + timeout
    last = "no response"
    while True:
        try:
            for registry_url in registry_urls:
                request = urllib.request.Request(
                    registry_url.rstrip("/") + "/route-link/v2/verify-key?" + query,
                    headers={"X-API-KEY": api_key}, method="GET")
                with opener.open(request, timeout=min(2, max(0.1, timeout))) as response:
                    if response.status != 200:
                        raise urllib.error.URLError(f"Registry authentication HTTP {response.status}")
            headers = {"X-API-KEY": api_key}
            if node_host:
                headers["Host"] = node_host
            request = urllib.request.Request(
                node_url.rstrip("/") + "/groups?" + query,
                headers=headers, method="GET")
            with opener.open(request, timeout=min(2, max(0.1, timeout))) as response:
                if response.status != 200:
                    raise urllib.error.URLError(f"Node Group HTTP {response.status}")
                value = json.load(response)
                valid = (isinstance(value, dict) and value.get("group") == group
                         and type(value.get("version")) is int and value["version"] > 0
                         and isinstance(value.get("context"), dict))
                if valid and require_template:
                    template = value["context"].get("templateID")
                    valid = isinstance(template, str) and bool(template)
                if valid:
                    # Context can contain private registry credentials. Never
                    # print it or include it in readiness diagnostics.
                    return {"group": value["group"], "version": value["version"]}
                last = "Node Group is absent, invalid or lacks the fixture default"
        except (OSError, urllib.error.URLError, ValueError) as error:
            last = f"Group query failed: {type(error).__name__}"
        if monotonic() >= deadline:
            raise AssertionError(f"timeout waiting for Group import: {last}")
        sleep(min(0.1, max(0, deadline - monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-url", required=True)
    parser.add_argument("--node-host", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--registry-url", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    value = wait_for_group(args.node_url, args.group, args.timeout,
                           require_template=True, registry_urls=args.registry_url,
                           api_key=os.environ["GROUP_API_KEY"], node_host=args.node_host)
    print("==> Group import ready: " + json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
