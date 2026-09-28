#!/usr/bin/env python3
"""Wait for Registry discovery and the expected local placement."""

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


BUILD_RESOURCES = {"cpu": 2000, "memory": 6442450944}


def wait_for_placer(url, group, expected_node, timeout=60, *,
                    build=True, route_key="e2e-placer-readiness",
                    registry_urls=(), api_key=None,
                    opener=None, monotonic=time.monotonic, sleep=time.sleep):
    """Poll read-only APIs; never use a sandbox or Build write as readiness."""
    if registry_urls and not api_key:
        raise ValueError("Registry readiness requires the fixture API key")
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
    placement = {
        "req_id": "e2e-placer-readiness",
        "group": group,
        "route_key": route_key,
        "build": build,
    }
    if build:
        placement["build_resources"] = BUILD_RESOURCES
    payload = json.dumps(placement).encode()
    deadline = monotonic() + timeout
    last = "no response"

    while True:
        try:
            # Local node_list readiness precedes the separately advertised
            # memberlist Ready metadata. Verify-key uses the same ready peer
            # selection as Reserve, without creating/reserving anything.
            for registry_url in registry_urls:
                request = urllib.request.Request(
                    registry_url.rstrip("/") + "/route-link/verify-key?"
                    + urllib.parse.urlencode({"group": group}),
                    headers={"X-API-KEY": api_key}, method="GET")
                with opener.open(request, timeout=min(2, max(0.1, timeout))) as response:
                    if response.status != 200:
                        raise urllib.error.URLError(
                            f"Registry {registry_url}: HTTP {response.status}")
            request = urllib.request.Request(
                url.rstrip("/") + "/placer-link/place", data=payload,
                headers={"Content-Type": "application/json"}, method="POST")
            with opener.open(request, timeout=min(2, max(0.1, timeout))) as response:
                raw = response.read()
                if response.status != 200:
                    last = f"HTTP {response.status}: {raw.decode(errors='replace')}"
                else:
                    try:
                        value = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as error:
                        last = f"malformed response: {error}"
                    else:
                        if (isinstance(value, dict) and not value.get("error")
                                and not value.get("no_node")
                                and value.get("node_id") == expected_node):
                            return value
                        last = f"not ready: {value!r}"
        except (OSError, urllib.error.URLError) as error:
            last = f"request failed: {error}"

        if monotonic() >= deadline:
            raise AssertionError(
                f"timeout waiting for placer to select {expected_node!r}: {last}")
        sleep(min(0.1, max(0, deadline - monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--expected-node", required=True)
    parser.add_argument("--route-key", required=True)
    parser.add_argument("--registry-url", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    value = wait_for_placer(
        args.url, args.group, args.expected_node, args.timeout,
        build=False, route_key=args.route_key, registry_urls=args.registry_url,
        api_key=os.environ["PLACER_API_KEY"])
    print("==> placer ready: " + json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
