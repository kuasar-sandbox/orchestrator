#!/usr/bin/env python3
"""Wait until the local placer can select the expected Build node."""

import argparse
import json
import time
import urllib.error
import urllib.request


BUILD_RESOURCES = {"cpu": 2000, "memory": 6442450944}


def wait_for_placer(url, group, expected_node, timeout=60, *,
                    opener=None, monotonic=time.monotonic, sleep=time.sleep):
    """Poll the read-only placement computation until it selects expected_node."""
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
    payload = json.dumps({
        "req_id": "e2e-placer-readiness",
        "group": group,
        "route_key": "e2e-placer-readiness",
        "build": True,
        "build_resources": BUILD_RESOURCES,
    }).encode()
    deadline = monotonic() + timeout
    last = "no response"

    while True:
        request = urllib.request.Request(
            url.rstrip("/") + "/placer-link/place", data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
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
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    value = wait_for_placer(args.url, args.group, args.expected_node, args.timeout)
    print("==> placer ready: " + json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
