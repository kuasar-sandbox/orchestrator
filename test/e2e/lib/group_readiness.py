#!/usr/bin/env python3
"""Wait for this Node's committed Group to be published by its current registration."""

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request


def wait_for_group(node_url, group, timeout=60, *, require_template=False,
                   registry_urls=(), api_key=None, node_host=None, node_id=None, after_fence=None, opener=None,
                   monotonic=time.monotonic, sleep=time.sleep):
    """Observe committed/published state only; the subsequent write checks admission."""
    if not registry_urls or not api_key or not node_id:
        raise ValueError("Group readiness requires Registry URLs, Node ID and the fixture API key")
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}))
    query = urllib.parse.urlencode({"group": group})
    status_query = urllib.parse.urlencode({"group": group, "node_id": node_id})
    deadline = monotonic() + timeout
    last = "no response"

    def read(url, *, host=None):
        headers = {"X-API-KEY": api_key}
        if host:
            headers["Host"] = host
        request = urllib.request.Request(url, headers=headers, method="GET")
        with opener.open(request, timeout=min(2, max(0.1, deadline - monotonic()))) as response:
            if response.status != 200:
                raise urllib.error.URLError("Group observation is unavailable")
            return json.load(response)

    def local_version():
        value = read(node_url.rstrip("/") + "/groups?" + query, host=node_host)
        valid = (isinstance(value, dict) and value.get("group") == group
                 and type(value.get("version")) is int and value["version"] > 0
                 and isinstance(value.get("context"), dict))
        if valid and require_template:
            template = value["context"].get("templateID")
            valid = isinstance(template, str) and bool(template)
        if not valid:
            raise ValueError("Node Group is absent, invalid or lacks the fixture default")
        # Context can contain private credentials. Return the revision only.
        return value["version"]

    while True:
        try:
            version = local_version()
            registration = None
            for registry_url in registry_urls:
                status = read(registry_url.rstrip("/") + "/route-link/v2/group-status?" + status_query)
                if not isinstance(status, dict) or status.get("group") != group or status.get("storage_readable") is not True:
                    raise ValueError("Registry Group observation is invalid")
                current = status.get("registration")
                if (status.get("node_id") != node_id or status.get("published") is not True
                        or status.get("group_version") != version or not isinstance(current, dict)
                        or current.get("node_id") != node_id or not current.get("fence")
                        or not current.get("link_owner")):
                    raise ValueError("Current Node registration has not published the committed Group")
                if registration is not None and current != registration:
                    raise ValueError("Registries observed different Node registrations")
                registration = current
            if after_fence is not None and registration["fence"] == after_fence:
                raise ValueError("Node has not registered again after the fixture restart")
            if local_version() != version:
                raise ValueError("Node Group changed during publication checks")
            return {"group": group, "node_id": node_id, "version": version,
                    "registration": registration}
        except (OSError, urllib.error.URLError, ValueError) as error:
            # Do not echo exception text: HTTP errors or private Group context
            # must never enter CI logs. The state machine is safe to identify.
            last = "Group publication query failed: " + type(error).__name__
        if monotonic() >= deadline:
            raise AssertionError("timeout waiting for Group publication: " + last)
        sleep(min(0.1, max(0, deadline - monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--node-url", required=True)
    parser.add_argument("--node-host", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--after-fence", help="require a new registration after a known restart")
    parser.add_argument("--fence-file", help="record the observed non-secret registration fence")
    parser.add_argument("--group", required=True)
    parser.add_argument("--registry-url", action="append", required=True)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    value = wait_for_group(args.node_url, args.group, args.timeout,
                           require_template=True, registry_urls=args.registry_url,
                           api_key=os.environ["GROUP_API_KEY"], node_host=args.node_host,
                           node_id=args.node_id, after_fence=args.after_fence)
    if args.fence_file:
        with open(args.fence_file, "x", encoding="utf-8") as output:
            output.write(value["registration"]["fence"] + "\n")
    print("==> Group publication ready: " + json.dumps(value), flush=True)


if __name__ == "__main__":
    main()
