#!/usr/bin/env python3
"""One bounded public UDS export request, shared by both measurement modes."""
import argparse
import base64
import http.client
import json
import math
import os
import re
import signal
import socket
import sys


MAX_RESPONSE_BYTES = 1024 * 1024


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate export response field")
        result[key] = value
    return result


def validate_response(raw):
    result = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(result, dict):
        raise ValueError("unexpected export response fields")
    required = {"result", "sandboxRef"}
    groups = {"sandboxRefs": "sandboxRef"}
    if "snapshotRef" in result:
        required.add("snapshotRef")
        groups["snapshotRefs"] = "snapshotRef"
    legacy = "refs" not in result
    if legacy:
        required.add("removedRefs")
    else:
        if type(result["refs"]) is not bool:
            raise ValueError("refs must be a boolean")
        required.add("refs")
        if result["refs"]:
            required.update(groups)
            required.add("removedRefs")
    if set(result) != required:
        raise ValueError("unexpected export response fields")
    root = result.get("snapshotRef", result["sandboxRef"])
    # The measurement fixture publishes to its manifest store. A local file
    # reference must never be accepted as a portable publication in this flow.
    for field in ("sandboxRef", "snapshotRef"):
        if field in result and (not isinstance(result[field], str) or not re.fullmatch(r"manifest://[0-9a-f]{64}", result[field])):
            raise ValueError("export response has a nonportable manifest root")
    kind = "snp" if "snapshotRef" in result else "sbx"
    template = "e2b-" + kind + "-" + base64.urlsafe_b64encode(root.encode()).decode().rstrip("=")
    if result["result"] != template:
        raise ValueError("export template does not match its publication root")
    def reference_set(field):
        refs = result[field]
        if not isinstance(refs, list) or any(not isinstance(ref, str) or not ref or ref.strip() != ref
                                            or any(ord(char) < 32 for char in ref) for ref in refs):
            raise ValueError("invalid " + field + " array")
        if refs != sorted(set(refs)):
            raise ValueError(field + " must be sorted and unique")
        return set(refs)

    final_refs = {result["sandboxRef"], root}
    if result.get("refs") is True:
        for group, field in groups.items():
            refs = reference_set(group)
            if result[field] not in refs:
                raise ValueError(group + " must include its final root")
            final_refs.update(refs)
    if legacy or result["refs"]:
        if reference_set("removedRefs") & final_refs:
            raise ValueError("removedRefs must exclude final references")
    if legacy:
        # A legacy difference cannot establish a complete dependency set.
        result.pop("removedRefs")
        result["refs"] = False
    return result


def export(socket_path, sid, timeout, api_key):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("export timeout must be a positive finite duration")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", sid):
        raise ValueError("invalid sandbox ID")
    if not api_key or "\r" in api_key or "\n" in api_key:
        raise ValueError("E2B_API_KEY is required and must be a single line")
    connection = UnixHTTPConnection(socket_path, timeout)
    previous_handler = signal.getsignal(signal.SIGALRM)

    def expired(_signum, _frame):
        raise TimeoutError("export request exceeded its total deadline")

    # socket timeouts alone can restart for each read. This process-level timer
    # also bounds a slow response body and is canceled before returning.
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        connection.request("POST", f"/sandboxes/{sid}/export",
                           body=json.dumps({"toTemplate": True, "keepSource": True}),
                           headers={"Content-Type": "application/json", "X-API-KEY": api_key})
        response = connection.getresponse()
        if response.status != 200:
            raise ValueError(f"export API returned HTTP {response.status}")
        raw = response.read(MAX_RESPONSE_BYTES + 1)
        if len(raw) > MAX_RESPONSE_BYTES:
            raise ValueError("export response exceeds 1 MiB")
        return validate_response(raw)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--sid", required=True)
    parser.add_argument("--timeout", type=float, required=True, help="total request deadline in seconds; no retries")
    args = parser.parse_args()
    try:
        result = export(args.socket, args.sid, args.timeout, os.environ.get("E2B_API_KEY", ""))
    except (ValueError, OSError, http.client.HTTPException) as error:
        # Do not echo request headers or server response bodies to diagnostics.
        message = str(error) if type(error) is ValueError or isinstance(error, TimeoutError) else type(error).__name__
        print("export API request failed: " + message, file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
