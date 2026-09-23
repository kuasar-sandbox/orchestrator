#!/usr/bin/env python3
"""Require real execution of the capture/publication CLI test groups."""
import os
from pathlib import Path
import re
import subprocess
import sys


GROUPS = (
    "TestCapturePairCLIToPausedDatabase",
    "TestUploadCaptureCLIToPausedDatabase",
    "TestExportPublicationCLIAPI",
)
PATTERN = "^(" + "|".join(GROUPS) + ")$"


def validate_results(output):
    started, finished = set(), {}
    for line in output.splitlines():
        match = re.fullmatch(r"=== RUN\s+(\S+)", line)
        if match:
            started.add(match[1])
        match = re.fullmatch(r"\s*--- (PASS|SKIP|FAIL): (\S+) \([^)]*\)", line)
        if match:
            finished[match[2]] = match[1]
    if {name.split("/")[0] for name in started} != set(GROUPS):
        raise ValueError("required CLI groups did not all run")
    if set(finished) != started or any(status != "PASS" for status in finished.values()):
        raise ValueError("CLI tests skipped, failed, or did not finish")
    if "PASS" not in output.splitlines():
        raise ValueError("CLI test executable did not report package PASS")
    for group in GROUPS:
        children = sorted(name for name in started if name.startswith(group + "/"))
        if group not in started or not children:
            raise ValueError(f"required CLI group has no executed subcases: {group}")
        print(f"==> {group}: PASS ({len(children)} current subcases)", flush=True)


def run(products, helper):
    products, helper = Path(products).resolve(), Path(helper).resolve()
    for path in (products / "sandbox-ctl", products / "node-ctl", helper):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"missing executable CLI test input: {path}")
    environment = {**os.environ, "KUASAR_TEST_SANDBOX_CTL": str(products / "sandbox-ctl")}
    listing = subprocess.run([str(helper), "-test.list=" + PATTERN], env=environment,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
    if listing.returncode or set(listing.stdout.splitlines()) != set(GROUPS):
        raise ValueError("CLI test executable does not select all required groups:\n" + listing.stdout)
    result = subprocess.run([str(helper), "-test.v", "-test.count=1", "-test.timeout=5m",
                             "-test.run=" + PATTERN], env=environment,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=330)
    print(result.stdout, end="", flush=True)
    if result.returncode:
        raise ValueError(f"CLI test executable failed: exit {result.returncode}")
    validate_results(result.stdout)


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            raise ValueError("usage: capture_cli.py <product-bin> <orch-cli.test>")
        run(*sys.argv[1:])
    except (ValueError, OSError, subprocess.TimeoutExpired) as error:
        print(f"capture CLI regression: {error}", file=sys.stderr)
        sys.exit(1)
