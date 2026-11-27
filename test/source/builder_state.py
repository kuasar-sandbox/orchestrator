#!/usr/bin/env python3
"""Require Builder contracts and their started subcases to run and pass.

The source gate supplies its ordinary ``go test -json ./...`` stream. The
standalone entrypoint can still execute just these contracts for local diagnosis.
Skips in unrelated, environment-sensitive tests are not Builder failures.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
MODULE = "github.com/kuasar-sandbox/orchestrator/"
REQUIRED = {
    "internal/store": {
        "TestCommitBuildTriggerStateMatrix",
        "TestCommitBuildTriggerConcurrentHasCompleteSingleWinner",
        "TestBuildActionStateMatrix",
    },
    "internal/orch": {
        "TestTriggerBuildStateMatrix",
        "TestTriggerBuildChecksOwnershipBeforeState",
        "TestTriggerBuildResourceAssertionPrecedesSideEffects",
        "TestTriggerBuildConcurrentHasSingleCompleteWinner",
        "TestTriggerBuildCannotOverwritePoolClaim",
        "TestTriggerBuildTerminalConflictPreservesTemplateAndAllowsNewRegistration",
        "TestBareAutoRejectsSandboxOptionsBeforeRegistrationAdmission",
        "TestAutoTriggerValidatesKnownTargetBeforeQueueing",
        "TestAcceptedBuildResultIgnoresUnsupportedOptions",
        "TestRecoveredAutoPreparesOnlyResolvedTarget",
        "TestBuildPrepareReplayBindsCommandPresence",
        "TestCompleteBuildPrepareExactRunReplayAndConflict",
        "TestBuildBaseDirFailureRetainsExecutionClaimUntilRetry",
        "TestCompleteBuildRetriesTransientCleanupWithoutRestart",
        "TestAcceptedBuildResultSurvivesTransientFenceFailure",
    },
    "internal/builder": {
        "TestBuildPublicationPlanDoesNotUseCheckpointModeForImageClass",
        "TestImportRefererWritebackFollowsFinalImagePolicy",
        "TestResolveBuildPublicationPlanMatrix",
        "TestPublishCheckpointArtifactArgsUseBareBuildID",
        "TestPublishCheckpointArtifactArgsManifestStore",
        "TestBuildArtifactRefLocationsStrictlyConvertsURIs",
    },
}


def validate(stream, output=sys.stdout):
    required = {(MODULE + package, name)
                for package, names in REQUIRED.items() for name in names}
    started, passed, rejected = set(), set(), []
    for number, line in enumerate(stream, 1):
        try:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("not an object")
        except ValueError:
            # Drain the producer even after invalid input, so a large Go test
            # stream cannot deadlock while the caller waits for its exit code.
            rejected.append(f"invalid Go JSON event at line {number}")
            continue
        if event.get("Output"):
            print(event["Output"], end="", file=output, flush=True)
        package = event.get("Package", "")
        name, action = event.get("Test", ""), event.get("Action")
        if not name and action == "fail":
            rejected.append(f"package failed: {package}")
        if (package, name.split("/", 1)[0]) not in required:
            continue
        key = (package, name)
        if action == "run":
            started.add(key)
        elif action == "pass":
            passed.add(key)
        elif action in {"fail", "skip"}:
            rejected.append(f"{action}: {package}/{name}")
    if rejected:
        raise ValueError(f"Builder source contracts rejected: {rejected}")
    if required - started or started - passed:
        raise ValueError("Builder source contracts "
                         f"missing={sorted(required - started)}, "
                         f"unfinished={sorted(started - passed)}")
    print(f"PASS source/builder-state: {len(required)} contracts, "
          f"{len(passed)} assertions/subcases", file=output, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", help="validate existing Go JSON output; '-' reads stdin")
    args = parser.parse_args()
    process = None
    if args.results == "-":
        stream = sys.stdin
    elif args.results:
        stream = open(args.results, encoding="utf-8")
    else:
        names = set().union(*REQUIRED.values())
        command = [os.environ.get("KUASAR_E2E_GO", "go"), "test", "-json",
                   "-count=1", "-timeout=5m", *("./" + package for package in REQUIRED),
                   "-run", "^(" + "|".join(sorted(names)) + ")$"]
        process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.PIPE, text=True)
        stream = process.stdout
    status = 0
    try:
        validate(stream)
    except ValueError as error:
        print(error, file=sys.stderr)
        status = 1
    finally:
        if stream is not sys.stdin:
            stream.close()
    return (process.wait() if process is not None else 0) or status


if __name__ == "__main__":
    raise SystemExit(main())
