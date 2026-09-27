#!/usr/bin/env bash
# Build policy/state matrices run against the maintained Go implementations.
# Real guest Build/flatten/publish operations remain in the product cases.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
python3 - "$work/results.jsonl" <<'PY'
import json
import os
import subprocess
import sys

required = {
    "TestCommitBuildTriggerStateMatrix",
    "TestCommitBuildTriggerConcurrentHasCompleteSingleWinner",
    "TestBuildActionStateMatrix",
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
    "TestBuildPublicationPlanDoesNotUseCheckpointModeForImageClass",
    "TestImportRefererWritebackFollowsFinalImagePolicy",
    "TestResolveBuildPublicationPlanMatrix",
    "TestPublishCheckpointArtifactArgsUseBareBuildID",
    "TestPublishCheckpointArtifactArgsManifestStore",
    "TestBuildArtifactRefLocationsStrictlyConvertsURIs",
    "TestBuildBaseDirFailureRetainsExecutionClaimUntilRetry",
    "TestCompleteBuildRetriesTransientCleanupWithoutRestart",
    "TestAcceptedBuildResultSurvivesTransientFenceFailure",
}
command = [os.environ.get("KUASAR_E2E_GO", "go"), "test", "-json", "-count=1", "-timeout=5m",
           "./internal/store", "./internal/orch", "./internal/builder",
           "-run", "^(" + "|".join(sorted(required)) + ")$"]
started, passed, rejected = set(), set(), []
with open(sys.argv[1], "w", encoding="utf-8") as evidence:
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
    for line in process.stdout:
        evidence.write(line)
        event = json.loads(line)
        if event.get("Output"):
            print(event["Output"], end="", flush=True)
        name, action = event.get("Test", ""), event.get("Action")
        if name.split("/", 1)[0] not in required:
            continue
        if action == "run":
            started.add(name)
        elif action == "pass":
            passed.add(name)
        elif action in {"fail", "skip"}:
            rejected.append(f"{action}: {name}")
    status = process.wait()
if status:
    raise SystemExit(status)
if rejected:
    raise SystemExit(f"Builder source contracts rejected: {rejected}")
if required - started or started - passed:
    raise SystemExit(f"Builder source contracts missing={sorted(required-started)}, unfinished={sorted(started-passed)}")
print(f"PASS source/builder-state: {len(required)} contracts, {len(passed)} assertions/subcases")
PY
