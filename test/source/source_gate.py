#!/usr/bin/env python3
"""One command catalog for the complete source gate and its declared stages.

The no-argument entry keeps the complete original shell order. This module
emits only its literal catalog; the existing Bash entry executes it in its
own strict shell, preserving startup environment and EXIT traps. The trusted
platform may run four independent owner stages in separate Workbench instances.
Its four race jobs discover and execute the entire package union themselves;
direct owner race-stage requests fail instead of accidentally running all four
copies of the full race gate. Older platforms keep using the complete entry.

The caller supplies the ordinary selected-source Go workspace and private
system-mode environment. This dispatcher does not prepare a different workspace,
change inherited build/test flags, add retries or suppress a failing command.
"""
import argparse
from typing import NamedTuple

PROTOCOL = "orchestrator-source-v1"
RACE_STAGES = ("race-1", "race-2", "race-3", "race-4")
STAGES = ("normal", *RACE_STAGES, "databases", "services", "vet-and-helpers")


class Command(NamedTuple):
    name: str
    stage: str
    shell: str


# Preserve the default order and each complete original shell fragment. In
# particular, normal tests retain their Builder JSON pipe, PostgreSQL owns its
# real server for both checks, and telemetry's temporary build retains its trap.
CATALOG = (
    Command('consumer-api', 'normal', r"""bash test/source/go_consumer.sh
"""),
    Command('normal-builder-contracts', 'normal', r"""# Validate the required Builder contracts from this ordinary run, including
# every started subcase, instead of executing the same tests a third time.
go test -json -count=1 -timeout=5m ./... \
    | python3 -B test/source/builder_state.py --results -
"""),
    Command('race-all', 'race', r"""# Give each race-instrumented package the source Workbench's two CPUs. Tests
# retain their own goroutine/subprocess concurrency and per-package deadline.
CGO_ENABLED=1 go test -race -p=1 -count=1 -timeout=5m ./...
"""),
    Command('native-fdb', 'databases', r"""# Native repositories and command wiring run against an isolated SSD database,
# through the same pinned loader and client runtime as the shipped executable.
python3 -B scripts/with-test-fdb.py -- \
    python3 -B scripts/cluster-native.py test --arch "$(uname -m)" -- \
    -race -p=1 -count=1 -timeout=5m
"""),
    Command('vet', 'vet-and-helpers', r"""go vet ./...
"""),
    Command('environment-tools', 'vet-and-helpers', r"""PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-environment-tools.py
"""),
    Command('telemetry-image-pull', 'vet-and-helpers', r"""PYTHONDONTWRITEBYTECODE=1 python3 scripts/test-telemetry-image-pull.py
"""),
    Command('runtask-privilege', 'services', r"""bash test/source/runtask_privilege.sh
"""),
    Command('vmm-cgroup', 'services', r"""bash test/source/vmm_cgroup.sh
"""),
    Command('builder-unit-upgrade', 'services', r"""REQUIRE_BUILDER=1 bash test/source/builder_unit_upgrade.sh
"""),
    Command('resource-pool', 'services', r"""bash test/source/resource_pool_units.sh
"""),
    Command('capture-cli', 'services', r"""bash test/source/capture_cli.sh
"""),
    Command('fixture-configs', 'services', r"""bash test/source/cluster_fixture_configs.sh
"""),
    Command('cluster-fdb', 'databases', r"""REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
"""),
    Command('cluster-sqlite', 'databases', r"""CLUSTER_STUB_BACKEND=sqlite REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
"""),
    Command('postgres', 'databases', r"""# Provision real PostgreSQL explicitly: these backend contracts must never
# silently skip because the DSN was absent from the required source gate.
bash scripts/with-test-postgres.sh -- bash -euc '
    test -n "$STORAGE_TEST_POSTGRES_DSN"
    CGO_ENABLED=1 go test -race -p=1 -count=1 -timeout=5m \
        ./internal/storage/sqlengine ./internal/cluster/metastore ./internal/registry
    CLUSTER_STUB_BACKEND=postgres REQUIRE_CLUSTER_STUB=1 bash test/source/cluster_stub.sh
'
"""),
    Command('telemetry-backends', 'services', r"""bash test/source/telemetry_backends.sh
"""),
    Command('helper-tests', 'vet-and-helpers', r"""# Helper/source regressions stay in this compiler-capable gate, not in the
# prepared product E2E runner. Discover the maintained helper tests as one set so
# execute/resource/cluster/capture regressions cannot disappear during cutover.
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s test/e2e/lib -p 'test_*.py' -v
"""),
    Command('telemetry-netns', 'services', r"""work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
bash scripts/ci-e2e-build.sh source "$(uname -m)" "$work"
telemetry_privilege=()
if [ "$(id -u)" -ne 0 ]; then telemetry_privilege=(sudo -n); fi
"${telemetry_privilege[@]}" env REQUIRE_TELEMETRY_NETNS=1 "$work/telemetry.test" -test.v -test.timeout=90s -test.run='^TestOTLPProxyNetNS'
"""),
)


def selected(stage=None):
    if stage is None:
        return CATALOG
    if stage in RACE_STAGES:
        raise ValueError("race partitions must be executed by the trusted platform")
    if stage not in STAGES:
        raise ValueError("unknown source stage")
    commands = tuple(command for command in CATALOG if command.stage == stage)
    if not commands:
        raise ValueError("empty source stage")
    return commands


def program(stage=None):
    return "".join(command.shell for command in selected(stage))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci-stage", choices=STAGES)
    args = parser.parse_args(argv)
    try:
        shell = program(args.ci_stage)
    except ValueError as error:
        parser.error(str(error))
    # The wrapper checks this command substitution before evaluating anything.
    # No path, shell fragment or arbitrary command can be supplied as an input.
    print(shell, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
