#!/usr/bin/env python3
"""Source-stage dispatch contracts; never execute the product gate here."""
import ast
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("source_gate_contract", ROOT / "test/source/source_gate.py")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)

EXPECTED = {
    "normal": ("consumer-api", "normal-builder-contracts"),
    "databases": ("native-fdb", "cluster-fdb", "cluster-sqlite", "postgres"),
    "services": ("runtask-privilege", "vmm-cgroup", "builder-unit-upgrade", "resource-pool",
                 "capture-cli", "fixture-configs", "telemetry-backends", "telemetry-netns"),
    "vet-and-helpers": ("vet", "environment-tools", "telemetry-image-pull", "helper-tests"),
}
ENVIRONMENT = ("TMPDIR", "GOTMPDIR", "GOCACHE", "GOWORK", "GOFLAGS", "CGO_ENABLED",
               "CFLAGS", "CGO_CFLAGS", "ORG", "TARGET_ARCH", "REQUIRE_WORKING_SET_PRIVILEGED")


class SourceGateStages(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="source-stage-contract-")
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.owner = self.root / "owner"
        self.foreign = self.root / "caller"
        self.foreign.mkdir()
        for relative in ("scripts/ci-source-checks.sh", "test/source/source_gate.py"):
            target = self.owner / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, target)
        self.log = self.root / "invocations.jsonl"
        event = self.root / "event.py"
        event.write_text("""import json, os, pathlib, signal, sys
names = json.loads(os.environ['STAGE_FIXTURE_ENV_KEYS'])
record = dict(name=sys.argv[1], cwd=os.getcwd(), env={name: os.environ.get(name) for name in names},
              startups=os.environ.get('STAGE_FIXTURE_STARTUPS'))
with pathlib.Path(os.environ['STAGE_FIXTURE_LOG']).open('a') as stream:
    stream.write(json.dumps(record) + '\\n')
if sys.argv[1] == os.environ.get('STAGE_FIXTURE_FAIL'):
    if os.environ.get('STAGE_FIXTURE_SIGNAL'):
        os.kill(os.getpid(), int(os.environ['STAGE_FIXTURE_SIGNAL']))
    raise SystemExit(int(os.environ.get('STAGE_FIXTURE_EXIT', '1')))
""")
        self.env = dict(os.environ, STAGE_FIXTURE_LOG=str(self.log), STAGE_FIXTURE_EVENT=str(event),
                        STAGE_FIXTURE_ENV_KEYS=json.dumps(ENVIRONMENT))
        # Only this private copy substitutes tiny commands. The original catalog
        # is independently checked byte-for-byte and through the real emitter.
        self.fixture_catalog(tuple(GATE.Command(row.name, row.stage,
            'python3 "$STAGE_FIXTURE_EVENT" ' + row.name + '\n') for row in GATE.CATALOG))

    def fixture_catalog(self, commands):
        original = (ROOT / "test/source/source_gate.py").read_text()
        tree = ast.parse(original)
        assignment = next(node for node in tree.body if isinstance(node, ast.Assign) and
                          any(isinstance(target, ast.Name) and target.id == "CATALOG" for target in node.targets))
        lines = original.splitlines(keepends=True)
        replacement = "CATALOG = (\n" + "".join("    " + repr(row) + ",\n" for row in commands) + ")\n"
        (self.owner / "test/source/source_gate.py").write_text(
            "".join(lines[:assignment.lineno - 1]) + replacement + "".join(lines[assignment.end_lineno:]))

    def entry(self, *arguments, **environment):
        return subprocess.run(["/bin/bash", str(self.owner / "scripts/ci-source-checks.sh"), *arguments],
                              cwd=self.foreign, env=dict(self.env, **environment), capture_output=True, text=True)

    def records(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_full_catalog_keeps_original_literal_body_and_order(self):
        # Digest of the original gate after its five-line cwd preamble, recorded
        # before extraction into the catalog. It includes every flag/comment,
        # pipeline, nested PostgreSQL command, namespace check and EXIT trap.
        self.assertEqual(hashlib.sha256(GATE.program().encode()).hexdigest(),
                         "5410112e65223946ac3afd13844aa3f105faffe624d7b9072d0cc6fc3d828eb4")
        self.assertEqual(len(GATE.CATALOG), 19)
        self.assertEqual(len({row.name for row in GATE.CATALOG}), 19)
        self.assertEqual(GATE.selected(), GATE.CATALOG)
        race = [row for row in GATE.CATALOG if row.stage == "race"]
        self.assertEqual([row.name for row in race], ["race-all"])
        self.assertIn("CGO_ENABLED=1 go test -race -p=1 -count=1 -timeout=5m ./...\n", race[0].shell)

    def test_nonrace_projection_and_platform_race_cover_complete_catalog_once(self):
        observed = []
        for stage, names in EXPECTED.items():
            selected = GATE.selected(stage)
            self.assertEqual(tuple(row.name for row in selected), names)
            self.assertEqual(selected, tuple(row for row in GATE.CATALOG if row.stage == stage))
            observed.extend(row.name for row in selected)
        observed.append("race-all")
        self.assertEqual(Counter(observed), Counter(row.name for row in GATE.CATALOG))
        self.assertEqual(GATE.STAGES, ("normal", "race-1", "race-2", "race-3", "race-4",
                                      "databases", "services", "vet-and-helpers"))

    def test_real_emitter_returns_only_exact_catalog_bytes_for_default_and_each_stage(self):
        for stage in (None, *EXPECTED):
            with self.subTest(stage=stage):
                arguments = [] if stage is None else ["--ci-stage", stage]
                result = subprocess.run([sys.executable, "-B", str(ROOT / "test/source/source_gate.py"), *arguments],
                                        capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, GATE.program(stage).encode())
                self.assertEqual(result.stderr, b"")

    def test_declaration_is_static_and_wrapper_checks_emitter_before_eval(self):
        text = (self.owner / "scripts/ci-source-checks.sh").read_text()
        marker = "# kuasar-source-stages: orchestrator-source-v1"
        self.assertEqual(text.splitlines().count(marker), 1)
        self.assertLess(text.index(marker), text.index("set -euo pipefail"))
        self.assertIn('cd "$ROOT"\n', text)
        self.assertTrue(text.endswith('source_gate_program="$(python3 -B test/source/source_gate.py "$@")"\n'
                                     'eval "$source_gate_program"\n'))
        self.assertEqual(GATE.PROTOCOL, "orchestrator-source-v1")

    def test_actual_default_wrapper_runs_complete_fixture_catalog_in_owner_order(self):
        result = self.entry()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([row["name"] for row in self.records()], [row.name for row in GATE.CATALOG])
        self.assertTrue(all(row["cwd"] == str(self.owner) for row in self.records()))

    def test_actual_nonrace_wrappers_run_exact_projected_fixture_commands(self):
        for stage, expected in EXPECTED.items():
            with self.subTest(stage=stage):
                before = len(self.records())
                result = self.entry("--ci-stage", stage)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(tuple(row["name"] for row in self.records()[before:]), expected)
                self.assertTrue(all(row["cwd"] == str(self.owner) for row in self.records()[before:]))

    def test_actual_wrapper_preserves_outer_workspace_flags_and_private_directories(self):
        values = {name: "fixture value " + name for name in ENVIRONMENT}
        values.update(CGO_ENABLED="0", TARGET_ARCH="x86_64", REQUIRE_WORKING_SET_PRIVILEGED="1")
        result = self.entry("--ci-stage", "services", **values)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(row["env"] == values for row in self.records()))

    def test_original_shell_startup_environment_is_not_executed_twice(self):
        startup = self.root / "startup.sh"
        startup.write_text('export STAGE_FIXTURE_STARTUPS=$(( ${STAGE_FIXTURE_STARTUPS:-0} + 1 ))\n')
        result = self.entry("--ci-stage", "normal", BASH_ENV=str(startup), STAGE_FIXTURE_STARTUPS="0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([row["startups"] for row in self.records()], ["1", "1"])

    def test_actual_failure_stops_later_commands_without_retry_or_success(self):
        result = self.entry(STAGE_FIXTURE_FAIL="native-fdb", STAGE_FIXTURE_EXIT="23")
        self.assertEqual(result.returncode, 23)
        self.assertEqual([row["name"] for row in self.records()],
                         ["consumer-api", "normal-builder-contracts", "race-all", "native-fdb"])

    def test_actual_child_signal_remains_a_failing_shell_status(self):
        result = self.entry("--ci-stage", "services", STAGE_FIXTURE_FAIL="runtask-privilege",
                            STAGE_FIXTURE_SIGNAL="15")
        self.assertEqual(result.returncode, 143)
        self.assertEqual([row["name"] for row in self.records()], ["runtask-privilege"])

    def test_unknown_and_platform_owned_race_requests_fail_before_execution(self):
        for arguments in (("--ci-stage", "missing"), ("--ci-stage",), ("extra",),
                          *(("--ci-stage", stage) for stage in GATE.RACE_STAGES)):
            with self.subTest(arguments=arguments):
                result = self.entry(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertFalse(self.log.exists())

    def test_real_shell_pipe_failure_stops_later_commands_and_runs_cleanup(self):
        self.fixture_catalog((GATE.Command("pipeline", "normal",
                              "trap 'printf cleanup > trace' EXIT\nfalse | true\nprintf reached > later\n"),
                              GATE.Command("later", "normal", "printf reached > last\n")))
        result = self.entry("--ci-stage", "normal")
        self.assertEqual(result.returncode, 1)
        self.assertEqual((self.owner / "trace").read_text(), "cleanup")
        self.assertFalse((self.owner / "later").exists())
        self.assertFalse((self.owner / "last").exists())

    def test_real_shell_rows_share_variables_and_exit_trap_on_failure(self):
        self.fixture_catalog((GATE.Command("create", "services",
                              "work=$(mktemp -d \"$PWD/owned.XXXXXX\")\n"
                              "printf '%s' \"$work\" > owned-path\n"
                              "trap 'rm -rf -- \"$work\"; printf cleaned > cleanup' EXIT\n"),
                              GATE.Command("use", "services", "test -d \"$work\"\nexit 37\n"),
                              GATE.Command("not-reached", "services", "touch later\n")))
        result = self.entry("--ci-stage", "services")
        self.assertEqual(result.returncode, 37)
        self.assertFalse(Path((self.owner / "owned-path").read_text()).exists())
        self.assertEqual((self.owner / "cleanup").read_text(), "cleaned")
        self.assertFalse((self.owner / "later").exists())

    def test_real_shell_unset_variable_is_fatal(self):
        command = 'unset SOURCE_STAGE_MISSING\nprintf "%s" "$SOURCE_STAGE_MISSING"\ntouch later\n'
        self.fixture_catalog((GATE.Command("unset", "normal", command),))
        self.assertNotEqual(self.entry("--ci-stage", "normal").returncode, 0)
        self.assertFalse((self.owner / "later").exists())


if __name__ == "__main__":
    unittest.main()
