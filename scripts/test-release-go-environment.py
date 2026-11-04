"""Check offline Go payload records and isolated local module fixtures."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/release-materials.sh"
ALLOWED = ["orchestrator:bin/node-ctl","orchestrator:bin/cluster-ctl","orchestrator:bin/node-stub-ctl","orchestrator:bin/e2b-key-ctl"]


class GoSourceEnvironment(unittest.TestCase):
    def test_workspace_gate_requires_explicit_platform_and_propagates_failure(self):
        script = (ROOT / 'scripts/test-release.sh').read_text()
        gate = script.split('# Cross-repository integration is opt-in;', 1)[1].split('\nfi', 1)[0] + '\nfi\n'
        gate = gate[gate.index('if [['):]
        marker = self.root / 'workspace-invoked'
        python = self.bin / 'python3'
        python.write_text('#!/bin/bash\nprintf "%s" "$*" > "$WORKSPACE_MARKER"\nexit 23\n')
        python.chmod(0o755)
        for selection in ({}, {'KUASAR_PLATFORM_ROOT': '/explicit platform'},
                          {'KUASAR_PLATFORM_DIR': '/explicit platform'}):
            with self.subTest(selection=selection):
                marker.unlink(missing_ok=True)
                result = subprocess.run(['bash', '-c', 'set -euo pipefail\nROOT="$1"\n' + gate, '_', str(ROOT)],
                    env=dict(self.environment, WORKSPACE_MARKER=str(marker), **selection), text=True, capture_output=True)
                self.assertEqual(result.returncode, 23 if selection else 0, result.stderr)
                self.assertEqual(marker.exists(), bool(selection))
                if selection:
                    self.assertIn('scripts/test-release-workspace.py', marker.read_text())

    def test_workspace_platform_aliases_resolve_before_fixture_chdir(self):
        import runpy
        from unittest.mock import patch
        selections = ({'KUASAR_PLATFORM_ROOT': 'selected platform'},
                      {'KUASAR_PLATFORM_DIR': 'selected platform'},
                      {'KUASAR_PLATFORM_DIR': 'selected platform', 'KUASAR_PLATFORM_ROOT': './selected platform'})
        for selection in selections:
            with self.subTest(selection=selection), patch.dict(os.environ, selection, clear=True):
                data = runpy.run_path(str(ROOT / 'scripts/test-release-workspace.py'))
                self.assertEqual(data['PLATFORM'], Path('selected platform').resolve())
                with self.assertRaisesRegex(RuntimeError, 'workspace tooling is missing'):
                    data['ReleaseWorkspaceTests'].setUpClass()
        with patch.dict(os.environ, {'KUASAR_PLATFORM_DIR': 'one', 'KUASAR_PLATFORM_ROOT': 'two'}, clear=True):
            with self.assertRaisesRegex(SystemExit, 'different platforms'):
                runpy.run_path(str(ROOT / 'scripts/test-release-workspace.py'))

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.capture = self.root / "observed.json"
        executable = self.bin / "go"
        executable.write_text(
            "#!/usr/bin/python3\nimport json, os, sys\n"
            "with open(sys.argv[1], 'w') as out: json.dump(dict(os.environ), out)\n")
        executable.chmod(0o755)
        self.environment = {
            "PATH": str(self.bin) + ":/usr/bin:/bin",
            "GOPROXY": "https://proxy.example.invalid",
            "GOSUMDB": "sum.golang.org https://sum.example.invalid",
        }

    def run_shell(self, command, extra=None):
        return subprocess.run(
            ["bash", "-c", 'set -euo pipefail\nfail() { echo "$*" >&2; exit 1; }\n'
             'source "$1"\n' + command, "_", str(HELPER), str(self.root / "verification"),
             str(self.capture)], env={**self.environment, **(extra or {})},
            text=True, capture_output=True, timeout=10)

    def test_fixture_proxy_ignores_private_routes_and_persistent_configuration(self):
        fixture = (ROOT / "scripts/test-release-materials.sh").read_text()
        prelude = fixture.split("export GOWORK=off", 1)[1].split("# An additional organization-owned module", 1)[0]
        prelude = "export GOWORK=off" + prelude
        configuration = self.root / "go-environment"
        private = "github.com/kuasar-sandbox/*"
        configuration.write_text("GOPRIVATE=" + private + "\nGONOPROXY=" + private
                                 + "\nGONOSUMDB=" + private + "\nGOINSECURE=" + private + "\n")
        # Go reports an empty file path when GOENV=off disables persistence.
        expected = {"GOENV": "", "GOPRIVATE": "", "GONOPROXY": "none", "GONOSUMDB": "none",
                    "GOINSECURE": "", "GOAUTH": "off", "GOTOOLCHAIN": "local",
                    "GOWORK": "off", "GOPROXY": "file://" + str(self.root / "proxy")}
        for ambient in (False, True):
            environment = dict(os.environ, GOENV=str(configuration))
            for name in ("GOPRIVATE", "GONOPROXY", "GONOSUMDB", "GOINSECURE"):
                environment.pop(name, None)
                if ambient:
                    environment[name] = private
            with self.subTest(ambient=ambient):
                result = subprocess.run(["bash", "-c", 'set -euo pipefail\nTMP="$1"\n' + prelude
                                         + '\n[ "$GOENV" = off ]\ngo env -json ' + " ".join(expected), "_", str(self.root)],
                                        env=environment, cwd=self.root, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), expected)




    def test_retired_source_records_are_rejected_without_running_go(self):
        stage = self.root / "stage"
        source = stage / "share/sources/fixture"
        source.mkdir(parents=True)
        (source / "SOURCES.tsv").write_text("retired source proof")
        result = self.run_shell('WORK="$2"; mkdir -p "$WORK"\n'
                                'release_materials_validate "' + str(stage) + '" fixture')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not include historical source records", result.stderr)
        self.assertFalse(self.capture.exists())


if __name__ == "__main__":
    unittest.main()
