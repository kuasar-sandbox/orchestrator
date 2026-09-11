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
                                        env=environment, capture_output=True, text=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout), expected)

    def test_only_exact_official_payload_keys_are_accepted(self):
        for identity in ALLOWED:
            unit, payload = identity.split(":", 1)
            result = self.run_shell("release_materials_go_payload_allowed " + unit + " " + payload)
            self.assertEqual(result.returncode, 0, result.stderr)
            aliases = (payload.replace("bin/", "bin/./", 1),
                       payload.replace("bin/", "bin//", 1), payload + "/",
                       payload + ":/unexpected", "bin/unexpected-tool")
            for alias in aliases:
                with self.subTest(alias=alias):
                    result = self.run_shell("release_materials_go_payload_allowed " + unit + " " + alias)
                    self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(self.run_shell(
            "release_materials_go_payload_allowed unknown bin/tool").returncode, 0)

    def test_validator_rejects_aliases_before_record_check(self):
        unit, official = ALLOWED[0].split(":", 1)
        stage = self.root / "stage"
        source = stage / "share/sources" / unit
        license_dir = stage / "share/licenses" / unit / "project"
        source.mkdir(parents=True)
        license_dir.mkdir(parents=True)
        (stage / "bin").mkdir()
        (stage / official).write_bytes(b"fixture payload; never executed")
        (license_dir / "LICENSE").write_text("fixture notice\n")
        for directory in (stage / "share", *[p for p in (stage / "share").rglob("*") if p.is_dir()]):
            directory.chmod(0o755)
        (source / "SOURCES.tsv").write_text(
            "payload\tname\tversion\tsource\tintegrity\tlicense_directory\n"
            + official + "\tfixture\tv1.0.0\thttps://example.invalid\th1:fixture\t"
            + "share/licenses/" + unit + "/project\n")
        (source / "GO-MODULES.tsv").write_text("module\tversion\tchecksum\n")
        (source / "MATERIALS.sha256").write_text("fixture inventory; record_check must come first\n")
        for file in source.iterdir():
            file.chmod(0o644)
        command = (
            'WORK="$2"; mkdir -p "$WORK"\n'
            'release_materials_require_go() { touch "$WORK/record_check-reached"; return 1; }\n'
            'release_materials_validate "' + str(stage) + '" ' + unit)
        marker = self.root / "verification/record_check-reached"
        original_sources = (source / "SOURCES.tsv").read_text()
        cases = [
            {"payload": official.replace("bin/", "bin/./", 1)},
            {"payload": official.replace("bin/", "bin//", 1)},
            {"source_repeats": 2},
            {"expect_record_check": True},
        ]
        for case in cases:
            payload = case.get("payload", official)
            header, row = original_sources.splitlines(keepends=True)
            rows = row * case.get("source_repeats", 1)
            (source / "SOURCES.tsv").write_text(header + rows)
            (source / "GO-MODULES.tsv").write_text("module\tversion\tchecksum\n")
            (source / "MATERIALS.sha256").write_text("fixture inventory; record_check must come first\n")
            (source / "GO-BUILD-INFO.tsv").write_text(
                "payload\trecord\tname\tversion_or_value\tchecksum\n"
                + (payload + "\ttoolchain\tgo\tgo1.26.4\t-\n") * 1)
            (source / "GO-BUILD-INFO.tsv").chmod(0o644)
            result = self.run_shell(command)
            self.assertNotEqual(result.returncode, 0)
            if "source_repeats" in case:
                self.assertIn("invalid SOURCES.tsv records", result.stderr)
            self.assertEqual(marker.exists(), case.get("expect_record_check", False),
                             "validator did not reject the malformed record before record_check")

    def test_required_payload_keys_do_not_repeat_record_check(self):
        unit = ALLOWED[0].split(":", 1)[0]
        payloads = [identity.split(":", 1)[1] for identity in ALLOWED]
        stage = self.root / "stage"
        source = stage / "share/sources" / unit
        source.mkdir(parents=True)
        info = source / "GO-BUILD-INFO.tsv"
        for absent in [None, *payloads]:
            info.write_text("payload\trecord\tname\tversion_or_value\tchecksum\n"
                            + "".join(payload + "\ttoolchain\tgo\tgo1.26.4\t-\n"
                                      for payload in payloads if payload != absent))
            for payload in payloads:
                command = 'release_materials_require_go_key "' + str(stage) + '" ' + unit + ' ' + payload
                result = self.run_shell(command)
                self.assertEqual(result.returncode == 0, payload != absent, result.stderr)
                if payload == absent:
                    self.assertIn("missing required Go payload record", result.stderr)
                self.assertFalse(self.capture.exists())
        release = (ROOT / "scripts/release.sh").read_text()
        self.assertNotIn('release_materials_require_go "$extract"', release)
        self.assertIn('release_materials_require_go_key "$extract"', release)

    def test_payload_gate_precedes_expensive_record_check(self):
        source = HELPER.read_text().split("release_materials_validate() {", 1)[1]
        self.assertLess(source.index('release_materials_go_payload_allowed "$unit" "$payload"'),
                        source.index('release_materials_require_go "$root" "$unit" "$payload"'))
        self.assertIn("NF != 5", source)


if __name__ == "__main__":
    unittest.main()
