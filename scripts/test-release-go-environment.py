"""Exercise the real filtered subprocess and exact payload gate, without network."""
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
            "GOPROXY": "https://proxy.example.invalid,direct",
            "GOSUMDB": "sum.golang.org https://sum.example.invalid",
        }

    def run_shell(self, command, extra=None):
        return subprocess.run(
            ["bash", "-c", 'set -euo pipefail\nfail() { echo "$*" >&2; exit 1; }\n'
             'source "$1"\n' + command, "_", str(HELPER), str(self.root / "verification"),
             str(self.capture)], env={**self.environment, **(extra or {})},
            text=True, capture_output=True, timeout=10)

    def test_caller_credentials_and_private_go_configuration_are_not_inherited(self):
        settings = {name: "fixture-must-not-propagate" for name in (
            "GOENV", "GOFLAGS", "GO111MODULE", "GOWORK", "GOTOOLCHAIN", "GOPRIVATE",
            "GONOPROXY", "GONOSUMDB", "GOINSECURE", "GIT_CONFIG_COUNT", "GIT_CONFIG_KEY_0",
            "GIT_CONFIG_VALUE_0", "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_DIR",
            "GIT_WORK_TREE", "GH_TOKEN", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY",
            "SSH_AUTH_SOCK", "NETRC", "GOAUTH", "GIT_ASKPASS", "GIT_SSH_COMMAND",
        )}
        settings["GOMODCACHE"] = str(self.root / "untrusted-cache")
        result = self.run_shell('release_materials_go_command "$2" "$3"', settings)
        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(self.capture.read_text())
        self.assertNotIn("fixture-must-not-propagate", observed.values())
        expected = {
            "GOENV": "off", "GOFLAGS": "", "GO111MODULE": "on", "GOWORK": "off",
            "GOTOOLCHAIN": "local", "GOPRIVATE": "", "GONOPROXY": "", "GONOSUMDB": "",
            "GOINSECURE": "", "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": "/bin/false", "GIT_SSH_COMMAND": "/bin/false",
            "GOPROXY": self.environment["GOPROXY"], "GOSUMDB": self.environment["GOSUMDB"],
        }
        for key, value in expected.items():
            self.assertEqual(observed.get(key), value, key)
        for name in ("home", "module-cache"):
            directory = self.root / "verification" / name
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(observed["GOMODCACHE"], str(self.root / "verification/module-cache"))
        self.assertEqual(observed["HOME"], str(self.root / "verification/home"))

    def test_unsigned_or_credentialed_routing_is_rejected_before_go(self):
        for values in (
            {"GOSUMDB": "off"}, {"GOPROXY": "file:///fixture"},
            {"GOPROXY": "http://proxy.example.invalid"},
            {"GOPROXY": "https://fixture:fixture@proxy.example.invalid"},
            {"GOSUMDB": "sum.golang.org https://fixture:fixture@sum.example.invalid"},
            {"HTTPS_PROXY": "http://fixture:fixture@proxy.example.invalid"},
        ):
            with self.subTest(setting=next(iter(values))):
                result = self.run_shell('release_materials_go_command "$2" "$3"', values)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.capture.exists())

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

    def test_validator_rejects_aliases_before_authentication(self):
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
        (source / "MATERIALS.sha256").write_text("fixture inventory; authentication must come first\n")
        for file in source.iterdir():
            file.chmod(0o644)
        command = (
            'WORK="$2"; mkdir -p "$WORK"\n'
            'release_materials_require_go() { touch "$WORK/authentication-reached"; return 1; }\n'
            'release_materials_validate "' + str(stage) + '" ' + unit)
        marker = self.root / "verification/authentication-reached"
        for payload, oversized in ((official.replace("bin/", "bin/./", 1), False),
                                   (official.replace("bin/", "bin//", 1), False),
                                   (official, True), (official, False)):
            (source / "GO-BUILD-INFO.tsv").write_text(
                "payload\trecord\tname\tversion_or_value\tchecksum\n"
                + (payload + "\ttoolchain\tgo\tgo1.26.4\t-\n") * (16385 if oversized else 1))
            (source / "GO-BUILD-INFO.tsv").chmod(0o644)
            result = self.run_shell(command)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(marker.exists(), payload == official and not oversized,
                             "validator did not reject the malformed record before authentication")

    def test_payload_gate_precedes_expensive_authentication(self):
        source = HELPER.read_text().split("release_materials_validate() {", 1)[1]
        self.assertLess(source.index('release_materials_go_payload_allowed "$unit" "$payload"'),
                        source.index('release_materials_require_go "$root" "$unit" "$payload"'))
        self.assertIn("NF != 5 || NR > 16385", source)


if __name__ == "__main__":
    unittest.main()
