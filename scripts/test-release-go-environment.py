"""Exercise the real filtered subprocess and exact payload gate, without network."""
import json
import os
import shutil
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
            "GOENV": "off", "GOAUTH": "off", "GOFLAGS": "", "GO111MODULE": "on", "GOWORK": "off",
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

    def test_toolchain_download_uses_the_same_private_unauthenticated_environment(self):
        jq = shutil.which("jq")
        self.assertIsNotNone(jq, "release tests require jq")
        (self.bin / "jq").symlink_to(jq)
        executable = self.bin / "go"
        executable.write_text("#!/usr/bin/python3\nimport json, os\n"
            + "with open(" + repr(str(self.capture)) + ", 'w') as out: json.dump(dict(os.environ), out)\n"
            + 'print(json.dumps({"Path":"golang.org/toolchain", "Version":"v0.0.1-go1.26.4.linux-amd64", "Sum":"h1:fixture", "Zip":"/fixture/toolchain.zip"}))\n')
        executable.chmod(0o755)
        command = 'WORK="$2"; RELEASE_MATERIALS_WORK="$2"; mkdir -p "$2"; _release_materials_download_go_toolchain go1.26.4'
        extra = {key: "fixture-secret-must-not-propagate" for key in
                 ("GOAUTH", "NETRC", "GH_TOKEN", "GITHUB_TOKEN", "GIT_CONFIG_PARAMETERS", "SSH_AUTH_SOCK")}
        result = self.run_shell(command, extra)
        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(self.capture.read_text())
        self.assertNotIn("fixture-secret-must-not-propagate", observed.values())
        self.assertEqual(observed["GOAUTH"], "off")
        self.assertEqual(observed["HOME"], str(self.root / "verification/toolchain-download/home"))
        self.assertEqual(observed["GOMODCACHE"], str(self.root / "verification/toolchain-download/module-cache"))
        self.capture.unlink()
        result = self.run_shell(command, {"HTTPS_PROXY": "https://fixture:fixture@proxy.example.invalid"})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.capture.exists())

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
        original_sources = (source / "SOURCES.tsv").read_text()
        cases = [
            {"payload": official.replace("bin/", "bin/./", 1)},
            {"payload": official.replace("bin/", "bin//", 1)},
            {"oversized": True},
            {"source_repeats": 2},
            {"source_rows": 16385},
            *({"large_table": name} for name in
              ("SOURCES.tsv", "GO-BUILD-INFO.tsv", "GO-MODULES.tsv", "MATERIALS.sha256")),
            {"expect_authentication": True},
        ]
        for case in cases:
            payload = case.get("payload", official)
            oversized = case.get("oversized", False)
            header, row = original_sources.splitlines(keepends=True)
            rows = "".join(row.replace("\tfixture\t", "\tfixture-" + str(i) + "\t")
                           for i in range(case["source_rows"])) if "source_rows" in case else row * case.get("source_repeats", 1)
            (source / "SOURCES.tsv").write_text(header + rows)
            (source / "GO-MODULES.tsv").write_text("module\tversion\tchecksum\n")
            (source / "MATERIALS.sha256").write_text("fixture inventory; authentication must come first\n")
            (source / "GO-BUILD-INFO.tsv").write_text(
                "payload\trecord\tname\tversion_or_value\tchecksum\n"
                + (payload + "\ttoolchain\tgo\tgo1.26.4\t-\n") * (16385 if oversized else 1))
            (source / "GO-BUILD-INFO.tsv").chmod(0o644)
            if "large_table" in case:
                with (source / case["large_table"]).open("r+b") as contents:
                    contents.truncate(16777217)
            result = self.run_shell(command)
            self.assertNotEqual(result.returncode, 0)
            if "source_repeats" in case or "source_rows" in case:
                self.assertIn("invalid SOURCES.tsv records", result.stderr)
            if "large_table" in case:
                self.assertIn("source material is too large", result.stderr)
            self.assertEqual(marker.exists(), case.get("expect_authentication", False),
                             "validator did not reject the malformed record before authentication")

    def test_required_payload_keys_do_not_repeat_authentication(self):
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

    def test_payload_gate_precedes_expensive_authentication(self):
        source = HELPER.read_text().split("release_materials_validate() {", 1)[1]
        self.assertLess(source.index('release_materials_go_payload_allowed "$unit" "$payload"'),
                        source.index('release_materials_require_go "$root" "$unit" "$payload"'))
        self.assertIn("NF != 5 || NR > 16385", source)


if __name__ == "__main__":
    unittest.main()
