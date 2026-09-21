"""Exercise the custom Proxy build and prepared executable handoff without the E2E."""

import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).resolve().parents[3] / "scripts/ci-e2e-build.sh").read_text()


def function(name):
    match = re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE)
    if match is None:
        raise AssertionError("missing proxy E2E helper: " + name)
    return match.group()


GO = function("e2e_go")
BUILD_CUSTOM_PROXY = function("build_custom_proxy")
SKIP = next(line for line in SOURCE.splitlines() if line.startswith("skip() {"))
TELEMETRY = SOURCE


class OrchestratorProxyGoSelection(unittest.TestCase):
    def setUp(self):
        self.fixture = tempfile.TemporaryDirectory(prefix="proxy-go-")
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.output = self.root / "custom-proxy"
        self.log = self.root / "go.log"

    def fake_go(self, directory, identity, status=0):
        binary = directory / "go"
        directory.mkdir(parents=True, exist_ok=True)
        binary.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' '{identity}' \"$GOWORK\" \"$@\" > '{self.log}'\n"
            f"printf '%s\\n' \"$PATH\" \"${{GOROOT-}}\" \"${{GOTOOLCHAIN-}}\" \"${{CGO_ENABLED-}}\" > '{self.root / 'go.env'}'\n"
            f"exit {status}\n"
        )
        binary.chmod(0o755)
        return binary

    def run_build(self, *, path, goroot_marker="unset", command="build_custom_proxy", required="1", extra=None):
        env = {
            **os.environ,
            "PATH": str(path),
            "CUSTOM_PROXY_SOURCE_ROOT": str(self.source),
            "CUSTOM_PROXY_BIN": str(self.output),
            "REQUIRE_PROXY": required,
            "WORK": str(self.root),
            "SCRIPT_DIR": str(Path(__file__).resolve().parents[1]),
            "telemetry_source": str(self.source),
            "GOTOOLCHAIN": os.environ.get("GOTOOLCHAIN", "auto"),
        }
        # Each fixture selects its own driver; the outer E2E may carry a Go entry.
        env.pop("KUASAR_E2E_GO", None)
        if goroot_marker == "unset":
            env.pop("GOROOT", None)
        else:
            env["GOROOT"] = str(goroot_marker)
        if extra:
            env.update(extra)
        script = "set -euo pipefail\n" + SKIP + "\n" + GO + "\n" + BUILD_CUSTOM_PROXY + "\n" + command + "\n"
        return subprocess.run(
            ["/bin/bash", "-c", script], env=env, text=True,
            capture_output=True, timeout=90, cwd=self.root,
        )

    def test_preserved_goroot_wins_over_mismatched_root_path(self):
        root_path = self.root / "root-path"
        selected_root = self.root / "selected go with spaces"
        self.fake_go(root_path, "root-path-go")
        selected = self.fake_go(selected_root / "bin", "goroot-go")

        # Reproduce the old root-side plain `go build`: sudo's root PATH wins
        # even though the selected installation's GOROOT remains preserved.
        legacy = subprocess.run(
            ["/bin/bash", "-c", "GOWORK=off go build"],
            env={**os.environ, "PATH": str(root_path), "GOROOT": str(selected_root)},
            text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(legacy.returncode, 0, legacy.stderr)
        self.assertEqual(self.log.read_text().splitlines()[0], "root-path-go")

        result = self.run_build(path=root_path, goroot_marker=selected_root)

        self.assertEqual(result.returncode, 0, result.stderr)
        lines = self.log.read_text().splitlines()
        self.assertEqual(lines[0], "goroot-go")
        self.assertEqual(lines[1], "off")
        self.assertEqual(
            lines[2:], ["build", "-o", str(self.output), "./examples/custom-proxy"]
        )
        self.assertNotEqual(selected, root_path / "go")

    def test_unset_or_empty_goroot_falls_back_to_path(self):
        root_path = self.root / "root-path"
        self.fake_go(root_path, "path-go")

        for goroot in ("unset", ""):
            with self.subTest(goroot=goroot):
                self.log.unlink(missing_ok=True)
                result = self.run_build(path=root_path, goroot_marker=goroot)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.log.read_text().splitlines()[0], "path-go")

    def test_missing_go_retains_skip_behavior(self):
        result = self.run_build(path=self.root / "empty-path")

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("go: command not found", result.stderr)
        self.assertIn("REQUIRE_PROXY=1; failing", result.stderr)
        self.assertFalse(self.log.exists())


    def test_invalid_explicit_goroot_never_falls_back(self):
        alternate = self.root / "alternate"
        self.fake_go(alternate, "wrong-driver")
        selected = self.root / "selected"
        for mode in ("missing", "not-executable", "directory"):
            with self.subTest(mode=mode):
                binary = selected / "bin" / "go"
                binary.parent.mkdir(parents=True, exist_ok=True)
                if mode == "not-executable":
                    binary.write_text("#!/bin/sh\nexit 0\n")
                    binary.chmod(0o644)
                elif mode == "directory":
                    binary.unlink()
                    binary.mkdir()
                self.log.unlink(missing_ok=True)
                result = self.run_build(path=alternate, goroot_marker=selected)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(str(binary), result.stderr)
                self.assertIn("REQUIRE_PROXY=1; failing", result.stderr)
                self.assertFalse(self.log.exists(), "invalid explicit root used PATH Go")

    def test_optional_custom_build_failure_keeps_skip_semantics(self):
        result = self.run_build(path=self.root / "empty-path", required="0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("skipping (failed to build examples/custom-proxy)", result.stdout)
        self.assertNotIn("REQUIRE_PROXY=1; failing", result.stderr)

    def telemetry_command(self, marker):
        commands = [line.strip() for line in TELEMETRY.splitlines() if marker in line]
        self.assertEqual(len(commands), 1)
        return commands[0]

    def test_both_sourced_telemetry_builds_use_selected_distribution(self):
        alternate = self.root / "alternate"
        selected = self.root / "selected"
        self.fake_go(alternate, "wrong-driver", 47)
        self.fake_go(selected / "bin", "selected-driver")
        for marker, args in (
            ('test -c -o "$WORK/telemetry.test"', ["test", "-c", "-o", str(self.root / "telemetry.test"), "./internal/telemetry"]),
            ('build -trimpath -o "$WORK/telemetry-grpc-probe"', ["build", "-trimpath", "-o", str(self.root / "telemetry-grpc-probe"), str(Path(__file__).resolve().parents[1] / "telemetryprobe/main.go")]),
        ):
            with self.subTest(marker=marker):
                command = self.telemetry_command(marker)
                result = self.run_build(path=alternate, goroot_marker=selected, command=command)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.log.read_text().splitlines(), ["selected-driver", "off", *args])
                self.assertEqual((self.root / "go.env").read_text().splitlines(),
                    [str(alternate), str(selected), os.environ.get("GOTOOLCHAIN", "auto"), "0"])

    def test_selected_driver_failure_is_not_retried_or_hidden(self):
        alternate = self.root / "alternate"
        selected = self.root / "selected"
        self.fake_go(alternate, "wrong-driver")
        driver = self.fake_go(selected / "bin", "selected-driver", 23)
        driver.write_text(driver.read_text().replace("exit 23", "echo compiler-diagnostic >&2\nexit 23"))
        for command, status in (
            ("build_custom_proxy", 1),
            (self.telemetry_command('test -c -o "$WORK/telemetry.test"'), 23),
            (self.telemetry_command('build -trimpath -o "$WORK/telemetry-grpc-probe"'), 23),
        ):
            with self.subTest(command=command):
                result = self.run_build(path=alternate, goroot_marker=selected, command=command)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertIn("compiler-diagnostic", result.stderr)
                self.assertEqual(self.log.read_text().splitlines()[0], "selected-driver")

    def test_real_go_builds_custom_and_source_free_grpc_probe(self):
        explicit = os.environ.get("GOROOT")
        go = str(Path(explicit) / "bin/go") if explicit else shutil.which("go")
        self.assertTrue(go, "a real Go installation is required")
        root = subprocess.check_output([go, "env", "GOROOT"], cwd=self.root, text=True).strip()
        alternate = self.root / "alternate"
        self.fake_go(alternate, "wrong-driver", 47)
        main = self.source / "examples/custom-proxy/main.go"
        main.parent.mkdir(parents=True)
        main.write_text('package main\nimport "fmt"\nfunc main() { fmt.Print("coherent-go") }\n')
        (self.source / "go.mod").write_text("module local.example/proxy\ngo 1.22\n")
        result = self.run_build(path=alternate, goroot_marker=root, extra={"CGO_ENABLED": "0"})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(subprocess.check_output([self.output], text=True), "coherent-go")
        # The actual gRPC probe is also compiled by exact-assets, which has no component source.
        result = self.run_build(path=alternate, goroot_marker=root,
            command=self.telemetry_command('build -trimpath -o "$WORK/telemetry-grpc-probe"'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / "telemetry-grpc-probe").is_file())
        self.assertFalse(self.log.exists(), "real builds invoked the mismatched PATH driver")

    def test_carried_go_entry_wins_after_sudo_path_reset(self):
        selected = self.fake_go(self.root / "chosen", "carried-go")
        alternate = self.root / "secure"
        self.fake_go(alternate, "wrong-system-go", 47)
        result = self.run_build(path=alternate, extra={"KUASAR_E2E_GO": str(selected)})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.log.read_text().splitlines()[0], "carried-go")

    def test_helper_is_wired_to_runtime_callers(self):
        self.assertIn("fixtures) build_custom_proxy; build_telemetry_grpc", SOURCE)
        self.assertIn("source) build_telemetry_netns", SOURCE)
        self.assertIn('"${KUASAR_E2E_GO:-${GOROOT:+$GOROOT/bin/}go}" "$@"', GO)
        self.assertNotIn("GOTOOLCHAIN=local", GO)


class ArtifactJournalContract(unittest.TestCase):
    def test_required_binary_journal_does_not_invoke_go(self):
        entry = Path(__file__).resolve().parents[1] / "e2e_journal_contract.sh"
        with tempfile.TemporaryDirectory() as temporary:
            tools = Path(temporary)
            (tools / "go").write_text("#!/bin/sh\nexit 97\n")
            (tools / "go").chmod(0o755)
            result = subprocess.run(["bash", str(entry)], text=True, capture_output=True,
                                    env={**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                                         "REQUIRE_PROXY": "1", "GOROOT": "/missing-source-toolchain"})
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("e2e_journal_contract: OK", result.stdout)


class PreparedCustomProxy(unittest.TestCase):
    def test_prepared_bytes_are_installed_for_the_runtime_user(self):
        entry = Path(__file__).resolve().parents[1] / "e2e_orchestrator_proxy.sh"
        setup = entry.read_text().split("CUSTOM_PROXY_EXTENSION_E2E=0\n", 1)[1].split(
            "declare -a PIDS=()", 1
        )[0]
        with tempfile.TemporaryDirectory(prefix="proxy-owner-") as temporary:
            root = Path(temporary)
            prepared = root / "prepared proxy"
            contents = b"#!/bin/sh\nprintf 'prepared-proxy\\n'\n"
            prepared.write_bytes(contents)
            prepared.chmod(0o555)
            # A root run reproduces hosted CI's unprivileged prepare -> sudo E2E.
            if os.geteuid() == 0:
                os.chown(prepared, 65534, 65534)
            original = prepared.stat()
            for artifact in ("0", "1"):
                with self.subTest(artifact=artifact):
                    work = root / ("work-" + artifact)
                    work.mkdir(mode=0o700)
                    result = subprocess.run(
                        ["bash", "-c", "set -euo pipefail\nskip() { exit 1; }\n" + setup
                         + '\n[ "$CUSTOM_PROXY_EXTENSION_E2E" = 1 ]\nprintf "%s\\n" "$CUSTOM_PROXY_BIN"'],
                        env={**os.environ, "WORK": str(work), "CUSTOM_PROXY_BIN": str(prepared),
                             "KUASAR_ARTIFACT_E2E": artifact},
                        capture_output=True, text=True, timeout=5,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    installed = Path(result.stdout.strip())
                    self.assertEqual(installed.parent, work)
                    self.assertEqual(installed.stat().st_uid, os.geteuid())
                    self.assertEqual(installed.stat().st_mode & 0o777, 0o700)
                    self.assertEqual(installed.read_bytes(), contents)
                    self.assertEqual(prepared.read_bytes(), contents)
                    self.assertEqual(subprocess.check_output([installed], text=True), "prepared-proxy\n")
                    current = prepared.stat()
                    self.assertEqual(
                        (current.st_ino, current.st_uid, current.st_gid, current.st_mode, current.st_mtime_ns),
                        (original.st_ino, original.st_uid, original.st_gid, original.st_mode, original.st_mtime_ns),
                    )


if __name__ == "__main__":
    unittest.main()
