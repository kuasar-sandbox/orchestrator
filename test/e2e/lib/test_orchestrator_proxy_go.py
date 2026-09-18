"""Exercise the custom Proxy build's Go selection without running the E2E."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest


SOURCE = (Path(__file__).resolve().parents[1] / "e2e_orchestrator_proxy.sh").read_text()


def function(name):
    match = re.search(r"(?ms)^" + name + r"\(\) \{[^\n]*\n.*?^\}", SOURCE)
    if match is None:
        raise AssertionError("missing proxy E2E helper: " + name)
    return match.group()


BUILD_CUSTOM_PROXY = function("build_custom_proxy")


class OrchestratorProxyGoSelection(unittest.TestCase):
    def setUp(self):
        self.fixture = tempfile.TemporaryDirectory(prefix="proxy-go-")
        self.addCleanup(self.fixture.cleanup)
        self.root = Path(self.fixture.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.output = self.root / "custom-proxy"
        self.log = self.root / "go.log"

    def fake_go(self, directory, identity):
        binary = directory / "go"
        directory.mkdir(parents=True, exist_ok=True)
        binary.write_text(
            "#!/bin/sh\n"
            f"printf '%s\\n' '{identity}' \"$GOWORK\" \"$@\" > '{self.log}'\n"
        )
        binary.chmod(0o755)
        return binary

    def run_build(self, *, path, goroot_marker="unset"):
        env = {
            **os.environ,
            "PATH": str(path),
            "CUSTOM_PROXY_SOURCE_ROOT": str(self.source),
            "CUSTOM_PROXY_BIN": str(self.output),
        }
        if goroot_marker == "unset":
            env.pop("GOROOT", None)
        else:
            env["GOROOT"] = str(goroot_marker)
        script = """set -euo pipefail
skip() { printf 'SKIP: %s\n' "$*" >&2; exit 42; }
""" + BUILD_CUSTOM_PROXY + "\nbuild_custom_proxy\n"
        return subprocess.run(
            ["/bin/bash", "-c", script], env=env, text=True,
            capture_output=True, timeout=5,
        )

    def test_preserved_goroot_wins_over_mismatched_root_path(self):
        root_path = self.root / "root-path"
        selected_root = self.root / "selected-go"
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

    def test_unset_goroot_falls_back_to_path(self):
        root_path = self.root / "root-path"
        self.fake_go(root_path, "path-go")

        result = self.run_build(path=root_path)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.log.read_text().splitlines()[0], "path-go")

    def test_invalid_explicit_goroot_does_not_fall_back_to_path(self):
        root_path = self.root / "root-path"
        self.fake_go(root_path, "path-go")
        invalid_root = self.root / "invalid-go"

        result = self.run_build(path=root_path, goroot_marker=invalid_root)

        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertIn(
            f"SKIP: selected GOROOT has no executable go: {invalid_root}/bin/go",
            result.stderr,
        )
        self.assertFalse(self.log.exists())

    def test_missing_go_retains_skip_behavior(self):
        result = self.run_build(path=self.root / "empty-path")

        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertIn("SKIP: go not on PATH (custom Proxy Extension build)", result.stderr)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
