#!/usr/bin/env python3
"""Packaging resolves the selected compiler in its module and caller policy."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "scripts/release-materials.sh"


class SelectedGoMaterials(unittest.TestCase):
    def test_effective_toolchain_and_persistent_policy_are_preserved(self):
        with tempfile.TemporaryDirectory(prefix="selected-go-") as directory:
            root = Path(directory)
            tools = root / "tools"
            tools.mkdir()
            distribution = root / "selected compiler"
            distribution.mkdir()
            (distribution / "LICENSE").write_text("selected compiler notices\n")
            module = root / "selected module"
            module.mkdir()
            (module / "go.mod").write_text("module fixture\n\ngo 1.26.1\n")
            configuration = root / "go-environment"
            configuration.write_text("GOTOOLCHAIN=auto\n")
            go = tools / "go"
            go.write_text('''#!/bin/sh
[ "$1" = -C ] && [ "$2" = "$EXPECTED_MODULE" ] || { echo wrong-module >&2; exit 61; }
[ "${GOWORK-unset}" = "$EXPECTED_WORKSPACE" ] || { echo caller-GOWORK-lost >&2; exit 65; }
[ "${GOENV-}" = "$EXPECTED_GOENV" ] || { echo caller-GOENV-lost >&2; exit 62; }
[ "${GOTOOLCHAIN-unset}" = "$EXPECTED_POLICY" ] || { echo caller-GOTOOLCHAIN-lost >&2; exit 63; }
case "$4" in GOROOT) echo "$SELECTED_ROOT";; GOVERSION) echo "${REPORTED_VERSION:-go1.26.7}";; *) exit 64;; esac
''')
            go.chmod(0o755)
            for policy, workspace in ((None, None), ("auto", "off"), ("go1.26.7+path", str(root / "selected.go.work"))):
                for mismatch in (False, True):
                    case = root / (str(policy) + str(mismatch))
                    env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"],
                               ROOT=str(module), EXPECTED_MODULE=str(module), SELECTED_ROOT=str(distribution),
                               GOENV=str(configuration), EXPECTED_GOENV=str(configuration),
                               EXPECTED_POLICY=policy if policy is not None else "unset",
                               REPORTED_VERSION="go1.24.3" if mismatch else "go1.26.7")
                    env["EXPECTED_WORKSPACE"] = workspace if workspace is not None else "unset"
                    if workspace is None:
                        env.pop("GOWORK", None)
                    else:
                        env["GOWORK"] = workspace
                    env.pop("RELEASE_MATERIALS_GO_ENV", None)
                    if policy is None:
                        env.pop("GOTOOLCHAIN", None)
                    else:
                        env["GOTOOLCHAIN"] = policy
                    script = '''set -euo pipefail
fail() { echo "$*" >&2; exit 1; }
source "$1"
release_materials_init "$2/stage" "$2/work" fixture
printf 'go1.26.7\n' > "$RELEASE_MATERIALS_WORK/go-toolchains"
printf 'bin/fixture\ttoolchain\tgo\tgo1.26.7\t-\n' > "$RELEASE_MATERIALS_WORK/go-build-info"
release_materials_finish
'''
                    result = subprocess.run(["bash", "-c", script, "test", str(HELPER), str(case)],
                                            cwd=directory, env=env, capture_output=True, text=True, timeout=10)
                    with self.subTest(policy=policy, mismatch=mismatch):
                        if mismatch:
                            self.assertNotEqual(result.returncode, 0)
                            self.assertIn("does not match", result.stderr)
                        else:
                            self.assertEqual(result.returncode, 0, result.stderr)
                            notice = case / "stage/share/licenses/fixture/go-toolchain/go1.26.7/LICENSE"
                            self.assertEqual(notice.read_text(), "selected compiler notices\n")


if __name__ == "__main__":
    unittest.main()
