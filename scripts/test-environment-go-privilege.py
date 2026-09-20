#!/usr/bin/env python3
"""Keep effective Go selection without extending sudo's root command PATH."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
ENTRIES = ['test/e2e/e2e_orchestrator_proxy.sh']


class GoPrivilegeBoundary(unittest.TestCase):
    def check_selection(self, switches):
        with tempfile.TemporaryDirectory(prefix="go-privilege-") as directory:
            root = Path(directory)
            caller, secure = root / "caller tools", root / "secure tools"
            caller.mkdir()
            secure.mkdir()
            bundled, selected = root / "bundled root", root / "selected root"
            bundled.mkdir()
            selected.mkdir()
            for directory in (caller, secure):
                for name in ("dirname", "env"):
                    (directory / name).symlink_to(shutil.which(name))
            (caller / "bash").write_text('#!/bin/sh\necho UNSAFE_CALLER_BASH\nexit 88\n')
            (secure / "go").write_text('#!/bin/sh\necho wrong-system-go\n')
            # The launcher must locate a named Go on PATH when a switch is needed.
            # No selected-root/bin/go exists: preserve the environment named entry.
            (caller / "go").write_text('''#!/bin/sh
if [ "$1" = -C ]; then shift 2; fi
if [ "${GOTOOLCHAIN-}" = local ]; then echo "$BUNDLED_ROOT"; exit 0; fi
if [ "$SWITCH_TOOLCHAIN" = yes ]; then exec go1.99.1 "$@"; fi
case "$1:${2-}" in env:GOROOT) echo "$BUNDLED_ROOT";; env:GOVERSION) echo go1.24.0;; version:) echo selected-environment-go;; *) exit 61;; esac
''')
            (caller / "go1.99.1").write_text('''#!/bin/sh
[ "$SWITCH_TOOLCHAIN" = yes ] || { echo bypassed-caller-wrapper; exit 62; }
case "$1:${2-}" in env:GOROOT) echo "$SELECTED_ROOT";; env:GOVERSION) echo go1.99.1;; version:) echo selected-environment-go;; *) exit 63;; esac
''')
            (caller / "sudo").write_text(
                '#!/usr/bin/python3\nimport os,sys\na=sys.argv[1:]\n'
                'while a and a[0].startswith("-"):a.pop(0)\n'
                'os.environ["PATH"]=os.environ["SECURE_PATH"]\nos.execvp(a[0],a)\n')
            for path in list(caller.iterdir()) + list(secure.iterdir()):
                if not path.is_symlink():
                    path.chmod(0o755)
            probe = root / "root probe.sh"
            probe.write_text('#!/bin/sh\n"$KUASAR_E2E_GO" version\n'
                             'test "$PATH" = "$SECURE_PATH" || exit 89\n'
                             'test "$GOROOT" = "$EXPECTED_ROOT" || exit 90\n'
                             'printf "%s\\n" "$GOTOOLCHAIN" "$1"\n')
            probe.chmod(0o755)
            env = dict(os.environ, PATH=str(caller), SECURE_PATH=str(secure),
                       GOTOOLCHAIN="go1.99.1+path" if switches else "auto",
                       BUNDLED_ROOT=str(bundled), SELECTED_ROOT=str(selected),
                       EXPECTED_ROOT=str(selected if switches else bundled),
                       SWITCH_TOOLCHAIN="yes" if switches else "no")
            env.pop("GOROOT", None)
            env.pop("KUASAR_E2E_GO", None)
            for entry in ENTRIES:
                lines = (ROOT / entry).read_text().splitlines()
                start = next(i for i, line in enumerate(lines) if "selected_go=$(command -v" in line)
                end = next(i for i in range(start, len(lines)) if "exec sudo" in lines[i])
                block = "\n".join(lines[start:end + 1])
                with self.subTest(entry=entry):
                    result = subprocess.run(["/bin/bash", "-eu", "-c", block,
                                             str(probe), "preserved argument"], env=env,
                                            capture_output=True, text=True, timeout=10)
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertEqual(result.stdout.splitlines(),
                                     ["selected-environment-go", env["GOTOOLCHAIN"], "preserved argument"])
                    self.assertNotIn("UNSAFE_CALLER_BASH", result.stdout)

    def test_unswitched_environment_wrapper_is_preserved(self):
        self.check_selection(False)

    def test_named_toolchain_survives_secure_path_reset(self):
        self.check_selection(True)


if __name__ == "__main__":
    unittest.main()
