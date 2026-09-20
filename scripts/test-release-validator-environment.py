"""Run the real host parser despite persistent and ambient cross-build settings."""
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
FUNCTION = re.search(r"(?ms)^validate_archive_paths\(\) \{\n.*?^\}", (ROOT / "scripts/release.sh").read_text()).group()


class ValidatorEnvironment(unittest.TestCase):
    def test_host_parser_ignores_persistent_and_ambient_build_settings(self):
        with tempfile.TemporaryDirectory(prefix="archive-parser-environment-") as temporary:
            work = Path(temporary)
            archive = work / "empty.tar.gz"
            with tarfile.open(archive, "w:gz"):
                pass
            marker = work / "unexpected-wrapper"
            wrapper = work / "toolexec"
            wrapper.write_text("#!/bin/sh\ntouch " + str(marker) + "\nexit 91\n")
            wrapper.chmod(0o755)
            config = work / "goenv"
            settings = "GOOS=windows\nGOARCH=arm64\nGOFLAGS=-toolexec=" + str(wrapper) + "\n"
            config.write_text(settings)
            command = 'set -euo pipefail\nROOT=$1\nfail() { echo "$*" >&2; exit 1; }\n' + FUNCTION + '\nvalidate_archive_paths "$2"\n'
            cases = (
                {},
                {"GOOS": "darwin", "GOARCH": "arm64"},
                {"GOFLAGS": "-toolexec=" + str(wrapper)},
                {"GOAMD64": "v4", "GOEXPERIMENT": "not-a-real-experiment"},
            )
            for overrides in cases:
                with self.subTest(overrides=sorted(overrides)):
                    environment = dict(os.environ, GOENV=str(config), **overrides)
                    result = subprocess.run(["bash", "-c", command, "_", str(ROOT), str(archive)],
                                            cwd=work, env=environment, text=True, capture_output=True, timeout=90)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("release archive: missing member", result.stderr)
                    self.assertFalse(marker.exists(), "ambient Go wrapper executed")
                    self.assertEqual(config.read_text(), settings)

    def test_parser_does_not_load_the_callers_product_module(self):
        with tempfile.TemporaryDirectory(prefix="archive-parser-module-") as temporary:
            work = Path(temporary)
            # A product can require a newer compiler than this local host helper.
            # It must not trigger module/toolchain selection for standalone parsing.
            (work / "go.mod").write_text("module caller.invalid\n\ngo 999.0.0\n")
            archive = work / "empty.tar.gz"
            with tarfile.open(archive, "w:gz"):
                pass
            command = 'set -euo pipefail\nROOT=$1\nfail() { echo "$*" >&2; exit 1; }\n' + FUNCTION + '\nvalidate_archive_paths "$2"\n'
            result = subprocess.run(["bash", "-c", command, "test", str(ROOT), str(archive)],
                                    cwd=work, env=dict(os.environ, GOPROXY="off", GOSUMDB="off"),
                                    text=True, capture_output=True, timeout=90)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("release archive: missing member", result.stderr)
            self.assertNotIn("go.mod requires", result.stderr)
            self.assertNotIn("downloading", result.stderr)


if __name__ == "__main__":
    unittest.main()
