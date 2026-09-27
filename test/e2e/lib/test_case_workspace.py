"""Real Unix socket and evidence ownership checks for prepared case workspaces."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


HELPER = Path(__file__).with_name("case_workspace.sh")


class CaseWorkspace(unittest.TestCase):
    def run_workspace(self, output, body, *args, keep=False):
        env = dict(os.environ, WORK=str(output), OUT=str(output))
        env.pop("E2E_KEEP", None)
        if keep:
            env["E2E_KEEP"] = "1"
        result = subprocess.run(
            ["bash", "-c", 'set -euo pipefail; . "$1"; shift; '
             'case_workspace_init; trap case_workspace_cleanup EXIT; ' + body,
             "_", str(HELPER), *args], env=env, text=True,
            capture_output=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def test_long_runner_path_allows_maximum_sandbox_socket(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / ("runner-" + "x" * 80) / "orchestrator.resource-reservation.sh"
            probe = '''import json, pathlib, socket, sys
work = pathlib.Path(sys.argv[1])
path = work / "run" / "sandboxes" / ("s" * 57) / "vsock.sock_5000"
path.parent.mkdir(parents=True)
with socket.socket(socket.AF_UNIX) as server:
    server.bind(str(path))
    print(json.dumps({"work": str(work), "socket_bytes": len(str(path).encode())}))
'''
            observed = json.loads(self.run_workspace(output, 'python3 -c "$1" "$WORK"', probe))
            self.assertLessEqual(observed["socket_bytes"], 107)
            self.assertFalse(Path(observed["work"]).exists())
            self.assertTrue(output.is_dir())

    def test_cleanup_keeps_logs_and_preserves_runner_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            (output / "prepared.input").write_text("prepared")
            runtime = self.run_workspace(output,
                'printf "%s\\n" "$WORK"; printf evidence > "$WORK/node.log"; '
                'printf private > "$WORK/config.yaml"').strip()
            self.assertFalse(Path(runtime).exists())
            self.assertEqual((output / "prepared.input").read_text(), "prepared")
            self.assertEqual((output / "runtime/node.log").read_text(), "evidence")
            self.assertFalse((output / "runtime/config.yaml").exists())

    def test_explicit_keep_retains_only_this_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            output = self.run_workspace(Path(directory), 'printf "%s\\n" "$WORK"', keep=True)
            runtime = Path(output.splitlines()[0])
            try:
                self.assertTrue(runtime.is_dir())
                self.assertIn("kept runtime directory: " + str(runtime), output)
            finally:
                shutil.rmtree(runtime)


if __name__ == "__main__":
    unittest.main()
