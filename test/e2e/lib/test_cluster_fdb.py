"""Exercise the prepared cluster's isolated database lifecycle without KVM."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("cluster_process.sh")
ROOT = SCRIPT.parents[3]


class PreparedClusterDatabase(unittest.TestCase):
    def run_fixture(self, *, mode="ready", missing=""):
        with tempfile.TemporaryDirectory(prefix="prepared-fdb-") as temporary:
            root = Path(temporary)
            work = root / "case"
            work.mkdir()
            runtime = root / "product/lib/cluster-ctl"
            runtime.mkdir(parents=True)
            (runtime / "libfdb_c.so").write_bytes(b"fixture")
            loader = runtime / "ld.so"
            loader.write_text('#!/bin/sh\n[ "$1" = --inhibit-cache ] && [ "$2" = --library-path ] || exit 81\nshift 3\nexec "$@"\n')
            loader.chmod(0o755)
            server = root / "fdbserver-test"
            server.write_text('#!' + sys.executable + '\nimport signal,time\n'
                              'signal.signal(signal.SIGTERM, lambda *args: exit(0))\n'
                              'while True: time.sleep(0.05)\n')
            server.chmod(0o755)
            cli = root / "fdbcli-test"
            cli.write_text('#!' + sys.executable + '\nimport json,os,sys\n'
                           'from pathlib import Path\n'
                           'args=sys.argv[1:]\n'
                           'with open(os.environ["COMMAND_LOG"],"a") as f: f.write(json.dumps(args)+"\\n")\n'
                           'assert args[0] == "-C" and Path(args[1]).is_file()\n'
                           'if args[3] == "configure new single ssd":\n'
                           '    assert args[4:] == ["--timeout","20"]\n'
                           '    sys.exit(1 if os.environ["MODE"] != "ready" else 0)\n'
                           'assert args[3] == "status minimal"\n'
                           'print("The database is available" if os.environ["MODE"] != "unavailable" else "unavailable")\n')
            cli.chmod(0o755)
            if missing == "server":
                server.unlink()
            if missing == "runtime":
                loader.unlink()
            external = root / "external.cluster"
            external.write_text("do-not-use\n")
            log = root / "commands.jsonl"
            script = r'''
set -euo pipefail
PIDS=()
CLUSTER_FDB_PID=""
cleanup() { for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT
fail() { echo "$*" >&2; exit 19; }
step() { echo "$*" >&2; }
free_port() { echo 45678; }
source "$PROCESS_SCRIPT"
write_registry_configs 3 1
start_cluster_fdb
printf '%s\n' "$CLUSTER_FDB_PID" > "$WORK/server.pid"
[ "${#PIDS[@]}" = 1 ]
'''
            result = subprocess.run(["bash", "-c", script], text=True, capture_output=True, timeout=8,
                                    env={**os.environ, "WORK": str(work), "BIN": str(root / "product/bin"),
                                         "FDB_SERVER_TEST_BIN": str(server), "FDB_CLI_TEST_BIN": str(cli),
                                         "FDB_CLUSTER_FILE": str(external), "COMMAND_LOG": str(log),
                                         "MODE": mode, "PROCESS_SCRIPT": str(SCRIPT)})
            calls = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            configs = [path.read_text() for path in sorted(work.glob("registry-*.yaml"))]
            cluster = work / "fdb/fdb.cluster"
            cluster_data = cluster.read_text() if cluster.exists() else ""
            pid_file = work / "server.pid"
            if pid_file.exists():
                with self.assertRaises(ProcessLookupError):
                    os.kill(int(pid_file.read_text()), 0)
            self.assertEqual(external.read_text(), "do-not-use\n")
            for call in calls:
                self.assertEqual(call[0:2], ["-C", str(cluster)])
            return result, configs, calls, cluster_data

    def test_configures_one_case_database_and_cleans_its_process(self):
        result, configs, calls, cluster = self.run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(configs), 3)
        prefixes = {re.search(r'^  prefix: "([^"]+)"$', config, re.M).group(1) for config in configs}
        self.assertEqual(len(prefixes), 1)
        self.assertRegex(next(iter(prefixes)), r"^/orchestrator-e2e/[0-9a-f]{32}/$")
        self.assertRegex(cluster, r"^test:[0-9a-f]{32}@127.0.0.1:45678\n$")
        self.assertEqual([call[3] for call in calls], ["configure new single ssd", "status minimal"])
        self.assertTrue(all('node_link: 1' in config for config in configs))

    def test_configure_timeout_is_reconciled_before_retry(self):
        result, configs, calls, _ = self.run_fixture(mode="unknown")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(configs), 3)
        self.assertEqual(sum(call[3] == "configure new single ssd" for call in calls), 1)

    def test_missing_prepared_inputs_or_unavailable_database_prevents_configs(self):
        for missing in ("server", "runtime"):
            with self.subTest(missing=missing):
                result, configs, calls, _ = self.run_fixture(missing=missing)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(configs or calls)
        result, configs, calls, _ = self.run_fixture(mode="unavailable")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("configuration failed", result.stderr)
        self.assertFalse(configs)
        self.assertEqual(sum(call[3] == "configure new single ssd" for call in calls), 3)

    def test_helpers_apply_to_cluster_cases_only(self):
        import fnmatch
        declaration = json.loads((ROOT / "release/helper-inputs.json").read_text())
        helpers = {row["name"]: row for row in declaration["helpers"]}
        for name, variable in (("fdbserver-test", "FDB_SERVER_TEST_BIN"), ("fdbcli-test", "FDB_CLI_TEST_BIN")):
            row = helpers[name]
            self.assertEqual(row["env"], variable)
            for case in ("orchestrator.cluster-lifecycle.sh", "orchestrator.cluster-recovery.sh", "orchestrator.cluster-reset.sh"):
                self.assertTrue(any(fnmatch.fnmatchcase(case, pattern) for pattern in row["cases"]))
            self.assertFalse(any(fnmatch.fnmatchcase("orchestrator.exec.sh", pattern) for pattern in row["cases"]))


if __name__ == "__main__":
    unittest.main()
