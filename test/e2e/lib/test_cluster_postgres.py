"""PostgreSQL fixture owns exact resources, requires transactions and keeps secrets private."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("postgres_fixture.sh")


class PostgresFixtureTest(unittest.TestCase):
    def fixture(self, mode="ready"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            docker = root / "docker"
            docker.write_text("#!" + sys.executable + r'''
import json, os, pathlib, sys
root=pathlib.Path(os.environ['WORK']);args=sys.argv[1:];safe=list(args)
for i,arg in enumerate(safe):
    if arg.startswith('PGPASSWORD='):safe[i]='PGPASSWORD=[private]'
with (root/'calls').open('a') as out:out.write(json.dumps(safe)+'\n')
mode=os.environ['FIXTURE_MODE']
if args[:2]==['image','inspect']:sys.exit(1 if mode=='missing-image' else 0)
if args[:2]==['network','create']:print('owned-network-id');sys.exit(0)
if args[0]=='create':
    password=os.environ.get('POSTGRES_PASSWORD','')
    assert len(password)==48
    (root/'password.private').write_text(password)
    assert '--rm' not in args and args[args.index('--network')+1]=='owned-network-id'
    print('owned-container-id');sys.exit(0)
if args[0]=='start':
    assert args[1]=='owned-container-id';sys.exit(1 if mode=='start-fails' else 0)
if args[0]=='port':print('127.0.0.1:54329');sys.exit(0)
if args[0]=='exec':
    assert args[1]=='--env' and args[2]=='PGPASSWORD='+(root/'password.private').read_text()
    assert args[3]=='owned-container-id' and '-h' in args and 'ON_ERROR_STOP=on' in args
    assert any('CREATE TABLE' in value for value in args) and any('SELECT id' in value for value in args)
    print('1');sys.exit(0)
if args[:2]==['rm','-fv']:
    assert args[2]=='owned-container-id';sys.exit(1 if mode=='cleanup-fails' else 0)
if args[:2]==['network','rm']:
    assert args[2]=='owned-network-id';sys.exit(0)
raise AssertionError(safe)
''')
            docker.chmod(0o755)
            result = subprocess.run(["bash", "-euc", '''
source "$FIXTURE_SCRIPT"
TEST_POSTGRES_CONTAINER="" TEST_POSTGRES_NETWORK=""
status=0
start_test_postgres || status=$?
if [ "$status" = 0 ]; then
    [ -n "$STORAGE_TEST_POSTGRES_DSN" ] || exit 9
fi
stop_test_postgres || { [ "$status" != 0 ] || status=1; }
exit "$status"
'''], env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
           "WORK": directory, "ORCHESTRATOR_POSTGRES_IMAGE": "prepared-image-id",
           "FIXTURE_SCRIPT": str(SCRIPT), "FIXTURE_MODE": mode},
                capture_output=True, text=True, timeout=10)
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()]
            private = root / "password.private"
            if private.exists():
                self.assertNotIn(private.read_text(), result.stdout + result.stderr + json.dumps(calls))
            return result, calls

    def test_real_transaction_probe_before_success_and_exact_resource_cleanup(self):
        result, calls = self.fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call[0] for call in calls], ["image", "network", "create", "start", "port", "exec", "rm", "network"])

    def test_prepared_image_is_required_without_hidden_pull(self):
        result, calls = self.fixture("missing-image")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls, [["image", "inspect", "prepared-image-id"]])

    def test_failed_start_still_removes_owned_container_and_network(self):
        result, calls = self.fixture("start-fails")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(calls[-2:], [["rm", "-fv", "owned-container-id"], ["network", "rm", "owned-network-id"]])

    def test_cleanup_failure_cannot_report_success(self):
        result, _ = self.fixture("cleanup-fails")
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
