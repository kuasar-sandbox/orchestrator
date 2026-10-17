"""Ownership checks for temporary systemd lifecycle units, without host mutation."""
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

LIB = Path(__file__).with_name('proxy_lifecycle.sh')
STATE = Path(__file__).with_name('execute_state.sh')


class LifecycleUnits(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work = self.root / 'work'
        self.work.mkdir(mode=0o700)
        self.units = self.root / 'units'
        self.units.mkdir()
        self.log = self.root / 'commands'
        self.key = 'e-abcdef'
        self.record = self.work / 'proxy-lifecycle.units'
        self.names = [f'kuasar-proxy-{self.key}{suffix}' for suffix in ('-data.socket', '-mmds.socket', '.service')]

    def shell(self, body, **extra):
        script = r'''
set -euo pipefail
. "$STATE"
. "$LIB"
systemctl() {
    printf '%s\n' "$*" >>"$COMMAND_LOG"
    case "$1" in
        show)
            if [ "$2" = --property=ControlGroup ]; then echo "${MOCK_CGROUP:-}"; return; fi
            if [ -f "$KUASAR_EXECUTE_UNIT_DIR/${!#}" ]; then echo loaded; else echo not-found; fi;;
        stop) [ "${FAIL_STOP:-0}" != 1 ];;
        reset-failed|daemon-reload) :;;
        *) return 97;;
    esac
}
journalctl() { echo owned-unit-journal; }
''' + body
        return subprocess.run(['bash', '-c', script], text=True, capture_output=True, timeout=5,
            env=dict(os.environ, STATE=str(STATE), LIB=str(LIB), KUASAR_EXECUTE_UNIT_DIR=str(self.units),
                     COMMAND_LOG=str(self.log), **extra))

    def reserve(self):
        result = self.shell(shlex.join(['proxy_lifecycle_reserve_units', str(self.work), self.key]))
        self.assertEqual(result.returncode, 0, result.stderr)

    def cleanup_units(self, **extra):
        return self.shell(shlex.join(['proxy_lifecycle_cleanup_units', str(self.work), self.key]), **extra)

    def test_foreign_existing_unit_prevents_reservation(self):
        (self.units / self.names[1]).write_text('foreign')
        result = self.shell(shlex.join(['proxy_lifecycle_reserve_units', str(self.work), self.key]))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.record.exists())
        self.assertNotIn('stop ', self.log.read_text())

    def test_complete_cleanup_selects_only_exact_reserved_units(self):
        self.reserve()
        for name in self.names:
            (self.units / name).write_text('owned')
        foreign = self.units / 'kuasar-proxy-foreign.service'
        foreign.write_text('keep')
        result = self.cleanup_units()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.record.exists())
        self.assertEqual(foreign.read_text(), 'keep')
        self.assertTrue(all(not (self.units/name).exists() for name in self.names))
        self.assertIn('stop ' + ' '.join(self.names), self.log.read_text())

    def test_stop_failure_preserves_retry_authority_and_files(self):
        self.reserve()
        (self.units/self.names[2]).write_text('owned')
        result = self.cleanup_units(FAIL_STOP='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.record.exists())
        self.assertTrue((self.units/self.names[2]).exists())
        self.assertEqual(self.cleanup_units().returncode, 0)

    def test_invalid_record_or_linked_unit_cannot_stop_a_service(self):
        self.reserve()
        for invalid in ('e-fedcba\n', self.key+'\n\n'):
            self.record.write_text(invalid)
            self.log.unlink(missing_ok=True)
            self.assertNotEqual(self.cleanup_units().returncode, 0)
            self.assertFalse(self.log.exists())
        self.record.write_text(self.key+'\n')
        (self.units/self.names[0]).symlink_to(self.record)
        self.assertNotEqual(self.cleanup_units().returncode, 0)
        self.assertFalse(self.log.exists())
