import contextlib
import ctypes
import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch, mock_open

HERE = Path(__file__).resolve().parent
SOURCE = HERE / 'workload.py'

class WitnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.code = SOURCE.read_text()
        cls.w = types.ModuleType('workload_proposal')
        exec(compile(cls.code, 'workload_proposal', 'exec'), cls.w.__dict__)

    def evidence(self, entry=None, raw=None, error=None):
        held = bytearray(b'\xff' * 20000)
        offset = 8192
        output = io.StringIO()
        if raw is None:
            raw = entry.to_bytes(8, sys.byteorder) if entry is not None else b''
        with patch('builtins.open', mock_open()), patch.object(self.w.os, 'pread', return_value=raw, side_effect=error) as read, contextlib.redirect_stderr(output):
            self.w.held_memory_witness(held, offset)
        line = output.getvalue()
        self.assertLess(len(line), 1200)
        self.assertEqual(bytes(held), b'\xff' * 20000)
        record = json.loads(line.removeprefix('held-memory witness '))
        base = ctypes.addressof((ctypes.c_ubyte * len(held)).from_buffer(held))
        self.assertEqual(record['buffer_base'], base)
        self.assertLessEqual(len(record['kernel_release']), 128)
        self.assertEqual(record['first_bad_va'], base + offset)
        self.assertEqual(ctypes.c_ubyte.from_address(base + offset).value, 255)
        self.assertEqual(read.call_args_list[0].args[1:], (8, ((base + offset) // record['page_size']) * 8))
        return record

    def test_visible_pfn_and_real_address_math(self):
        r = self.evidence((1 << 63) | 12345)
        self.assertEqual(r['pagemap_raw'], '0x8000000000003039')
        self.assertEqual(r['pfn'], 12345)
        self.assertEqual(r['guest_physical_address'], 12345 * r['page_size'] + r['first_bad_va'] % r['page_size'])

    def test_zero_page_and_missing_neighbor_metadata(self):
        page = ((1 << 63) | 8064).to_bytes(8, sys.byteorder)
        zero_flags = (1 << 24).to_bytes(8, sys.byteorder)
        r = self.evidence(error=[page, zero_flags, page, page, bytes(8), bytes(8)])
        self.assertTrue(r["kernel_zero_page"])
        self.assertEqual(r["kpageflags"], "0x0000000001000000")
        self.assertEqual([x["present"] for x in r["neighbor_mappings"]], [True, True, False, False])

    def test_nonpresent_swapped_and_masked(self):
        for entry, status in [(0, 'not_present_or_swapped'), ((1 << 62) | 7, 'not_present_or_swapped'), (1 << 63, 'pfn_zero_or_masked')]:
            with self.subTest(entry=entry):
                r = self.evidence(entry)
                self.assertEqual(r['pagemap_status'], status)
                self.assertNotIn('guest_physical_address', r)
                self.assertNotIn('pfn', r)

    def test_short_read_and_errors(self):
        self.assertEqual(self.evidence(raw=b'\x01' * 3)['pagemap_status'], 'short_read')
        for error in [PermissionError('denied'), OSError('io failure')]:
            r = self.evidence(error=error)
            self.assertEqual(r['pagemap_status'], 'unavailable')
            self.assertEqual(r['diagnostic_error'], type(error).__name__)
        with patch('builtins.open', side_effect=PermissionError()), contextlib.redirect_stderr(io.StringIO()) as output:
            self.w.held_memory_witness(bytearray(16), 0)
        self.assertIn('unavailable', output.getvalue())

    def test_mismatch_and_diagnostic_failure_preserve_exact_exception(self):
        held = bytearray(b'\xff' * (1 << 20))
        held[4096:4112] = bytes(16)
        expected = 'held-memory contents changed: first_page=1 offset=4096 observed=0 prefix=' + '00' * 16 + ' tick=0 held=1048576'
        for failure in [None, OSError('stderr'), MemoryError(), SystemExit(7)]:
            with self.subTest(failure=failure), patch.object(self.w, 'grow_to', return_value=held), patch.object(self.w, 'held_memory_witness', side_effect=failure) as witness:
                with self.assertRaises(RuntimeError) as caught:
                    self.w.run_hold(10, 1)
                self.assertEqual(str(caught.exception), expected)
                witness.assert_called_once_with(held, 4096)
        with patch.object(self.w, 'grow_to', return_value=held), patch.object(self.w.os, 'pread', side_effect=PermissionError()), contextlib.redirect_stderr(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, 'held-memory contents changed'):
                self.w.run_hold(10, 1)
        self.assertIn('unavailable', output.getvalue())

    def test_success_path_does_not_collect_witness(self):
        with tempfile.TemporaryDirectory(dir=HERE) as tmp, patch.object(self.w, 'grow_to', return_value=bytearray(b'\xff' * (1 << 20))), patch.object(self.w.time, 'time', side_effect=[0, 0, 0, 2]), patch.object(self.w.time, 'sleep'), patch.object(self.w, 'env_str', return_value=tmp + '/state'), patch.object(self.w, 'held_memory_witness') as witness:
            self.w.run_hold(1, 1)
            witness.assert_not_called()
            self.assertEqual(json.loads(Path(tmp + '/state').read_text())['tick'], 1)

if __name__ == '__main__':
    unittest.main()
