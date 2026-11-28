"""Runtime rejection must be observed from the exact fixture within a fixed budget."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from cluster_legacy_runtime import wait_runtime_rejection


REJECTION = "no compatible runtime bundle: identity mismatch: runtime-v1.bundle\n"
PRIVATE = "fixture-private-capability"


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class RuntimeRejectionObservation(unittest.TestCase):
    def setUp(self):
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.node_log = self.work / "cluster-node.log"
        self.node_log.write_text("")
        self.rows = [{"id": "fixture-sid", "run_id": "fixture-run", "dead_run_id": ""}]
        self.diagnostic = {"case": "C02-public-cluster-legacy-runtime"}
        self.clock = Clock()
        self.query = Mock(return_value=SimpleNamespace(stdout=""))

    def observe(self, **kwargs):
        return wait_runtime_rejection(
            self.work, self.rows, 1000.9, self.diagnostic,
            run=self.query, monotonic=self.clock.monotonic, sleep=self.clock.sleep, **kwargs)

    def test_delayed_journal_becomes_visible_without_changing_query_identity(self):
        self.query.side_effect = [SimpleNamespace(stdout=text) for text in
                                  ("", "", "", REJECTION)]
        self.assertEqual(self.observe(), [REJECTION.strip()])
        self.assertEqual(self.clock.sleeps, [.05])
        expected = [
            ["journalctl", "KUASAR_SANDBOX_ID=fixture-sid", "--since", "@998", "--no-pager", "-o", "cat"],
            ["journalctl", "-u", "sandbox-runner@fixture-run.service", "--since", "@998", "--no-pager", "-o", "cat"],
        ] * 2
        self.assertEqual([call.args[0] for call in self.query.call_args_list], expected)
        self.assertEqual(self.diagnostic["journal_reads"], 4)
        for call in self.query.call_args_list:
            self.assertTrue(call.kwargs["check"])
            self.assertGreater(call.kwargs["timeout"], 0)
            self.assertLessEqual(call.kwargs["timeout"], 5)

    def test_node_log_is_reread_after_initial_absence(self):
        def query(*args, **kwargs):
            self.node_log.write_text(REJECTION)
            return SimpleNamespace(stdout="")

        self.query.side_effect = query
        self.assertEqual(self.observe(), [REJECTION.strip()])
        self.assertEqual(self.clock.sleeps, [.05])

    def test_visible_rejection_needs_no_sleep(self):
        self.query.return_value = SimpleNamespace(stdout=REJECTION)
        self.assertTrue(self.observe())
        self.assertEqual(self.clock.sleeps, [])

    def test_permanently_absent_evidence_fails_at_the_fixed_deadline(self):
        with self.assertRaisesRegex(AssertionError, "missing Node runtime rejection"):
            self.observe(timeout=.1)
        self.assertAlmostEqual(self.clock.now, 100.1)
        self.assertFalse(self.diagnostic["runtime_rejection_present"])
        self.assertFalse(self.diagnostic["digest_mismatch_present"])
        self.assertFalse(self.diagnostic["captured_runtime_present"])

    def test_query_duration_consumes_the_same_budget(self):
        def query(*args, **kwargs):
            self.clock.now += .06
            return SimpleNamespace(stdout="")

        self.query.side_effect = query
        with self.assertRaises(AssertionError):
            self.observe(timeout=.1)
        self.assertEqual(self.query.call_count, 2)
        self.assertAlmostEqual(self.query.call_args_list[0].kwargs["timeout"], .1)
        self.assertAlmostEqual(self.query.call_args_list[1].kwargs["timeout"], .04)
        self.assertEqual(self.clock.sleeps, [])

    def test_complete_evidence_returned_after_deadline_is_rejected(self):
        def query(*args, **kwargs):
            self.clock.now += .11
            return SimpleNamespace(stdout=REJECTION)

        self.query.side_effect = query
        with self.assertRaisesRegex(AssertionError, "exceeded observation budget"):
            self.observe(timeout=.1)
        self.assertEqual(self.query.call_count, 1)
        self.assertEqual(self.clock.sleeps, [])

    def test_other_rejections_and_partial_evidence_never_pass(self):
        samples = (
            "proxy unavailable; " + PRIVATE,
            "no compatible runtime bundle: runtime-v1.bundle",
            "identity mismatch: runtime-v1.bundle",
            "no compatible runtime bundle: identity mismatch: runtime-v2.bundle",
        )
        for sample in samples:
            with self.subTest(sample=sample):
                self.query.return_value = SimpleNamespace(stdout=sample)
                with self.assertRaises(AssertionError):
                    self.observe(timeout=.05)

    def test_journal_failures_are_not_retried_or_replaced_by_later_success(self):
        errors = (
            FileNotFoundError("journalctl"),
            subprocess.CalledProcessError(1, "journalctl", stderr=PRIVATE),
            subprocess.TimeoutExpired("journalctl", 5, stderr=PRIVATE),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__):
                self.query.reset_mock()
                self.query.side_effect = [error, SimpleNamespace(stdout=REJECTION)]
                with self.assertRaises(type(error)) as raised:
                    self.observe()
                self.assertIs(raised.exception, error)
                self.query.assert_called_once()
                self.assertEqual(self.clock.sleeps, [])

    def test_frozen_dead_run_and_sandbox_identity_survive_row_changes(self):
        self.rows[0].update(run_id="", dead_run_id="fixture-dead-run")

        def query(*args, **kwargs):
            self.rows[0].update(id="unrelated-sid", dead_run_id="unrelated-run")
            return SimpleNamespace(stdout=REJECTION if self.query.call_count > 2 else "")

        self.query.side_effect = query
        self.assertTrue(self.observe())
        self.assertEqual(self.query.call_count, 4)
        for call in self.query.call_args_list:
            self.assertNotIn("unrelated", " ".join(call.args[0]))
        self.assertIn("sandbox-runner@fixture-dead-run.service", self.query.call_args.args[0])

    def test_missing_log_read_failure_is_not_retried(self):
        self.node_log.unlink()
        with self.assertRaises(FileNotFoundError):
            self.observe()
        self.query.assert_not_called()
        self.assertEqual(self.clock.sleeps, [])

    def test_private_log_content_never_enters_diagnostics_or_output(self):
        self.query.return_value = SimpleNamespace(stdout=PRIVATE)
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            with self.assertRaises(AssertionError):
                self.observe(timeout=.05)
        self.assertEqual(stdout.getvalue() + stderr.getvalue(), "")
        self.assertNotIn(PRIVATE, json.dumps(self.diagnostic))
        self.assertTrue(all(isinstance(value, (str, bool, int)) for value in self.diagnostic.values()))


if __name__ == "__main__":
    unittest.main()
