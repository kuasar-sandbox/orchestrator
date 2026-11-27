"""Required Builder coverage must survive reuse of the full Go test stream."""

import importlib.util
import io
import json
from pathlib import Path
import unittest


SOURCE = Path(__file__).resolve().parents[2] / "source" / "builder_state.py"
SPEC = importlib.util.spec_from_file_location("source_builder", SOURCE)
BUILDER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BUILDER)


def results():
    events = []
    for package, names in BUILDER.REQUIRED.items():
        for name in sorted(names):
            for test in (name, name + "/case"):
                events.extend({"Package": BUILDER.MODULE + package, "Test": test,
                               "Action": action} for action in ("run", "pass"))
    return events


class SourceBuilder(unittest.TestCase):
    def validate(self, events):
        output = io.StringIO()
        BUILDER.validate(io.StringIO("".join(json.dumps(event) + "\n" for event in events)), output)
        return output.getvalue()

    def test_complete_contracts_and_subcases_pass(self):
        self.assertIn("24 contracts, 48 assertions/subcases", self.validate(results()))

    def test_missing_contract_and_parent_pass_without_run_fail(self):
        events = results()
        missing = events[0]["Test"]
        for incomplete in ([], [e for e in events if not e["Test"].startswith(missing)],
                           [e for e in events if e["Action"] != "run"]):
            with self.assertRaisesRegex(ValueError, "missing="):
                self.validate(incomplete)

    def test_started_child_must_finish(self):
        events = results()
        events.append({**events[0], "Test": events[0]["Test"] + "/new-child"})
        with self.assertRaisesRegex(ValueError, "unfinished="):
            self.validate(events)

    def test_required_parent_and_children_cannot_skip_or_fail(self):
        for action in ("skip", "fail"):
            for index in (1, 3):
                events = results()
                events[index]["Action"] = action
                with self.subTest(action=action, index=index), self.assertRaises(ValueError):
                    self.validate(events)

    def test_unrelated_environment_sensitive_skip_is_allowed(self):
        events = results()
        events.extend(({"Package": BUILDER.MODULE + "internal/orch",
                        "Test": "TestNeedsRealSystemd", "Action": "skip"},
                       {"Package": BUILDER.MODULE + "internal/other", "Action": "skip"}))
        self.validate(events)

    def test_same_name_in_another_package_cannot_supply_missing_contract(self):
        events = results()
        missing = events[0]["Test"]
        for event in events:
            if event["Test"].startswith(missing):
                event["Package"] = BUILDER.MODULE + "internal/other"
        with self.assertRaisesRegex(ValueError, "missing="):
            self.validate(events)

    def test_package_failure_is_not_hidden_by_successful_builder_contracts(self):
        with self.assertRaisesRegex(ValueError, "package failed"):
            self.validate(results() + [{"Package": BUILDER.MODULE + "internal/other", "Action": "fail"}])

    def test_invalid_output_is_rejected_after_draining_stream(self):
        stream = io.StringIO("not JSON\n[]\n")
        with self.assertRaisesRegex(ValueError, "invalid Go JSON event"):
            BUILDER.validate(stream, io.StringIO())
        self.assertEqual(stream.read(), "")


if __name__ == "__main__":
    unittest.main()
