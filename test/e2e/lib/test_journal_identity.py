import contextlib
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

from journal_identity import validate


def records(tags, *, stable="local-1", run="run-1", build=False):
    identity = {"KUASAR_RUN_ID": run}
    if build:
        identity["KUASAR_BUILD_ID"] = "build-1"
    else:
        identity.update(KUASAR_SANDBOX_ID="local-1", KUASAR_STABLE_ID=stable)
    return [dict(identity, SYSLOG_IDENTIFIER=tag, _TRANSPORT="journal") for tag in tags]


class JournalIdentityTest(unittest.TestCase):
    def check(self, kind, rows):
        with contextlib.redirect_stdout(io.StringIO()):
            validate(kind, rows)

    def reject(self, kind, rows):
        with self.assertRaises(AssertionError):
            self.check(kind, rows)

    def test_quiet_standalone_requires_real_streams_and_default_stable_id(self):
        self.check("sandbox", records(["console", "sandbox-ctl"]))
        self.reject("sandbox", records(["console"]))
        self.reject("sandbox", records(["sandbox-ctl"]))
        self.reject("sandbox", records(["console", "sandbox-ctl"], stable="different"))

    def test_emitting_cluster_requires_all_streams_with_distinct_identity(self):
        tags = ["sandbox", "console", "sandbox-ctl"]
        self.check("cluster", records(tags, stable="logical-1"))
        for missing in tags:
            self.reject("cluster", records([tag for tag in tags if tag != missing], stable="logical-1"))
        self.reject("cluster", records(tags))

    def test_emitting_build_requires_all_streams(self):
        tags = ["build", "console", "sandbox-ctl"]
        self.check("build", records(tags, build=True))
        for missing in tags:
            self.reject("build", records([tag for tag in tags if tag != missing], build=True))

    def test_bad_actual_app_record_is_not_hidden_by_quiet_allowance(self):
        for key in ("KUASAR_RUN_ID", "KUASAR_SANDBOX_ID", "KUASAR_STABLE_ID"):
            for value in (None, "", 42):
                rows = records(["console", "sandbox-ctl", "sandbox"])
                rows[-1][key] = value
                self.reject("sandbox", rows)

    def test_conflicting_stable_ids_in_one_attempt_are_rejected(self):
        rows = records(["console", "sandbox-ctl", "sandbox"])
        rows[-1]["KUASAR_STABLE_ID"] = "other"
        self.reject("sandbox", rows)

    def test_same_run_cannot_change_both_sandbox_identity_fields(self):
        rows = records(["console", "sandbox-ctl", "sandbox"])
        rows[-1].update(KUASAR_SANDBOX_ID="wrong", KUASAR_STABLE_ID="wrong")
        self.reject("sandbox", rows)
        self.reject("sandbox", list(reversed(rows)))

    def test_distinct_runs_can_have_distinct_sandbox_identities(self):
        rows = records(["console", "sandbox-ctl"])
        other = records(["console", "sandbox-ctl"], run="run-2", stable="local-2")
        for row in other:
            row["KUASAR_SANDBOX_ID"] = "local-2"
        self.check("sandbox", rows + other)

    def test_same_run_cannot_change_build_or_object_kind(self):
        rows = records(["build", "console", "sandbox-ctl"], build=True)
        wrong = records(["build"], build=True)
        wrong[0]["KUASAR_BUILD_ID"] = "different-build"
        self.reject("build", rows + wrong)
        self.reject("build", list(reversed(rows + wrong)))
        mixed = records(["console", "sandbox-ctl"]) + records(["build"], build=True)
        self.reject("sandbox", mixed)
        self.reject("sandbox", list(reversed(mixed)))

    def test_repeated_build_phases_keep_the_same_owner(self):
        self.check("build", records(["build", "console", "sandbox-ctl"], build=True) * 2)

    def test_cross_object_fields_and_tags_are_rejected(self):
        for key in ("KUASAR_SANDBOX_ID", "KUASAR_STABLE_ID"):
            rows = records(["build", "console", "sandbox-ctl"], build=True)
            rows[0][key] = "leak"
            self.reject("build", rows)
        self.reject("build", records(["sandbox", "console", "sandbox-ctl"], build=True))
        self.reject("sandbox", records(["build", "console", "sandbox-ctl"]))

    def test_streams_from_different_attempts_cannot_fill_coverage(self):
        self.reject("cluster", records(["console", "sandbox-ctl"], stable="logical-1") +
                    records(["sandbox"], stable="logical-1", run="run-2"))
        self.reject("sandbox", records(["console"]) + records(["sandbox-ctl"], run="run-2"))
        self.reject("build", records(["console", "sandbox-ctl"], build=True) +
                    records(["build"], build=True, run="run-2"))

    def test_native_transport_and_nonempty_input_are_required(self):
        rows = records(["console", "sandbox-ctl"])
        for row in rows:
            row["_TRANSPORT"] = "stdout"
        self.reject("sandbox", rows)
        self.reject("sandbox", [])


class BuilderJournalLookupTest(unittest.TestCase):
    def lookup(self, entries):
        source = (Path(__file__).resolve().parents[1] / "e2e_run_builder.sh").read_text()
        function = re.search(r"(?ms)^build_journal_unit\(\).*?^\}", source).group()
        with tempfile.TemporaryDirectory() as temporary:
            journal = Path(temporary) / "journal.jsonl"
            journal.write_text(entries)
            # cat is a real finite pipe producer, with journalctl's SIGPIPE
            # behavior when the consumer exits before draining its output.
            script = 'set -euo pipefail\nJOURNAL=$1\njournalctl() { cat "$JOURNAL"; }\n'
            script += function + '\nbuild_journal_unit fixture\n'
            return subprocess.run(["bash", "-c", script, "_", str(journal)],
                                  text=True, capture_output=True, timeout=10)

    def test_match_with_trailing_output_keeps_unit_and_success(self):
        unit = "sandbox-builder@fixture.service"
        entries = json.dumps({"_SYSTEMD_UNIT": unit}) + "\n"
        entries += json.dumps({"_SYSTEMD_UNIT": "sandbox-builder@later.service"}) + "\n"
        entries += "{}\n" * (1 << 20)  # More than a pipe buffer, without sleeps.
        result = self.lookup(entries)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, unit + "\n")

    def test_missing_builder_unit_still_fails(self):
        result = self.lookup('invalid JSON\n{"_SYSTEMD_UNIT":"other.service"}\n{}\n')
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
