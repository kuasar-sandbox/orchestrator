from __future__ import annotations

import unittest
from pathlib import Path

import working_set_report as report


FLAGS = {
    "A": (True, True),
    "B": (False, True),
    "C": (True, False),
    "D": (False, False),
}

REQUIRED_METRICS = (
    "b_memory_logical_bytes",
    "b_memory_allocated_bytes",
    "w_memory_logical_bytes",
    "w_memory_allocated_bytes",
    "w_disk_logical_bytes",
    "w_disk_allocated_bytes",
    "w_memory_resident_bytes",
    "w_memory_resident_ratio",
    "snapshot_pause_ms",
    "snapshot_dump_ms",
    "snapshot_wall_ms",
    "publish_wall_ms",
    "restore_ack_ms",
    "app_ready_ms",
    "first_request_ms",
    "uffd_faults",
    "uffd_pages_copied",
    "uffd_pages_zeroed",
    "lazy_load_ratio",
    "cache_origin_requests",
)


def complete_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for group, (drop_caches, merge_ref) in FLAGS.items():
        prefetch_modes = ("off", "memory") if group == "D" else ("off",)
        for prefetch in prefetch_modes:
            row: dict[str, object] = {name: 1 for name in REQUIRED_METRICS}
            row.update(
                {
                    "group": group,
                    "iteration": 1,
                    "drop_caches": drop_caches,
                    "merge_ref": merge_ref,
                    "prefetch": prefetch,
                    "prefetch_result": "completed" if prefetch == "memory" else "disabled",
                    "prefetch_started_ms": 4.0 if prefetch == "memory" else None,
                    "prefetch_duration_ms": 5.0 if prefetch == "memory" else None,
                    "cache_origin_bytes": None,
                    "disk_reads": {
                        name: {
                            "role": role,
                            "bytes": 4096,
                            "p50_us": 2.0,
                            "p99_us": 3.0,
                        }
                        for name, role in report.EXPECTED_DISK_ROLES.items()
                    },
                }
            )
            rows.append(row)
    return rows


class WorkingSetReportTest(unittest.TestCase):
    def test_nearest_rank_percentile(self) -> None:
        self.assertEqual(report.percentile([1, 2, 3, 4], 50), 2)
        self.assertEqual(report.percentile([1, 2, 3, 4], 95), 4)
        self.assertIsNone(report.percentile([], 99))

    def test_complete_matrix_renders_prefetch_and_missing_bytes(self) -> None:
        rows = complete_rows()
        report.validate(rows, allow_partial=False)
        rendered = report.render({"inputs": {"iterations": 1}}, rows, Path("samples.jsonl"))
        self.assertIn("| D/memory | false | false | memory | completed | 1 |", rendered)
        self.assertIn("cache/store origin transferred bytes", rendered)
        self.assertIn("N/A/N/A/N/A", rendered)
        self.assertIn("| A/off | blk4 | dataset.top |", rendered)

    def test_rejects_missing_scenario(self) -> None:
        with self.assertRaisesRegex(SystemExit, "incomplete matrix"):
            report.validate(complete_rows()[:-1], allow_partial=False)

    def test_rejects_duplicate_sample(self) -> None:
        rows = complete_rows()
        rows.append(dict(rows[0]))
        with self.assertRaisesRegex(SystemExit, "duplicate sample"):
            report.validate(rows, allow_partial=False)

    def test_rejects_wrong_policy_mapping(self) -> None:
        rows = complete_rows()
        rows[0]["merge_ref"] = False
        with self.assertRaisesRegex(SystemExit, "want True/True"):
            report.validate(rows, allow_partial=False)

    def test_rejects_wrong_disk_backend_role(self) -> None:
        rows = complete_rows()
        rows[0]["disk_reads"]["blk4"]["role"] = "dataset.base"
        with self.assertRaisesRegex(SystemExit, "invalid disk role for blk4"):
            report.validate(rows, allow_partial=False)


if __name__ == "__main__":
    unittest.main()
