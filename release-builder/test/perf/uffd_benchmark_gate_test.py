from __future__ import annotations

import unittest

import uffd_benchmark_gate as gate


def benchmark_line(
    fixture: str,
    pattern: str,
    strategy: str,
    ns_per_op: float,
    source_bytes: float,
    uffd_bytes: float,
) -> str:
    return (
        f"BenchmarkUFFDFaultStrategies/{fixture}/{pattern}/{strategy}-8 "
        f"1000 {ns_per_op} ns/op 1.0 MB/s {source_bytes} source-B/op "
        f"{uffd_bytes} uffd-B/op 40 B/op 1 allocs/op"
    )


def passing_output() -> str:
    lines: list[str] = []
    for fixture in gate.FIXTURES:
        for pattern in gate.PATTERNS:
            if fixture in gate.MANIFEST_FIXTURES:
                values = {
                    "A_SyncFullBatch": (1_000_000, gate.MAX_TAIL_BYTES, gate.MAX_TAIL_BYTES),
                    "B_FaultFirstNoTail": (1_000_000, gate.PAGE_SIZE, gate.PAGE_SIZE),
                    "C_FaultFirstSerialTail": (1_000_000, gate.MAX_TAIL_BYTES, gate.MAX_TAIL_BYTES),
                }
            elif fixture == "Zero":
                values = {
                    "A_SyncFullBatch": (100, 0, gate.MAX_TAIL_BYTES),
                    "B_FaultFirstNoTail": (100, 0, gate.PAGE_SIZE),
                    "C_FaultFirstSerialTail": (100, 0, gate.PAGE_SIZE + gate.INITIAL_TAIL_BYTES),
                }
            else:
                values = {
                    "A_SyncFullBatch": (100_000, gate.MAX_TAIL_BYTES, gate.MAX_TAIL_BYTES),
                    "B_FaultFirstNoTail": (1_000, gate.PAGE_SIZE, gate.PAGE_SIZE),
                    "C_FaultFirstSerialTail": (
                        10_000,
                        gate.PAGE_SIZE + gate.INITIAL_TAIL_BYTES,
                        gate.PAGE_SIZE + gate.INITIAL_TAIL_BYTES,
                    ),
                }
            for strategy, values_for_strategy in values.items():
                for _ in range(gate.MIN_SAMPLES):
                    lines.append(
                        benchmark_line(
                            fixture,
                            pattern,
                            strategy,
                            *values_for_strategy,
                        )
                    )
    return "\n".join(lines)


class UffdBenchmarkGateTest(unittest.TestCase):
    def test_parser_accepts_unsuffixed_single_cpu_name(self) -> None:
        line = benchmark_line(
            "OrdinaryData",
            "Sequential1VCPU",
            "A_SyncFullBatch",
            100_000,
            gate.MAX_TAIL_BYTES,
            gate.MAX_TAIL_BYTES,
        ).replace("A_SyncFullBatch-8", "A_SyncFullBatch")
        samples = gate.parse(line)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0].strategy, "A_SyncFullBatch")

    def test_complete_benchmark_passes(self) -> None:
        results, failures = gate.evaluate(gate.parse(passing_output()))
        self.assertFalse(failures)
        self.assertEqual(
            len(results),
            len(gate.FIXTURES) * len(gate.PATTERNS) * len(gate.STRATEGIES),
        )

    def test_ratio_regression_fails(self) -> None:
        output = passing_output().replace(
            "OrdinaryData/Sequential1VCPU/C_FaultFirstSerialTail-8 1000 10000",
            "OrdinaryData/Sequential1VCPU/C_FaultFirstSerialTail-8 1000 50000",
        )
        _, failures = gate.evaluate(gate.parse(output))
        self.assertTrue(
            any(
                "OrdinaryData/Sequential1VCPU/C_FaultFirstSerialTail" in failure
                and "ratio" in failure
                for failure in failures
            )
        )

    def test_absolute_regression_fails_when_ratio_passes(self) -> None:
        output = passing_output().replace(
            "OrdinaryData/Sequential1VCPU/A_SyncFullBatch-8 1000 100000",
            "OrdinaryData/Sequential1VCPU/A_SyncFullBatch-8 1000 1000000",
        ).replace(
            "OrdinaryData/Sequential1VCPU/B_FaultFirstNoTail-8 1000 1000",
            "OrdinaryData/Sequential1VCPU/B_FaultFirstNoTail-8 1000 25000",
        )
        _, failures = gate.evaluate(gate.parse(output))
        self.assertTrue(
            any(
                "OrdinaryData/Sequential1VCPU/B_FaultFirstNoTail" in failure
                and "ns/op" in failure
                for failure in failures
            )
        )

    def test_benchmark_failure_forces_failed_report(self) -> None:
        results, failures = gate.evaluate(
            gate.parse(passing_output()), benchmark_status=42
        )
        self.assertTrue(
            any(
                "benchmark command exited with status 42" in failure
                for failure in failures
            )
        )
        self.assertIn("Result: FAIL", gate.render_markdown(results, failures))

    def test_logical_read_amplification_fails(self) -> None:
        output = passing_output().replace(
            "4096 source-B/op 4096 uffd-B/op",
            "1048576 source-B/op 1048576 uffd-B/op",
        )
        _, failures = gate.evaluate(gate.parse(output))
        self.assertTrue(any("source-B/op" in failure for failure in failures))

    def test_missing_samples_fail(self) -> None:
        lines = passing_output().splitlines()
        needle = "ManifestHit/Random2VCPU/B_FaultFirstNoTail"
        removed = False
        filtered: list[str] = []
        for line in lines:
            if needle in line and not removed:
                removed = True
                continue
            filtered.append(line)
        _, failures = gate.evaluate(gate.parse("\n".join(filtered)))
        self.assertTrue(any("sample count 4" in failure for failure in failures))

    def test_markdown_records_failure(self) -> None:
        results, failures = gate.evaluate(gate.parse(passing_output()))
        rendered = gate.render_markdown(results, failures)
        self.assertIn("Result: PASS", rendered)
        self.assertIn("| OrdinaryData | Sequential1VCPU |", rendered)


if __name__ == "__main__":
    unittest.main()
