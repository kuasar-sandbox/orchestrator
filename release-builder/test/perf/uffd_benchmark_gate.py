#!/usr/bin/env python3
"""Evaluate the standardized sandboxer UFFD A/B/C benchmark.

The gate uses medians and same-process ratios so host speed is mostly divided
out. Absolute ceilings remain deliberately loose and catch common-mode stalls
that a ratio alone would miss.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


BENCHMARK_RE = re.compile(
    r"^BenchmarkUFFDFaultStrategies/"
    r"(?P<fixture>[^/]+)/(?P<pattern>[^/]+)/(?P<strategy>[^-\s]+)(?:-\d+)?\s+"
    r"\d+\s+(?P<ns>[0-9.]+)\s+ns/op"
    r".*?\s(?P<source>[0-9.]+)\s+source-B/op"
    r".*?\s(?P<uffd>[0-9.]+)\s+uffd-B/op"
)

STRATEGIES = (
    "A_SyncFullBatch",
    "B_FaultFirstNoTail",
    "C_FaultFirstSerialTail",
)
FIXTURES = (
    "OrdinaryData",
    "ManifestHit",
    "ManifestColdCopy",
    "LocalPlaintextTar",
    "LocalEncryptedTar",
    "Zero",
)
PATTERNS = ("Sequential1VCPU", "Random2VCPU")
DATA_FIXTURES = {
    "OrdinaryData",
    "LocalPlaintextTar",
    "LocalEncryptedTar",
}
MANIFEST_FIXTURES = {"ManifestHit", "ManifestColdCopy"}
MIN_SAMPLES = 5
PAGE_SIZE = 4096
INITIAL_TAIL_BYTES = 64 << 10
MAX_TAIL_BYTES = 1 << 20


@dataclass(frozen=True)
class Sample:
    fixture: str
    pattern: str
    strategy: str
    ns_per_op: float
    source_bytes_per_op: float
    uffd_bytes_per_op: float


@dataclass(frozen=True)
class Threshold:
    max_ratio: float
    max_ns_per_op: float
    max_source_bytes: float
    max_uffd_bytes: float


@dataclass(frozen=True)
class Result:
    fixture: str
    pattern: str
    strategy: str
    samples: int
    ns_per_op: float
    source_bytes_per_op: float
    uffd_bytes_per_op: float
    ratio_to_sync: float
    threshold: Threshold | None
    passed: bool
    failures: tuple[str, ...]


def threshold_for(fixture: str, pattern: str, strategy: str) -> Threshold | None:
    if strategy == "A_SyncFullBatch":
        return None

    if fixture in DATA_FIXTURES:
        if pattern == "Sequential1VCPU":
            ratio = 0.10 if strategy == "B_FaultFirstNoTail" else 0.25
            absolute = 20_000.0 if strategy == "B_FaultFirstNoTail" else 100_000.0
        else:
            ratio = 0.25 if strategy == "B_FaultFirstNoTail" else 0.50
            absolute = 30_000.0 if strategy == "B_FaultFirstNoTail" else 150_000.0
        logical_bytes = PAGE_SIZE if strategy == "B_FaultFirstNoTail" else PAGE_SIZE + INITIAL_TAIL_BYTES
        return Threshold(ratio, absolute, logical_bytes, logical_bytes)

    if fixture in MANIFEST_FIXTURES:
        ratio = 1.50 if pattern == "Sequential1VCPU" else 1.75
        absolute = 5_000_000.0 if pattern == "Sequential1VCPU" else 8_000_000.0
        logical_bytes = PAGE_SIZE if strategy == "B_FaultFirstNoTail" else MAX_TAIL_BYTES
        return Threshold(ratio, absolute, logical_bytes, MAX_TAIL_BYTES)

    if fixture == "Zero":
        ratio = 2.0 if pattern == "Sequential1VCPU" else 3.0
        absolute = 10_000.0 if pattern == "Sequential1VCPU" else 20_000.0
        logical_bytes = PAGE_SIZE if strategy == "B_FaultFirstNoTail" else PAGE_SIZE + INITIAL_TAIL_BYTES
        return Threshold(ratio, absolute, 0.0, logical_bytes)

    raise ValueError(f"no threshold for {fixture}/{pattern}/{strategy}")


def parse(text: str) -> list[Sample]:
    samples: list[Sample] = []
    for line in text.splitlines():
        match = BENCHMARK_RE.match(line.strip())
        if not match:
            continue
        samples.append(
            Sample(
                fixture=match.group("fixture"),
                pattern=match.group("pattern"),
                strategy=match.group("strategy"),
                ns_per_op=float(match.group("ns")),
                source_bytes_per_op=float(match.group("source")),
                uffd_bytes_per_op=float(match.group("uffd")),
            )
        )
    return samples


def _median(samples: list[Sample], field: str) -> float:
    return float(statistics.median(getattr(sample, field) for sample in samples))


def evaluate(
    samples: list[Sample], benchmark_status: int = 0
) -> tuple[list[Result], list[str]]:
    grouped: dict[tuple[str, str, str], list[Sample]] = {}
    for sample in samples:
        grouped.setdefault((sample.fixture, sample.pattern, sample.strategy), []).append(sample)

    failures: list[str] = []
    if benchmark_status != 0:
        failures.append(f"benchmark command exited with status {benchmark_status}")
    results: list[Result] = []
    for fixture in FIXTURES:
        for pattern in PATTERNS:
            sync_key = (fixture, pattern, "A_SyncFullBatch")
            sync_samples = grouped.get(sync_key, [])
            if len(sync_samples) < MIN_SAMPLES:
                failures.append(
                    f"{fixture}/{pattern}/A_SyncFullBatch has {len(sync_samples)} samples, want >= {MIN_SAMPLES}"
                )
                sync_ns = 0.0
            else:
                sync_ns = _median(sync_samples, "ns_per_op")

            for strategy in STRATEGIES:
                key = (fixture, pattern, strategy)
                selected = grouped.get(key, [])
                local_failures: list[str] = []
                if len(selected) < MIN_SAMPLES:
                    local_failures.append(
                        f"sample count {len(selected)} is below {MIN_SAMPLES}"
                    )
                if not selected:
                    failures.extend(f"{fixture}/{pattern}/{strategy}: {failure}" for failure in local_failures)
                    continue

                ns_per_op = _median(selected, "ns_per_op")
                source_bytes = _median(selected, "source_bytes_per_op")
                uffd_bytes = _median(selected, "uffd_bytes_per_op")
                ratio = ns_per_op / sync_ns if sync_ns > 0 else float("inf")
                threshold = threshold_for(fixture, pattern, strategy)
                if threshold is not None:
                    if ratio > threshold.max_ratio:
                        local_failures.append(
                            f"ratio {ratio:.3f} exceeds {threshold.max_ratio:.3f}"
                        )
                    if ns_per_op > threshold.max_ns_per_op:
                        local_failures.append(
                            f"{ns_per_op:.1f} ns/op exceeds {threshold.max_ns_per_op:.1f}"
                        )
                    # Go's parallel benchmark calibration can slightly skew
                    # average byte counts, so these structural gates are upper
                    # bounds rather than exact equalities.
                    tolerance = 1.01
                    if source_bytes > threshold.max_source_bytes * tolerance:
                        local_failures.append(
                            f"{source_bytes:.1f} source-B/op exceeds {threshold.max_source_bytes:.1f}"
                        )
                    if uffd_bytes > threshold.max_uffd_bytes * tolerance:
                        local_failures.append(
                            f"{uffd_bytes:.1f} uffd-B/op exceeds {threshold.max_uffd_bytes:.1f}"
                        )

                result = Result(
                    fixture=fixture,
                    pattern=pattern,
                    strategy=strategy,
                    samples=len(selected),
                    ns_per_op=ns_per_op,
                    source_bytes_per_op=source_bytes,
                    uffd_bytes_per_op=uffd_bytes,
                    ratio_to_sync=ratio,
                    threshold=threshold,
                    passed=not local_failures,
                    failures=tuple(local_failures),
                )
                results.append(result)
                failures.extend(
                    f"{fixture}/{pattern}/{strategy}: {failure}"
                    for failure in local_failures
                )

    return results, failures


def render_markdown(results: list[Result], failures: list[str]) -> str:
    lines = [
        "# UFFD performance gate",
        "",
        "Medians use five same-process benchmark samples. Ratios are relative to the",
        "synchronous full-batch control from the same run.",
        "",
        "| Fixture | Pattern | Strategy | median ns/op | ratio to A | source B/op | UFFD B/op | Gate |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for result in results:
        lines.append(
            "| {fixture} | {pattern} | {strategy} | {ns:.1f} | {ratio:.3f} | "
            "{source:.1f} | {uffd:.1f} | {gate} |".format(
                fixture=result.fixture,
                pattern=result.pattern,
                strategy=result.strategy,
                ns=result.ns_per_op,
                ratio=result.ratio_to_sync,
                source=result.source_bytes_per_op,
                uffd=result.uffd_bytes_per_op,
                gate="PASS" if result.passed else "FAIL",
            )
        )
    lines.extend(["", f"Result: {'PASS' if not failures else 'FAIL'}"])
    if failures:
        lines.extend(["", "Failures:"])
        lines.extend(f"- {failure}" for failure in failures)
    return "\n".join(lines) + "\n"


def result_json(results: list[Result], failures: list[str]) -> dict[str, object]:
    encoded_results: list[dict[str, object]] = []
    for result in results:
        encoded = asdict(result)
        encoded["failures"] = list(result.failures)
        encoded_results.append(encoded)
    return {
        "passed": not failures,
        "minimum_samples": MIN_SAMPLES,
        "results": encoded_results,
        "failures": failures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    parser.add_argument("--benchmark-status", type=int, default=0)
    args = parser.parse_args(argv)

    samples = parse(args.input.read_text(encoding="utf-8"))
    results, failures = evaluate(samples, benchmark_status=args.benchmark_status)
    markdown = render_markdown(results, failures)
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(
        json.dumps(result_json(results, failures), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.markdown_output.write_text(markdown, encoding="utf-8")
    print(markdown, end="")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
