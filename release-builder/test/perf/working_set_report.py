#!/usr/bin/env python3
"""Render the sandbox working-set A/B/C/D JSONL samples as Markdown."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable


EXPECTED = {
    "A": (True, True),
    "B": (False, True),
    "C": (True, False),
    "D": (False, False),
}

EXPECTED_DISK_ROLES = {
    "blk0": "root.base",
    "blk1": "root.top",
    "blk2": "scratch.top",
    "blk3": "dataset.base",
    "blk4": "dataset.top",
}


def percentile(values: Iterable[float], percent: int) -> float | None:
    ordered = sorted(float(value) for value in values if value is not None)
    if not ordered:
        return None
    rank = max(1, math.ceil(percent / 100 * len(ordered)))
    return ordered[rank - 1]


def fmt(value: float | None, unit: str = "", scale: float = 1.0) -> str:
    if value is None:
        return "N/A"
    value /= scale
    if abs(value) >= 100:
        rendered = f"{value:.0f}"
    elif abs(value) >= 10:
        rendered = f"{value:.1f}"
    else:
        rendered = f"{value:.2f}"
    return rendered + unit


def triplet(rows: list[dict[str, Any]], key: str, unit: str = "", scale: float = 1.0) -> str:
    values = [row.get(key) for row in rows if row.get(key) is not None]
    return "/".join(fmt(percentile(values, p), unit, scale) for p in (50, 95, 99))


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{number}: {error}") from error
        if not isinstance(row, dict):
            raise SystemExit(f"{path}:{number}: sample must be a JSON object")
        rows.append(row)
    if not rows:
        raise SystemExit(f"{path}: no samples")
    return rows


def validate(rows: list[dict[str, Any]], allow_partial: bool) -> None:
    seen: set[tuple[str, str]] = set()
    seen_samples: set[tuple[str, str, int]] = set()
    required_metrics = (
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
    for row in rows:
        group = row.get("group")
        prefetch = row.get("prefetch")
        if group not in EXPECTED:
            raise SystemExit(f"unknown group in sample: {group!r}")
        if prefetch not in {"off", "memory"}:
            raise SystemExit(f"unknown prefetch mode in sample: {prefetch!r}")
        want_drop, want_merge = EXPECTED[group]
        if row.get("drop_caches") is not want_drop or row.get("merge_ref") is not want_merge:
            raise SystemExit(
                f"group {group} flags are drop_caches={row.get('drop_caches')!r} "
                f"merge_ref={row.get('merge_ref')!r}, want {want_drop}/{want_merge}"
            )
        want_prefetch_result = "completed" if prefetch == "memory" else "disabled"
        if row.get("prefetch_result") != want_prefetch_result:
            raise SystemExit(
                f"group {group}/{prefetch} prefetch result={row.get('prefetch_result')!r}, "
                f"want {want_prefetch_result!r}"
            )
        iteration = row.get("iteration")
        if not isinstance(iteration, int) or isinstance(iteration, bool) or iteration < 1:
            raise SystemExit(f"invalid iteration for {group}/{prefetch}: {iteration!r}")
        sample = (group, prefetch, iteration)
        if sample in seen_samples:
            raise SystemExit(f"duplicate sample: {sample}")
        seen_samples.add(sample)
        missing_metrics = [name for name in required_metrics if row.get(name) is None]
        if missing_metrics:
            raise SystemExit(f"sample {sample} missing metrics: {missing_metrics}")
        disk_reads = row.get("disk_reads")
        if not isinstance(disk_reads, dict) or set(disk_reads) != set(EXPECTED_DISK_ROLES):
            raise SystemExit(f"sample {sample} has invalid disk_reads")
        for name, role in EXPECTED_DISK_ROLES.items():
            entry = disk_reads[name]
            if not isinstance(entry, dict) or entry.get("role") != role:
                raise SystemExit(f"sample {sample} has invalid disk role for {name}")
            if any(entry.get(metric) is None for metric in ("bytes", "p50_us", "p99_us")):
                raise SystemExit(f"sample {sample} has incomplete disk metrics for {name}")
        if prefetch == "memory" and (
            row.get("prefetch_started_ms") is None or row.get("prefetch_duration_ms") is None
        ):
            raise SystemExit(f"sample {sample} missing prefetch timing")
        seen.add((group, prefetch))
    required = {(group, "off") for group in EXPECTED} | {("D", "memory")}
    missing = sorted(required - seen)
    if missing and not allow_partial:
        raise SystemExit(f"incomplete matrix, missing samples: {missing}")
    if not allow_partial:
        expected_iterations = {
            iteration
            for group, prefetch, iteration in seen_samples
            if (group, prefetch) == ("A", "off")
        }
        for scenario in sorted(required):
            iterations = {
                iteration
                for group, prefetch, iteration in seen_samples
                if (group, prefetch) == scenario
            }
            if iterations != expected_iterations:
                raise SystemExit(
                    f"scenario {scenario} iterations={sorted(iterations)}, "
                    f"want {sorted(expected_iterations)}"
                )


def scenario_rows(rows: list[dict[str, Any]]) -> list[tuple[str, list[dict[str, Any]]]]:
    result: list[tuple[str, list[dict[str, Any]]]] = []
    for group in EXPECTED:
        selected = [row for row in rows if row["group"] == group and row["prefetch"] == "off"]
        if selected:
            result.append((f"{group}/off", selected))
    selected = [row for row in rows if row["group"] == "D" and row["prefetch"] == "memory"]
    if selected:
        result.append(("D/memory", selected))
    return result


def metric_table(scenarios: list[tuple[str, list[dict[str, Any]]]]) -> list[str]:
    metrics = [
        ("B memory logical", "b_memory_logical_bytes", " MiB", 1024 * 1024),
        ("B memory allocated", "b_memory_allocated_bytes", " MiB", 1024 * 1024),
        ("W self logical", "w_memory_logical_bytes", " MiB", 1024 * 1024),
        ("W self allocated", "w_memory_allocated_bytes", " MiB", 1024 * 1024),
        ("W disk tops logical", "w_disk_logical_bytes", " MiB", 1024 * 1024),
        ("W disk tops allocated", "w_disk_allocated_bytes", " MiB", 1024 * 1024),
        ("MemoryResident", "w_memory_resident_bytes", " MiB", 1024 * 1024),
        ("MemoryResident ratio", "w_memory_resident_ratio", "%", 0.01),
        ("snapshot pause", "snapshot_pause_ms", " ms", 1),
        ("snapshot dump", "snapshot_dump_ms", " ms", 1),
        ("local capture wall", "snapshot_wall_ms", " ms", 1),
        ("manifest publish wall", "publish_wall_ms", " ms", 1),
        ("restore-to-ack", "restore_ack_ms", " ms", 1),
        ("application ready", "app_ready_ms", " ms", 1),
        ("first representative request", "first_request_ms", " ms", 1),
        ("UFFD faults", "uffd_faults", "", 1),
        ("UFFD pages copied", "uffd_pages_copied", "", 1),
        ("UFFD pages zeroed", "uffd_pages_zeroed", "", 1),
        ("lazy-load ratio", "lazy_load_ratio", "%", 0.01),
        ("cache origin requests", "cache_origin_requests", "", 1),
        ("cache/store origin transferred bytes", "cache_origin_bytes", " B", 1),
        ("prefetch started after restore T0", "prefetch_started_ms", " ms", 1),
        ("prefetch completion duration", "prefetch_duration_ms", " ms", 1),
    ]
    lines = [
        "| Metric (p50/p95/p99) | " + " | ".join(name for name, _ in scenarios) + " |",
        "|---|" + "---:|" * len(scenarios),
    ]
    for label, key, unit, scale in metrics:
        lines.append(
            f"| {label} | "
            + " | ".join(triplet(selected, key, unit, scale) for _, selected in scenarios)
            + " |"
        )
    return lines


def disk_table(scenarios: list[tuple[str, list[dict[str, Any]]]]) -> list[str]:
    lines = [
        "| Scenario | Backend | Logical disk role | read bytes p50/p95/p99 | per-run read latency p50 median | per-run read latency p99 median |",
        "|---|---|---|---:|---:|---:|",
    ]
    for scenario, selected in scenarios:
        names = sorted({name for row in selected for name in (row.get("disk_reads") or {})})
        for name in names:
            entries = [(row.get("disk_reads") or {}).get(name) for row in selected]
            entries = [entry for entry in entries if isinstance(entry, dict)]
            byte_values = [entry.get("bytes") for entry in entries if entry.get("bytes") is not None]
            p50_values = [entry.get("p50_us") for entry in entries if entry.get("p50_us") is not None]
            p99_values = [entry.get("p99_us") for entry in entries if entry.get("p99_us") is not None]
            byte_triplet = "/".join(fmt(percentile(byte_values, p), " B") for p in (50, 95, 99))
            lines.append(
                f"| {scenario} | {name} | {entries[0]['role']} | {byte_triplet} | "
                f"{fmt(percentile(p50_values, 50), ' µs')} | {fmt(percentile(p99_values, 50), ' µs')} |"
            )
    return lines


def render(environment: dict[str, Any], rows: list[dict[str, Any]], samples_path: Path) -> str:
    scenarios = scenario_rows(rows)
    lines = [
        "# Working-set snapshot A/B/C/D performance report",
        "",
        "This is a descriptive first report; it defines no pass/fail performance threshold. "
        "Percentiles use the nearest-rank method over independent restores of the same immutable B.",
        "",
        "| Scenario | drop-caches | merge-ref | prefetch | result | N |",
        "|---|---:|---:|---|---|---:|",
    ]
    for name, selected in scenarios:
        row = selected[0]
        lines.append(
            f"| {name} | {str(row['drop_caches']).lower()} | {str(row['merge_ref']).lower()} "
            f"| {row['prefetch']} | {row['prefetch_result']} | {len(selected)} |"
        )
    lines.extend(["", "## Artifact, capture, publication, and restore", "", *metric_table(scenarios)])
    lines.extend(["", "## Disk reads", "", *disk_table(scenarios)])
    lines.extend(
        [
            "",
            "## Prefetch and transfer notes",
            "",
            "- The A/B/C/D comparison fixes restore prefetch to `off`; `D/memory` is the paired self-prefetch observation.",
            "- `cache_origin_requests` comes from cache-ctl's existing tiered origin counters.",
            "- Cache/store transferred bytes are `N/A`: the existing info surface exposes request counters but not byte totals; no new metrics protocol is introduced.",
            "- Each `D/memory` raw row records prefetch start/completion offsets and result. Parent and disk prefetch are rejected by the harness's log assertions.",
            "",
            "## Environment",
            "",
            "```json",
            json.dumps(environment, indent=2, sort_keys=True),
            "```",
            "",
            f"Raw samples: `{samples_path.name}`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", required=True, type=Path)
    parser.add_argument("--environment", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()

    rows = load_jsonl(args.samples)
    validate(rows, args.allow_partial)
    environment = json.loads(args.environment.read_text(encoding="utf-8"))
    if not isinstance(environment, dict):
        raise SystemExit("environment must be a JSON object")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(environment, rows, args.samples), encoding="utf-8")


if __name__ == "__main__":
    main()
