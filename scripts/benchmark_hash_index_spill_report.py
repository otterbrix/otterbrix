#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import shutil
import struct
import time
from dataclasses import dataclass
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import measure_load_only
from benchlib import measure_lookup
from benchlib import timing_quantile_pairs
from benchlib import write_scenario


INDEX_NAME = "idx_id_hash"
TABLE_NAME = "kv"
DEFAULT_BUCKET_COUNT = 1024
LOGICAL_TYPE_INTEGER = 13
MAX_LOAD_FACTOR = 0.75
PAGE_SIZE = 4096


@dataclass
class Row:
    section: str
    scenario: str
    metric: str
    value: float | str
    unit: str
    status: str


@dataclass
class LinearHashState:
    bucket_count: int
    level: int
    split_bucket: int
    safe_additional_budget: int


def fnv1a_32(data: bytes) -> int:
    h = 2166136261
    for b in data:
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def encode_integer_key(v: int) -> bytes:
    return bytes([LOGICAL_TYPE_INTEGER]) + struct.pack("<i", v)


def key_hash(v: int) -> int:
    return fnv1a_32(encode_integer_key(v))


def bucket_id_linear_from_hash(h: int, bucket_count: int, level: int, split_bucket: int) -> int:
    if bucket_count <= 0:
        return 0
    base = 1 << level
    bucket = h % base
    if bucket < split_bucket:
        bucket = h % (base << 1)
    return bucket


def make_ids_for_layout(count: int, bucket_count: int, level: int, split_bucket: int, target_bucket: int,
                        *, colliding: bool, start: int) -> list[int]:
    ids: list[int] = []
    x = start
    while len(ids) < count:
        b = bucket_id_linear_from_hash(key_hash(x), bucket_count, level, split_bucket)
        if (b == target_bucket) == colliding:
            ids.append(x)
        x += 1
        if x > 2_000_000_000:
            raise RuntimeError("failed to generate enough ids for requested linear-hash layout")
    return ids


def linear_int_points(start: int, end: int, points: int) -> list[int]:
    if points <= 0:
        raise RuntimeError("--spill-points must be > 0")
    if start <= 0 or end <= 0:
        raise RuntimeError("spill collision range must be > 0")
    if end < start:
        raise RuntimeError("--spill-collision-end must be >= --spill-collision-start")
    if points == 1:
        return [start]
    values = []
    for i in range(points):
        v = round(start + (end - start) * (i / (points - 1)))
        values.append(int(v))
    out: list[int] = []
    for v in values:
        if not out or out[-1] != v:
            out.append(v)
    return out


def write_integer_csv(path: Path, ids: list[int], payload_bytes: int) -> None:
    payload = "x" * payload_bytes
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for i in ids:
            f.write(f"{i},{payload}\n")


def table_setup_sql_rel(db: str, csv_name: str, with_index: str) -> str:
    lines = [
        f"-- @database {db}",
        "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');",
        f"-- @load_csv {csv_name} {TABLE_NAME} ,",
    ]
    if with_index == "hash":
        lines.append(f"CREATE INDEX {INDEX_NAME} ON {db}.{TABLE_NAME} USING hash (id);")
    return "\n".join(lines)


def append_setup_sql_rel(db: str, csv_name: str) -> str:
    return f"-- @database {db}\n-- @load_csv {csv_name} {TABLE_NAME} ,\n"


def lookup_sql(db: str, key: int) -> str:
    return f"-- @expected_rows 1\nSELECT * FROM {db}.{TABLE_NAME} WHERE id = {key};"


def append(rows: list[Row], section: str, scenario: str, metric: str, value: float | str, unit: str, status: str) -> None:
    rows.append(Row(section=section, scenario=scenario, metric=metric, value=value, unit=unit, status=status))


def read_linear_hash_state(index_dir: Path, baseline_rows: int) -> LinearHashState:
    hash_bin = index_dir / "hash_index.bin"
    raw = hash_bin.read_bytes()[:PAGE_SIZE]
    if len(raw) < 36:
        raise RuntimeError(f"hash_index.bin too small: {hash_bin}")
    bucket_count = struct.unpack_from("<I", raw, 16)[0]
    level = struct.unpack_from("<I", raw, 28)[0]
    split_bucket = struct.unpack_from("<I", raw, 32)[0]
    safe_additional_budget = max(0, int(math.floor(bucket_count * MAX_LOAD_FACTOR)) - baseline_rows)
    return LinearHashState(bucket_count, level, split_bucket, safe_additional_budget)


def find_index_dir(scenario_dir: Path, index_name: str = INDEX_NAME) -> Path:
    for root_name in ("wal", "disk"):
        root = scenario_dir / root_name
        if not root.is_dir():
            continue
        for candidate in root.rglob(index_name):
            if candidate.is_dir():
                return candidate
        for hash_bin in root.rglob("hash_index.bin"):
            return hash_bin.parent
    raise RuntimeError(f"cannot locate index directory under {scenario_dir}")


def run_append_and_lookup(runner: Path, scenario_dir: Path, runs: int, checkpoint_mb: int, show_output: bool):
    load_ms = measure_load_only(
        runner,
        scenario_dir,
        query_file="append.sql",
        checkpoint_mb=checkpoint_mb,
        suppress_output=not show_output,
    )
    query = measure_lookup(
        runner,
        scenario_dir,
        query_file="lookup.sql",
        runs=runs,
        out_name="result.csv",
        checkpoint_mb=checkpoint_mb,
        skip_load=True,
        suppress_output=not show_output,
    )
    return load_ms, query


def run_spill_suite(args, workspace: Path, bench_tag: str, rows_out: list[Row]) -> None:
    collision_points = linear_int_points(args.spill_collision_start, args.spill_collision_end, args.spill_points)
    runner = Path(args.runner).resolve()
    scenarios = [("no_index", "none"), ("hash_index", "hash")]

    baseline_rows = max(1, args.rows)
    baseline_ids = list(range(1, baseline_rows + 1))

    # Phase 1: baseline load for both scenarios
    for scenario_name, idx_kind in scenarios:
        db = f"benchdb_spill_{scenario_name}_{bench_tag}"
        d = workspace / f"baseline_{scenario_name}"
        d.mkdir(parents=True, exist_ok=True)
        baseline_csv = d / "baseline.csv"
        write_integer_csv(baseline_csv, baseline_ids, args.payload_bytes)
        setup_sql = table_setup_sql_rel(db, baseline_csv.name, idx_kind)
        write_scenario(d, setup_sql, "lookup.sql", lookup_sql(db, baseline_ids[len(baseline_ids) // 2]))
        measure_load_only(
            runner,
            d,
            query_file="lookup.sql",
            checkpoint_mb=args.checkpoint_mb,
            suppress_output=not args.show_runner_output,
        )

    # Phase 2: inspect actual current layout of hash index after baseline load
    hash_index_dir = find_index_dir(workspace / "baseline_hash_index")
    state = read_linear_hash_state(hash_index_dir, baseline_rows)
    append(rows_out, "layout", "hash_index", "baseline_rows", float(baseline_rows), "rows", "OK")
    append(rows_out, "layout", "hash_index", "bucket_count", float(state.bucket_count), "buckets", "OK")
    append(rows_out, "layout", "hash_index", "level", float(state.level), "level", "OK")
    append(rows_out, "layout", "hash_index", "split_bucket", float(state.split_bucket), "bucket", "OK")
    append(rows_out, "layout", "hash_index", "safe_additional_budget", float(state.safe_additional_budget), "rows", "OK")

    effective_points = [min(p, state.safe_additional_budget) for p in collision_points]
    effective_points = [p for i, p in enumerate(effective_points) if p > 0 and (i == 0 or p != effective_points[i - 1])]
    if not effective_points:
        raise RuntimeError("no safe spill points remain before next rehash; reduce baseline rows or requested collision range")

    target_bucket = args.spill_target_bucket % max(1, state.bucket_count)

    # Phase 3: append colliding block that matches current layout, but stays below next rehash threshold
    for collision_count in effective_points:
        colliding_ids = make_ids_for_layout(
            collision_count,
            state.bucket_count,
            state.level,
            state.split_bucket,
            target_bucket,
            colliding=True,
            start=20_000_000,
        )
        lookup_key = colliding_ids[len(colliding_ids) // 2]

        for scenario_name, _idx_kind in scenarios:
            baseline_dir = workspace / f"baseline_{scenario_name}"
            phase_dir = workspace / f"spill_{scenario_name}_{collision_count}"
            shutil.copytree(baseline_dir, phase_dir)

            append_csv = phase_dir / "append.csv"
            write_integer_csv(append_csv, colliding_ids, args.payload_bytes)
            db = f"benchdb_spill_{scenario_name}_{bench_tag}"
            (phase_dir / "append.sql").write_text(append_setup_sql_rel(db, append_csv.name), encoding="utf-8")
            (phase_dir / "lookup.sql").write_text(lookup_sql(db, lookup_key), encoding="utf-8")

            load_ms, query = run_append_and_lookup(
                runner,
                phase_dir,
                args.runs,
                args.checkpoint_mb,
                args.show_runner_output,
            )

            scenario = f"{scenario_name}_{collision_count}"
            append(rows_out, "spill", scenario, "collision_count", float(collision_count), "keys", "OK")
            append(rows_out, "spill", scenario, "baseline_rows", float(baseline_rows), "rows", "OK")
            append(rows_out, "spill", scenario, "bucket_count", float(state.bucket_count), "buckets", "OK")
            append(rows_out, "spill", scenario, "safe_additional_budget", float(state.safe_additional_budget), "rows", "OK")
            append(rows_out, "spill", scenario, "insert_ops_s", collision_count / max(1e-9, load_ms / 1000.0), "ops/s", "OK")
            append(rows_out, "spill", scenario, "query_ops_s", args.runs / max(1e-9, query.wall_ms / 1000.0), "ops/s", query.verified)
            append(rows_out, "spill", scenario, "query_avg_ms", query.avg_ms, "ms", query.verified)
            append(rows_out, "spill", scenario, "query_median_ms", query.median_ms, "ms", query.verified)
            for metric_name, value in timing_quantile_pairs(query.quantiles_ms, prefix="query_"):
                append(rows_out, "spill", scenario, metric_name, value, "ms", query.verified)

        hash_avg = next(float(r.value) for r in rows_out if r.section == "spill" and r.scenario == f"hash_index_{collision_count}" and r.metric == "query_avg_ms")
        no_avg = next(float(r.value) for r in rows_out if r.section == "spill" and r.scenario == f"no_index_{collision_count}" and r.metric == "query_avg_ms")
        hash_insert = next(float(r.value) for r in rows_out if r.section == "spill" and r.scenario == f"hash_index_{collision_count}" and r.metric == "insert_ops_s")
        no_insert = next(float(r.value) for r in rows_out if r.section == "spill" and r.scenario == f"no_index_{collision_count}" and r.metric == "insert_ops_s")
        append(rows_out, "spill_compare", f"compare_{collision_count}", "collision_count", float(collision_count), "keys", "OK")
        append(rows_out, "spill_compare", f"compare_{collision_count}", "hash_vs_no_query_avg_ratio", hash_avg / max(1e-9, no_avg), "x", "OK")
        append(rows_out, "spill_compare", f"compare_{collision_count}", "hash_vs_no_insert_ops_ratio", hash_insert / max(1e-9, no_insert), "x", "OK")


def write_report(path: Path, rows: list[Row]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["section", "scenario", "metric", "value", "unit", "status"])
        for r in rows:
            value = f"{r.value:.6f}" if isinstance(r.value, float) and not math.isnan(r.value) else r.value
            writer.writerow([r.section, r.scenario, r.metric, value, r.unit, r.status])


def plot_report(rows: list[Row], out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    def metric_value(section: str, scenario: str, metric: str) -> float:
        for row in rows:
            if row.section == section and row.scenario == scenario and row.metric == metric:
                return float(row.value)
        return float("nan")

    spill_points = sorted({int(float(row.value)) for row in rows if row.section == "spill" and row.metric == "collision_count"})

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        ys = [metric_value("spill", f"{scenario}_{p}", "insert_ops_s") for p in spill_points]
        axes[0].plot(spill_points, ys, marker="o", linewidth=2, color=color, label=scenario)
    axes[0].set_title("Insert throughput after last rehash, before next")
    axes[0].set_xlabel("added colliding rows")
    axes[0].set_ylabel("ops/s")
    axes[0].grid(True, linestyle=":", alpha=0.5)
    axes[0].legend()

    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        ys = [metric_value("spill", f"{scenario}_{p}", "query_ops_s") for p in spill_points]
        axes[1].plot(spill_points, ys, marker="o", linewidth=2, color=color, label=scenario)
    axes[1].set_title("Lookup throughput after last rehash, before next")
    axes[1].set_xlabel("added colliding rows")
    axes[1].set_ylabel("ops/s")
    axes[1].grid(True, linestyle=":", alpha=0.5)
    axes[1].legend()

    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        ys = [metric_value("spill", f"{scenario}_{p}", "query_avg_ms") for p in spill_points]
        axes[2].plot(spill_points, ys, marker="o", linewidth=2, color=color, label=scenario)
    axes[2].set_title("Lookup latency after last rehash, before next")
    axes[2].set_xlabel("added colliding rows")
    axes[2].set_ylabel("avg ms")
    axes[2].grid(True, linestyle=":", alpha=0.5)
    axes[2].legend()

    fig.tight_layout()
    fig.savefig(out_dir / "benchmark_hash_index_spill.png", dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Two-phase spill benchmark: baseline load, inspect current hash layout, then add colliding rows before next rehash.")
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="", help="Workspace path (default: temp-like dir under benchmark_runs).")
    parser.add_argument("--output-dir", default="benchmark_runs/hash_index_spill_report", help="Where to save CSV and plots.")
    parser.add_argument("--rows", type=int, default=100000, help="Baseline dataset size loaded before spill phase.")
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--checkpoint-mb", type=int, default=0)
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument("--keep-workspace", action="store_true")
    parser.add_argument("--spill-collision-start", type=int, default=50)
    parser.add_argument("--spill-collision-end", type=int, default=5000)
    parser.add_argument("--spill-points", type=int, default=8)
    parser.add_argument("--spill-target-bucket", type=int, default=0)
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    workspace = Path(args.workspace).resolve() if args.workspace else output_dir / f"workspace_{int(time.time())}"
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    rows_out: list[Row] = []
    bench_tag = f"{int(time.time())}"
    try:
        run_spill_suite(args, workspace, bench_tag, rows_out)
        out_csv = output_dir / "benchmark_hash_index_spill_report.csv"
        write_report(out_csv, rows_out)
        plot_report(rows_out, output_dir)
        print(f"Saved table: {out_csv}")
        print(f"Saved plot: {output_dir / 'benchmark_hash_index_spill.png'}")
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()