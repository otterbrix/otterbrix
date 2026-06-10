#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import shutil
import struct
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from statistics import pstdev

from benchlib import QUANTILE_PCTS
from benchlib import generate_csv
from benchlib import mem_total_bytes
from benchlib import measure_load_only
from benchlib import measure_lookup
from benchlib import RestartMetrics
from benchlib import timing_quantile_pairs
from benchlib import write_scenario


INDEX_NAME = "idx_id_hash"
SHARED_DATA_CSV = "benchmark_data.csv"
TABLE_NAME = "kv"
DEFAULT_BUCKET_COUNT = 1024
LOGICAL_TYPE_INTEGER = 13  # components::types::logical_type::INTEGER


@dataclass
class Row:
    section: str
    scenario: str
    metric: str
    value: float | str
    unit: str
    status: str


def fnv1a_32(data: bytes) -> int:
    h = 2166136261
    for b in data:
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def encode_integer_key(v: int) -> bytes:
    # Must match components/index/logical_value_binary_codec.hpp for INTEGER:
    # [logical_type:uint8][int32 little-endian]
    return bytes([LOGICAL_TYPE_INTEGER]) + struct.pack("<i", v)


def bucket_id_for_integer(v: int, bucket_count: int) -> int:
    return fnv1a_32(encode_integer_key(v)) % bucket_count


def make_colliding_ids(count: int, bucket_count: int, target_bucket: int, start: int = 1) -> list[int]:
    ids: list[int] = []
    x = start
    while len(ids) < count:
        if bucket_id_for_integer(x, bucket_count) == target_bucket:
            ids.append(x)
        x += 1
        if x > 2_000_000_000:
            raise RuntimeError("failed to generate enough colliding integer ids")
    return ids


def write_integer_csv(path: Path, ids: list[int], payload_bytes: int) -> None:
    payload = "x" * payload_bytes
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for i in ids:
            f.write(f"{i},{payload}\n")


def table_setup_sql(db: str, csv_path: Path, with_index: str) -> str:
    lines = [
        f"-- @database {db}",
        "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');",
        f"-- @load_csv {csv_path} {TABLE_NAME} ,",
    ]
    if with_index == "single":
        lines.append(f"CREATE INDEX idx_id ON {db}.{TABLE_NAME} (id);")
    elif with_index == "hash":
        lines.append(f"CREATE INDEX {INDEX_NAME} ON {db}.{TABLE_NAME} USING hash (id);")
    return "\n".join(lines)


def lookup_sql(db: str, key: int) -> str:
    return f"-- @expected_rows 1\nSELECT * FROM {db}.{TABLE_NAME} WHERE id = {key};"


def append(rows: list[Row], section: str, scenario: str, metric: str, value: float | str, unit: str, status: str) -> None:
    rows.append(Row(section=section, scenario=scenario, metric=metric, value=value, unit=unit, status=status))


def compute_shared_dataset_rows(args) -> tuple[int, int, int]:
    mem_total = mem_total_bytes()
    if mem_total <= 0:
        raise RuntimeError("cannot determine RAM size from /proc/meminfo")

    target_bytes = int(mem_total * args.memory_pressure_factor)
    approx_row_bytes = max(1, args.payload_bytes + 24)
    rows_needed = math.ceil(target_bytes / approx_row_bytes)
    if rows_needed > args.memory_max_rows:
        raise RuntimeError(
            f"rows_needed={rows_needed} exceeds --memory-max-rows={args.memory_max_rows}; "
            "increase cap or payload bytes to preserve >RAM guarantee"
        )

    if args.rows > 0:
        if args.rows != rows_needed:
            print(
                f"Note: --rows {args.rows} ignored; shared dataset uses "
                f"{rows_needed} rows from memory-pressure sizing"
            )
    dataset_est = rows_needed * approx_row_bytes
    return rows_needed, dataset_est, mem_total


def prepare_shared_dataset(args, workspace: Path) -> tuple[Path, int, int, int]:
    shared_rows, dataset_est, mem_total = compute_shared_dataset_rows(args)
    csv_path = workspace / SHARED_DATA_CSV
    generate_csv(csv_path, shared_rows, args.payload_bytes, shuffle_ids=args.shuffle_ids)
    return csv_path, shared_rows, dataset_est, mem_total


def run_throughput_suite(
    args,
    workspace: Path,
    bench_tag: str,
    rows_out: list[Row],
    *,
    csv_path: Path,
    base_rows: int,
) -> None:
    scenarios = [("no_index", "none"), ("hash_index", "hash")]
    rng = random.Random(args.seed)
    key = rng.randrange(1, base_rows + 1)

    for scenario_name, idx_kind in scenarios:
        runner = Path(args.runner).resolve()

        # Load-only measurements are sensitive to OS/page-cache/jitter.
        # Repeat full load-only runs (fresh DB name + scenario directory) and aggregate.
        load_ms_samples: list[float] = []
        insert_ops_samples: list[float] = []
        for i in range(args.throughput_load_repeats):
            db = f"benchdb_thr_{scenario_name}_{bench_tag}_{i}"
            d = workspace / f"throughput_{scenario_name}_{i}"
            setup_sql = table_setup_sql(db, csv_path, with_index=idx_kind)
            write_scenario(d, setup_sql, "lookup.sql", lookup_sql(db, key))
            load_ms = measure_load_only(
                runner,
                d,
                query_file="lookup.sql",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=not args.show_runner_output,
            )
            load_ms_samples.append(load_ms)
            insert_ops_samples.append(base_rows / max(1e-9, load_ms / 1000.0))

        # Query throughput: run query phase multiple times on the same loaded dataset.
        # We load the dataset once (including index build), then repeat --skip-load lookups.
        db = f"benchdb_thr_{scenario_name}_{bench_tag}_query"
        d = workspace / f"throughput_{scenario_name}_query"
        setup_sql = table_setup_sql(db, csv_path, with_index=idx_kind)
        write_scenario(d, setup_sql, "lookup.sql", lookup_sql(db, key))
        _ = measure_load_only(
            runner,
            d,
            query_file="lookup.sql",
            checkpoint_mb=args.checkpoint_mb,
            suppress_output=not args.show_runner_output,
        )

        query_wall_ms_samples: list[float] = []
        query_ops_samples: list[float] = []
        query_avg_ms_samples: list[float] = []
        query_median_ms_samples: list[float] = []
        # Track quantiles per run too (pXX are averaged later)
        query_quantiles_samples: dict[str, list[float]] = {f"query_p{p}_ms": [] for p in QUANTILE_PCTS}
        verified = "OK"
        for i in range(args.throughput_query_repeats):
            query = measure_lookup(
                runner,
                d,
                query_file="lookup.sql",
                runs=args.runs,
                out_name=f"result_{i}.csv",
                checkpoint_mb=args.checkpoint_mb,
                skip_load=True,
                suppress_output=not args.show_runner_output,
            )
            query_wall_ms_samples.append(query.wall_ms)
            query_ops_samples.append(args.runs / max(1e-9, query.wall_ms / 1000.0))
            query_avg_ms_samples.append(query.avg_ms)
            query_median_ms_samples.append(query.median_ms)
            for p in QUANTILE_PCTS:
                query_quantiles_samples[f"query_p{p}_ms"].append(query.quantiles_ms.get(p, float("nan")))
            if query.verified != "OK":
                verified = "FAIL"

        insert_ops_s = mean(insert_ops_samples)
        query_ops_s = mean(query_ops_samples)

        # Main aggregated metrics (backwards compatible names = mean values)
        append(rows_out, "throughput", scenario_name, "insert_ops_s", insert_ops_s, "ops/s", "OK")
        append(rows_out, "throughput", scenario_name, "query_ops_s", query_ops_s, "ops/s", verified)
        append(rows_out, "throughput", scenario_name, "query_avg_ms", mean(query_avg_ms_samples), "ms", verified)
        append(rows_out, "throughput", scenario_name, "query_median_ms", mean(query_median_ms_samples), "ms", verified)
        for p in QUANTILE_PCTS:
            vals = [v for v in query_quantiles_samples[f"query_p{p}_ms"] if not math.isnan(v)]
            append(rows_out, "throughput", scenario_name, f"query_p{p}_ms", mean(vals) if vals else float("nan"), "ms", verified)

        # Additional dispersion diagnostics
        append(rows_out, "throughput", scenario_name, "insert_load_ms_mean", mean(load_ms_samples), "ms", "OK")
        append(rows_out, "throughput", scenario_name, "insert_load_ms_stdev", pstdev(load_ms_samples) if len(load_ms_samples) > 1 else 0.0, "ms", "OK")
        append(rows_out, "throughput", scenario_name, "insert_ops_s_stdev", pstdev(insert_ops_samples) if len(insert_ops_samples) > 1 else 0.0, "ops/s", "OK")
        append(rows_out, "throughput", scenario_name, "query_wall_ms_mean", mean(query_wall_ms_samples), "ms", verified)
        append(rows_out, "throughput", scenario_name, "query_wall_ms_stdev", pstdev(query_wall_ms_samples) if len(query_wall_ms_samples) > 1 else 0.0, "ms", verified)
        append(rows_out, "throughput", scenario_name, "query_ops_s_stdev", pstdev(query_ops_samples) if len(query_ops_samples) > 1 else 0.0, "ops/s", verified)


def run_spill_suite(args, workspace: Path, bench_tag: str, rows_out: list[Row]) -> None:
    # Build a dataset intentionally forcing many keys into one hash bucket.
    colliding_ids = make_colliding_ids(
        count=args.spill_collision_count,
        bucket_count=DEFAULT_BUCKET_COUNT,
        target_bucket=args.spill_target_bucket,
        start=1,
    )

    used = set(colliding_ids)
    control_ids: list[int] = []
    x = 10_000_000
    while len(control_ids) < len(colliding_ids):
        if x not in used and bucket_id_for_integer(x, DEFAULT_BUCKET_COUNT) != args.spill_target_bucket:
            control_ids.append(x)
        x += 1

    # Dataset includes both colliding and control keys.
    all_ids = colliding_ids + control_ids
    csv_path = workspace / "spill_data.csv"
    write_integer_csv(csv_path, all_ids, args.payload_bytes)

    runner = Path(args.runner).resolve()
    db = f"benchdb_spill_{bench_tag}"
    setup_sql = table_setup_sql(db, csv_path, with_index="hash")

    collision_key = colliding_ids[len(colliding_ids) // 2]
    control_key = control_ids[len(control_ids) // 2]

    # Collision lookup scenario
    d_collision = workspace / "spill_collision_lookup"
    write_scenario(d_collision, setup_sql, "lookup.sql", lookup_sql(db, collision_key))
    m_collision = measure_lookup(
        runner,
        d_collision,
        query_file="lookup.sql",
        runs=args.runs,
        out_name="result.csv",
        checkpoint_mb=args.checkpoint_mb,
        suppress_output=not args.show_runner_output,
    )

    # Control lookup scenario (same setup style, non-colliding key)
    d_control = workspace / "spill_control_lookup"
    write_scenario(d_control, setup_sql, "lookup.sql", lookup_sql(db, control_key))
    m_control = measure_lookup(
        runner,
        d_control,
        query_file="lookup.sql",
        runs=args.runs,
        out_name="result.csv",
        checkpoint_mb=args.checkpoint_mb,
        suppress_output=not args.show_runner_output,
    )

    ratio = m_collision.avg_ms / max(1e-9, m_control.avg_ms)
    degradation_ok = ratio <= args.max_spill_degradation_ratio
    status = "OK" if (degradation_ok and m_collision.verified == "OK" and m_control.verified == "OK") else "FAIL"

    append(rows_out, "spill", "collision_lookup", "query_avg_ms", m_collision.avg_ms, "ms", m_collision.verified)
    append(rows_out, "spill", "control_lookup", "query_avg_ms", m_control.avg_ms, "ms", m_control.verified)
    for metric_name, value in timing_quantile_pairs(m_collision.quantiles_ms, prefix="collision_query_"):
        append(rows_out, "spill", "collision_lookup", metric_name, value, "ms", m_collision.verified)
    for metric_name, value in timing_quantile_pairs(m_control.quantiles_ms, prefix="control_query_"):
        append(rows_out, "spill", "control_lookup", metric_name, value, "ms", m_control.verified)
    append(rows_out, "spill", "comparison", "degradation_ratio_collision_vs_control", ratio, "x", status)
    append(rows_out, "spill", "comparison", "collision_count", float(len(colliding_ids)), "keys", "OK")
    append(rows_out, "spill", "comparison", "degradation_threshold", args.max_spill_degradation_ratio, "x", "OK")


def run_memory_pressure_suite(
    args,
    workspace: Path,
    bench_tag: str,
    rows_out: list[Row],
    *,
    csv_path: Path,
    rows_needed: int,
    dataset_est: int,
    mem_total: int,
) -> None:
    runner = Path(args.runner).resolve()
    key = max(1, rows_needed // 2)

    scenarios = [
        ("no_index", "none"),
        ("hash_index", "hash"),
    ]

    results: dict[str, RestartMetrics] = {}
    derived: dict[str, dict[str, float]] = {}
    for scenario_name, idx_kind in scenarios:
        db = f"benchdb_mem_{scenario_name}_{bench_tag}"
        setup_sql = table_setup_sql(db, csv_path, with_index=idx_kind)
        d = workspace / f"memory_pressure_{scenario_name}"
        write_scenario(d, setup_sql, "lookup.sql", lookup_sql(db, key))

        # Phase 1: one end-to-end load+shutdown to create persisted dataset (> RAM) and (optionally) index.
        load_shutdown_ms = measure_load_only(
            runner,
            d,
            query_file="lookup.sql",
            checkpoint_mb=args.checkpoint_mb,
            suppress_output=not args.show_runner_output,
        )

        # Phase 2: one timed restart/lookup batch, controlled by --memory-restart-runs.
        # No extra reloads: benchmark_runner is executed with --skip-load.
        q = measure_lookup(
            runner,
            d,
            query_file="lookup.sql",
            runs=args.memory_restart_runs,
            out_name="restart_result.csv",
            checkpoint_mb=args.checkpoint_mb,
            skip_load=True,
            suppress_output=not args.show_runner_output,
        )

        restart = RestartMetrics(
            load_shutdown_ms=load_shutdown_ms,
            restart_avg_ms=q.avg_ms,
            restart_median_ms=q.median_ms,
            restart_quantiles_ms=q.quantiles_ms,
            restart_wall_ms=q.wall_ms,
            startup_overhead_ms=q.overhead_ms,
            verified=q.verified,
        )
        results[scenario_name] = restart

        insert_ops_s = rows_needed / max(1e-9, restart.load_shutdown_ms / 1000.0)
        query_ops_s = args.memory_restart_runs / max(1e-9, restart.restart_wall_ms / 1000.0)
        derived[scenario_name] = {
            "insert_ops_s": insert_ops_s,
            "query_ops_s": query_ops_s,
        }

        checks = {
            "dataset_gt_ram": dataset_est > mem_total,
            "restart_verified": restart.verified == "OK",
            "startup_overhead_cap": restart.startup_overhead_ms <= args.max_memory_startup_overhead_ms,
            "insert_ops_floor": insert_ops_s >= args.min_memory_insert_ops_s,
            "query_ops_floor": query_ops_s >= args.min_memory_query_ops_s,
        }
        overall = "OK" if all(checks.values()) else "FAIL"

        append(rows_out, "memory_pressure", scenario_name, "rows", float(rows_needed), "rows", "OK")
        append(rows_out, "memory_pressure", scenario_name, "dataset_est_bytes", float(dataset_est), "bytes", "OK")
        append(rows_out, "memory_pressure", scenario_name, "ram_bytes", float(mem_total), "bytes", "OK")
        append(rows_out, "memory_pressure", scenario_name, "insert_ops_s", insert_ops_s, "ops/s", overall)
        append(rows_out, "memory_pressure", scenario_name, "query_ops_s", query_ops_s, "ops/s", overall)
        append(rows_out, "memory_pressure", scenario_name, "restart_avg_ms", restart.restart_avg_ms, "ms", restart.verified)
        append(rows_out, "memory_pressure", scenario_name, "restart_median_ms", restart.restart_median_ms, "ms", restart.verified)
        for metric_name, value in timing_quantile_pairs(restart.restart_quantiles_ms, prefix="restart_"):
            append(rows_out, "memory_pressure", scenario_name, metric_name, value, "ms", restart.verified)
        append(rows_out, "memory_pressure", scenario_name, "startup_overhead_ms", restart.startup_overhead_ms, "ms", overall)

        for k, v in checks.items():
            append(rows_out, "memory_pressure", scenario_name, f"check_{k}", "1" if v else "0", "bool", "OK" if v else "FAIL")

    # Convenience comparisons (speedups): hash vs no_index, and hash vs single_index.
    def safe_speedup(base: float, cur: float) -> float:
        return float("nan") if cur <= 0.0 else (base / cur)

    if "no_index" in results and "hash_index" in results:
        append(
            rows_out,
            "memory_pressure",
            "comparison",
            "speedup_hash_vs_no_restart_avg",
            safe_speedup(results["no_index"].restart_avg_ms, results["hash_index"].restart_avg_ms),
            "x",
            "OK",
        )
        append(
            rows_out,
            "memory_pressure",
            "comparison",
            "speedup_hash_vs_no_restart_p99",
            safe_speedup(
                results["no_index"].restart_quantiles_ms.get(99, float("nan")),
                results["hash_index"].restart_quantiles_ms.get(99, float("nan")),
            ),
            "x",
            "OK",
        )
        append(
            rows_out,
            "memory_pressure",
            "comparison",
            "speedup_hash_vs_no_query_ops_s",
            safe_speedup(derived["hash_index"]["query_ops_s"], derived["no_index"]["query_ops_s"]),
            "x",
            "OK",
        )

    if "single_index" in results and "hash_index" in results:
        append(
            rows_out,
            "memory_pressure",
            "comparison",
            "speedup_hash_vs_single_restart_avg",
            safe_speedup(results["single_index"].restart_avg_ms, results["hash_index"].restart_avg_ms),
            "x",
            "OK",
        )


def write_report(path: Path, rows: list[Row]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["section", "scenario", "metric", "value", "unit", "status"])
        for r in rows:
            if isinstance(r.value, float):
                value = f"{r.value:.6f}"
            else:
                value = r.value
            writer.writerow([r.section, r.scenario, r.metric, value, r.unit, r.status])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Requirement-focused benchmark suite: throughput, spill/overflow, memory pressure."
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="", help="Workspace path (default: temp dir).")
    parser.add_argument(
        "--rows",
        type=int,
        default=0,
        help="Ignored: throughput and memory_pressure share one dataset sized from RAM.",
    )
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=20, help="Timed runs per query scenario.")
    parser.add_argument(
        "--throughput-load-repeats",
        type=int,
        default=3,
        help="How many times to repeat the end-to-end load-only phase in throughput suite (new DB each time).",
    )
    parser.add_argument(
        "--throughput-query-repeats",
        type=int,
        default=5,
        help="How many times to repeat the query phase (skip-load) in throughput suite on the same loaded dataset.",
    )
    parser.add_argument("--seed", type=int, default=1234567)
    parser.add_argument("--shuffle-ids", action="store_true")
    parser.add_argument("--checkpoint-mb", type=int, default=0)
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument("--keep-workspace", action="store_true")

    # Spill / overflow controls
    parser.add_argument("--spill-collision-count", type=int, default=900, help="Number of keys targeting one bucket.")
    parser.add_argument("--spill-target-bucket", type=int, default=0)
    parser.add_argument("--max-spill-degradation-ratio", type=float, default=3.0)

    # Memory pressure controls
    parser.add_argument("--memory-pressure-factor", type=float, default=1.25, help="Target dataset size = RAM * factor.")
    parser.add_argument("--memory-max-rows", type=int, default=2_500_000)
    parser.add_argument("--memory-restart-runs", type=int, default=10)
    parser.add_argument("--max-memory-startup-overhead-ms", type=float, default=120_000.0)
    parser.add_argument("--min-memory-insert-ops-s", type=float, default=100.0)
    parser.add_argument("--min-memory-query-ops-s", type=float, default=5.0)
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    workspace = (
        Path(args.workspace).resolve()
        if args.workspace
        else Path("/tmp") / f"otterbrix_hash_req_{int(time.time())}_{os.getpid()}"
    )
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    rows_out: list[Row] = []
    bench_tag = f"{int(time.time())}_{os.getpid()}"

    try:
        csv_path, shared_rows, dataset_est, mem_total = prepare_shared_dataset(args, workspace)
        print(
            f"Shared dataset: {csv_path} ({shared_rows} rows, "
            f"est {dataset_est / (1024 ** 3):.2f} GiB, RAM {mem_total / (1024 ** 3):.2f} GiB)"
        )

        run_throughput_suite(
            args,
            workspace,
            bench_tag,
            rows_out,
            csv_path=csv_path,
            base_rows=shared_rows,
        )
        run_spill_suite(args, workspace, bench_tag, rows_out)
        run_memory_pressure_suite(
            args,
            workspace,
            bench_tag,
            rows_out,
            csv_path=csv_path,
            rows_needed=shared_rows,
            dataset_est=dataset_est,
            mem_total=mem_total,
        )

        out_csv = workspace / "benchmark_hash_requirements.csv"
        write_report(out_csv, rows_out)
        print(f"Saved report: {out_csv}")
        print("section,scenario,metric,value,unit,status")
        for r in rows_out:
            v = f"{r.value:.6f}" if isinstance(r.value, float) else str(r.value)
            print(f"{r.section},{r.scenario},{r.metric},{v},{r.unit},{r.status}")
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()

