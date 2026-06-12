#!/usr/bin/env python3
"""
Benchmark: extended Bitcask-focused measurement suite.

What this script measures
The script generates one base dataset and executes multiple benchmark groups,
writing normalized rows into `bitcask_extended_summary.csv`:
`benchmark,scenario,metric,value,unit,status`.

Common metrics
- avg_ms / median_ms: per-run query latency from runner CSV.
- overhead_ms: max(0, wall_ms - timed_total_ms), where timed_total_ms is
  reconstructed from runner CSV (`sum(avg_ms * nruns)`).
- load_shutdown_ms: wall time for `--load-only` execution.
- startup_overhead_ms: restart wall time minus timed query total during
  `--skip-load` phase.
- status: "OK" only when all underlying runner checks are verified.

Benchmark groups
1) write_load_only
   - Compares btree vs bitcask_hash load/persist/shutdown cost.
   - Metric: load_shutdown_ms.

2) mixed_read_write
   - Multi-statement transactional-style mix:
     SELECT, UPDATE, INSERT, DELETE, SELECT on a hot key.
   - Captures avg_ms, median_ms, overhead_ms for btree vs bitcask_hash.

3) range_query
   - Bounded range lookup (`id >= start AND id <= end`) over ~5k ids.
   - Captures avg_ms, median_ms, overhead_ms for btree vs bitcask_hash.

4) startup_scaling (bitcask only)
   - Varies `bitcask_flush_threshold` and `bitcask_segment_record_limit`.
   - Measures load_shutdown_ms + restart metrics to quantify startup scaling.

5) flush_threshold_sensitivity (bitcask only)
   - Sweeps flush threshold values for point lookup behavior.
   - Captures avg_ms, median_ms, overhead_ms.

6) hotkey_vs_uniform (bitcask only)
   - Compares fixed hot key lookup vs uniformly sampled key lookup.
   - Captures avg_ms, median_ms, overhead_ms.

7) large_rowid_list (bitcask only)
   - Stress case with many logical matches for the same key (`id=7`).
   - Captures avg_ms, median_ms, overhead_ms.

Operational notes
- Workspace is recreated on each run.
- Workspace is removed unless `--keep-workspace` is provided.
"""
import argparse
import csv
import os
import random
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import generate_csv
from benchlib import measure_load_only
from benchlib import measure_lookup
from benchlib import measure_restart
from benchlib import timing_quantile_pairs
from benchlib import write_scenario


@dataclass
class BenchRow:
    benchmark: str
    scenario: str
    metric: str
    value: float
    unit: str
    status: str


def append_row(rows: list[BenchRow], benchmark: str, scenario: str, metric: str, value: float, unit: str, status: str) -> None:
    rows.append(BenchRow(benchmark, scenario, metric, value, unit, status))


def append_query_quantiles(rows: list[BenchRow], benchmark: str, scenario: str, query) -> None:
    for metric_name, value in timing_quantile_pairs(query.quantiles_ms):
        append_row(rows, benchmark, scenario, metric_name, value, "ms", query.verified)


def append_restart_quantiles(rows: list[BenchRow], benchmark: str, scenario: str, restart) -> None:
    for metric_name, value in timing_quantile_pairs(restart.restart_quantiles_ms, prefix="restart_"):
        append_row(rows, benchmark, scenario, metric_name, value, "ms", restart.verified)


def write_summary_csv(path: Path, rows: list[BenchRow]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["benchmark", "scenario", "metric", "value", "unit", "status"])
        for row in rows:
            writer.writerow([row.benchmark, row.scenario, row.metric, f"{row.value:.3f}", row.unit, row.status])


def prepare_base_setup(db_name: str, csv_path: Path, use_hash_index: bool, flush_threshold: int, segment_limit: int) -> str:
    index_sql = f"CREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);" if use_hash_index else f"CREATE INDEX idx_id ON {db_name}.kv (id);"
    return (
        f"-- @database {db_name}\n"
        f"CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk', bitcask_flush_threshold = {flush_threshold}, bitcask_segment_record_limit = {segment_limit});\n"
        f"-- @load_csv {csv_path} kv ,\n"
        f"{index_sql};"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Extended Bitcask index benchmarks.")
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_bitcask_extended", help="Workspace path.")
    parser.add_argument("--rows", type=int, default=200_000, help="Base number of rows.")
    parser.add_argument("--payload-bytes", type=int, default=256, help="Payload size.")
    parser.add_argument("--runs", type=int, default=15, help="Timed runs for query benchmarks.")
    parser.add_argument("--restart-runs", type=int, default=7, help="Timed runs for restart benchmark.")
    parser.add_argument("--seed", type=int, default=1234567, help="Random seed.")
    parser.add_argument("--shuffle-ids", action="store_true", help="Use pseudo-random id permutation.")
    parser.add_argument("--keep-workspace", action="store_true", help="Do not remove workspace after run.")
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument(
        "--checkpoint-mb",
        type=int,
        default=0,
        metavar="N",
        help="Forward --csv-checkpoint-mb=N to benchmark_runner during load (0=off, periodic CHECKPOINT every N MiB).",
    )
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    suppress_output = not args.show_runner_output
    workspace = Path(args.workspace)
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    summary_rows: list[BenchRow] = []
    rng = random.Random(args.seed)

    try:
        base_csv = workspace / "base_data.csv"
        generate_csv(base_csv, args.rows, args.payload_bytes, args.shuffle_ids)
        bench_tag = f"{int(time.time())}_{os.getpid()}"

        # 1) Write throughput/load-only cost
        for scenario, use_hash in [("btree", False), ("bitcask_hash", True)]:
            db_name = f"benchdb_write_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"write_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, use_hash, 1000, 200)
            query_sql = "-- @expected_rows 1\nSELECT * FROM " + db_name + ".kv WHERE id = 1;"
            write_scenario(scenario_dir, setup_sql, "query.sql", query_sql)
            load_ms = measure_load_only(
                runner,
                scenario_dir,
                query_file="query.sql",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "write_load_only", scenario, "load_shutdown_ms", load_ms, "ms", "OK")
            append_row(
                summary_rows,
                "write_load_only",
                scenario,
                "insert_ops_s",
                args.rows / max(1e-9, load_ms / 1000.0),
                "ops/s",
                "OK",
            )

        # 2) Mixed workload (multi-statement timed query)
        for scenario, use_hash in [("btree", False), ("bitcask_hash", True)]:
            db_name = f"benchdb_mixed_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"mixed_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, use_hash, 1000, 200)
            hot_key = rng.randrange(1, args.rows + 1)
            mixed_sql = (
                "-- @expected_rows 1\n"
                f"SELECT * FROM {db_name}.kv WHERE id = {hot_key};\n"
                f"UPDATE {db_name}.kv SET payload = 'u1' WHERE id = {hot_key};\n"
                f"INSERT INTO {db_name}.kv (id, payload) VALUES ({args.rows + 10}, 'ins1');\n"
                f"DELETE FROM {db_name}.kv WHERE id = {args.rows + 10};\n"
                f"SELECT * FROM {db_name}.kv WHERE id = {hot_key};"
            )
            write_scenario(scenario_dir, setup_sql, "query.sql", mixed_sql)
            query = measure_lookup(
                runner,
                scenario_dir,
                query_file="query.sql",
                runs=args.runs,
                out_name="result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "mixed_read_write", scenario, "avg_ms", query.avg_ms, "ms", query.verified)
            append_row(summary_rows, "mixed_read_write", scenario, "median_ms", query.median_ms, "ms", query.verified)
            append_query_quantiles(summary_rows, "mixed_read_write", scenario, query)
            append_row(summary_rows, "mixed_read_write", scenario, "overhead_ms", query.overhead_ms, "ms", query.verified)
            append_row(
                summary_rows,
                "mixed_read_write",
                scenario,
                "query_ops_s",
                args.runs / max(1e-9, query.wall_ms / 1000.0),
                "ops/s",
                query.verified,
            )

        # 3) Range query benchmark
        for scenario, use_hash in [("btree", False), ("bitcask_hash", True)]:
            db_name = f"benchdb_range_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"range_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, use_hash, 1000, 200)
            start_id = max(1, args.rows // 2)
            end_id = min(args.rows, start_id + 5000)
            range_sql = f"-- @expected_rows {end_id - start_id + 1}\nSELECT * FROM {db_name}.kv WHERE id >= {start_id} AND id <= {end_id};"
            write_scenario(scenario_dir, setup_sql, "query.sql", range_sql)
            query = measure_lookup(
                runner,
                scenario_dir,
                query_file="query.sql",
                runs=args.runs,
                out_name="result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "range_query", scenario, "avg_ms", query.avg_ms, "ms", query.verified)
            append_row(summary_rows, "range_query", scenario, "median_ms", query.median_ms, "ms", query.verified)
            append_query_quantiles(summary_rows, "range_query", scenario, query)
            append_row(summary_rows, "range_query", scenario, "overhead_ms", query.overhead_ms, "ms", query.verified)
            append_row(
                summary_rows,
                "range_query",
                scenario,
                "query_ops_s",
                args.runs / max(1e-9, query.wall_ms / 1000.0),
                "ops/s",
                query.verified,
            )

        # 4) Startup scaling by segment settings (bitcask only)
        for flush_threshold, segment_limit in [(1000, 200), (200, 50), (50, 20)]:
            scenario = f"flush_{flush_threshold}_seg_{segment_limit}"
            db_name = f"benchdb_restart_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"restart_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, flush_threshold, segment_limit)
            key = rng.randrange(1, args.rows + 1)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            write_scenario(scenario_dir, setup_sql, "query.sql", query_sql)
            restart = measure_restart(
                runner,
                scenario_dir,
                query_file="query.sql",
                restart_runs=args.restart_runs,
                restart_out_name="restart.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "startup_scaling", scenario, "load_shutdown_ms", restart.load_shutdown_ms, "ms", "OK")
            append_row(summary_rows, "startup_scaling", scenario, "restart_avg_ms", restart.restart_avg_ms, "ms", restart.verified)
            append_row(summary_rows, "startup_scaling", scenario, "restart_median_ms", restart.restart_median_ms, "ms", restart.verified)
            append_restart_quantiles(summary_rows, "startup_scaling", scenario, restart)
            append_row(summary_rows, "startup_scaling", scenario, "startup_overhead_ms", restart.startup_overhead_ms, "ms", restart.verified)
            append_row(
                summary_rows,
                "startup_scaling",
                scenario,
                "insert_ops_s",
                args.rows / max(1e-9, restart.load_shutdown_ms / 1000.0),
                "ops/s",
                "OK",
            )
            append_row(
                summary_rows,
                "startup_scaling",
                scenario,
                "restart_query_ops_s",
                args.restart_runs / max(1e-9, restart.restart_wall_ms / 1000.0),
                "ops/s",
                restart.verified,
            )

        # 5) Flush-threshold sensitivity (point lookup)
        for flush_threshold in [50, 200, 1000, 5000]:
            scenario = f"flush_{flush_threshold}"
            db_name = f"benchdb_flush_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"flush_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, flush_threshold, 200)
            key = rng.randrange(1, args.rows + 1)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            write_scenario(scenario_dir, setup_sql, "query.sql", query_sql)
            query = measure_lookup(
                runner,
                scenario_dir,
                query_file="query.sql",
                runs=args.runs,
                out_name="result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "avg_ms", query.avg_ms, "ms", query.verified)
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "median_ms", query.median_ms, "ms", query.verified)
            append_query_quantiles(summary_rows, "flush_threshold_sensitivity", scenario, query)
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "overhead_ms", query.overhead_ms, "ms", query.verified)
            append_row(
                summary_rows,
                "flush_threshold_sensitivity",
                scenario,
                "query_ops_s",
                args.runs / max(1e-9, query.wall_ms / 1000.0),
                "ops/s",
                query.verified,
            )

        # 6) Hot-key vs uniform point lookup (bitcask only)
        for scenario, key in [("hot_key", 1), ("uniform_key", rng.randrange(1, args.rows + 1))]:
            db_name = f"benchdb_hot_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"hot_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, 1000, 200)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            write_scenario(scenario_dir, setup_sql, "query.sql", query_sql)
            query = measure_lookup(
                runner,
                scenario_dir,
                query_file="query.sql",
                runs=args.runs,
                out_name="result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "avg_ms", query.avg_ms, "ms", query.verified)
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "median_ms", query.median_ms, "ms", query.verified)
            append_query_quantiles(summary_rows, "hotkey_vs_uniform", scenario, query)
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "overhead_ms", query.overhead_ms, "ms", query.verified)
            append_row(
                summary_rows,
                "hotkey_vs_uniform",
                scenario,
                "query_ops_s",
                args.runs / max(1e-9, query.wall_ms / 1000.0),
                "ops/s",
                query.verified,
            )

        # 7) Large row-id list behavior (bitcask only): same key receives many logical matches
        duplicate_rows = min(args.rows, 40_000)
        dup_csv = workspace / "dup_data.csv"
        with dup_csv.open("w", encoding="utf-8", newline="") as f:
            f.write("id,payload\n")
            for i in range(duplicate_rows):
                f.write(f"7,row_{i}\n")
            for i in range(1, 20_001):
                f.write(f"{1000000 + i},x\n")
        db_name = f"benchdb_large_rowlist_{bench_tag}"
        scenario_dir = workspace / "large_rowlist"
        setup_sql = prepare_base_setup(db_name, dup_csv, True, 1000, 200)
        query_sql = f"-- @expected_rows {duplicate_rows}\nSELECT * FROM {db_name}.kv WHERE id = 7;"
        write_scenario(scenario_dir, setup_sql, "query.sql", query_sql)
        query = measure_lookup(
            runner,
            scenario_dir,
            query_file="query.sql",
            runs=args.runs,
            out_name="result.csv",
            checkpoint_mb=args.checkpoint_mb,
            suppress_output=suppress_output,
        )
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "avg_ms", query.avg_ms, "ms", query.verified)
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "median_ms", query.median_ms, "ms", query.verified)
        append_query_quantiles(summary_rows, "large_rowid_list", "bitcask_hash", query)
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "overhead_ms", query.overhead_ms, "ms", query.verified)
        append_row(
            summary_rows,
            "large_rowid_list",
            "bitcask_hash",
            "query_ops_s",
            args.runs / max(1e-9, query.wall_ms / 1000.0),
            "ops/s",
            query.verified,
        )

        out_csv = workspace / "bitcask_extended_summary.csv"
        write_summary_csv(out_csv, summary_rows)

        print(f"Extended benchmarks finished. Summary: {out_csv}")
        print("benchmark,scenario,metric,value,unit,status")
        for row in summary_rows:
            print(f"{row.benchmark},{row.scenario},{row.metric},{row.value:.3f},{row.unit},{row.status}")
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
