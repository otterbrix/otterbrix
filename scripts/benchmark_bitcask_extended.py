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
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def run_process(args: list[str], cwd: Path, suppress_output: bool) -> None:
    if suppress_output:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, args, output=proc.stdout)
        return
    subprocess.run(args, cwd=str(cwd), check=True)


def generate_csv(csv_path: Path, rows: int, payload_bytes: int, shuffle_ids: bool) -> None:
    if rows <= 0:
        die("--rows must be > 0")
    payload = "x" * payload_bytes
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        if shuffle_ids:
            step = rows - 1
            for i in range(rows):
                row_id = ((i * step) % rows) + 1
                f.write(f"{row_id},{payload}\n")
        else:
            for i in range(rows):
                f.write(f"{i + 1},{payload}\n")


def create_scenario(path: Path, setup_sql: str, query_sql: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "_setup.sql").write_text(setup_sql.strip() + "\n", encoding="utf-8")
    (path / "query.sql").write_text(query_sql.strip() + "\n", encoding="utf-8")


def read_runner_csv(csv_path: Path) -> tuple[float, float, float, str]:
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    if not rows:
        die(f"Cannot parse benchmark output: {csv_path}")
    avg_ms = sum(float(r["avg_ms"]) for r in rows) / len(rows)
    median_ms = sum(float(r["median_ms"]) for r in rows) / len(rows)
    timed_total_ms = sum(float(r["avg_ms"]) * float(r["nruns"]) for r in rows)
    verified = "OK" if all(r["verified"] == "OK" for r in rows) else "FAIL"
    return avg_ms, median_ms, timed_total_ms, verified


def run_query_benchmark(
    runner: Path, scenario_dir: Path, runs: int, suppress_output: bool
) -> tuple[float, float, float, str]:
    out_csv = scenario_dir / "result.csv"
    cmd = [str(runner), "--file=query.sql", f"--runs={runs}", "--disk", f"--out={out_csv}"]
    wall_start = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output)
    wall_ms = (time.perf_counter() - wall_start) * 1000.0
    avg_ms, median_ms, timed_total_ms, verified = read_runner_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, overhead_ms, verified


def run_load_only_benchmark(runner: Path, scenario_dir: Path, suppress_output: bool) -> float:
    cmd = [str(runner), "--file=query.sql", "--disk", "--load-only"]
    wall_start = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output)
    return (time.perf_counter() - wall_start) * 1000.0


def run_restart_benchmark(
    runner: Path, scenario_dir: Path, runs: int, suppress_output: bool
) -> tuple[float, float, float, str]:
    out_csv = scenario_dir / "restart.csv"
    cmd = [
        str(runner),
        "--file=query.sql",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
        f"--out={out_csv}",
    ]
    wall_start = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output)
    wall_ms = (time.perf_counter() - wall_start) * 1000.0
    avg_ms, median_ms, timed_total_ms, verified = read_runner_csv(out_csv)
    startup_overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, startup_overhead_ms, verified


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
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        die(f"runner does not exist: {runner}")

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
            create_scenario(scenario_dir, setup_sql, query_sql)
            load_ms = run_load_only_benchmark(runner, scenario_dir, suppress_output)
            append_row(summary_rows, "write_load_only", scenario, "load_shutdown_ms", load_ms, "ms", "OK")

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
            create_scenario(scenario_dir, setup_sql, mixed_sql)
            avg_ms, median_ms, overhead_ms, status = run_query_benchmark(runner, scenario_dir, args.runs, suppress_output)
            append_row(summary_rows, "mixed_read_write", scenario, "avg_ms", avg_ms, "ms", status)
            append_row(summary_rows, "mixed_read_write", scenario, "median_ms", median_ms, "ms", status)
            append_row(summary_rows, "mixed_read_write", scenario, "overhead_ms", overhead_ms, "ms", status)

        # 3) Range query benchmark
        for scenario, use_hash in [("btree", False), ("bitcask_hash", True)]:
            db_name = f"benchdb_range_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"range_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, use_hash, 1000, 200)
            start_id = max(1, args.rows // 2)
            end_id = min(args.rows, start_id + 5000)
            range_sql = f"-- @expected_rows {end_id - start_id + 1}\nSELECT * FROM {db_name}.kv WHERE id >= {start_id} AND id <= {end_id};"
            create_scenario(scenario_dir, setup_sql, range_sql)
            avg_ms, median_ms, overhead_ms, status = run_query_benchmark(runner, scenario_dir, args.runs, suppress_output)
            append_row(summary_rows, "range_query", scenario, "avg_ms", avg_ms, "ms", status)
            append_row(summary_rows, "range_query", scenario, "median_ms", median_ms, "ms", status)
            append_row(summary_rows, "range_query", scenario, "overhead_ms", overhead_ms, "ms", status)

        # 4) Startup scaling by segment settings (bitcask only)
        for flush_threshold, segment_limit in [(1000, 200), (200, 50), (50, 20)]:
            scenario = f"flush_{flush_threshold}_seg_{segment_limit}"
            db_name = f"benchdb_restart_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"restart_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, flush_threshold, segment_limit)
            key = rng.randrange(1, args.rows + 1)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            create_scenario(scenario_dir, setup_sql, query_sql)
            load_ms = run_load_only_benchmark(runner, scenario_dir, suppress_output)
            avg_ms, median_ms, startup_overhead_ms, status = run_restart_benchmark(
                runner, scenario_dir, args.restart_runs, suppress_output
            )
            append_row(summary_rows, "startup_scaling", scenario, "load_shutdown_ms", load_ms, "ms", "OK")
            append_row(summary_rows, "startup_scaling", scenario, "restart_avg_ms", avg_ms, "ms", status)
            append_row(summary_rows, "startup_scaling", scenario, "restart_median_ms", median_ms, "ms", status)
            append_row(summary_rows, "startup_scaling", scenario, "startup_overhead_ms", startup_overhead_ms, "ms", status)

        # 5) Flush-threshold sensitivity (point lookup)
        for flush_threshold in [50, 200, 1000, 5000]:
            scenario = f"flush_{flush_threshold}"
            db_name = f"benchdb_flush_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"flush_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, flush_threshold, 200)
            key = rng.randrange(1, args.rows + 1)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            create_scenario(scenario_dir, setup_sql, query_sql)
            avg_ms, median_ms, overhead_ms, status = run_query_benchmark(runner, scenario_dir, args.runs, suppress_output)
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "avg_ms", avg_ms, "ms", status)
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "median_ms", median_ms, "ms", status)
            append_row(summary_rows, "flush_threshold_sensitivity", scenario, "overhead_ms", overhead_ms, "ms", status)

        # 6) Hot-key vs uniform point lookup (bitcask only)
        for scenario, key in [("hot_key", 1), ("uniform_key", rng.randrange(1, args.rows + 1))]:
            db_name = f"benchdb_hot_{scenario}_{bench_tag}"
            scenario_dir = workspace / f"hot_{scenario}"
            setup_sql = prepare_base_setup(db_name, base_csv, True, 1000, 200)
            query_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};"
            create_scenario(scenario_dir, setup_sql, query_sql)
            avg_ms, median_ms, overhead_ms, status = run_query_benchmark(runner, scenario_dir, args.runs, suppress_output)
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "avg_ms", avg_ms, "ms", status)
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "median_ms", median_ms, "ms", status)
            append_row(summary_rows, "hotkey_vs_uniform", scenario, "overhead_ms", overhead_ms, "ms", status)

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
        create_scenario(scenario_dir, setup_sql, query_sql)
        avg_ms, median_ms, overhead_ms, status = run_query_benchmark(runner, scenario_dir, args.runs, suppress_output)
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "avg_ms", avg_ms, "ms", status)
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "median_ms", median_ms, "ms", status)
        append_row(summary_rows, "large_rowid_list", "bitcask_hash", "overhead_ms", overhead_ms, "ms", status)

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
