#!/usr/bin/env python3
import argparse
import csv
import os
import random
import shutil
import subprocess
import sys
import time
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


def generate_lookup_sql(path: Path, db_name: str, rows: int, seed: int) -> None:
    rng = random.Random(seed)
    key = rng.randrange(1, rows + 1)
    path.write_text(
        "-- @expected_rows 1\n" f"SELECT * FROM {db_name}.kv WHERE id = {key};\n",
        encoding="utf-8",
    )


def create_scenario(path: Path, setup_sql: str, lookup_sql: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "_setup.sql").write_text(setup_sql + "\n", encoding="utf-8")
    (path / "lookup.sql").write_text(lookup_sql, encoding="utf-8")


def read_runner_csv(csv_path: Path) -> tuple[float, float, str, float]:
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
    return avg_ms, median_ms, verified, timed_total_ms


def bench_lookup(runner: Path, scenario_dir: Path, runs: int, suppress_output: bool) -> tuple[float, float, str, float, float]:
    out_csv = scenario_dir / "lookup_result.csv"
    cmd = [str(runner), "--file=lookup.sql", f"--runs={runs}", "--disk", f"--out={out_csv}"]
    wall_start = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output)
    wall_ms = (time.perf_counter() - wall_start) * 1000.0
    avg_ms, median_ms, verified, timed_total_ms = read_runner_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, verified, wall_ms, overhead_ms


def bench_restart_startup(
    runner: Path,
    scenario_dir: Path,
    runs: int,
    suppress_output: bool,
) -> tuple[float, float, str, float, float, float]:
    load_cmd = [str(runner), "--file=lookup.sql", "--disk", "--load-only"]
    t0 = time.perf_counter()
    run_process(load_cmd, scenario_dir, suppress_output)
    load_shutdown_ms = (time.perf_counter() - t0) * 1000.0

    out_csv = scenario_dir / "restart_result.csv"
    restart_cmd = [
        str(runner),
        "--file=lookup.sql",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
        f"--out={out_csv}",
    ]
    t1 = time.perf_counter()
    run_process(restart_cmd, scenario_dir, suppress_output)
    restart_wall_ms = (time.perf_counter() - t1) * 1000.0

    avg_ms, median_ms, verified, timed_total_ms = read_runner_csv(out_csv)
    startup_overhead_ms = max(0.0, restart_wall_ms - timed_total_ms)
    return load_shutdown_ms, avg_ms, median_ms, verified, restart_wall_ms, startup_overhead_ms


def ratio(base: float, cur: float) -> str:
    if cur <= 0.0:
        return "n/a"
    return f"{base / cur:.2f}x"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare no index vs btree vs hash(bitcask) for key lookup and restart startup."
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_index_metrics", help="Workspace path.")
    parser.add_argument("--rows", type=int, required=True, help="Number of rows.")
    parser.add_argument("--payload-bytes", type=int, default=512, help="Payload size.")
    parser.add_argument("--lookup-runs", type=int, default=20, help="Runs for lookup phase.")
    parser.add_argument("--restart-runs", type=int, default=7, help="Runs for restart phase.")
    parser.add_argument("--seed", type=int, default=1234567, help="Random seed.")
    parser.add_argument("--shuffle-ids", action="store_true", help="Use pseudo-random id permutation.")
    parser.add_argument("--show-runner-output", action="store_true")
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        die(f"runner does not exist: {runner}")

    workspace = Path(args.workspace)
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    try:
        csv_path = workspace / "data.csv"
        print(f"Generating dataset: rows={args.rows}, payload_bytes={args.payload_bytes}")
        generate_csv(csv_path, args.rows, args.payload_bytes, args.shuffle_ids)

        bench_tag = f"{int(time.time())}_{os.getpid()}"
        db_name = f"benchdb_{bench_tag}"
        key = random.Random(args.seed).randrange(1, args.rows + 1)
        lookup_sql = f"-- @expected_rows 1\nSELECT * FROM {db_name}.kv WHERE id = {key};\n"
        common_setup = (
            f"-- @database {db_name}\n"
            "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
            f"-- @load_csv {csv_path} kv ,"
        )

        scenarios = [
            ("no_index", common_setup),
            ("single_field_index", common_setup + f"\nCREATE INDEX idx_id ON {db_name}.kv (id);"),
            ("hash_single_field_index", common_setup + f"\nCREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);"),
        ]

        scenario_dirs: dict[str, Path] = {}
        for name, setup_sql in scenarios:
            p = workspace / f"scenario_{name}"
            create_scenario(p, setup_sql, lookup_sql)
            scenario_dirs[name] = p

        suppress_output = not args.show_runner_output

        lookup_metrics: dict[str, tuple[float, float, str, float, float]] = {}
        restart_metrics: dict[str, tuple[float, float, float, str, float, float]] = {}
        for name, _ in scenarios:
            lookup_metrics[name] = bench_lookup(runner, scenario_dirs[name], args.lookup_runs, suppress_output)
            restart_metrics[name] = bench_restart_startup(
                runner, scenario_dirs[name], args.restart_runs, suppress_output
            )

        base_lookup = lookup_metrics["no_index"][0]
        base_restart = restart_metrics["no_index"][1]

        print("\n=== Lookup Metrics ===")
        print("scenario                  avg_ms    median_ms   wall_ms    overhead_ms   speedup_vs_no_index   status")
        for name, _ in scenarios:
            avg_ms, median_ms, status, wall_ms, overhead_ms = lookup_metrics[name]
            print(
                f"{name:<24} {avg_ms:>8.3f} {median_ms:>11.3f} {wall_ms:>9.3f} {overhead_ms:>12.3f}"
                f" {ratio(base_lookup, avg_ms):>20} {status:>8}"
            )

        print("\n=== Restart Startup Metrics ===")
        print(
            "scenario                  load+shutdown_ms  restart_avg_ms  restart_median_ms  restart_wall_ms"
            "  startup_overhead_ms  speedup_vs_no_index  status"
        )
        for name, _ in scenarios:
            load_ms, avg_ms, median_ms, status, wall_ms, startup_overhead_ms = restart_metrics[name]
            print(
                f"{name:<24} {load_ms:>16.3f} {avg_ms:>15.3f} {median_ms:>18.3f} {wall_ms:>15.3f}"
                f" {startup_overhead_ms:>20.3f} {ratio(base_restart, avg_ms):>19} {status:>7}"
            )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
