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


def create_benchmark_layout(directory: Path, setup_sql: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "_setup.sql").write_text(setup_sql + "\n", encoding="utf-8")


def generate_lookup_sql(
    query_path: Path,
    database_name: str,
    rows: int,
    seed: int,
) -> None:
    lines = ["-- @expected_rows 1"]
    rng = random.Random(seed)
    key = rng.randrange(1, rows + 1)
    lines.append(f"SELECT * FROM {database_name}.kv WHERE id = {key};")
    query_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_process(args: list[str], cwd: Path | None = None, suppress_output: bool = True) -> None:
    if suppress_output:
        proc = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, args, output=proc.stdout)
        return
    subprocess.run(args, cwd=str(cwd) if cwd else None, check=True)


def run_load_and_shutdown(runner: Path, scenario_dir: Path, suppress_runner_output: bool) -> float:
    cmd = [str(runner), "--file=lookup.sql", "--disk", "--load-only"]
    wall_start = time.perf_counter()
    run_process(cmd, cwd=scenario_dir, suppress_output=suppress_runner_output)
    return (time.perf_counter() - wall_start) * 1000.0


def run_restart_and_measure(
    runner: Path,
    scenario_dir: Path,
    runs: int,
    suppress_runner_output: bool,
) -> tuple[float, float, str, float, float]:
    out_csv = scenario_dir / "restart_result.csv"
    cmd = [
        str(runner),
        "--file=lookup.sql",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
        f"--out={out_csv}",
    ]
    wall_start = time.perf_counter()
    run_process(cmd, cwd=scenario_dir, suppress_output=suppress_runner_output)
    restart_wall_ms = (time.perf_counter() - wall_start) * 1000.0

    rows = []
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        die(f"Cannot parse restart benchmark output for scenario '{scenario_dir.name}'")

    avg_ms = sum(float(r["avg_ms"]) for r in rows) / len(rows)
    median_ms = sum(float(r["median_ms"]) for r in rows) / len(rows)
    timed_total_ms = sum(float(r["avg_ms"]) * float(r["nruns"]) for r in rows)
    startup_overhead_ms = max(0.0, restart_wall_ms - timed_total_ms)
    verified = "OK" if all(r["verified"] == "OK" for r in rows) else "FAIL"
    return avg_ms, median_ms, verified, restart_wall_ms, startup_overhead_ms


def human_size(path: Path) -> str:
    size = path.stat().st_size
    units = ["B", "K", "M", "G", "T"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)}{unit}"
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{size}B"


def print_table_header() -> None:
    cols = [
        ("scenario", 24),
        ("load+shutdown", 14),
        ("restart avg", 12),
        ("restart median", 14),
        ("restart wall", 13),
        ("startup overhead", 16),
        ("status", 8),
    ]
    line = " | ".join(name.ljust(width) for name, width in cols)
    sep = "-+-".join("-" * width for _, width in cols)
    print(line)
    print(sep)


def print_table_row(
    scenario: str,
    load_shutdown_ms: float,
    restart_avg_ms: float,
    restart_median_ms: float,
    restart_wall_ms: float,
    startup_overhead_ms: float,
    status: str,
) -> None:
    print(
        f"{scenario:<24} | "
        f"{load_shutdown_ms:>11.3f} ms | "
        f"{restart_avg_ms:>9.3f} ms | "
        f"{restart_median_ms:>11.3f} ms | "
        f"{restart_wall_ms:>10.3f} ms | "
        f"{startup_overhead_ms:>13.3f} ms | "
        f"{status:<8}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure restart startup time for key lookup scenarios with persisted disk data."
    )
    parser.add_argument("--runner", default="", help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_restart_startup_bench", help="Workspace path.")
    parser.add_argument("--rows", type=int, default=0, help="Number of rows.")
    parser.add_argument("--payload-bytes", type=int, default=512, help="Payload size in bytes.")
    parser.add_argument("--runs", type=int, default=7, help="Timed runs in restart phase.")
    parser.add_argument("--shuffle-ids", action="store_true", help="Use pseudo-random id permutation.")
    parser.add_argument("--seed", type=int, default=1234567, help="Pseudo-random seed.")
    parser.add_argument("--show-runner-output", action="store_true", help="Do not suppress benchmark_runner output.")
    args = parser.parse_args()

    if not args.runner:
        die("benchmark_runner not found. Use --runner PATH.")
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
        gen_start = time.perf_counter()
        generate_csv(csv_path, args.rows, args.payload_bytes, args.shuffle_ids)
        gen_ms = (time.perf_counter() - gen_start) * 1000.0
        print(f"Dataset file: {csv_path} ({human_size(csv_path)}), generated in {gen_ms:.1f} ms")

        bench_tag = f"{int(time.time())}_{os.getpid()}"
        db_name = f"benchdb_{bench_tag}"

        no_index_dir = workspace / "scenario_no_index"
        single_index_dir = workspace / "scenario_single_field_index"
        hash_index_dir = workspace / "scenario_hash_single_field_index"

        load_setup_sql = (
            f"-- @database {db_name}\n"
            "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
            f"-- @load_csv {csv_path} kv ,"
        )

        create_benchmark_layout(no_index_dir, load_setup_sql)
        generate_lookup_sql(no_index_dir / "lookup.sql", db_name, args.rows, args.seed)
        create_benchmark_layout(single_index_dir, load_setup_sql + f"\nCREATE INDEX idx_id ON {db_name}.kv (id);")
        generate_lookup_sql(single_index_dir / "lookup.sql", db_name, args.rows, args.seed)
        create_benchmark_layout(
            hash_index_dir,
            load_setup_sql + f"\nCREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);",
        )
        generate_lookup_sql(hash_index_dir / "lookup.sql", db_name, args.rows, args.seed)

        scenarios = [
            ("no_index", no_index_dir),
            ("single_field_index", single_index_dir),
            ("hash_single_field_index", hash_index_dir),
        ]

        print()
        print("Running restart startup benchmark: phase1(load+shutdown) -> phase2(restart with --skip-load)...")
        print_table_header()

        for name, scenario_dir in scenarios:
            load_shutdown_ms = run_load_and_shutdown(
                runner,
                scenario_dir,
                suppress_runner_output=not args.show_runner_output,
            )
            avg_ms, median_ms, verified, restart_wall_ms, startup_overhead_ms = run_restart_and_measure(
                runner,
                scenario_dir,
                args.runs,
                suppress_runner_output=not args.show_runner_output,
            )
            print_table_row(
                name,
                load_shutdown_ms,
                avg_ms,
                median_ms,
                restart_wall_ms,
                startup_overhead_ms,
                verified,
            )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
