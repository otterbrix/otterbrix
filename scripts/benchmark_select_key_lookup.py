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

def die(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def generate_csv(csv_path: Path, rows: int, payload_bytes: int, shuffle_ids: bool):
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

def create_benchmark_layout(directory: Path, setup_sql: str):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "_setup.sql").write_text(setup_sql + "\n", encoding="utf-8")

def generate_lookup_sql(
    query_path: Path,
    database_name: str,
    rows: int,
    seed: int,
):
    lines = ["-- @expected_rows 1"]
    rng = random.Random(seed)
    key = rng.randrange(1, rows + 1)
    lines.append(f"SELECT * FROM {database_name}.kv WHERE id = {key};")
    query_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def create_query_file(directory: Path, query_sql: str, expected_rows: int = 1) -> None:
    text = f"-- @expected_rows {expected_rows}\n{query_sql}\n"
    (directory / "lookup.sql").write_text(text, encoding="utf-8")

def run_process(args: list[str], cwd: Path | None = None):
    subprocess.run(args, cwd=str(cwd) if cwd else None, check=True, stdout=subprocess.DEVNULL)

def run_scenario(runner: Path, scenario_dir: Path, runs: int):
    out_csv = scenario_dir / "result.csv"
    cmd = [
        str(runner),
        "--file=lookup.sql",
        f"--runs={runs}",
        "--disk",
        f"--out={out_csv}",
    ]
    run_process(cmd, cwd=scenario_dir)

    rows = []
    with out_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    if not rows:
        print(f"Cannot parse benchmark output for scenario '{scenario_dir.name}'", file=sys.stderr)
        sys.exit(1)
    avg_ms = sum(float(r["avg_ms"]) for r in rows) / len(rows)
    median_ms = sum(float(r["median_ms"]) for r in rows) / len(rows)
    verified = "OK" if all(r["verified"] == "OK" for r in rows) else "FAIL"
    return avg_ms, median_ms, verified

def human_size(path: Path):
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

def main():
    parser = argparse.ArgumentParser(description="Benchmark key lookup with and without index.")
    parser.add_argument("--runner", default="", help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_key_lookup_bench", help="Workspace path.")
    parser.add_argument("--rows", type=int, default=0, help="Number of rows.")
    parser.add_argument("--payload-bytes", type=int, default=512, help="Payload size in bytes.")
    parser.add_argument("--runs", type=int, default=7, help="Timed runs per scenario.")
    parser.add_argument("--shuffle-ids", action="store_true", help="Use pseudo-random id permutation.")
    parser.add_argument("--seed", type=int, default=1234567, help="Pseudo-random seed.")
    args = parser.parse_args()

    if args.runner is None:
        print(f"benchmark_runner not found. Use --runner PATH.", file=sys.stderr)
        sys.exit(1)
    runner = Path(args.runner).resolve()
    rows = args.rows

    workspace = Path(args.workspace)
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    csv_path = workspace / "data.csv"
    print(f"Generating dataset: rows={rows}, payload_bytes={args.payload_bytes}")
    generate_csv(csv_path, rows, args.payload_bytes, args.shuffle_ids)
    print(f"Dataset file: {csv_path} ({human_size(csv_path)})")

    bench_tag = f"{int(time.time())}_{os.getpid()}"
    db_name = f"benchdb_{bench_tag}"

    no_index_dir = workspace / "scenario_no_index"
    single_index_dir = workspace / "scenario_single_field_index"
    hash_index_dir = workspace / "scenario_hash_single_field_index"

    load_setup_sql = (
        f"-- @database {db_name}\n"
        "CREATE TABLE kv (id INTEGER, payload STRING);\n"
        f"-- @load_csv {csv_path} kv ,"
    )

    create_benchmark_layout(no_index_dir, load_setup_sql)
    generate_lookup_sql(no_index_dir / "lookup.sql", db_name, rows, args.seed)

    create_benchmark_layout(single_index_dir, load_setup_sql + f"\nCREATE INDEX idx_id ON {db_name}.kv (id);")
    generate_lookup_sql(single_index_dir / "lookup.sql", db_name, rows, args.seed)

    create_benchmark_layout(
        hash_index_dir,
        load_setup_sql + f"\nCREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);",
    )
    generate_lookup_sql(hash_index_dir / "lookup.sql", db_name, rows, args.seed)

    print()
    print(f"Running benchmark scenarios with {args.runs} timed runs each...")
    print(f"Workload lookups per scenario file: 1 (shuffle_ids={1 if args.shuffle_ids else 0})")
    print("scenario,avg_ms,median_ms,verified")

    scenarios = [
        ("no_index", no_index_dir),
        ("single_field_index", single_index_dir),
        ("hash_single_field_index", hash_index_dir),
    ]
    for name, d in scenarios:
        avg_ms, median_ms, verified = run_scenario(runner, d, args.runs)
        print(f"{name},{avg_ms:.3f},{median_ms:.3f},{verified}")

    shutil.rmtree(workspace, ignore_errors=True)

if __name__ == "__main__":
    main()
