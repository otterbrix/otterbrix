#!/usr/bin/env python3
"""
Automated crash/restart perf+correctness scenarios for disk hash index.

This script:
1) Builds a baseline disk table + hash index.
2) Runs long workloads with --skip-load.
3) Forcefully crashes benchmark_runner with SIGKILL at key points.
4) Restarts and measures startup/query latency.
5) Validates basic correctness after restart.

Output:
  <workspace>/crash_restart_perf_correctness.csv
Columns:
  scenario,description,crash_forced,crash_signal,restart_status,restart_avg_ms,
  restart_median_ms,restart_wall_ms,startup_overhead_ms,index_header_ok,error
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import generate_csv
from benchlib import quantile_csv_headers
from benchlib import quantile_field
from benchlib import read_runner_csv as read_runner_stats
from benchlib import runner_cmd
from benchlib import run_process


PAGE_SIZE = 4096
INDEX_NAME = "idx_id_hash"
TABLE_NAME = "kv"


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


@dataclass
class Scenario:
    name: str
    description: str
    workload_sql: str
    crash_runs: int = 200_000


@dataclass
class RestartMetrics:
    avg_ms: float
    median_ms: float
    quantiles_ms: dict[int, float] = field(default_factory=dict)
    wall_ms: float = 0.0
    startup_overhead_ms: float = 0.0
    verified: str = "FAIL"


def setup_sql(db_name: str) -> str:
    return (
        f"-- @database {db_name}\n"
        f"CREATE TABLE {TABLE_NAME} (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
        f"-- @load_csv data.csv {TABLE_NAME} ,\n"
        f"CREATE INDEX {INDEX_NAME} ON {db_name}.{TABLE_NAME} USING hash (id);\n"
    )


def baseline_lookup_sql(db_name: str, key: int) -> str:
    return f"-- @expected_rows 1\nSELECT * FROM {db_name}.{TABLE_NAME} WHERE id = {key};\n"



def run_load_only(runner: Path, cwd: Path, checkpoint_mb: int, show_output: bool) -> None:
    cmd = runner_cmd(runner, "lookup.sql", disk=True, load_only=True, checkpoint_mb=checkpoint_mb)
    try:
        run_process(cmd, cwd, suppress_output=not show_output)
    except subprocess.CalledProcessError as exc:
        output = "" if show_output else (exc.output or "")[-4000:]
        raise RuntimeError(f"load-only failed: exit={exc.returncode}\n{output}") from exc


def run_restart_lookup(
    runner: Path,
    cwd: Path,
    runs: int,
    show_output: bool,
) -> RestartMetrics:
    out_csv = cwd / "restart_result.csv"
    cmd = runner_cmd(runner, "lookup.sql", runs=runs, out_csv=out_csv, disk=True, skip_load=True)
    t0 = time.perf_counter()
    try:
        run_process(cmd, cwd, suppress_output=not show_output)
    except subprocess.CalledProcessError as exc:
        output = "" if show_output else (exc.output or "")[-4000:]
        raise RuntimeError(f"restart lookup failed: exit={exc.returncode}\n{output}") from exc
    wall_ms = (time.perf_counter() - t0) * 1000.0
    stats = read_runner_stats(out_csv)
    startup_overhead_ms = max(0.0, wall_ms - stats.timed_total_ms)
    return RestartMetrics(
        avg_ms=stats.avg_ms,
        median_ms=stats.median_ms,
        quantiles_ms=stats.quantiles_ms,
        wall_ms=wall_ms,
        startup_overhead_ms=startup_overhead_ms,
        verified=stats.verified,
    )


def run_forced_crash(
    runner: Path,
    cwd: Path,
    sql_file: str,
    runs: int,
    crash_delay_sec: float,
    show_output: bool,
) -> tuple[bool, int | None]:
    cmd = [
        str(runner),
        f"--file={sql_file}",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=None if show_output else subprocess.PIPE,
        stderr=subprocess.STDOUT if not show_output else None,
        text=True,
    )

    deadline = time.monotonic() + max(0.05, crash_delay_sec)
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            # Workload ended unexpectedly before forced crash.
            return False, proc.returncode
        time.sleep(0.02)

    if proc.poll() is None:
        proc.kill()  # SIGKILL on Unix
        proc.wait(timeout=10)
        return True, proc.returncode

    return False, proc.returncode


def find_hash_index_bin(root: Path) -> Path | None:
    wal_root = root / "wal"
    if not wal_root.is_dir():
        return None
    matches = sorted(wal_root.rglob("hash_index.bin"))
    if not matches:
        return None
    for p in matches:
        if p.parent.name == INDEX_NAME:
            return p
    return matches[0]


def validate_hash_header(hash_bin: Path) -> bool:
    raw = hash_bin.read_bytes()
    if len(raw) < PAGE_SIZE:
        return False
    page_size = int.from_bytes(raw[12:16], byteorder="little", signed=False)
    bucket_count = int.from_bytes(raw[16:20], byteorder="little", signed=False)
    next_overflow = int.from_bytes(raw[20:28], byteorder="little", signed=False)
    if page_size != PAGE_SIZE:
        return False
    if bucket_count == 0:
        return False
    if next_overflow < 1 + bucket_count:
        return False
    return True


def write_file(path: Path, content: str) -> None:
    path.write_text(content.strip() + "\n", encoding="utf-8")


def scenario_workloads(db_name: str, lookup_key: int) -> list[Scenario]:
    read_hot = (
        f"-- @expected_rows 1\n"
        f"SELECT * FROM {db_name}.{TABLE_NAME} WHERE id = {lookup_key};\n"
    )
    write_churn = (
        f"-- @expected_rows 1\n"
        f"UPDATE {db_name}.{TABLE_NAME} SET payload = 'upd' WHERE id = {lookup_key};\n"
        f"INSERT INTO {db_name}.{TABLE_NAME} (id, payload) VALUES (2000000000, 'tmp');\n"
        f"DELETE FROM {db_name}.{TABLE_NAME} WHERE id = 2000000000;\n"
        f"SELECT * FROM {db_name}.{TABLE_NAME} WHERE id = {lookup_key};\n"
    )
    rehash_push = (
        f"-- @expected_rows 1\n"
        f"INSERT INTO {db_name}.{TABLE_NAME} (id, payload) VALUES (777777777, 'rehash_stress');\n"
        f"SELECT * FROM {db_name}.{TABLE_NAME} WHERE id = {lookup_key};\n"
    )
    return [
        Scenario(
            name="crash_during_read_hotpath",
            description="SIGKILL during read-heavy point lookup loop",
            workload_sql=read_hot,
        ),
        Scenario(
            name="crash_during_write_churn",
            description="SIGKILL during update/insert/delete churn loop",
            workload_sql=write_churn,
        ),
        Scenario(
            name="crash_during_rehash_pressure",
            description="SIGKILL during insert pressure likely triggering rehash",
            workload_sql=rehash_push,
        ),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Automated forced crash/restart perf+correctness scenarios for disk hash index."
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="", help="Workspace path (default: temp dir).")
    parser.add_argument("--rows", type=int, default=120_000, help="Rows in baseline dataset.")
    parser.add_argument("--payload-bytes", type=int, default=256, help="Payload size per row.")
    parser.add_argument("--lookup-key", type=int, default=1, help="Stable key validated after restart.")
    parser.add_argument("--crash-delay-sec", type=float, default=1.2, help="Delay before SIGKILL.")
    parser.add_argument("--restart-runs", type=int, default=10, help="Timed runs in restart validation.")
    parser.add_argument("--checkpoint-mb", type=int, default=0, metavar="N")
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument("--keep-workspace", action="store_true")
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        die(f"runner does not exist: {runner}")
    if args.rows <= 0:
        die("--rows must be > 0")
    if args.lookup_key <= 0:
        die("--lookup-key must be > 0")

    workspace = (
        Path(args.workspace).resolve()
        if args.workspace
        else Path(tempfile.gettempdir()) / f"otterbrix_crash_restart_{int(time.time())}"
    )
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, str]] = []
    db_name = f"benchdb_crash_{int(time.time())}_{os.getpid()}"

    try:
        scenarios = scenario_workloads(db_name, args.lookup_key)
        for sc in scenarios:
            scenario_dir = workspace / sc.name
            scenario_dir.mkdir(parents=True, exist_ok=True)

            generate_csv(scenario_dir / "data.csv", args.rows, args.payload_bytes)
            write_file(scenario_dir / "_setup.sql", setup_sql(db_name))
            write_file(scenario_dir / "lookup.sql", baseline_lookup_sql(db_name, args.lookup_key))
            write_file(scenario_dir / "workload.sql", sc.workload_sql)

            error = ""
            restart_status = "FAIL"
            crash_forced = False
            crash_signal = ""
            restart_avg_ms = 0.0
            restart_median_ms = 0.0
            restart_quantiles_ms: dict[int, float] = {}
            restart_wall_ms = 0.0
            startup_overhead_ms = 0.0
            index_header_ok = False

            try:
                run_load_only(
                    runner=runner,
                    cwd=scenario_dir,
                    checkpoint_mb=args.checkpoint_mb,
                    show_output=args.show_runner_output,
                )
                # Pre-crash health check.
                metrics = run_restart_lookup(
                    runner=runner,
                    cwd=scenario_dir,
                    runs=1,
                    show_output=args.show_runner_output,
                )
                if metrics.verified != "OK":
                    raise RuntimeError("pre-crash health check failed")

                crash_forced, rc = run_forced_crash(
                    runner=runner,
                    cwd=scenario_dir,
                    sql_file="workload.sql",
                    runs=sc.crash_runs,
                    crash_delay_sec=args.crash_delay_sec,
                    show_output=args.show_runner_output,
                )
                if rc is None:
                    crash_signal = "unknown"
                elif rc < 0:
                    crash_signal = f"SIG{-rc}"
                else:
                    crash_signal = f"EXIT_{rc}"

                if not crash_forced:
                    raise RuntimeError(
                        "process finished before forced crash; increase --crash-delay-sec or --crash-runs"
                    )

                post = run_restart_lookup(
                    runner=runner,
                    cwd=scenario_dir,
                    runs=args.restart_runs,
                    show_output=args.show_runner_output,
                )
                restart_status = post.verified
                restart_avg_ms = post.avg_ms
                restart_median_ms = post.median_ms
                restart_quantiles_ms = post.quantiles_ms
                restart_wall_ms = post.wall_ms
                startup_overhead_ms = post.startup_overhead_ms

                hash_bin = find_hash_index_bin(scenario_dir)
                if hash_bin is None:
                    raise RuntimeError("hash_index.bin not found after restart")
                index_header_ok = validate_hash_header(hash_bin)
                if not index_header_ok:
                    raise RuntimeError("hash_index.bin header validation failed")
            except Exception as exc:
                error = str(exc)

            summary_rows.append(
                {
                    "scenario": sc.name,
                    "description": sc.description,
                    "crash_forced": "YES" if crash_forced else "NO",
                    "crash_signal": crash_signal,
                    "restart_status": restart_status,
                    "restart_avg_ms": f"{restart_avg_ms:.3f}",
                    "restart_median_ms": f"{restart_median_ms:.3f}",
                    **{quantile_field(p): f"{restart_quantiles_ms.get(p, float('nan')):.3f}" for p in QUANTILE_PCTS},
                    "restart_wall_ms": f"{restart_wall_ms:.3f}",
                    "startup_overhead_ms": f"{startup_overhead_ms:.3f}",
                    "index_header_ok": "YES" if index_header_ok else "NO",
                    "error": error,
                }
            )

        out_csv = workspace / "crash_restart_perf_correctness.csv"
        with out_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "scenario",
                    "description",
                    "crash_forced",
                    "crash_signal",
                    "restart_status",
                    "restart_avg_ms",
                    "restart_median_ms",
                    *quantile_csv_headers(),
                    "restart_wall_ms",
                    "startup_overhead_ms",
                    "index_header_ok",
                    "error",
                ],
            )
            writer.writeheader()
            writer.writerows(summary_rows)

        print(f"Saved report: {out_csv}")
        print(",".join(out_csv.read_text(encoding="utf-8").splitlines()[:1]))
        for row in summary_rows:
            print(
                f"{row['scenario']}: crash={row['crash_forced']}({row['crash_signal']}), "
                f"restart={row['restart_status']}, header_ok={row['index_header_ok']}, "
                f"avg_ms={row['restart_avg_ms']}, error={row['error'] or '-'}"
            )
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()

