#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import random
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import human_bytes
from benchlib import mem_total_bytes
from benchlib import read_runner_csv
from benchlib import run_process


TABLE_NAME = "kv"
HASH_INDEX_NAME = "idx_id_hash"


@dataclass
class Row:
    scenario: str
    point_index: int
    rows_total: int
    rows_added: int
    metric: str
    value: float | str
    unit: str
    status: str


def write_rows_csv(path: Path, start_id: int, row_count: int, payload_bytes: int, seed: int) -> None:
    rng = random.Random(seed + start_id + row_count)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    payload = "".join(rng.choice(alphabet) for _ in range(payload_bytes))
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for i in range(row_count):
            f.write(f"{start_id + i},{payload}{i}\n")

def setup_sql(db_name: str, csv_name: str, with_hash_index: bool) -> str:
    lines = [
        f"-- @database {db_name}",
        f"CREATE TABLE {TABLE_NAME} (id INTEGER, payload STRING) WITH (storage = 'disk');",
        f"-- @load_csv {csv_name} {TABLE_NAME} ,",
    ]
    if with_hash_index:
        lines.append(f"CREATE INDEX {HASH_INDEX_NAME} ON {db_name}.{TABLE_NAME} USING hash (id);")
    return "\n".join(lines) + "\n"


def append_sql(db_name: str, csv_name: str) -> str:
    # sql_benchmark_t requires at least one SQL statement terminated by ';'.
    # The @load_csv directives are comments and get stripped before query parsing,
    # so we add a no-op statement to keep the file a valid SQL benchmark.
    return (
        f"-- @database {db_name}\n"
        f"-- @load_csv {csv_name} {TABLE_NAME} ,\n"
        "SELECT 1;\n"
    )


def lookup_sql(db_name: str, key: int) -> str:
    return f"-- @expected_rows 1\nSELECT * FROM {db_name}.{TABLE_NAME} WHERE id = {key};\n"


def runner_cmd(
    runner: Path,
    sql_file: str,
    *,
    out_csv: Path | None = None,
    runs: int | None = None,
    load_only: bool = False,
    skip_load: bool = False,
    no_setup: bool = False,
    checkpoint_mb: int = 0,
) -> list[str]:
    cmd = [str(runner), f"--file={sql_file}", "--disk"]
    if runs is not None:
        cmd.append(f"--runs={runs}")
    if out_csv is not None:
        cmd.append(f"--out={out_csv}")
    if load_only:
        cmd.append("--load-only")
    if skip_load:
        cmd.append("--skip-load")
    if no_setup:
        cmd.append("--no-setup")
    if checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    return cmd


def run_load_only(
    runner: Path,
    scenario_dir: Path,
    sql_file: str,
    *,
    skip_load: bool,
    no_setup: bool,
    checkpoint_mb: int,
    show_output: bool,
) -> float:
    cmd = runner_cmd(
        runner,
        sql_file,
        load_only=True,
        skip_load=skip_load,
        no_setup=no_setup,
        checkpoint_mb=checkpoint_mb,
    )
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output=not show_output)
    return (time.perf_counter() - t0) * 1000.0


def lookup_ops_s(runs: int, timed_total_ms: float) -> float:
    return runs / max(1e-9, timed_total_ms / 1000.0)


def run_lookup(
    runner: Path,
    scenario_dir: Path,
    sql_file: str,
    *,
    runs: int,
    checkpoint_mb: int,
    show_output: bool,
) -> tuple[float, float, dict[int, float], str, float, float, float]:
    out_csv = scenario_dir / f"{Path(sql_file).stem}_result.csv"
    cmd = runner_cmd(
        runner,
        sql_file,
        out_csv=out_csv,
        runs=runs,
        skip_load=True,
        checkpoint_mb=checkpoint_mb,
    )
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output=not show_output)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    stats = read_runner_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - stats.timed_total_ms)
    return (
        stats.avg_ms,
        stats.median_ms,
        stats.quantiles_ms,
        stats.verified,
        stats.timed_total_ms,
        wall_ms,
        overhead_ms,
    )


def append_row(rows: list[Row], scenario: str, point_index: int, rows_total: int, rows_added: int,
               metric: str, value: float | str, unit: str, status: str) -> None:
    rows.append(Row(scenario, point_index, rows_total, rows_added, metric, value, unit, status))


def write_report(path: Path, rows: list[Row]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["scenario", "point_index", "rows_total", "rows_added", "metric", "value", "unit", "status"])
        for r in rows:
            value = f"{r.value:.6f}" if isinstance(r.value, float) and not math.isnan(r.value) else r.value
            writer.writerow([r.scenario, r.point_index, r.rows_total, r.rows_added, r.metric, value, r.unit, r.status])


def plot_report(rows: list[Row], out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    def metric_values(metric: str, scenario: str) -> tuple[list[int], list[float]]:
        pts = sorted({r.rows_total for r in rows if r.metric == metric and r.scenario == scenario})
        vals = []
        for pt in pts:
            for r in rows:
                if r.scenario == scenario and r.rows_total == pt and r.metric == metric:
                    vals.append(float(r.value))
                    break
        return pts, vals

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        x, y = metric_values("insert_ops_s", scenario)
        axes[0].plot(x, y, marker="o", linewidth=2, color=color, label=scenario)
    axes[0].set_title("Insert throughput vs dataset size")
    axes[0].set_xlabel("rows total")
    axes[0].set_ylabel("ops/s")
    axes[0].grid(True, linestyle=":", alpha=0.5)
    axes[0].legend()

    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        x, y = metric_values("query_ops_s", scenario)
        axes[1].plot(x, y, marker="o", linewidth=2, color=color, label=scenario)
    axes[1].set_title("Lookup throughput vs dataset size")
    axes[1].set_xlabel("rows total")
    axes[1].set_ylabel("ops/s")
    axes[1].grid(True, linestyle=":", alpha=0.5)
    axes[1].legend()

    for scenario, color in (("no_index", "#999999"), ("hash_index", "#F58518")):
        x, y = metric_values("query_avg_ms", scenario)
        axes[2].plot(x, y, marker="o", linewidth=2, color=color, label=scenario)
    axes[2].set_title("Lookup latency vs dataset size")
    axes[2].set_xlabel("rows total")
    axes[2].set_ylabel("avg ms")
    axes[2].grid(True, linestyle=":", alpha=0.5)
    axes[2].legend()

    fig.tight_layout()
    fig.savefig(out_dir / "benchmark_hash_index_memory_growth.png", dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark throughput and lookup latency for hash_index vs no_index under dataset > RAM growth."
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--output-dir", default="benchmark_runs/hash_index_memory_growth")
    parser.add_argument("--workspace", default="")
    parser.add_argument("--payload-bytes", type=int, default=65536)
    parser.add_argument("--initial-rows", type=int, default=0, help="If 0, derive from --memory-pressure-factor.")
    parser.add_argument("--memory-pressure-factor", type=float, default=1.10,
                        help="Initial dataset target = RAM * factor when --initial-rows=0.")
    parser.add_argument("--additional-rows", type=int, default=5000)
    parser.add_argument("--points", type=int, default=5)
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--checkpoint-mb", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1234567)
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument("--keep-workspace", action="store_true")
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")
    if args.points <= 0:
        raise RuntimeError("--points must be > 0")
    if args.additional_rows <= 0:
        raise RuntimeError("--additional-rows must be > 0")

    mem_total = mem_total_bytes()
    if args.initial_rows > 0:
        initial_rows = args.initial_rows
    else:
        if mem_total <= 0:
            raise RuntimeError("cannot determine RAM size; pass --initial-rows explicitly")
        target_bytes = int(mem_total * args.memory_pressure_factor)
        approx_row_bytes = max(1, args.payload_bytes + 24)
        initial_rows = max(1, math.ceil(target_bytes / approx_row_bytes))

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    workspace = Path(args.workspace).resolve() if args.workspace else Path(tempfile.gettempdir()) / f"otterbrix_mem_growth_{int(time.time())}"
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    print(f"Initial rows: {initial_rows}")
    if mem_total > 0:
        approx_bytes = initial_rows * max(1, args.payload_bytes + 24)
        print(f"Estimated initial dataset size: {human_bytes(approx_bytes)} vs RAM {human_bytes(mem_total)}")

    report_rows: list[Row] = []
    scenarios = {"no_index": False, "hash_index": True}

    try:
        for scenario_name, with_hash_index in scenarios.items():
            scenario_dir = workspace / scenario_name
            scenario_dir.mkdir(parents=True, exist_ok=True)
            db_name = f"benchdb_mem_growth_{scenario_name}_{int(time.time())}"

            data0 = scenario_dir / "data_point_0.csv"
            write_rows_csv(data0, start_id=1, row_count=initial_rows, payload_bytes=args.payload_bytes, seed=args.seed)
            (scenario_dir / "_setup.sql").write_text(
                setup_sql(db_name, data0.name, with_hash_index), encoding="utf-8"
            )
            lookup_key = max(1, initial_rows // 2)
            (scenario_dir / "lookup.sql").write_text(lookup_sql(db_name, lookup_key), encoding="utf-8")

            load_ms = run_load_only(
                runner,
                scenario_dir,
                "lookup.sql",
                skip_load=False,
                no_setup=False,
                checkpoint_mb=args.checkpoint_mb,
                show_output=args.show_runner_output,
            )
            avg_ms, median_ms, quantiles_ms, verified, timed_total_ms, _, _ = run_lookup(
                runner,
                scenario_dir,
                "lookup.sql",
                runs=args.runs,
                checkpoint_mb=args.checkpoint_mb,
                show_output=args.show_runner_output,
            )
            total_rows = initial_rows
            append_row(report_rows, scenario_name, 0, total_rows, initial_rows, "load_ms", load_ms, "ms", "OK")
            append_row(report_rows, scenario_name, 0, total_rows, initial_rows, "insert_ops_s",
                       initial_rows / max(1e-9, load_ms / 1000.0), "ops/s", "OK")
            append_row(report_rows, scenario_name, 0, total_rows, initial_rows, "query_ops_s",
                       lookup_ops_s(args.runs, timed_total_ms), "ops/s", verified)
            append_row(report_rows, scenario_name, 0, total_rows, initial_rows, "query_avg_ms", avg_ms, "ms", verified)
            append_row(report_rows, scenario_name, 0, total_rows, initial_rows, "query_median_ms", median_ms, "ms", verified)
            for p in QUANTILE_PCTS:
                append_row(report_rows, scenario_name, 0, total_rows, initial_rows, f"query_p{p}_ms",
                           quantiles_ms.get(p, float("nan")), "ms", verified)

            next_id = initial_rows + 1
            for point in range(1, args.points):
                delta_csv = scenario_dir / f"data_point_{point}.csv"
                write_rows_csv(delta_csv, start_id=next_id, row_count=args.additional_rows,
                               payload_bytes=args.payload_bytes, seed=args.seed + point)
                next_id += args.additional_rows
                total_rows += args.additional_rows
                lookup_key = max(1, total_rows // 2)
                (scenario_dir / f"append_{point}.sql").write_text(append_sql(db_name, delta_csv.name), encoding="utf-8")
                (scenario_dir / f"lookup_{point}.sql").write_text(lookup_sql(db_name, lookup_key), encoding="utf-8")

                append_ms = run_load_only(
                    runner,
                    scenario_dir,
                    f"append_{point}.sql",
                    skip_load=False,
                    no_setup=True,
                    checkpoint_mb=args.checkpoint_mb,
                    show_output=args.show_runner_output,
                )
                avg_ms, median_ms, quantiles_ms, verified, timed_total_ms, _, _ = run_lookup(
                    runner,
                    scenario_dir,
                    f"lookup_{point}.sql",
                    runs=args.runs,
                    checkpoint_mb=args.checkpoint_mb,
                    show_output=args.show_runner_output,

                )
                append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, "load_ms", append_ms, "ms", "OK")
                append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, "insert_ops_s",
                           args.additional_rows / max(1e-9, append_ms / 1000.0), "ops/s", "OK")
                append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, "query_ops_s",
                           lookup_ops_s(args.runs, timed_total_ms), "ops/s", verified)
                append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, "query_avg_ms", avg_ms, "ms", verified)
                append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, "query_median_ms", median_ms, "ms", verified)
                for p in QUANTILE_PCTS:
                    append_row(report_rows, scenario_name, point, total_rows, args.additional_rows, f"query_p{p}_ms",
                               quantiles_ms.get(p, float("nan")), "ms", verified)

        out_csv = output_dir / "benchmark_hash_index_memory_growth.csv"
        write_report(out_csv, report_rows)
        plot_report(report_rows, output_dir)
        print(f"Saved table: {out_csv}")
        print(f"Saved plot: {output_dir / 'benchmark_hash_index_memory_growth.png'}")
    finally:
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()