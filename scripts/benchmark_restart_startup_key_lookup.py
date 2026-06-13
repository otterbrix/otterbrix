#!/usr/bin/env python3
import argparse
import shutil
import time
from pathlib import Path

from benchlib import STANDARD_SCENARIO_NAMES
from benchlib import RestartMetrics
from benchlib import choose_lookup_key
from benchlib import generate_csv
from benchlib import human_size
from benchlib import lookup_sql
from benchlib import make_bench_db_name
from benchlib import measure_restart
from benchlib import write_standard_lookup_scenarios

QUANTILE_PCTS = (50, 75, 90, 95, 98, 99)
SCENARIO_NAMES = STANDARD_SCENARIO_NAMES


def print_table_header() -> None:
    quantile_cols = [f"restart p{p}" for p in QUANTILE_PCTS]
    cols = [
        ("scenario", 24),
        ("load+shutdown", 14),
        ("restart avg", 12),
        ("restart median", 14),
        *[(name, 12) for name in quantile_cols],
        ("restart wall", 13),
        ("startup overhead", 16),
        ("status", 8),
    ]
    line = " | ".join(name.ljust(width) for name, width in cols)
    sep = "-+-".join("-" * width for _, width in cols)
    print(line)
    print(sep)


def print_table_row(name: str, metrics: RestartMetrics) -> None:
    quantile_parts = " | ".join(
        f"{metrics.restart_quantiles_ms.get(p, float('nan')):>9.3f} ms" for p in QUANTILE_PCTS
    )
    print(
        f"{name:<24} | "
        f"{metrics.load_shutdown_ms:>11.3f} ms | "
        f"{metrics.restart_avg_ms:>9.3f} ms | "
        f"{metrics.restart_median_ms:>11.3f} ms | "
        f"{quantile_parts} | "
        f"{metrics.restart_wall_ms:>10.3f} ms | "
        f"{metrics.startup_overhead_ms:>13.3f} ms | "
        f"{metrics.verified:<8}"
    )


def run_restart_startup_benchmark(
    runner: Path,
    workspace: Path,
    *,
    rows: int,
    payload_bytes: int = 512,
    runs: int = 7,
    shuffle_ids: bool = False,
    seed: int = 1234567,
    checkpoint_mb: int = 0,
    suppress_output: bool = True,
    cleanup: bool = True,
) -> dict[str, RestartMetrics]:
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    try:
        csv_path = workspace / "data.csv"
        gen_start = time.perf_counter()
        generate_csv(csv_path, rows, payload_bytes, shuffle_ids)
        gen_ms = (time.perf_counter() - gen_start) * 1000.0
        print(
            f"Dataset file: {csv_path} ({human_size(csv_path)}), "
            f"rows={rows}, payload_bytes={payload_bytes}, generated in {gen_ms:.1f} ms"
        )

        db_name = make_bench_db_name()
        query_sql = lookup_sql(db_name, choose_lookup_key(rows, seed))
        scenario_dirs = write_standard_lookup_scenarios(
            workspace,
            db_name,
            csv_path,
            query_sql,
            storage_disk=True,
        )

        results: dict[str, RestartMetrics] = {}
        for name in SCENARIO_NAMES:
            results[name] = measure_restart(
                runner,
                scenario_dirs[name],
                query_file="lookup.sql",
                restart_runs=runs,
                restart_out_name="restart_result.csv",
                checkpoint_mb=checkpoint_mb,
                suppress_output=suppress_output,
            )
            if results[name].verified != "OK":
                raise RuntimeError(f"Scenario {name} failed verification")
        return results
    finally:
        if cleanup:
            shutil.rmtree(workspace, ignore_errors=True)


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
    parser.add_argument(
        "--checkpoint-mb",
        type=int,
        default=0,
        metavar="N",
        help="Forward --csv-checkpoint-mb=N to benchmark_runner during load (0=off, periodic CHECKPOINT every N MiB).",
    )
    args = parser.parse_args()

    if not args.runner:
        raise RuntimeError("benchmark_runner not found. Use --runner PATH.")
    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    workspace = Path(args.workspace)
    print(f"Generating dataset: rows={args.rows}, payload_bytes={args.payload_bytes}")

    print()
    print("Running restart startup benchmark: phase1(load+shutdown) -> phase2(restart with --skip-load)...")
    print_table_header()

    results = run_restart_startup_benchmark(
        runner,
        workspace,
        rows=args.rows,
        payload_bytes=args.payload_bytes,
        runs=args.runs,
        shuffle_ids=args.shuffle_ids,
        seed=args.seed,
        checkpoint_mb=args.checkpoint_mb,
        suppress_output=not args.show_runner_output,
        cleanup=True,
    )
    for name in SCENARIO_NAMES:
        print_table_row(name, results[name])


if __name__ == "__main__":
    main()
