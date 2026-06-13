#!/usr/bin/env python3
import argparse
import shutil
from pathlib import Path

from benchlib import STANDARD_SCENARIO_NAMES
from benchlib import QueryMetrics
from benchlib import choose_lookup_key
from benchlib import generate_csv
from benchlib import human_size
from benchlib import lookup_sql
from benchlib import make_bench_db_name
from benchlib import measure_lookup
from benchlib import quantile_csv_headers
from benchlib import write_standard_lookup_scenarios

QUANTILE_PCTS = (50, 75, 90, 95, 98, 99)

SCENARIO_NAMES = STANDARD_SCENARIO_NAMES


def csv_header() -> str:
    quantile_cols = ",".join(quantile_csv_headers())
    return f"scenario,avg_ms,median_ms,{quantile_cols},wall_ms,setup_overhead_ms,verified"


def format_result_row(name: str, metrics: QueryMetrics) -> str:
    quantiles = ",".join(f"{metrics.quantiles_ms.get(p, float('nan')):.3f}" for p in QUANTILE_PCTS)
    return (
        f"{name},{metrics.avg_ms:.3f},{metrics.median_ms:.3f},{quantiles},"
        f"{metrics.wall_ms:.3f},{metrics.overhead_ms:.3f},{metrics.verified}"
    )


def run_key_lookup_benchmark(
    runner: Path,
    workspace: Path,
    *,
    rows: int,
    payload_bytes: int = 512,
    key_bytes: int = 0,
    runs: int = 7,
    shuffle_ids: bool = False,
    seed: int = 1234567,
    checkpoint_mb: int = 0,
    suppress_output: bool = True,
    cleanup: bool = True,
) -> dict[str, QueryMetrics]:
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    try:
        csv_path = workspace / "data.csv"
        generate_csv(csv_path, rows, payload_bytes, shuffle_ids, key_bytes)
        print(f"Dataset: rows={rows}, payload_bytes={payload_bytes}, file={csv_path} ({human_size(csv_path)})")

        db_name = make_bench_db_name()
        id_type = "STRING" if key_bytes > 0 else "INTEGER"
        key_value = choose_lookup_key(rows, seed, key_bytes)
        query_sql = lookup_sql(db_name, key_value, key_is_string=key_bytes > 0)
        scenario_dirs = write_standard_lookup_scenarios(
            workspace,
            db_name,
            csv_path,
            query_sql,
            id_type=id_type,
            storage_disk=False,
        )

        results: dict[str, QueryMetrics] = {}
        for name in SCENARIO_NAMES:
            results[name] = measure_lookup(
                runner,
                scenario_dirs[name],
                query_file="lookup.sql",
                runs=runs,
                out_name="result.csv",
                checkpoint_mb=checkpoint_mb,
                suppress_output=suppress_output,
            )
            if results[name].verified != "OK":
                raise RuntimeError(f"Scenario {name} failed verification")
        return results
    finally:
        if cleanup:
            shutil.rmtree(workspace, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Benchmark key lookup with and without index.")
    parser.add_argument("--runner", default="", help="Path to benchmark_runner binary.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_key_lookup_bench", help="Workspace path.")
    parser.add_argument("--rows", type=int, default=0, help="Number of rows.")
    parser.add_argument("--payload-bytes", type=int, default=512, help="Payload size in bytes.")
    parser.add_argument("--key-bytes", type=int, default=0, help="Key size in bytes (0 = INTEGER keys).")
    parser.add_argument("--runs", type=int, default=7, help="Timed runs per scenario.")
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

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    workspace = Path(args.workspace)
    print()
    print(f"Running benchmark scenarios with {args.runs} timed runs each...")
    print(f"Workload lookups per scenario file: 1 (shuffle_ids={1 if args.shuffle_ids else 0})")
    print(csv_header())

    results = run_key_lookup_benchmark(
        runner,
        workspace,
        rows=args.rows,
        payload_bytes=args.payload_bytes,
        key_bytes=args.key_bytes,
        runs=args.runs,
        shuffle_ids=args.shuffle_ids,
        seed=args.seed,
        checkpoint_mb=args.checkpoint_mb,
        suppress_output=not args.show_runner_output,
        cleanup=True,
    )
    for name in SCENARIO_NAMES:
        print(format_result_row(name, results[name]))


if __name__ == "__main__":
    main()
