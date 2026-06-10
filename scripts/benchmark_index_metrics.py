#!/usr/bin/env python3
import argparse
import shutil
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import STANDARD_SCENARIO_NAMES
from benchlib import choose_lookup_key
from benchlib import generate_csv
from benchlib import lookup_sql
from benchlib import make_bench_db_name
from benchlib import measure_lookup
from benchlib import measure_restart
from benchlib import speedup
from benchlib import write_standard_lookup_scenarios


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
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    try:
        csv_path = workspace / "data.csv"
        print(f"Generating dataset: rows={args.rows}, payload_bytes={args.payload_bytes}")
        generate_csv(csv_path, args.rows, args.payload_bytes, args.shuffle_ids)

        db_name = make_bench_db_name()
        scenario_dirs = write_standard_lookup_scenarios(
            workspace,
            db_name,
            csv_path,
            lookup_sql(db_name, choose_lookup_key(args.rows, args.seed)),
            storage_disk=True,
        )

        suppress_output = not args.show_runner_output

        lookup_metrics = {}
        restart_metrics = {}
        for name in STANDARD_SCENARIO_NAMES:
            lookup_metrics[name] = measure_lookup(
                runner,
                scenario_dirs[name],
                query_file="lookup.sql",
                runs=args.lookup_runs,
                out_name="lookup_result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )
            restart_metrics[name] = measure_restart(
                runner,
                scenario_dirs[name],
                query_file="lookup.sql",
                restart_runs=args.restart_runs,
                restart_out_name="restart_result.csv",
                checkpoint_mb=args.checkpoint_mb,
                suppress_output=suppress_output,
            )

        base_lookup = lookup_metrics["no_index"].avg_ms
        base_restart = restart_metrics["no_index"].restart_avg_ms

        print("\n=== Lookup Metrics ===")
        quantile_header = " ".join(f"{'p' + str(p):>8}" for p in QUANTILE_PCTS)
        print(
            "scenario                  avg_ms    median_ms   "
            f"{quantile_header}   wall_ms    query_ops_s   overhead_ms   speedup_vs_no_index   status"
        )
        for name in STANDARD_SCENARIO_NAMES:
            m = lookup_metrics[name]
            query_ops_s = args.lookup_runs / max(1e-9, m.wall_ms / 1000.0)
            quantile_vals = " ".join(
                f"{m.quantiles_ms.get(p, float('nan')):>8.3f}" for p in QUANTILE_PCTS
            )
            print(
                f"{name:<24} {m.avg_ms:>8.3f} {m.median_ms:>11.3f} {quantile_vals}"
                f" {m.wall_ms:>9.3f} {query_ops_s:>12.2f} {m.overhead_ms:>12.3f}"
                f" {speedup(base_lookup, m.avg_ms):>20} {m.verified:>8}"
            )

        print("\n=== Restart Startup Metrics ===")
        restart_quantile_header = " ".join(f"{'p' + str(p):>8}" for p in QUANTILE_PCTS)
        print(
            "scenario                  load+shutdown_ms  insert_ops_s  restart_avg_ms  restart_median_ms  "
            f"{restart_quantile_header}  restart_wall_ms  restart_query_ops_s  startup_overhead_ms  speedup_vs_no_index  status"
        )
        for name in STANDARD_SCENARIO_NAMES:
            m = restart_metrics[name]
            insert_ops_s = args.rows / max(1e-9, m.load_shutdown_ms / 1000.0)
            restart_query_ops_s = args.restart_runs / max(1e-9, m.restart_wall_ms / 1000.0)
            restart_quantile_vals = " ".join(
                f"{m.restart_quantiles_ms.get(p, float('nan')):>8.3f}" for p in QUANTILE_PCTS
            )
            print(
                f"{name:<24} {m.load_shutdown_ms:>16.3f} {insert_ops_s:>12.2f} {m.restart_avg_ms:>15.3f}"
                f" {m.restart_median_ms:>18.3f} {restart_quantile_vals}"
                f" {m.restart_wall_ms:>15.3f} {restart_query_ops_s:>18.2f}"
                f" {m.startup_overhead_ms:>20.3f}"
                f" {speedup(base_restart, m.restart_avg_ms):>19} {m.verified:>7}"
            )
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
