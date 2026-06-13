#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import tempfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from benchlib import QueryMetrics
from benchlib import logspace_int

from benchmark_select_key_lookup import QUANTILE_PCTS
from benchmark_select_key_lookup import SCENARIO_NAMES
from benchmark_select_key_lookup import run_key_lookup_benchmark

LOOKUP_SCENARIOS_2 = ("single_field_index", "hash_single_field_index")


def metrics_to_row(rows: int, scenario: str, metrics: QueryMetrics) -> list[str]:
    return [
        str(rows),
        scenario,
        f"{metrics.avg_ms:.6f}",
        f"{metrics.median_ms:.6f}",
        *[f"{metrics.quantiles_ms.get(p, float('nan')):.6f}" for p in QUANTILE_PCTS],
        f"{metrics.overhead_ms:.6f}",
    ]


def save_table(out_csv: Path, row_points: list[int], all_metrics: dict[int, dict[str, QueryMetrics]]) -> None:
    quantile_headers = [f"p{p}_ms" for p in QUANTILE_PCTS]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["rows", "scenario", "avg_ms", "median_ms", *quantile_headers, "setup_overhead_ms"])
        for rows in row_points:
            for scenario in SCENARIO_NAMES:
                writer.writerow(metrics_to_row(rows, scenario, all_metrics[rows][scenario]))


def plot_results(row_points: list[int], all_metrics: dict[int, dict[str, QueryMetrics]], out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    series_avg = {s: [all_metrics[r][s].avg_ms for r in row_points] for s in SCENARIO_NAMES}
    series_setup = {s: [all_metrics[r][s].overhead_ms for r in row_points] for s in SCENARIO_NAMES}

    plt.figure(figsize=(10, 6))
    for scenario in SCENARIO_NAMES:
        plt.plot(row_points, series_avg[scenario], marker="o", label=scenario)
    plt.xscale("log")
    plt.xlabel("rows")
    plt.ylabel("avg query time (ms)")
    plt.title("Average Query Time by Index Type")
    plt.grid(True, which="both", linestyle=":", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "avg_query_time_by_index.png", dpi=150)
    plt.close()

    plt.figure(figsize=(10, 6))
    for scenario in LOOKUP_SCENARIOS_2:
        plt.plot(row_points, series_avg[scenario], marker="o", label=scenario)
    plt.xscale("log")
    plt.xlabel("rows")
    plt.ylabel("avg query time (ms)")
    plt.title("Average Query Time by Index Type")
    plt.grid(True, which="both", linestyle=":", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "avg_query_time_by_index_2.png", dpi=150)
    plt.close()

    for p in QUANTILE_PCTS:
        plt.figure(figsize=(10, 6))
        for scenario in SCENARIO_NAMES:
            ys = [all_metrics[r][scenario].quantiles_ms.get(p, float("nan")) for r in row_points]
            plt.plot(row_points, ys, marker="o", label=scenario)
        plt.xscale("log")
        plt.xlabel("rows")
        plt.ylabel(f"p{p} query time (ms)")
        plt.title(f"p{p} Query Time by Index Type")
        plt.grid(True, which="both", linestyle=":", alpha=0.6)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / f"p{p}_query_time_by_index.png", dpi=150)
        plt.close()

    plt.figure(figsize=(10, 6))
    for scenario in SCENARIO_NAMES:
        plt.plot(row_points, series_setup[scenario], marker="o", label=scenario)
    plt.xscale("log")
    plt.xlabel("rows")
    plt.ylabel("setup overhead (ms)")
    plt.title("Setup Time by Index Type")
    plt.grid(True, which="both", linestyle=":", alpha=0.6)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_dir / "setup_time_by_index.png", dpi=150)
    plt.close()


def run_one(
    runner: Path,
    rows: int,
    payload_bytes: int,
    runs: int,
    shuffle_ids: bool,
    checkpoint_mb: int,
    key_bytes: int,
) -> tuple[int, dict[str, QueryMetrics]]:
    workspace = Path(tempfile.gettempdir()) / f"otterbrix_key_lookup_{rows}"
    metrics = run_key_lookup_benchmark(
        runner,
        workspace,
        rows=rows,
        payload_bytes=payload_bytes,
        key_bytes=key_bytes,
        runs=runs,
        shuffle_ids=shuffle_ids,
        seed=1234567 + rows,
        checkpoint_mb=checkpoint_mb,
        suppress_output=True,
        cleanup=True,
    )
    return rows, metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Run parallel select_key_lookup sweep and build plots.")
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--points", type=int, default=10, help="Number of row points in range.")
    parser.add_argument("--rows-min", type=int, default=100)
    parser.add_argument("--rows-max", type=int, default=500000)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--key-bytes", type=int, default=0)
    parser.add_argument("--shuffle-ids", action="store_true")
    parser.add_argument("--output-dir", default="benchmark_plots")
    parser.add_argument(
        "--checkpoint-mb",
        type=int,
        default=0,
        metavar="N",
        help="Forward --csv-checkpoint-mb=N to benchmark_runner during load (0=off).",
    )
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    row_points = logspace_int(args.rows_min, args.rows_max, args.points)
    if len(row_points) != args.points:
        raise RuntimeError(f"Failed to generate {args.points} unique row points: {row_points}")

    print(f"Rows: {row_points}")
    print(f"Parallel workers: {args.max_workers}")

    all_metrics: dict[int, dict[str, QueryMetrics]] = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = [
            pool.submit(
                run_one,
                runner,
                rows,
                args.payload_bytes,
                args.runs,
                args.shuffle_ids,
                args.checkpoint_mb,
                args.key_bytes,
            )
            for rows in row_points
        ]
        for future in as_completed(futures):
            rows, metrics = future.result()
            all_metrics[rows] = metrics
            print(f"Completed rows={rows}")

    out_csv = output_dir / "benchmark_index_comparison.csv"
    save_table(out_csv, row_points, all_metrics)
    plot_results(row_points, all_metrics, output_dir)
    print(f"Saved table: {out_csv}")


if __name__ == "__main__":
    main()
