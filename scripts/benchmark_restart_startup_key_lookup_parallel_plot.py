#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import tempfile
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from pathlib import Path

from benchlib import RestartMetrics
from benchlib import logspace_int

from benchmark_restart_startup_key_lookup import QUANTILE_PCTS
from benchmark_restart_startup_key_lookup import SCENARIO_NAMES
from benchmark_restart_startup_key_lookup import run_restart_startup_benchmark

RESTART_PLOT_METRICS = (
    ("load_shutdown_ms", "Load + Shutdown (ms)", "load_shutdown_by_index.png"),
    ("restart_avg_ms", "Restart Avg Query Time (ms)", "restart_avg_by_index.png"),
    ("restart_median_ms", "Restart Median Query Time (ms)", "restart_median_by_index.png"),
    ("restart_wall_ms", "Restart Wall Time (ms)", "restart_wall_by_index.png"),
    ("startup_overhead_ms", "Startup Overhead (ms)", "startup_overhead_by_index.png"),
)


def metric_value(metrics: RestartMetrics, key: str) -> float:
    if key == "load_shutdown_ms":
        return metrics.load_shutdown_ms
    if key == "restart_avg_ms":
        return metrics.restart_avg_ms
    if key == "restart_median_ms":
        return metrics.restart_median_ms
    if key == "restart_wall_ms":
        return metrics.restart_wall_ms
    if key == "startup_overhead_ms":
        return metrics.startup_overhead_ms
    raise KeyError(key)


def metrics_to_row(rows: int, scenario: str, metrics: RestartMetrics) -> list[str]:
    return [
        str(rows),
        scenario,
        f"{metrics.load_shutdown_ms:.6f}",
        f"{metrics.restart_avg_ms:.6f}",
        f"{metrics.restart_median_ms:.6f}",
        *[f"{metrics.restart_quantiles_ms.get(p, float('nan')):.6f}" for p in QUANTILE_PCTS],
        f"{metrics.restart_wall_ms:.6f}",
        f"{metrics.startup_overhead_ms:.6f}",
    ]


def save_table(out_csv: Path, row_points: list[int], all_metrics: dict[int, dict[str, RestartMetrics]]) -> None:
    quantile_headers = [f"p{p}_ms" for p in QUANTILE_PCTS]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rows",
                "scenario",
                "load_shutdown_ms",
                "restart_avg_ms",
                "restart_median_ms",
                *quantile_headers,
                "restart_wall_ms",
                "startup_overhead_ms",
            ]
        )
        for rows in row_points:
            for scenario in SCENARIO_NAMES:
                writer.writerow(metrics_to_row(rows, scenario, all_metrics[rows][scenario]))


def plot_results(row_points: list[int], all_metrics: dict[int, dict[str, RestartMetrics]], out_dir: Path) -> None:
    import matplotlib.pyplot as plt

    for metric_key, metric_title, filename in RESTART_PLOT_METRICS:
        plt.figure(figsize=(10, 6))
        for scenario in SCENARIO_NAMES:
            ys = [metric_value(all_metrics[r][scenario], metric_key) for r in row_points]
            plt.plot(row_points, ys, marker="o", label=scenario)
        plt.xscale("log")
        plt.xlabel("rows")
        plt.ylabel(metric_title)
        plt.title(metric_title)
        plt.grid(True, which="both", linestyle=":", alpha=0.6)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / filename, dpi=150)
        plt.close()

    for p in QUANTILE_PCTS:
        plt.figure(figsize=(10, 6))
        for scenario in SCENARIO_NAMES:
            ys = [all_metrics[r][scenario].restart_quantiles_ms.get(p, float("nan")) for r in row_points]
            plt.plot(row_points, ys, marker="o", label=scenario)
        plt.xscale("log")
        plt.xlabel("rows")
        plt.ylabel(f"p{p} restart query time (ms)")
        plt.title(f"Restart p{p} Query Time by Index Type")
        plt.grid(True, which="both", linestyle=":", alpha=0.6)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / f"restart_p{p}_by_index.png", dpi=150)
        plt.close()

    fig, axes = plt.subplots(3, 2, figsize=(14, 14))
    for i, (metric_key, metric_title, _) in enumerate(RESTART_PLOT_METRICS):
        ax = axes[i // 2][i % 2]
        for scenario in SCENARIO_NAMES:
            ys = [metric_value(all_metrics[r][scenario], metric_key) for r in row_points]
            ax.plot(row_points, ys, marker="o", label=scenario)
        ax.set_xscale("log")
        ax.set_xlabel("rows")
        ax.set_ylabel("ms")
        ax.set_title(metric_title)
        ax.grid(True, which="both", linestyle=":", alpha=0.6)
        ax.legend()
    axes[2][1].axis("off")
    fig.tight_layout()
    fig.savefig(out_dir / "restart_startup_all_metrics_grid.png", dpi=150)
    plt.close(fig)


def run_one(
    runner: Path,
    rows: int,
    payload_bytes: int,
    runs: int,
    shuffle_ids: bool,
    checkpoint_mb: int,
) -> tuple[int, dict[str, RestartMetrics]]:
    workspace = Path(tempfile.gettempdir()) / f"otterbrix_restart_startup_{rows}"
    metrics = run_restart_startup_benchmark(
        runner,
        workspace,
        rows=rows,
        payload_bytes=payload_bytes,
        runs=runs,
        shuffle_ids=shuffle_ids,
        seed=1234567 + rows,
        checkpoint_mb=checkpoint_mb,
        suppress_output=True,
        cleanup=True,
    )
    return rows, metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Run parallel restart startup sweep and build plots.")
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--runs", type=int, default=50)
    parser.add_argument("--points", type=int, default=10, help="Number of row points in range.")
    parser.add_argument("--rows-min", type=int, default=100)
    parser.add_argument("--rows-max", type=int, default=500000)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--shuffle-ids", action="store_true")
    parser.add_argument("--output-dir", default="benchmark_plots_restart")
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

    all_metrics: dict[int, dict[str, RestartMetrics]] = {}
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
            )
            for rows in row_points
        ]
        for future in as_completed(futures):
            rows, metrics = future.result()
            all_metrics[rows] = metrics
            print(f"Completed rows={rows}")

    out_csv = output_dir / "benchmark_restart_startup_index_comparison.csv"
    save_table(out_csv, row_points, all_metrics)
    plot_results(row_points, all_metrics, output_dir)
    print(f"Saved table: {out_csv}")


if __name__ == "__main__":
    main()
