#!/usr/bin/env python3
import argparse
import random
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from benchlib import QUANTILE_PCTS
from benchlib import format_process_error
from benchlib import human_bytes
from benchlib import read_runner_csv
from benchlib import run_process as run_bench_process
from benchlib import speedup


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)


def estimate_rows(target_bytes: int, payload_bytes: int) -> int:
    # Approx CSV row size: id + comma + payload + newline.
    approx_row = 16 + 1 + payload_bytes + 1
    return max(1, target_bytes // approx_row)


def generate_csv(path: Path, rows: int, payload_bytes: int, seed: int) -> None:
    rng = random.Random(seed)
    payload_alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    payload = "".join(rng.choice(payload_alphabet) for _ in range(payload_bytes))
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for i in range(rows):
            f.write(f"{i + 1},{payload}\n")


def run_process(cmd: list[str], cwd: Path, timeout_sec: float, show_output: bool) -> str:
    timeout = None if timeout_sec <= 0 else timeout_sec
    return run_bench_process(cmd, cwd, suppress_output=not show_output, timeout_sec=timeout)


def read_result_csv(path: Path) -> tuple[float, float, dict[int, float], str, float]:
    stats = read_runner_csv(path)
    return stats.avg_ms, stats.median_ms, stats.quantiles_ms, stats.verified, stats.timed_total_ms


def prepare_scenario(path: Path, setup_sql: str, lookup_sql: str, csv_src: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "_setup.sql").write_text(setup_sql + "\n", encoding="utf-8")
    (path / "lookup.sql").write_text(lookup_sql + "\n", encoding="utf-8")
    # benchmark_runner tokenizes @load_csv on spaces — use a relative path inside the scenario dir.
    shutil.copy2(csv_src, path / "data.csv")


def load_only(
    runner: Path, scenario_dir: Path, timeout_sec: float, show_output: bool, checkpoint_mb: int
) -> float:
    cmd = [str(runner), "--file=lookup.sql", "--disk", "--load-only"]
    if checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    if show_output:
        cmd.append("--verbose")
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, timeout_sec, show_output)
    return (time.perf_counter() - t0) * 1000.0


def warm_lookup(
    runner: Path, scenario_dir: Path, runs: int, timeout_sec: float, show_output: bool
) -> tuple[float, float, dict[int, float], str, float, float]:
    out_csv = scenario_dir / "warm_lookup.csv"
    # Data was loaded by load_only() in a prior process; do not re-run sibling _setup.sql.
    cmd = [
        str(runner),
        "--file=lookup.sql",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
        f"--out={out_csv}",
    ]
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, timeout_sec, show_output)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    avg_ms, median_ms, quantiles_ms, status, timed_total_ms = read_result_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, quantiles_ms, status, wall_ms, overhead_ms


def cold_lookup(
    runner: Path, scenario_dir: Path, runs: int, timeout_sec: float, show_output: bool
) -> tuple[float, float, dict[int, float], str, float, float]:
    out_csv = scenario_dir / "cold_lookup.csv"
    cmd = [
        str(runner),
        "--file=lookup.sql",
        f"--runs={runs}",
        "--disk",
        "--skip-load",
        f"--out={out_csv}",
    ]
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, timeout_sec, show_output)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    avg_ms, median_ms, quantiles_ms, status, timed_total_ms = read_result_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, quantiles_ms, status, wall_ms, overhead_ms


def in_process_lookup(
    runner: Path,
    scenario_dir: Path,
    runs: int,
    timeout_sec: float,
    show_output: bool,
    checkpoint_mb: int,
) -> tuple[float, float, dict[int, float], str, float, float]:
    """Load sibling _setup.sql and run lookup in the same runner process."""
    out_csv = scenario_dir / "in_process_lookup.csv"
    cmd = [str(runner), "--file=lookup.sql", f"--runs={runs}", "--disk", f"--out={out_csv}"]
    if checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    if show_output:
        cmd.append("--verbose")
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, timeout_sec, show_output)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    avg_ms, median_ms, quantiles_ms, status, timed_total_ms = read_result_csv(out_csv)
    overhead_ms = max(0.0, wall_ms - timed_total_ms)
    return avg_ms, median_ms, quantiles_ms, status, wall_ms, overhead_ms


def format_quantile_summary(quantiles_ms: dict[int, float]) -> str:
    return " ".join(f"p{p}={quantiles_ms.get(p, float('nan')):.3f}" for p in QUANTILE_PCTS)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simple benchmark for hash index on large (optionally RAM-exceeding) datasets."
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner.")
    parser.add_argument("--rows", type=int, default=0, help="Rows count (overrides --target-gb if > 0).")
    parser.add_argument("--target-gb", type=float, default=1.0, help="Approximate CSV size target in GiB.")
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=65536,
        help="Payload size per row (default 64 KiB: ~16k rows for 1 GiB vs ~1M at 1 KiB).",
    )
    parser.add_argument("--warm-runs", type=int, default=30, help="Point lookups per warm scenario run.")
    parser.add_argument("--cold-runs", type=int, default=10, help="Point lookups per cold restart run.")
    parser.add_argument("--seed", type=int, default=1234567, help="Random seed.")
    parser.add_argument("--timeout-sec", type=float, default=3600.0, help="Timeout per runner invocation.")
    parser.add_argument("--workspace", default="", help="Workspace dir (default: temp dir).")
    parser.add_argument(
        "--keep-workspace-on-failure",
        action="store_true",
        help="Do not delete workspace if benchmark fails.",
    )
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Quick sanity check: load+lookup in one runner process per scenario (no --skip-load).",
    )
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
        die(f"runner does not exist: {runner}")

    if args.rows > 0:
        rows = args.rows
    else:
        target_bytes = int(args.target_gb * (1024**3))
        rows = estimate_rows(target_bytes, args.payload_bytes)

    workspace = (
        Path(args.workspace).resolve()
        if args.workspace
        else Path(tempfile.gettempdir()) / f"otterbrix_hash_index_large_{int(time.time())}"
    )
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    success = False
    try:
        csv_path = workspace / "data.csv"
        print(f"Generating data: rows={rows}, payload_bytes={args.payload_bytes}")
        generate_csv(csv_path, rows, args.payload_bytes, args.seed)
        csv_size = csv_path.stat().st_size
        print(f"Dataset file: {csv_path} ({human_bytes(csv_size)})")
        print("Tip: set --target-gb above RAM size to force out-of-memory working set.")

        db_name = f"benchdb_{int(time.time())}"
        lookup_key = random.Random(args.seed + 1).randrange(1, rows + 1)
        lookup_sql = (
            f"-- @expected_rows 1\n"
            f"SELECT COUNT(*) FROM {db_name}.kv WHERE id = {lookup_key};"
        )
        setup_common = (
            f"-- @database {db_name}\n"
            "CREATE TABLE kv (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
            "-- @load_csv data.csv kv ,"
        )

        scenarios = {
            "no_index": setup_common,
            "hash_index": setup_common + f"\nCREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);",
        }

        scenario_dirs: dict[str, Path] = {}
        for name, setup_sql in scenarios.items():
            d = workspace / f"scenario_{name}"
            prepare_scenario(d, setup_sql, lookup_sql, csv_path)
            scenario_dirs[name] = d

        if args.smoke:
            print("\nSmoke test (load + lookup in one process per scenario)...")
            smoke: dict[str, tuple[float, float, dict[int, float], str, float, float]] = {}
            for name in ("no_index", "hash_index"):
                print(f"  smoke {name}...")
                try:
                    smoke[name] = in_process_lookup(
                        runner,
                        scenario_dirs[name],
                        max(1, args.warm_runs),
                        args.timeout_sec,
                        args.show_runner_output,
                        args.checkpoint_mb,
                    )
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    die(format_process_error(exc, name))
            print("\n=== Smoke Summary ===")
            print("scenario    wall_ms  avg_ms  median_ms  status")
            all_ok = True
            for name in ("no_index", "hash_index"):
                avg, med, quantiles, status, wall, _ = smoke[name]
                if status != "OK":
                    all_ok = False
                print(
                    f"{name:<10} {wall:>8.2f} {avg:>8.3f} {med:>10.3f} {status:>7}  "
                    f"{format_quantile_summary(quantiles)}"
                )
            print(f"\nWarm avg speedup (hash/no-index): {speedup(smoke['no_index'][0], smoke['hash_index'][0])}")
            if not all_ok:
                die("Smoke test failed verification.")
            success = True
            return

        print("\nLoading data for each scenario...")
        load_ms: dict[str, float] = {}
        for name in ("no_index", "hash_index"):
            print(f"  load {name}...")
            try:
                load_ms[name] = load_only(
                    runner, scenario_dirs[name], args.timeout_sec, args.show_runner_output, args.checkpoint_mb
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                details = format_process_error(exc, name)
                raise RuntimeError(
                    f"Load stage failed.\n{details}\n\n"
                    f"Workspace kept at: {workspace}\n"
                    "Try smaller dataset (--target-gb/--rows) or smaller --payload-bytes."
                ) from exc

        print("\nWarm lookup benchmark (normal run)...")
        warm: dict[str, tuple[float, float, dict[int, float], str, float, float]] = {}
        for name in ("no_index", "hash_index"):
            print(f"  warm {name}...")
            warm[name] = warm_lookup(
                runner, scenario_dirs[name], args.warm_runs, args.timeout_sec, args.show_runner_output
            )

        print("\nCold restart lookup benchmark (--skip-load)...")
        cold: dict[str, tuple[float, float, dict[int, float], str, float, float]] = {}
        for name in ("no_index", "hash_index"):
            print(f"  cold {name}...")
            cold[name] = cold_lookup(
                runner, scenario_dirs[name], args.cold_runs, args.timeout_sec, args.show_runner_output
            )

        print("\n=== Summary ===")
        print("scenario    load_ms    warm_avg_ms  warm_med_ms  cold_avg_ms  cold_med_ms  status")
        all_ok = True
        for name in ("no_index", "hash_index"):
            w_avg, w_med, w_quantiles, w_status, _, _ = warm[name]
            c_avg, c_med, c_quantiles, c_status, _, _ = cold[name]
            status = "OK" if w_status == "OK" and c_status == "OK" else "FAIL"
            if status != "OK":
                all_ok = False
            print(
                f"{name:<10} {load_ms[name]:>9.2f} {w_avg:>12.3f} {w_med:>12.3f} {c_avg:>12.3f} {c_med:>12.3f} {status:>7}"
            )
            print(f"  warm quantiles: {format_quantile_summary(w_quantiles)}")
            print(f"  cold quantiles: {format_quantile_summary(c_quantiles)}")

        print("\n=== Hash Index Gain ===")
        print(f"Warm avg speedup: {speedup(warm['no_index'][0], warm['hash_index'][0])}")
        print(f"Cold avg speedup: {speedup(cold['no_index'][0], cold['hash_index'][0])}")
        print(f"Load-time overhead (hash/no-index): {speedup(load_ms['hash_index'], load_ms['no_index'])}")
        if not all_ok:
            die(
                "One or more scenarios failed verification. "
                "If warm/cold fail after load, check for runner segfault on "
                "point lookups with --skip-load (known engine issue)."
            )
        success = True
    finally:
        if success or not args.keep_workspace_on_failure:
            shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    main()
