#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import random
import subprocess
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

QUANTILE_PCTS = (50, 75, 90, 95, 98, 99)
STANDARD_SCENARIO_NAMES = ("no_index", "single_field_index", "hash_single_field_index")


def quantile_field(p: int) -> str:
    return f"p{p}_ms"


def quantile_csv_headers() -> list[str]:
    return [quantile_field(p) for p in QUANTILE_PCTS]


def read_quantiles_from_row(row: dict[str, str]) -> dict[int, float]:
    quantiles: dict[int, float] = {}
    for p in QUANTILE_PCTS:
        key = quantile_field(p)
        if key in row and row[key]:
            quantiles[p] = float(row[key])
        elif p == 50 and "median_ms" in row:
            quantiles[p] = float(row["median_ms"])
    return quantiles


def aggregate_quantiles(rows: list[dict[str, str]]) -> dict[int, float]:
    quantiles: dict[int, float] = {}
    for p in QUANTILE_PCTS:
        values = [read_quantiles_from_row(row).get(p) for row in rows]
        values = [v for v in values if v is not None]
        if values:
            quantiles[p] = sum(values) / len(values)
    return quantiles


def format_quantile_values(quantiles_ms: dict[int, float]) -> list[str]:
    return [f"{quantiles_ms.get(p, float('nan')):.3f}" for p in QUANTILE_PCTS]


def quantile_metric_names(prefix: str = "") -> list[str]:
    return [f"{prefix}{quantile_field(p)}" for p in QUANTILE_PCTS]


def timing_quantile_pairs(quantiles_ms: dict[int, float], prefix: str = "") -> list[tuple[str, float]]:
    return [(f"{prefix}p{p}_ms", quantiles_ms.get(p, float("nan"))) for p in QUANTILE_PCTS]


@dataclass
class RunnerStats:
    avg_ms: float
    median_ms: float
    quantiles_ms: dict[int, float] = field(default_factory=dict)
    timed_total_ms: float = 0.0
    verified: str = "FAIL"


@dataclass
class QueryMetrics:
    avg_ms: float
    median_ms: float
    quantiles_ms: dict[int, float] = field(default_factory=dict)
    wall_ms: float = 0.0
    overhead_ms: float = 0.0
    verified: str = "FAIL"


@dataclass
class RestartMetrics:
    load_shutdown_ms: float
    restart_avg_ms: float
    restart_median_ms: float
    restart_quantiles_ms: dict[int, float] = field(default_factory=dict)
    restart_wall_ms: float = 0.0
    startup_overhead_ms: float = 0.0
    verified: str = "FAIL"


def die(msg: str) -> None:
    raise RuntimeError(msg)


def format_key(row_id: int, key_bytes: int = 0) -> str:
    if key_bytes <= 0:
        return str(row_id)
    base = f"k{row_id:020d}"
    if len(base) >= key_bytes:
        return base[:key_bytes]
    return base + ("x" * (key_bytes - len(base)))


def human_size(path: Path) -> str:
    size = path.stat().st_size
    value = float(size)
    units = ("B", "K", "M", "G", "T")
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{int(value)}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= 1024
    return f"{size}B"


def human_bytes(size: int, *, precision: int = 2, space: bool = True) -> str:
    value = float(size)
    units = ("B", "KB", "MB", "GB", "TB")
    sep = " " if space else ""
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{int(value)}{sep}{unit}" if unit == "B" else f"{value:.{precision}f}{sep}{unit}"
        value /= 1024.0
    return f"{size}{sep}B"


def mem_total_bytes() -> int:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return 0
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            parts = line.split()
            return int(parts[1]) * 1024
    return 0


def format_process_error(exc: Exception, scenario_name: str) -> str:
    if isinstance(exc, subprocess.TimeoutExpired):
        return f"[{scenario_name}] timeout after {exc.timeout} sec\nCommand: {' '.join(exc.cmd)}"

    if isinstance(exc, subprocess.CalledProcessError):
        signal_suffix = f" (signal {-exc.returncode})" if exc.returncode < 0 else ""
        message = f"[{scenario_name}] command failed with exit code {exc.returncode}{signal_suffix}\nCommand: {' '.join(exc.cmd)}"
        if exc.output:
            message += "\n--- runner output tail ---\n" + "\n".join(exc.output.splitlines()[-80:])
        return message

    return f"[{scenario_name}] unexpected error: {exc}"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_dict_rows_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def make_bench_db_name(prefix: str = "benchdb") -> str:
    return f"{prefix}_{int(time.time())}_{os.getpid()}"


def choose_lookup_key(rows: int, seed: int, key_bytes: int = 0) -> str:
    key = random.Random(seed).randrange(1, rows + 1)
    return format_key(key, key_bytes)


def lookup_sql(db_name: str, key_value: str, *, key_is_string: bool = False, select_expr: str = "*") -> str:
    literal = f"'{key_value}'" if key_is_string else key_value
    return f"-- @expected_rows 1\nSELECT {select_expr} FROM {db_name}.kv WHERE id = {literal};\n"


def standard_setup_sql(db_name: str, csv_path: Path, *, id_type: str = "INTEGER", storage_disk: bool = False) -> str:
    storage_clause = " WITH (storage = 'disk')" if storage_disk else ""
    return (
        f"-- @database {db_name}\n"
        f"CREATE TABLE kv (id {id_type}, payload STRING){storage_clause};\n"
        f"-- @load_csv {csv_path} kv ,"
    )


def standard_index_setups(db_name: str, base_setup_sql: str) -> dict[str, str]:
    return {
        "no_index": base_setup_sql,
        "single_field_index": base_setup_sql + f"\nCREATE INDEX idx_id ON {db_name}.kv (id);",
        "hash_single_field_index": base_setup_sql + f"\nCREATE INDEX idx_id_hash ON {db_name}.kv USING hash (id);",
    }


def write_standard_lookup_scenarios(
    workspace: Path,
    db_name: str,
    csv_path: Path,
    query_sql: str,
    *,
    id_type: str = "INTEGER",
    storage_disk: bool = False,
) -> dict[str, Path]:
    setups = standard_index_setups(db_name, standard_setup_sql(db_name, csv_path, id_type=id_type, storage_disk=storage_disk))
    scenario_dirs: dict[str, Path] = {}
    for name in STANDARD_SCENARIO_NAMES:
        scenario_dir = workspace / f"scenario_{name}"
        write_scenario(scenario_dir, setups[name], "lookup.sql", query_sql)
        scenario_dirs[name] = scenario_dir
    return scenario_dirs


def generate_csv(csv_path: Path,
                 rows: int,
                 payload_bytes: int,
                 shuffle_ids: bool = False,
                 key_bytes: int = 0) -> None:
    if rows <= 0:
        die("--rows must be > 0")
    payload = "x" * payload_bytes
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        if shuffle_ids:
            step = rows - 1
            for i in range(rows):
                row_id = ((i * step) % rows) + 1
                f.write(f"{format_key(row_id, key_bytes)},{payload}\n")
        else:
            for i in range(rows):
                f.write(f"{format_key(i + 1, key_bytes)},{payload}\n")


def write_scenario(path: Path, setup_sql: str, query_filename: str, query_sql: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "_setup.sql").write_text(setup_sql.strip() + "\n", encoding="utf-8")
    (path / query_filename).write_text(query_sql.strip() + "\n", encoding="utf-8")


def run_process(args: list[str],
                cwd: Path,
                suppress_output: bool = True,
                timeout_sec: float | None = None) -> str:
    proc = subprocess.run(
        args,
        cwd=str(cwd),
        check=False,
        stdout=subprocess.PIPE if suppress_output else None,
        stderr=subprocess.STDOUT if suppress_output else None,
        text=True,
        timeout=timeout_sec,
    )
    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, args, output=proc.stdout)
    return proc.stdout or ""


def read_runner_csv(csv_path: Path) -> RunnerStats:
    rows: list[dict[str, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    if not rows:
        die(f"cannot parse benchmark output: {csv_path}")
    avg_ms = sum(float(r["avg_ms"]) for r in rows) / len(rows)
    median_ms = sum(float(r["median_ms"]) for r in rows) / len(rows)
    timed_total_ms = sum(float(r["avg_ms"]) * float(r["nruns"]) for r in rows)
    verified = "OK" if all(r["verified"] == "OK" for r in rows) else "FAIL"
    return RunnerStats(
        avg_ms=avg_ms,
        median_ms=median_ms,
        quantiles_ms=aggregate_quantiles(rows),
        timed_total_ms=timed_total_ms,
        verified=verified,
    )


def runner_cmd(runner: Path,
               query_file: str,
               *,
               runs: int | None = None,
               out_csv: Path | None = None,
               disk: bool = True,
               load_only: bool = False,
               skip_load: bool = False,
               checkpoint_mb: int = 0) -> list[str]:
    cmd = [str(runner), f"--file={query_file}"]
    if runs is not None:
        cmd.append(f"--runs={runs}")
    if disk:
        cmd.append("--disk")
    if out_csv is not None:
        cmd.append(f"--out={out_csv}")
    if load_only:
        cmd.append("--load-only")
    if skip_load:
        cmd.append("--skip-load")
    if checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    return cmd


def measure_lookup(runner: Path,
                   scenario_dir: Path,
                   *,
                   query_file: str = "lookup.sql",
                   runs: int = 10,
                   out_name: str = "result.csv",
                   checkpoint_mb: int = 0,
                   skip_load: bool = False,
                   suppress_output: bool = True) -> QueryMetrics:
    out_csv = scenario_dir / out_name
    cmd = runner_cmd(runner,
                     query_file,
                     runs=runs,
                     out_csv=out_csv,
                     disk=True,
                     load_only=False,
                     skip_load=skip_load,
                     checkpoint_mb=checkpoint_mb)
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output=suppress_output)
    wall_ms = (time.perf_counter() - t0) * 1000.0
    stats = read_runner_csv(out_csv)
    overhead = max(0.0, wall_ms - stats.timed_total_ms)
    return QueryMetrics(avg_ms=stats.avg_ms,
                        median_ms=stats.median_ms,
                        quantiles_ms=stats.quantiles_ms,
                        wall_ms=wall_ms,
                        overhead_ms=overhead,
                        verified=stats.verified)


def measure_load_only(runner: Path,
                      scenario_dir: Path,
                      *,
                      query_file: str = "lookup.sql",
                      checkpoint_mb: int = 0,
                      suppress_output: bool = True) -> float:
    cmd = runner_cmd(runner,
                     query_file,
                     disk=True,
                     load_only=True,
                     skip_load=False,
                     checkpoint_mb=checkpoint_mb)
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output=suppress_output)
    return (time.perf_counter() - t0) * 1000.0


def measure_restart(runner: Path,
                    scenario_dir: Path,
                    *,
                    query_file: str = "lookup.sql",
                    restart_runs: int = 7,
                    restart_out_name: str = "restart_result.csv",
                    checkpoint_mb: int = 0,
                    suppress_output: bool = True) -> RestartMetrics:
    load_shutdown_ms = measure_load_only(
        runner,
        scenario_dir,
        query_file=query_file,
        checkpoint_mb=checkpoint_mb,
        suppress_output=suppress_output,
    )
    out_csv = scenario_dir / restart_out_name
    cmd = runner_cmd(runner,
                     query_file,
                     runs=restart_runs,
                     out_csv=out_csv,
                     disk=True,
                     skip_load=True)
    t0 = time.perf_counter()
    run_process(cmd, scenario_dir, suppress_output=suppress_output)
    restart_wall_ms = (time.perf_counter() - t0) * 1000.0
    stats = read_runner_csv(out_csv)
    startup_overhead_ms = max(0.0, restart_wall_ms - stats.timed_total_ms)
    return RestartMetrics(
        load_shutdown_ms=load_shutdown_ms,
        restart_avg_ms=stats.avg_ms,
        restart_median_ms=stats.median_ms,
        restart_quantiles_ms=stats.quantiles_ms,
        restart_wall_ms=restart_wall_ms,
        startup_overhead_ms=startup_overhead_ms,
        verified=stats.verified,
    )


def speedup(base: float, cur: float) -> str:
    if cur <= 0.0:
        return "n/a"
    return f"{base / cur:.2f}x"


def logspace_int(start: int, stop: int, count: int) -> list[int]:
    import math
    if count < 2:
        return [start]
    values = []
    a = math.log10(start)
    b = math.log10(stop)
    for i in range(count):
        t = i / (count - 1)
        v = int(round(10 ** (a + (b - a) * t)))
        values.append(max(start, min(stop, v)))
    dedup = sorted(set(values))
    if len(dedup) != count:
        step = (stop - start) / max(1, count - 1)
        dedup = sorted({int(round(start + i * step)) for i in range(count)})
    return dedup
