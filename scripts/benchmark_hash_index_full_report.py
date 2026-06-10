#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from io import StringIO
from subprocess import TimeoutExpired
from typing import Any

from benchlib import QUANTILE_PCTS
from benchlib import mem_total_bytes
from benchlib import quantile_field
from benchlib import read_csv_rows
from benchlib import write_dict_rows_csv


@dataclass
class CmdResult:
    returncode: int
    stdout: str
    stderr: str


def ts() -> str:
    return time.strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}")


def run_cmd(cmd: list[str],
            cwd: Path,
            *,
            verbose: bool = False,
            label: str = "",
            timeout_sec: int = 0,
            heartbeat_sec: int = 60) -> CmdResult:
    if verbose:
        head = f"{label}: " if label else ""
        log(f"{head}running command: {' '.join(cmd)}")
        log(f"{head}cwd: {cwd}")
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    start = time.monotonic()
    next_heartbeat = start + max(5, heartbeat_sec)
    while True:
        try:
            stdout, stderr = proc.communicate(timeout=2)
            break
        except TimeoutExpired:
            now = time.monotonic()
            if timeout_sec > 0 and now - start >= timeout_sec:
                proc.kill()
                out, err = proc.communicate()
                msg = f"command timeout after {timeout_sec}s"
                if verbose:
                    head = f"{label}: " if label else ""
                    log(f"{head}{msg}")
                return CmdResult(
                    returncode=124,
                    stdout=out or "",
                    stderr=((err or "") + ("\n" if err else "") + msg),
                )
            if now >= next_heartbeat:
                head = f"{label}: " if label else ""
                elapsed = int(now - start)
                next_heartbeat = now + max(5, heartbeat_sec)
    if verbose:
        head = f"{label}: " if label else ""
        log(
            f"{head}exit={proc.returncode}, "
            f"stdout_lines={len((stdout or '').splitlines())}, "
            f"stderr_lines={len((stderr or '').splitlines())}"
        )
    return CmdResult(proc.returncode, stdout or "", stderr or "")


def parse_lookup_stdout(stdout: str) -> dict[str, dict[str, float | str]]:
    csv_lines: list[str] = []
    for raw in stdout.splitlines():
        line = raw.strip()
        if line.startswith("scenario,"):
            csv_lines = [line]
            continue
        if not csv_lines or "," not in line:
            continue
        if line.split(",")[0] in {"no_index", "single_field_index", "hash_single_field_index"}:
            csv_lines.append(line)

    parsed: dict[str, dict[str, float | str]] = {}
    if not csv_lines:
        return parsed

    reader = csv.DictReader(StringIO("\n".join(csv_lines)))
    for row in reader:
        scenario = row["scenario"]
        if scenario not in {"no_index", "single_field_index", "hash_single_field_index"}:
            continue
        overhead_raw = row.get("setup_overhead_ms") or row.get("overhead_ms", "nan")
        entry: dict[str, float | str] = {
            "avg_ms": float(row["avg_ms"]),
            "median_ms": float(row["median_ms"]),
            "wall_ms": float(row["wall_ms"]),
            "overhead_ms": float(overhead_raw),
            "verified": row["verified"],
        }
        for p in QUANTILE_PCTS:
            key = quantile_field(p)
            if key in row and row[key]:
                entry[key] = float(row[key])
        parsed[scenario] = entry
    return parsed


def to_float(value: str) -> float:
    try:
        return float(value)
    except Exception:
        return float("nan")


def load_config(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to read config: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("config root must be an object")
    return raw


def section_dict(config: dict[str, Any], name: str) -> dict[str, Any]:
    v = config.get(name, {})
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise RuntimeError(f"config section '{name}' must be an object")
    return v


def cfg_get(config: dict[str, Any], section: str, key: str, default: Any) -> Any:
    sec = section_dict(config, section)
    if key in sec:
        return sec[key]
    global_sec = section_dict(config, "global")
    return global_sec.get(key, default)


def as_int(name: str, value: Any) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError("bool is not int")
        return int(value)
    except Exception as exc:
        raise RuntimeError(f"config value '{name}' must be int, got {value!r}") from exc


def as_float(name: str, value: Any) -> float:
    try:
        if isinstance(value, bool):
            raise ValueError("bool is not float")
        return float(value)
    except Exception as exc:
        raise RuntimeError(f"config value '{name}' must be float, got {value!r}") from exc


def as_bool(name: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise RuntimeError(f"config value '{name}' must be bool, got {value!r}")


def parse_rows(value: Any, name: str) -> list[int]:
    if isinstance(value, str):
        rows = [int(x.strip()) for x in value.split(",") if x.strip()]
    elif isinstance(value, list):
        rows = [as_int(name, x) for x in value]
    else:
        raise RuntimeError(f"config value '{name}' must be comma string or int list")
    if not rows:
        raise RuntimeError(f"config value '{name}' cannot be empty")
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a full hash-index evidence suite using existing benchmarks: "
            "lookup latency, throughput, spill/memory-pressure, crash/restart."
        )
    )
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner binary.")
    parser.add_argument("--config", default="", help="Path to JSON config with per-benchmark settings.")
    parser.add_argument("--workspace", default="/tmp/otterbrix_hash_full_report", help="Output workspace directory.")
    parser.add_argument(
        "--latency-rows",
        default="50000,120000,250000,500000",
        help="Comma-separated dataset sizes for lookup-latency checks.",
    )
    parser.add_argument(
        "--btree-max-rows",
        type=int,
        default=250000,
        help=(
            "Rows threshold for running single_field_index comparisons. "
            "For rows above this threshold, btree comparison is skipped as not cost-effective."
        ),
    )
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--lookup-runs", type=int, default=20)
    parser.add_argument("--checkpoint-mb", type=int, default=0)
    parser.add_argument("--seed", type=int, default=1234567)
    parser.add_argument("--show-runner-output", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print detailed progress and command diagnostics.")
    parser.add_argument("--keep-workspaces", action="store_true")
    parser.add_argument("--memory-pressure-factor", type=float, default=1.25)
    parser.add_argument("--memory-max-rows", type=int, default=25_000_000)
    parser.add_argument("--spill-collision-count", type=int, default=900)
    parser.add_argument("--crash-delay-sec", type=float, default=1.2)
    parser.add_argument("--heartbeat-sec", type=int, default=60, help="Progress heartbeat for long-running child processes.")
    parser.add_argument("--lookup-timeout-sec", type=int, default=0, help="Timeout for each latency child run (0 = no timeout).")
    parser.add_argument("--requirements-timeout-sec", type=int, default=0, help="Timeout for requirements phase child run (0 = no timeout).")
    parser.add_argument("--crash-timeout-sec", type=int, default=0, help="Timeout for crash/restart phase child run (0 = no timeout).")
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.exists():
        raise RuntimeError(f"runner does not exist: {runner}")

    config: dict[str, Any] = {}
    if args.config:
        cfg_path = Path(args.config).resolve()
        if not cfg_path.exists():
            raise RuntimeError(f"config does not exist: {cfg_path}")
        config = load_config(cfg_path)

    latency_rows = parse_rows(cfg_get(config, "latency", "rows", args.latency_rows), "latency.rows")
    btree_max_rows = as_int("latency.btree_max_rows", cfg_get(config, "latency", "btree_max_rows", args.btree_max_rows))
    latency_payload_bytes = as_int(
        "latency.payload_bytes", cfg_get(config, "latency", "payload_bytes", args.payload_bytes)
    )
    latency_runs = as_int("latency.lookup_runs", cfg_get(config, "latency", "lookup_runs", args.lookup_runs))
    latency_checkpoint_mb = as_int("latency.checkpoint_mb", cfg_get(config, "latency", "checkpoint_mb", args.checkpoint_mb))
    latency_seed = as_int("latency.seed", cfg_get(config, "latency", "seed", args.seed))
    lookup_timeout_sec = as_int(
        "latency.lookup_timeout_sec", cfg_get(config, "latency", "lookup_timeout_sec", args.lookup_timeout_sec)
    )

    requirements_rows_default = min(max(latency_rows), btree_max_rows)
    req_rows = as_int("requirements.rows", cfg_get(config, "requirements", "rows", requirements_rows_default))
    req_runs = as_int("requirements.runs", cfg_get(config, "requirements", "runs", latency_runs))
    req_payload_bytes = as_int(
        "requirements.payload_bytes", cfg_get(config, "requirements", "payload_bytes", latency_payload_bytes)
    )
    req_checkpoint_mb = as_int(
        "requirements.checkpoint_mb", cfg_get(config, "requirements", "checkpoint_mb", latency_checkpoint_mb)
    )
    req_memory_pressure_factor = as_float(
        "requirements.memory_pressure_factor",
        cfg_get(config, "requirements", "memory_pressure_factor", args.memory_pressure_factor),
    )
    req_memory_max_rows = as_int(
        "requirements.memory_max_rows", cfg_get(config, "requirements", "memory_max_rows", args.memory_max_rows)
    )
    req_spill_collision_count = as_int(
        "requirements.spill_collision_count",
        cfg_get(config, "requirements", "spill_collision_count", args.spill_collision_count),
    )
    requirements_timeout_sec = as_int(
        "requirements.timeout_sec", cfg_get(config, "requirements", "timeout_sec", args.requirements_timeout_sec)
    )

    crash_rows_default = min(max(latency_rows), btree_max_rows)
    crash_rows = as_int("crash.rows", cfg_get(config, "crash", "rows", crash_rows_default))
    crash_restart_runs = as_int(
        "crash.restart_runs", cfg_get(config, "crash", "restart_runs", max(5, latency_runs // 2))
    )
    crash_payload_bytes = as_int("crash.payload_bytes", cfg_get(config, "crash", "payload_bytes", latency_payload_bytes))
    crash_delay_sec = as_float("crash.crash_delay_sec", cfg_get(config, "crash", "crash_delay_sec", args.crash_delay_sec))
    crash_checkpoint_mb = as_int(
        "crash.checkpoint_mb", cfg_get(config, "crash", "checkpoint_mb", latency_checkpoint_mb)
    )
    crash_timeout_sec = as_int("crash.timeout_sec", cfg_get(config, "crash", "timeout_sec", args.crash_timeout_sec))

    heartbeat_sec = as_int("global.heartbeat_sec", cfg_get(config, "global", "heartbeat_sec", args.heartbeat_sec))
    show_runner_output = as_bool(
        "global.show_runner_output", cfg_get(config, "global", "show_runner_output", args.show_runner_output)
    )
    keep_workspaces = as_bool("global.keep_workspaces", cfg_get(config, "global", "keep_workspaces", args.keep_workspaces))

    script_dir = Path(__file__).resolve().parent
    workspace = Path(args.workspace).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    log(f"workspace: {workspace}")
    log(f"runner: {runner}")
    if args.config:
        log(f"config: {Path(args.config).resolve()}")
    log(f"latency rows: {','.join(str(x) for x in latency_rows)}")
    log(f"btree max rows: {btree_max_rows}")
    log(f"heartbeat: every {max(5, heartbeat_sec)}s")
    if args.verbose:
        log("verbose mode: ON")

    report_rows: list[dict[str, str]] = []
    md_lines: list[str] = []
    md_lines.append("# Hash Index Full Report")
    md_lines.append("")
    md_lines.append(f"- runner: `{runner}`")
    md_lines.append(f"- generated_at: `{int(time.time())}`")
    md_lines.append(f"- btree_max_rows: `{btree_max_rows}`")
    if args.config:
        md_lines.append(f"- config: `{Path(args.config).resolve()}`")
    md_lines.append("")

    # 1) Lookup latency across N, with btree comparison where feasible.
    log("phase 1/3: lookup latency sweep")
    md_lines.append("## Lookup Latency")
    md_lines.append("")
    for n in latency_rows:
        log(f"phase 1/3: N={n} start")
        scenario_workspace = workspace / f"latency_{n}"
        cmd = [
            sys.executable,
            str(script_dir / "benchmark_select_key_lookup.py"),
            "--runner",
            str(runner),
            "--workspace",
            str(scenario_workspace),
            "--rows",
            str(n),
            "--payload-bytes",
            str(latency_payload_bytes),
            "--runs",
            str(latency_runs),
            "--seed",
            str(latency_seed + n),
        ]
        if latency_checkpoint_mb > 0:
            cmd.extend(["--checkpoint-mb", str(latency_checkpoint_mb)])
        if show_runner_output:
            cmd.append("--show-runner-output")
        if n > btree_max_rows:
            # Still run script for no/single/hash visibility, but mark single comparison as non-decisive.
            md_lines.append(
                f"- N={n}: `single_field_index` comparison marked as **non-decisive** "
                f"(N > btree_max_rows={btree_max_rows})."
            )
            log(f"phase 1/3: N={n} btree comparison marked non-decisive")

        res = run_cmd(
            cmd,
            cwd=Path.cwd(),
            verbose=args.verbose,
            label=f"lookup_N={n}",
            timeout_sec=lookup_timeout_sec,
            heartbeat_sec=heartbeat_sec,
        )
        if res.returncode != 0:
            raise RuntimeError(
                f"lookup benchmark failed for N={n}\nstdout:\n{res.stdout[-4000:]}\nstderr:\n{res.stderr[-4000:]}"
            )
        parsed = parse_lookup_stdout(res.stdout)
        if not parsed:
            raise RuntimeError(f"cannot parse lookup output for N={n}\nstdout:\n{res.stdout[-2000:]}")

        hash_avg = to_float(str(parsed.get("hash_single_field_index", {}).get("avg_ms", "nan")))
        single_avg = to_float(str(parsed.get("single_field_index", {}).get("avg_ms", "nan")))
        speedup_vs_single = single_avg / hash_avg if hash_avg > 0 and single_avg > 0 else float("nan")
        decisiveness = "yes" if n <= btree_max_rows else "no"
        log(
            f"phase 1/3: N={n} parsed scenarios={sorted(parsed.keys())}, "
            f"hash_avg_ms={hash_avg:.3f}, single_avg_ms={single_avg:.3f}, speedup={speedup_vs_single:.3f}x"
        )

        report_rows.append(
            {
                "section": "lookup_latency",
                "scenario": f"N={n}",
                "metric": "hash_vs_single_speedup_by_avg_ms",
                "value": f"{speedup_vs_single:.6f}",
                "unit": "x",
                "status": "OK",
                "compare_with_btree_decisive": decisiveness,
                "note": "",
            }
        )
        md_lines.append(
            f"- N={n}: hash avg={hash_avg:.3f} ms, single avg={single_avg:.3f} ms, "
            f"speedup(hash/single)={speedup_vs_single:.3f}x, decisive={decisiveness}"
        )
        log(f"phase 1/3: N={n} done")

    # 2) Throughput + spill + memory pressure requirements benchmark.
    log("phase 2/3: throughput + spill + memory pressure")
    md_lines.append("")
    md_lines.append("## Throughput, Spill, Memory Pressure")
    md_lines.append("")
    req_ws = workspace / "requirements"
    mem_total = mem_total_bytes()
    if mem_total > 0:
        approx_row_bytes = max(1, req_payload_bytes + 24)
        est_rows_needed = int((mem_total * req_memory_pressure_factor + approx_row_bytes - 1) // approx_row_bytes)
        log(
            "phase 2/3 preflight: memory-pressure estimate "
            f"rows_needed~{est_rows_needed}, memory_max_rows={req_memory_max_rows}, "
            f"payload_bytes={req_payload_bytes}, factor={req_memory_pressure_factor}"
        )
        if est_rows_needed > req_memory_max_rows:
            log(
                "phase 2/3 preflight warning: estimated rows exceed --memory-max-rows; "
                "requirements phase may fail fast (intentional safety check)"
            )
    req_cmd = [
        sys.executable,
        str(script_dir / "benchmark_hash_index_requirements.py"),
        "--runner",
        str(runner),
        "--workspace",
        str(req_ws),
        "--rows",
        str(req_rows),
        "--runs",
        str(req_runs),
        "--payload-bytes",
        str(req_payload_bytes),
        "--checkpoint-mb",
        str(req_checkpoint_mb),
        "--memory-pressure-factor",
        str(req_memory_pressure_factor),
        "--memory-max-rows",
        str(req_memory_max_rows),
        "--spill-collision-count",
        str(req_spill_collision_count),
        "--keep-workspace",
    ]
    if show_runner_output:
        req_cmd.append("--show-runner-output")

    req_res = run_cmd(
        req_cmd,
        cwd=Path.cwd(),
        verbose=args.verbose,
        label="requirements",
        timeout_sec=requirements_timeout_sec,
        heartbeat_sec=heartbeat_sec,
    )
    if req_res.returncode != 0:
        raise RuntimeError(
            "benchmark_hash_index_requirements.py failed\n"
            f"stdout:\n{req_res.stdout[-4000:]}\nstderr:\n{req_res.stderr[-4000:]}"
        )
    req_csv = req_ws / "benchmark_hash_requirements.csv"
    log(f"phase 2/3: reading requirements csv: {req_csv}")
    req_rows = read_csv_rows(req_csv)
    if not req_rows:
        raise RuntimeError(f"empty requirements report: {req_csv}")

    # Pull key requirement signals for markdown visibility.
    throughput_rows = [r for r in req_rows if r.get("section") == "throughput" and r.get("metric") in {"insert_ops_s", "query_ops_s"}]
    spill_rows = [r for r in req_rows if r.get("section") == "spill"]
    mem_checks = [r for r in req_rows if r.get("section") == "memory_pressure" and r.get("metric", "").startswith("check_")]
    log(
        f"phase 2/3: rows={len(req_rows)}, throughput_rows={len(throughput_rows)}, "
        f"spill_rows={len(spill_rows)}, memory_checks={len(mem_checks)}"
    )

    md_lines.append(f"- throughput rows captured: {len(throughput_rows)}")
    md_lines.append(f"- spill rows captured: {len(spill_rows)}")
    md_lines.append(f"- memory-pressure checks: {len(mem_checks)}")
    for r in mem_checks:
        md_lines.append(f"  - {r['metric']} = {r['value']} ({r['status']})")

    for r in req_rows:
        report_rows.append(
            {
                "section": r.get("section", ""),
                "scenario": r.get("scenario", ""),
                "metric": r.get("metric", ""),
                "value": r.get("value", ""),
                "unit": r.get("unit", ""),
                "status": r.get("status", ""),
                "compare_with_btree_decisive": "n/a",
                "note": "",
            }
        )
    log("phase 2/3: done")

    # 3) Crash/restart simulation benchmark.
    log("phase 3/3: crash/restart simulation")
    md_lines.append("")
    md_lines.append("## Crash / Restart")
    md_lines.append("")
    crash_ws = workspace / "crash"
    crash_cmd = [
        sys.executable,
        str(script_dir / "benchmark_crash_restart_perf_correctness.py"),
        "--runner",
        str(runner),
        "--workspace",
        str(crash_ws),
        "--rows",
        str(crash_rows),
        "--payload-bytes",
        str(crash_payload_bytes),
        "--restart-runs",
        str(crash_restart_runs),
        "--crash-delay-sec",
        str(crash_delay_sec),
        "--checkpoint-mb",
        str(crash_checkpoint_mb),
        "--keep-workspace",
    ]
    if show_runner_output:
        crash_cmd.append("--show-runner-output")

    crash_res = run_cmd(
        crash_cmd,
        cwd=Path.cwd(),
        verbose=args.verbose,
        label="crash_restart",
        timeout_sec=crash_timeout_sec,
        heartbeat_sec=heartbeat_sec,
    )
    if crash_res.returncode != 0:
        raise RuntimeError(
            "benchmark_crash_restart_perf_correctness.py failed\n"
            f"stdout:\n{crash_res.stdout[-4000:]}\nstderr:\n{crash_res.stderr[-4000:]}"
        )
    crash_csv = crash_ws / "crash_restart_perf_correctness.csv"
    log(f"phase 3/3: reading crash csv: {crash_csv}")
    crash_rows = read_csv_rows(crash_csv)
    if not crash_rows:
        raise RuntimeError(f"empty crash report: {crash_csv}")
    log(f"phase 3/3: crash scenarios parsed={len(crash_rows)}")

    for r in crash_rows:
        md_lines.append(
            f"- {r.get('scenario')}: crash_forced={r.get('crash_forced')}, "
            f"restart_status={r.get('restart_status')}, header_ok={r.get('index_header_ok')}, "
            f"restart_avg_ms={r.get('restart_avg_ms')}, error={r.get('error') or '-'}"
        )
        report_rows.append(
            {
                "section": "crash_restart",
                "scenario": r.get("scenario", ""),
                "metric": "restart_status",
                "value": r.get("restart_status", ""),
                "unit": "status",
                "status": "OK" if r.get("restart_status") == "OK" and r.get("index_header_ok") == "YES" else "FAIL",
                "compare_with_btree_decisive": "n/a",
                "note": r.get("error", ""),
            }
        )
        if args.verbose:
            log(
                f"phase 3/3: {r.get('scenario')} -> restart={r.get('restart_status')}, "
                f"header_ok={r.get('index_header_ok')}, error={r.get('error') or '-'}"
            )
    log("phase 3/3: done")

    # Persist final unified report.
    out_csv = workspace / "hash_index_full_report.csv"
    log(f"writing unified csv: {out_csv} (rows={len(report_rows)})")
    write_dict_rows_csv(out_csv, report_rows)
    out_md = workspace / "hash_index_full_report.md"
    log(f"writing unified markdown: {out_md}")
    out_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"Saved unified CSV report: {out_csv}")
    print(f"Saved unified Markdown report: {out_md}")

    if not keep_workspaces:
        log("cleanup: removing heavy child workspaces")
        # Keep final report files, remove heavy benchmark artifacts.
        for p in [workspace / f"latency_{n}" for n in latency_rows]:
            shutil.rmtree(p, ignore_errors=True)
        shutil.rmtree(req_ws, ignore_errors=True)
        shutil.rmtree(crash_ws, ignore_errors=True)
        log("cleanup: done")
    else:
        log("keep-workspaces: ON, child artifacts preserved")


if __name__ == "__main__":
    main()

