"""
Benchmark (otterbrix main, NATIVE relation API): optimized vs non-optimized
execution.

Rewrite of bench_optimizer.py for otterbrix `main`. The original benchmark was
written against the `otterbrix.experimental.spark` DataFrame wrapper, which is
NOT part of main (it lives only on the pythonpkg-integration feature branches).
This version drops that wrapper entirely and talks to main's native relation
API (`OtterBrixPyConnection` / `OtterBrixPyRelation`):

    conn = otterbrix.connect(<db path>)
    rel  = conn.from_df(pandas_df)
    rel.filter(...).select(...).group(...).join(...).sort(...)
    rel.optimize = True | False        # <-- the opt/no_opt toggle
    rows = rel.fetchall()

The opt/no_opt switch is native to main: a relation carries an `optimize` flag
(`py_relation_t::optimize_`, default False) that is forwarded to
`py_connection_t::execute(node, optimize)`, where the optimizer pass runs ONLY
when the flag is set (pyconnection.cpp):

    if (optimize) { node = components::planner::optimize(node->resource(), node, nullptr); }

So `optimize=False` executes the raw logical plan and `optimize=True` runs the
predicate-pushdown optimizer — exactly the comparison the original benchmark made
via `createDataFrame(..., optimize=...)`, but with no feature-branch code.

Scenarios (parity with bench_optimizer.py):
  1. filter                  — rel.filter(age > 50)                              O(n)
  2. project_filter          — rel.select(id,name,age).filter(age > 50)          O(n)   [projection pushdown]
  3. chained_filters         — rel.filter(a).filter(b).filter(c)                 O(n)
  4. groupby_agg             — rel.group(group_key, sum(value))                   O(n*k)
  5. filter_over_groupby_key — rel.group(...).filter(group_key == 'g0')          O(n)   [post-agg filter]
  6. filter_over_sort_sel_*  — rel.sort(name).filter(value > t)  @ ~1/10/50/90%  O(n log n)
  7. join_filter_sel_*       — rel.join(rel2).filter(value > t)  @ ~1/10/50/90%  O(n^2) [pushdown below join]

Modes: no_opt (optimize=False), opt (optimize=True). Each (scenario, size) pair
measures both modes interleaved with randomized within-pair order, so
environmental drift cancels instead of aliasing onto the optimize flag. A
no_opt/opt difference is flagged significant only if it clears both a Welch
t-test AND a practical-equivalence band (EQUIV_BAND).

Correctness gate: per pair, no_opt and opt result row counts must match; a
mismatch means optimize=True changed the result (an engine bug) and the pair's
timings are not comparable (flagged in the `correctness` CSV column).

Output: results_native.csv (+ results_native.env.json), same schema as the
original results.csv so old vs new runs diff on (scenario, mode, rows).

NOTE: absolute timings here are NOT directly comparable to the old Spark-API
baseline (different Python API + different engine build); the comparable
quantity is the opt/no_opt speedup ratio per scenario.
"""

import gc
import os
import sys
import time
import json
import atexit
import shutil
import tempfile
import platform
import subprocess
import statistics
import random
from datetime import datetime
from typing import List, Tuple, Callable, Dict, Any

import pandas as pd

# Native otterbrix relation API (main). Importing experimental.spark is
# intentionally avoided — it is not part of main.
import otterbrix
from otterbrix import ColumnExpression, ConstantExpression


# ---------------------------------------------------------------------------
# Data generation (identical to bench_optimizer.py so workloads match)
# ---------------------------------------------------------------------------

def generate_main_data(n: int) -> List[Tuple]:
    """n rows of (id, name, age, value, group_key) — int/str/int/float/str."""
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve",
             "Frank", "Grace", "Hank", "Ivy", "Jack"]
    random.seed(42)
    return [
        (i, names[i % len(names)], random.randint(1, 100),
         round(random.uniform(0.0, 1000.0), 2), f"g{i % 50}")
        for i in range(n)
    ]

MAIN_SCHEMA = ["id", "name", "age", "value", "group_key"]


def generate_join_data(n: int) -> List[Tuple]:
    """n rows of (id, extra_value) for join benchmarks."""
    random.seed(99)
    return [(i, round(random.uniform(0, 500), 2)) for i in range(n)]

JOIN_SCHEMA = ["id", "extra_value"]


def _main_df(n: int) -> pd.DataFrame:
    return pd.DataFrame(generate_main_data(n), columns=MAIN_SCHEMA)


def _join_df(n: int) -> pd.DataFrame:
    return pd.DataFrame(generate_join_data(n), columns=JOIN_SCHEMA)


# ---------------------------------------------------------------------------
# Per-scenario data sizes & time budgets (same as bench_optimizer.py)
# ---------------------------------------------------------------------------

SIZES_FILTER = [10_000, 100_000, 1_000_000]
SIZES_PROJECT_FILTER = [100_000, 1_000_000]
SIZES_CHAINED = [10_000, 100_000]
SIZES_FILTER_OVER_GROUPBY_KEY = [100_000, 1_000_000]
SIZES_FAST = [10_000, 100_000]
SIZES_GROUPBY = [10_000, 100_000]
SIZES_JOIN_SELECTIVITY = [5_000, 10_000]

SCENARIO_SIZES = {
    "filter":                  SIZES_FILTER,
    "project_filter":          SIZES_PROJECT_FILTER,
    "chained_filters":         SIZES_CHAINED,
    "filter_over_groupby_key": SIZES_FILTER_OVER_GROUPBY_KEY,
    "filter_over_sort_sel":    SIZES_FAST,
    "groupby_agg":             SIZES_GROUPBY,
    "selectivity":             SIZES_JOIN_SELECTIVITY,
}

# threshold -> approximate % of rows passing (value ~ uniform 0..1000)
SELECTIVITY_POINTS = [(100, 90), (500, 50), (900, 10), (990, 1)]


# ---------------------------------------------------------------------------
# Timing & statistics helpers (API-agnostic; lifted from bench_optimizer.py)
# ---------------------------------------------------------------------------

WARMUP_RUNS = 2
EQUIV_BAND = 0.05
MIN_RUNS = 8


def runs_for_size(n: int, scenario: str = "filter") -> int:
    is_join = scenario == "selectivity"
    is_groupby = scenario == "groupby_agg"
    if is_join:
        if n <= 1_000:
            return 30
        if n <= 5_000:
            return 15
        return 10
    if is_groupby:
        if n <= 1_000:
            return 50
        if n <= 10_000:
            return 30
        return 15
    if n <= 1_000:
        return 100
    if n <= 10_000:
        return 50
    if n <= 100_000:
        return 30
    return 20


def time_budget(scenario: str) -> float:
    if scenario == "selectivity":
        return 120
    if scenario == "groupby_agg":
        return 60
    return 30


def _compute_stats(times: List[float]) -> Dict[str, Any]:
    from math import sqrt
    sorted_times = sorted(times)
    n = len(sorted_times)
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if n > 1 else 0.0
    try:
        from scipy.stats import t as t_dist
        t_val = t_dist.ppf(0.975, df=n - 1) if n > 1 else 1.96
    except ImportError:
        t_val = 1.96
    sem = stdev / sqrt(n) if n > 1 else 0.0
    ci_half = t_val * sem
    cv = stdev / mean if mean > 0 else 0.0
    return {
        "mean": mean,
        "median": statistics.median(times),
        "stdev": stdev,
        "min": min(times),
        "max": max(times),
        "p95": sorted_times[min(int(n * 0.95), n - 1)],
        "p99": sorted_times[min(int(n * 0.99), n - 1)],
        "sem": sem,
        "t_val": t_val,
        "ci_95_low": mean - ci_half,
        "ci_95_high": mean + ci_half,
        "cv": cv,
        "actual_runs": n,
        "runs": times,
    }


def measure_paired(fn_a: Callable[[], Any], fn_b: Callable[[], Any],
                   warmup: int = WARMUP_RUNS, runs: int = 20,
                   budget: float = 60.0) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Measure fn_a and fn_b interleaved with randomized within-pair order."""
    for _ in range(warmup):
        fn_a()
        fn_b()
    gc.disable()
    times_a: List[float] = []
    times_b: List[float] = []
    wall_start = time.perf_counter()
    for i in range(runs):
        order = [(times_a, fn_a), (times_b, fn_b)]
        if random.random() < 0.5:
            order.reverse()
        for bucket, fn in order:
            t0 = time.perf_counter_ns()
            fn()
            t1 = time.perf_counter_ns()
            bucket.append((t1 - t0) / 1e9)
        if i >= MIN_RUNS - 1 and (time.perf_counter() - wall_start) > budget:
            break
    gc.enable()
    return _compute_stats(times_a), _compute_stats(times_b)


def measure_memory(fn: Callable[[], Any]) -> float:
    import tracemalloc
    gc.collect()
    tracemalloc.start()
    fn()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak / (1024 * 1024)


def compare_significance(runs_a: List[float], runs_b: List[float]) -> Dict[str, Any]:
    """Welch t-test + bootstrap CI for the mean_a/mean_b speedup ratio.

    Pass baseline ``no_opt`` as ``runs_a`` and ``opt`` as ``runs_b`` so the ratio
    is speedup vs the unoptimized baseline.
    """
    import numpy as np
    mean_a = np.mean(runs_a)
    mean_b = np.mean(runs_b)
    speedup = mean_a / mean_b if mean_b > 0 else float("inf")
    try:
        from scipy.stats import ttest_ind
        _, p_value = ttest_ind(runs_a, runs_b, equal_var=False)
    except ImportError:
        p_value = float("nan")
    rng = np.random.default_rng(42)
    n_boot = 10_000
    boot = []
    arr_a = np.array(runs_a)
    arr_b = np.array(runs_b)
    for _ in range(n_boot):
        sa = rng.choice(arr_a, size=len(arr_a), replace=True)
        sb = rng.choice(arr_b, size=len(arr_b), replace=True)
        mb = np.mean(sb)
        if mb > 0:
            boot.append(np.mean(sa) / mb)
    boot = np.array(boot)
    ci_low = float(np.percentile(boot, 2.5))
    ci_high = float(np.percentile(boot, 97.5))
    stat_sig = bool(p_value < 0.05) if not np.isnan(p_value) else False
    practical_sig = ci_low > 1.0 + EQUIV_BAND or ci_high < 1.0 - EQUIV_BAND
    return {
        "speedup": float(speedup),
        "ci_95": (ci_low, ci_high),
        "p_value": float(p_value),
        "significant": bool(stat_sig and practical_sig),
    }


# ---------------------------------------------------------------------------
# Native-API scenarios
#
# Each factory returns run(): the relation chain is rebuilt and executed on
# every call (the timed work is fetchall()); the optimize flag is set on the
# final relation, which gates the optimizer for the whole plan at execute time.
# ---------------------------------------------------------------------------

def _col(conn, name: str) -> ColumnExpression:
    return ColumnExpression(name, conn)


def _const(conn, value) -> ConstantExpression:
    return ConstantExpression(value, conn)


def _run(rel, optimize: bool):
    rel.optimize = optimize
    return rel.fetchall()


def scenario_filter(conn, base, optimize) -> Callable:
    """filter(age > 50)"""
    def run():
        q = base.filter(_col(conn, "age") > _const(conn, 50))
        return _run(q, optimize)
    return run


def scenario_project_filter(conn, base, optimize) -> Callable:
    """select(id,name,age).filter(age > 50) — projection pushdown."""
    def run():
        q = base.select(_col(conn, "id"), _col(conn, "name"), _col(conn, "age")) \
                .filter(_col(conn, "age") > _const(conn, 50))
        return _run(q, optimize)
    return run


def scenario_chained_filters(conn, base, optimize) -> Callable:
    """filter(age>20).filter(age<80).filter(value>100)"""
    def run():
        q = base.filter(_col(conn, "age") > _const(conn, 20)) \
                .filter(_col(conn, "age") < _const(conn, 80)) \
                .filter(_col(conn, "value") > _const(conn, 100))
        return _run(q, optimize)
    return run


def scenario_groupby(conn, base, optimize) -> Callable:
    """group(group_key, sum(value))"""
    def run():
        q = base.group(_col(conn, "group_key"), _col(conn, "value").sum())
        return _run(q, optimize)
    return run


def scenario_filter_over_groupby_key(conn, base, optimize) -> Callable:
    """group(group_key, sum(value)).filter(group_key == 'g0') — post-agg filter."""
    def run():
        q = base.group(_col(conn, "group_key"), _col(conn, "value").sum()) \
                .filter(_col(conn, "group_key") == _const(conn, "g0"))
        return _run(q, optimize)
    return run


def scenario_filter_over_sort_sel(conn, base, threshold, optimize) -> Callable:
    """sort(name).filter(value > threshold) — pushdown below sort."""
    def run():
        q = base.sort(_col(conn, "name")) \
                .filter(_col(conn, "value") > _const(conn, threshold))
        return _run(q, optimize)
    return run


def scenario_join_filter_sel(conn, base_l, base_r, threshold, optimize) -> Callable:
    """join(other, id==id).filter(value > threshold) — pushdown below a join."""
    def run():
        cond = ColumnExpression("id", conn, "left") == ColumnExpression("id", conn, "right")
        q = base_l.join(base_r, cond, "inner") \
                  .filter(_col(conn, "value") > _const(conn, threshold))
        return _run(q, optimize)
    return run


REGULAR_SCENARIOS = {
    "filter": scenario_filter,
    "project_filter": scenario_project_filter,
    "chained_filters": scenario_chained_filters,
    "groupby_agg": scenario_groupby,
    "filter_over_groupby_key": scenario_filter_over_groupby_key,
}


# ---------------------------------------------------------------------------
# Result assembly (same schema as bench_optimizer.py + correctness columns)
# ---------------------------------------------------------------------------

def _make_result(scenario, mode, size, stats, mem_mb):
    return {
        "scenario": scenario,
        "mode": mode,
        "rows": size,
        "mean_s": stats["mean"],
        "median_s": stats["median"],
        "p95_s": stats["p95"],
        "p99_s": stats["p99"],
        "stdev_s": stats["stdev"],
        "min_s": stats["min"],
        "max_s": stats["max"],
        "actual_runs": stats["actual_runs"],
        "sem": stats["sem"],
        "t_val": stats["t_val"],
        "ci_95_low": stats["ci_95_low"],
        "ci_95_high": stats["ci_95_high"],
        "cv": stats["cv"],
        "throughput_rows_per_s": size / stats["mean"] if stats["mean"] > 0 else 0,
        "peak_memory_mb": mem_mb,
        "plan_nodes": None,
        "raw_runs": stats["runs"],
    }


def _build_tasks(sizes_override=None, only=None):
    """List of (kind, size, name, threshold) no_opt/opt PAIR tasks."""
    def _sizes_for(key):
        if sizes_override is not None:
            return sizes_override
        return SCENARIO_SIZES.get(key, SIZES_FAST)

    def _inc(key):
        return only is None or key in only

    tasks = []
    for name in REGULAR_SCENARIOS:
        if not _inc(name):
            continue
        for size in _sizes_for(name):
            tasks.append(("regular", size, name, None))
    if _inc("selectivity"):
        for size in _sizes_for("selectivity"):
            for thr, pct in SELECTIVITY_POINTS:
                tasks.append(("join_sel", size, f"join_filter_sel_{pct}", thr))
    if _inc("filter_over_sort_sel"):
        for size in _sizes_for("filter_over_sort_sel"):
            for thr, pct in SELECTIVITY_POINTS:
                tasks.append(("sort_sel", size, f"filter_over_sort_sel_{pct}", thr))
    return tasks


def run_benchmarks(conn, sizes=None, only=None) -> list:
    from tqdm import tqdm

    tasks = _build_tasks(sizes, only)
    main_cache: Dict[int, Any] = {}
    join_cache: Dict[int, Any] = {}

    def base_main(size):
        if size not in main_cache:
            main_cache[size] = conn.from_df(_main_df(size))
        return main_cache[size]

    def base_join(size):
        if size not in join_cache:
            join_cache[size] = conn.from_df(_join_df(size))
        return join_cache[size]

    results = []
    pbar = tqdm(total=len(tasks), desc="Benchmarks (native)", unit="pair")
    for kind, size, name, thr in tasks:
        if kind == "regular":
            base = base_main(size)
            fn_no = REGULAR_SCENARIOS[name](conn, base, False)
            fn_opt = REGULAR_SCENARIOS[name](conn, base, True)
            runs_key = "groupby_agg" if name in ("groupby_agg", "filter_over_groupby_key") else "filter"
        elif kind == "sort_sel":
            base = base_main(size)
            fn_no = scenario_filter_over_sort_sel(conn, base, thr, False)
            fn_opt = scenario_filter_over_sort_sel(conn, base, thr, True)
            runs_key = "filter_over_sort_sel"
        else:  # join_sel
            base_l = base_main(size)
            base_r = base_join(size)
            fn_no = scenario_join_filter_sel(conn, base_l, base_r, thr, False)
            fn_opt = scenario_join_filter_sel(conn, base_l, base_r, thr, True)
            runs_key = "selectivity"

        pbar.set_postfix_str(f"{name} n={size:,}")

        # Correctness gate: optimize must not change the row count.
        rows_no = len(fn_no())
        rows_opt = len(fn_opt())
        correctness = "ok" if rows_no == rows_opt else "MISMATCH"
        if correctness != "ok":
            pbar.write(f"  !! {name} n={size:,}: no_opt={rows_no} vs opt={rows_opt} rows "
                       f"— RESULT MISMATCH (timings not comparable)")

        n_runs = runs_for_size(size, scenario=runs_key)
        budget = 2 * time_budget(runs_key)
        stats_no, stats_opt = measure_paired(fn_no, fn_opt, runs=n_runs, budget=budget)
        res_no = _make_result(name, "no_opt", size, stats_no, measure_memory(fn_no))
        res_opt = _make_result(name, "opt", size, stats_opt, measure_memory(fn_opt))
        res_no["result_rows"], res_opt["result_rows"] = rows_no, rows_opt
        res_no["correctness"] = res_opt["correctness"] = correctness
        results.append(res_no)
        results.append(res_opt)
        pbar.update(1)
    pbar.close()

    # Pairwise opt/no_opt significance per (scenario, rows).
    lookup = {(r["scenario"], r["mode"], r["rows"]): r.get("raw_runs", []) for r in results}
    for r in results:
        runs_no = lookup.get((r["scenario"], "no_opt", r["rows"]), [])
        runs_opt = lookup.get((r["scenario"], "opt", r["rows"]), [])
        if runs_no and runs_opt:
            cmp = compare_significance(runs_no, runs_opt)
            r["sig"] = "*" if cmp["significant"] else "ns"
            r["p_value"] = cmp["p_value"]
            if r["mode"] == "no_opt":
                r["speedup"] = r["bootstrap_l"] = r["bootstrap_r"] = 1.0
            else:
                r["speedup"] = cmp["speedup"]
                r["bootstrap_l"], r["bootstrap_r"] = cmp["ci_95"]
        else:
            r["sig"] = ""
            r["p_value"] = r["speedup"] = r["bootstrap_l"] = r["bootstrap_r"] = None
    return results


# ---------------------------------------------------------------------------
# Environment + output
# ---------------------------------------------------------------------------

def collect_environment() -> Dict[str, Any]:
    env: Dict[str, Any] = {
        "timestamp": datetime.now().astimezone().isoformat(timespec="seconds"),
        "os": f"{platform.system()} {platform.release()}",
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
    }
    cpu_model = platform.processor() or "unknown"
    try:
        if platform.system() == "Darwin":
            cpu_model = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        elif platform.system() == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        cpu_model = line.split(":", 1)[1].strip()
                        break
    except Exception:
        pass
    env["cpu_model"] = cpu_model
    try:
        import psutil
        freq = psutil.cpu_freq()
        env["cpu_cores_physical"] = psutil.cpu_count(logical=False)
        env["cpu_cores_logical"] = psutil.cpu_count(logical=True)
        env["cpu_freq_max_mhz"] = round(freq.max, 1) if freq else None
        env["ram_total_gb"] = round(psutil.virtual_memory().total / 1024 ** 3, 2)
    except ImportError:
        env["cpu_cores_physical"] = None
        env["cpu_cores_logical"] = os.cpu_count()
        env["cpu_freq_max_mhz"] = None
        env["ram_total_gb"] = None
    versions = {}
    for mod in ("numpy", "scipy", "pandas", "tqdm"):
        try:
            versions[mod] = __import__(mod).__version__
        except Exception:
            versions[mod] = None
    versions["otterbrix"] = getattr(otterbrix, "__version__", None)
    env["libraries"] = versions
    env["benchmark_config"] = {
        "engine": "otterbrix main — native relation API (OtterBrixPyRelation)",
        "optimize_toggle": "rel.optimize -> py_connection_t::execute(node, optimize)",
        "warmup_runs": WARMUP_RUNS,
        "otterbrix_module": getattr(otterbrix, "__file__", None),
        "scenario_sizes": SCENARIO_SIZES,
    }
    return env


def print_environment(env: Dict[str, Any]) -> None:
    print(f"\n{'='*60}")
    print("TEST ENVIRONMENT (saved for reproducibility)")
    print(f"{'='*60}")
    print(f"  Timestamp : {env['timestamp']}")
    print(f"  OS        : {env['platform']}")
    print(f"  CPU       : {env['cpu_model']}")
    print(f"  Cores     : {env['cpu_cores_physical']} physical / {env['cpu_cores_logical']} logical")
    if env.get("ram_total_gb"):
        print(f"  RAM       : {env['ram_total_gb']} GB")
    print(f"  Python    : {env['python']}")
    print(f"  Engine    : {env['benchmark_config']['engine']}")
    print(f"  otterbrix : {env['benchmark_config']['otterbrix_module']}")
    libs = ", ".join(f"{k} {v}" for k, v in env["libraries"].items() if v)
    print(f"  Libraries : {libs}")


def save_csv(results: list, path: str = None):
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results_native.csv")
    df = pd.DataFrame(results)
    if "raw_runs" in df.columns:
        df = df.drop(columns=["raw_runs"])
    df.to_csv(path, index=False)
    print(f"\nResults saved to {path}")
    env = collect_environment()
    env_path = os.path.splitext(path)[0] + ".env.json"
    with open(env_path, "w") as f:
        json.dump(env, f, indent=2, ensure_ascii=False)
    print(f"Environment metadata saved to {env_path}")
    print_environment(env)
    return df


def print_summary_table(df, raw_results=None):
    print(f"\n{'='*120}")
    print("SUMMARY: No Optimization vs Optimization (otterbrix main, native API)")
    print(f"  Sig '*' = p<0.05 AND 95% CI outside equivalence band "
          f"[{1 - EQUIV_BAND:.2f}, {1 + EQUIV_BAND:.2f}]; else 'ns'")
    print(f"{'='*120}")
    print(f"{'Scenario':25s} {'Rows':>8s} {'NoOpt(s)':>10s} {'Opt(s)':>10s} "
          f"{'Speedup':>8s} {'Sig?':>5s} {'NO CV':>6s} {'O CV':>6s} {'OK?':>8s}")
    print(f"{'-'*120}")
    lookup = {}
    if raw_results:
        for r in raw_results:
            lookup[(r["scenario"], r["mode"], r["rows"])] = r.get("raw_runs", [])
    for scenario in df["scenario"].unique():
        for size in sorted(df["rows"].unique()):
            row_no = df[(df["scenario"] == scenario) & (df["mode"] == "no_opt") & (df["rows"] == size)]
            row_opt = df[(df["scenario"] == scenario) & (df["mode"] == "opt") & (df["rows"] == size)]
            if row_no.empty or row_opt.empty:
                continue
            no = row_no.iloc[0]
            o = row_opt.iloc[0]
            speedup = no["mean_s"] / o["mean_s"] if o["mean_s"] > 0 else float("inf")
            sig = ""
            runs_no = lookup.get((scenario, "no_opt", size), [])
            runs_opt = lookup.get((scenario, "opt", size), [])
            if runs_no and runs_opt:
                sig = "*" if compare_significance(runs_no, runs_opt)["significant"] else "ns"
            corr = no.get("correctness", "")
            print(f"{scenario:25s} {size:8,d} {no['mean_s']:10.4f} {o['mean_s']:10.4f} "
                  f"{speedup:7.2f}x {sig:>5s} {no['cv']:5.1%} {o['cv']:5.1%} {corr:>8s}")
        print()


def _make_connection():
    """Connect to an isolated, throwaway on-disk database (auto-removed)."""
    db_dir = tempfile.mkdtemp(prefix="ob_bench_")
    atexit.register(lambda: shutil.rmtree(db_dir, ignore_errors=True))
    return otterbrix.connect(os.path.join(db_dir, "benchdb"))


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="otterbrix main native opt/no_opt relation benchmark "
                    "(rewrite of bench_optimizer.py without the spark wrapper)")
    parser.add_argument("--sizes", type=int, nargs="+", default=None,
                        help="Override data sizes for ALL scenarios (e.g. --sizes 1000 5000)")
    parser.add_argument("--scenarios", nargs="+", default=None,
                        help="Run only these scenario keys: filter, project_filter, "
                             "chained_filters, groupby_agg, filter_over_groupby_key, "
                             "selectivity, filter_over_sort_sel.")
    parser.add_argument("--out", default=None, help="CSV output path (default: results_native.csv)")
    args = parser.parse_args()

    sizes = args.sizes if args.sizes else None
    only = set(args.scenarios) if args.scenarios else None

    conn = _make_connection()
    try:
        results = run_benchmarks(conn, sizes=sizes, only=only)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    df = save_csv(results, path=args.out)
    print_summary_table(df, raw_results=results)


if __name__ == "__main__":
    main()
