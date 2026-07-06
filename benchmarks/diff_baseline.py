"""
Diff two opt/no_opt benchmark CSVs on (scenario, rows): old baseline vs new run.

Usage:
    python diff_baseline.py OLD.csv NEW.csv

Both CSVs use the schema produced by bench_optimizer*.py (columns scenario, mode,
rows, mean_s, sig, ...). For each (scenario, rows) the pushdown speedup is
no_opt_mean / opt_mean. The script prints old vs new speedups side by side and
flags where the optimizer's effect changed.

CAVEAT: when OLD is the Spark-API baseline and NEW is main's native-API run, the
ABSOLUTE timings are not comparable (different Python API + engine build). The
comparable quantity is the opt/no_opt SPEEDUP RATIO per scenario — that is what
the right-hand columns emphasize.
"""

import sys
import pandas as pd


def load(path):
    df = pd.read_csv(path)
    by_key = {}
    for _, r in df.iterrows():
        by_key.setdefault((str(r["scenario"]), int(r["rows"])), {})[str(r["mode"])] = r
    out = {}
    for key, modes in by_key.items():
        if "no_opt" in modes and "opt" in modes:
            no = float(modes["no_opt"]["mean_s"])
            op = float(modes["opt"]["mean_s"])
            out[key] = {
                "noopt_s": no,
                "opt_s": op,
                "speedup": (no / op) if op > 0 else float("inf"),
                "sig": str(modes["opt"].get("sig", "") or ""),
            }
    return out


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python diff_baseline.py OLD.csv NEW.csv")
    old_path, new_path = sys.argv[1], sys.argv[2]
    old, new = load(old_path), load(new_path)

    print(f"OLD = {old_path}")
    print(f"NEW = {new_path}")
    print("Speedup = no_opt_mean / opt_mean (pushdown gain).  '-' = scenario/size "
          "absent in that run.")
    print("NOTE: absolute s comparable only if same API+engine; speedup ratio is "
          "the cross-run-comparable metric.\n")

    hdr = (f"{'Scenario':24s} {'Rows':>9s} | {'OLD spd':>9s} {'OLD sig':>7s} "
           f"{'OLD noopt':>10s} | {'NEW spd':>9s} {'NEW sig':>7s} {'NEW noopt':>10s} | {'spd Δ':>8s}")
    print(hdr)
    print("-" * len(hdr))

    keys = sorted(set(old) | set(new), key=lambda k: (k[0], k[1]))
    for k in keys:
        o = old.get(k)
        n = new.get(k)
        scen, rows = k
        o_spd = f"{o['speedup']:.2f}x" if o else "-"
        o_sig = (o["sig"] if o else "")
        o_no = f"{o['noopt_s']:.4f}" if o else "-"
        n_spd = f"{n['speedup']:.2f}x" if n else "-"
        n_sig = (n["sig"] if n else "")
        n_no = f"{n['noopt_s']:.4f}" if n else "-"
        delta = f"{(n['speedup'] - o['speedup']):+.2f}" if (o and n) else "-"
        print(f"{scen:24s} {rows:9,d} | {o_spd:>9s} {o_sig:>7s} {o_no:>10s} | "
              f"{n_spd:>9s} {n_sig:>7s} {n_no:>10s} | {delta:>8s}")


if __name__ == "__main__":
    main()
