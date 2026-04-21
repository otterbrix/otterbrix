#!/usr/bin/env python3
"""
Сравнение времени выполнения: duckdb / mongodb / otterbrix.
Запуск: python3 bench_compare.py
"""

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Данные ───────────────────────────────────────────────────────────────────
labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]

duckdb    = [75,  306, 262, 241, 211]
mongodb   = [361, 454, 420,  72,  89]
otterbrix = [21,  333, 273,  77, 167]

unit  = "ms"
title = "JSONBench (500к документов)"
# ─────────────────────────────────────────────────────────────────────────────

x     = np.arange(len(labels))
width = 0.25

fig, ax = plt.subplots(figsize=(10, 5))

bars_duckdb    = ax.bar(x + width, duckdb,    width, label="duckdb",
                        color="#D64C18", edgecolor="white", linewidth=0.8)
bars_mongodb   = ax.bar(x,         mongodb,   width, label="mongodb",
                        color="#763A9F", edgecolor="white", linewidth=0.8)
bars_otterbrix = ax.bar(x - width, otterbrix, width, label="otterbrix",
                        color="#55A868", edgecolor="white", linewidth=0.8)

all_vals = duckdb + mongodb + otterbrix
y_max = max(all_vals)

# Значения над столбцами
for bars, vals, color in [
    (bars_duckdb,    duckdb,    "#D64C18"),
    (bars_mongodb,   mongodb,   "#763A9F"),
    (bars_otterbrix, otterbrix, "#55A868"),
]:
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2,
                val + y_max * 0.01,
                f"{val}", ha="center", va="bottom", fontsize=8, color=color)

ax.set_title(title, fontsize=14, pad=16)
ax.set_ylabel(f"Время, {unit}", fontsize=11)
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=11)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v)}"))
ax.legend(fontsize=10)
ax.spines[["top", "right"]].set_visible(False)
ax.set_ylim(0, y_max * 1.18)
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)

plt.tight_layout()
out = "bench_compare.png"
plt.savefig(out, dpi=150)
print(f"Saved: {out}")
