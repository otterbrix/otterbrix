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

duckdb    = [3.5,  4.5, 4.6, 8.3, 8.9]
mongodb   = [0.486,  1.133, 1.498,  0.582, 0.719]
otterbrix = [0.021,  0.333, 0.273,  0.077, 0.167]

unit  = "sec"
title = "Время выполнения запросов (500к документов)"
# ─────────────────────────────────────────────────────────────────────────────

x     = np.arange(len(labels))
width = 0.25

fig, ax = plt.subplots(figsize=(10, 5))

bars_duckdb    = ax.bar(x - width, duckdb,    width, label="old",
                        color="#4C72B0", edgecolor="white", linewidth=0.8)
bars_mongodb   = ax.bar(x,         mongodb,   width, label="new",
                        color="#DD8452", edgecolor="white", linewidth=0.8)
bars_otterbrix = ax.bar(x + width, otterbrix, width, label="new optimized",
                        color="#55A868", edgecolor="white", linewidth=0.8)

all_vals = duckdb + mongodb + otterbrix
y_max = max(all_vals)

# Значения над столбцами
for bars, vals, color in [
    (bars_duckdb,    duckdb,    "#4C72B0"),
    (bars_mongodb,   mongodb,   "#DD8452"),
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
