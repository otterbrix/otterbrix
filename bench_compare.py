#!/usr/bin/env python3
"""
Сравнение времени выполнения: 5 пар чисел (до / после).
Запуск: python3 bench_compare.py
"""

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Данные ───────────────────────────────────────────────────────────────────
labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]

before = [32,  460, 520, 86,  101]   # синяя колонка
after  = [18,  183, 38,  39,  42]   # оранжевая колонка

unit = "ms"
title = "Время выполнения запросов (100к документов)"
legend_before = "Otterbrix"
legend_after  = "Posgresql"
# ─────────────────────────────────────────────────────────────────────────────

x = np.arange(len(labels))
width = 0.35

fig, ax = plt.subplots(figsize=(9, 5))

bars_before = ax.bar(x - width / 2, before, width, label=legend_before,
                     color="#4C72B0", edgecolor="white", linewidth=0.8)
bars_after  = ax.bar(x + width / 2, after,  width, label=legend_after,
                     color="#DD8452", edgecolor="white", linewidth=0.8)

# Значения над столбцами
for bar in bars_before:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + max(before + after) * 0.01,
            f"{h}", ha="center", va="bottom", fontsize=9, color="#4C72B0")

for bar in bars_after:
    h = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, h + max(before + after) * 0.01,
            f"{h}", ha="center", va="bottom", fontsize=9, color="#DD8452")

# Speedup над парой
for i, (b, a) in enumerate(zip(before, after)):
    speedup = b / a
    ax.text(x[i], max(b, a) + max(before + after) * 0.06,
            f"×{speedup:.1f}", ha="center", va="bottom",
            fontsize=9, color="#444", fontweight="bold")

ax.set_title(title, fontsize=14, pad=16)
ax.set_ylabel(f"Время, {unit}", fontsize=11)
ax.set_xticks(x)
ax.set_xticklabels(labels, fontsize=11)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{int(v)}"))
ax.legend(fontsize=10)
ax.spines[["top", "right"]].set_visible(False)
ax.set_ylim(0, max(before + after) * 1.22)
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)

plt.tight_layout()
out = "bench_compare.png"
plt.savefig(out, dpi=150)
print(f"Saved: {out}")
