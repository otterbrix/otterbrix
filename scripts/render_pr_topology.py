#!/usr/bin/env python3
import subprocess
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

REPO = "/Users/seliverstow/Desktop/diploma33/otterbrix"
OUT = "/Users/seliverstow/Desktop/diploma33/otterbrix-branch-topology.png"


def git(*args):
    return subprocess.check_output(["git", "-C", REPO] + list(args), text=True).strip()


def count_from_main(branch):
    return git("rev-list", "--count", f"origin/main..{branch}")


NODES = {
    "main": {"branch": "origin/main", "pr": None, "color": "#444444", "x": 0, "y": 0},
    "staging": {
        "branch": "feat/pythonpkg-integration-staging",
        "pr": ("#13", "staging PR", "closed"),
        "color": "#777777",
        "x": 0,
        "y": 3,
    },
    "copy": {
        "branch": "feat/pythonpkg-integration-copy",
        "pr": ("#10", "simple copy", "open"),
        "color": "#888888",
        "x": 0,
        "y": 6,
    },
    "gitmv": {
        "branch": "feat/pythonpkg-integration-git-mv",
        "pr": ("#11", "simple git mv", "open"),
        "color": "#999999",
        "x": 0,
        "y": 9,
    },
    "int1": {
        "branch": "feat/pythonpkg-integration-1",
        "pr": ("#12", "main", "open"),
        "color": "#D4652A",
        "x": 0,
        "y": 12,
    },
    "int2": {
        "branch": "feat/pythonpkg-integration-2",
        "pr": ("#14", "main 2", "draft"),
        "color": "#2E9E5E",
        "x": -10,
        "y": 16,
    },
    "int3": {
        "branch": "feat/pythonpkg-integration-3",
        "pr": ("#15", "update LICENSE", "draft"),
        "color": "#C0392B",
        "x": 0,
        "y": 16,
    },
    "int4": {
        "branch": "feat/pythonpkg-integration-4",
        "pr": ("#17", "main 3", "draft"),
        "color": "#B8941E",
        "x": 10,
        "y": 16,
    },
    "reuse": {
        "branch": "feat/reuse-toml",
        "pr": ("#16", "REUSE.toml", "merged"),
        "color": "#6B52A8",
        "x": 0,
        "y": 20,
    },
}

STATUS_STYLE = {
    "open": ("#E8F5E9", "#2E7D32"),
    "draft": ("#FFF8E1", "#F57F17"),
    "merged": ("#E3F2FD", "#1565C0"),
    "closed": ("#FFEBEE", "#C62828"),
}


def pos(key):
    return NODES[key]["x"], NODES[key]["y"]


def draw_segment(x1, y1, x2, y2, color, lw=4.0):
    ax.plot([x1, x2], [y1, y2], color=color, lw=lw, solid_capstyle="round", zorder=2)


def draw_arrow(x1, y1, x2, y2, color, lw=4.0):
    ax.add_patch(
        FancyArrowPatch(
            (x1, y1),
            (x2, y2),
            arrowstyle="-|>",
            mutation_scale=24,
            lw=lw,
            color=color,
            shrinkA=0,
            shrinkB=0,
            zorder=3,
        )
    )


def label_pr(x, y, pr, color):
    ax.text(
        x,
        y,
        pr,
        fontsize=12,
        fontweight="bold",
        ha="center",
        va="center",
        color=color,
        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor=color, alpha=0.98),
        zorder=9,
    )


def trunk_edge(head, base, pr, color):
    xh, yh = pos(head)
    xb, yb = pos(base)
    draw_arrow(xh, yh - 0.65, xb, yb + 0.65, color)
    label_pr(1.0, (yh + yb) / 2, pr, color)


def side_edge(head, base, pr, color, entry_y):
    xh, yh = pos(head)
    xb, yb = pos(base)
    draw_segment(xh, yh - 0.65, xh, entry_y, color)
    draw_segment(xh, entry_y, xb, entry_y, color)
    draw_arrow(xb, entry_y, xb, yb + 0.65, color)
    label_pr((xh + xb) / 2, entry_y + 0.45, pr, color)


fig, ax = plt.subplots(figsize=(24, 18))
ax.set_xlim(-15, 17)
ax.set_ylim(-2, 23)
ax.axis("off")
ax.set_title("otterbrix: цепочка PR (head → base)", fontsize=20, pad=22, fontweight="bold")

ax.plot([0, 0], [-0.2, 12.0], color="#ECECEC", lw=12, zorder=0, solid_capstyle="round")

trunk_edge("staging", "main", "#13", "#777777")
trunk_edge("copy", "staging", "#10", "#888888")
trunk_edge("gitmv", "copy", "#11", "#999999")
trunk_edge("int1", "gitmv", "#12", "#D4652A")

side_edge("int2", "int1", "#14", "#2E9E5E", entry_y=12.85)
trunk_edge("int3", "int1", "#15", "#C0392B")
side_edge("int4", "int1", "#17", "#B8941E", entry_y=12.15)
trunk_edge("reuse", "int3", "#16", "#6B52A8")

for key, nd in NODES.items():
    x, y = nd["x"], nd["y"]
    color = nd["color"]
    branch = nd["branch"]
    short = branch.replace("feat/", "").replace("origin/", "origin/")
    cnt = "0" if key == "main" else count_from_main(branch)
    sha = git("rev-parse", branch)[:7]

    ax.scatter([x], [y], s=820, c=color, zorder=6, edgecolors="white", linewidths=4)

    bx = x - 5.3 if x < -3 else x + 1.5
    by = y - 1.2
    bh = 2.55 if nd["pr"] else 2.05
    bw = 4.9
    ax.add_patch(
        FancyBboxPatch(
            (bx, by),
            bw,
            bh,
            boxstyle="round,pad=0.12,rounding_size=0.18",
            facecolor="white",
            edgecolor=color,
            linewidth=2.5,
            zorder=5,
        )
    )
    tx = bx + bw / 2
    ax.text(tx, by + bh - 0.45, short, ha="center", va="center", fontsize=12, fontweight="bold", color=color, zorder=8)
    ax.text(tx, by + bh - 0.95, f"{sha}  (+{cnt} от origin/main)", ha="center", va="center", fontsize=9, color="#777777", zorder=8)
    if nd["pr"]:
        pr_num, title, status = nd["pr"]
        bg, fg = STATUS_STYLE[status]
        ax.text(tx, by + 0.65, f"PR {pr_num}: {title}", ha="center", va="center", fontsize=9.5, color="#444444", zorder=8)
        ax.text(
            tx,
            by + 0.2,
            status,
            ha="center",
            va="center",
            fontsize=9,
            fontweight="bold",
            color=fg,
            zorder=8,
            bbox=dict(boxstyle="round,pad=0.25", facecolor=bg, edgecolor=fg, linewidth=1.2),
        )

legend_items = [
    mpatches.Patch(color="#444444", label="origin/main"),
    mpatches.Patch(facecolor="#E8F5E9", edgecolor="#2E7D32", label="PR open"),
    mpatches.Patch(facecolor="#FFF8E1", edgecolor="#F57F17", label="PR draft"),
    mpatches.Patch(facecolor="#E3F2FD", edgecolor="#1565C0", label="PR merged"),
    mpatches.Patch(facecolor="#FFEBEE", edgecolor="#C62828", label="PR closed"),
]
ax.legend(handles=legend_items, loc="lower right", fontsize=10, frameon=True)
ax.text(
    -14.5,
    -1.3,
    "Стрелка: head → base  |  PR #13 закрыт без merge  |  github.com/prawwtocol/otterbrix/pulls",
    fontsize=10,
    color="#888888",
)

plt.tight_layout()
plt.savefig(OUT, dpi=170, bbox_inches="tight", facecolor="white")
print(f"Saved: {OUT}")
