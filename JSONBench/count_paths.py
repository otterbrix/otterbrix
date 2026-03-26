#!/usr/bin/env python3
import json
import sys
from collections import defaultdict

DATA_FILE = "file_0001.json"
N_ROWS = 100_000
THRESHOLD = 10_000


def flatten(obj, prefix=""):
    out = {}
    for k, v in obj.items():
        path = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(flatten(v, path))
        elif v is not None:
            out[path] = v
    return out


counts = defaultdict(int)
total = 0

with open(DATA_FILE) as f:
    for line in f:
        if total >= N_ROWS:
            break
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        for path in flatten(row):
            counts[path] += 1
        total += 1

print(f"Rows parsed: {total}")
print(f"Unique paths: {len(counts)}")
print()

dense  = {p: c for p, c in counts.items() if c >= THRESHOLD}
sparse = {p: c for p, c in counts.items() if c <  THRESHOLD}

print(f"Dense  (>= {THRESHOLD}): {len(dense)}")
print(f"Sparse (<  {THRESHOLD}): {len(sparse)}")
print()

print("=== DENSE paths ===")
for p, c in sorted(dense.items(), key=lambda x: -x[1]):
    print(f"  {c:>7}  {p}")

print()
print("=== SPARSE paths (sorted by count desc) ===")
for p, c in sorted(sparse.items(), key=lambda x: -x[1]):
    print(f"  {c:>7}  {p}")
