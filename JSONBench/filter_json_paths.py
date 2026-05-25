#!/usr/bin/env python3
"""
Filters JSONL file to keep only JSON paths used in JSONBench queries.

Used paths (from test_jsonbench.cpp):
  - did
  - time_us
  - kind
  - commit.operation
  - commit.collection

Usage:
  python3 filter_json_paths.py input.json output.json
"""

import json
import sys


KEEP_PATHS = {
    "did",
    "time_us",
    "kind",
}

KEEP_NESTED = {
    "commit": {"operation", "collection"},
}


def filter_doc(doc: dict) -> dict:
    result = {}
    for key in KEEP_PATHS:
        if key in doc:
            result[key] = doc[key]
    for parent_key, children in KEEP_NESTED.items():
        if parent_key in doc and isinstance(doc[parent_key], dict):
            filtered_parent = {}
            for child_key in children:
                if child_key in doc[parent_key]:
                    filtered_parent[child_key] = doc[parent_key][child_key]
            if filtered_parent:
                result[parent_key] = filtered_parent
    return result


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} input.json output.json")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    count = 0
    with open(input_path, "r") as fin, open(output_path, "w") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            filtered = filter_doc(doc)
            fout.write(json.dumps(filtered, separators=(",", ":")) + "\n")
            count += 1

    print(f"Filtered {count} documents -> {output_path}")

    # Show sample
    with open(output_path, "r") as f:
        sample = f.readline().strip()
    print(f"Sample: {sample}")

    # Show size comparison
    import os
    in_size = os.path.getsize(input_path) / (1024 * 1024)
    out_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Size: {in_size:.1f} MB -> {out_size:.1f} MB ({out_size/in_size*100:.0f}%)")


if __name__ == "__main__":
    main()
