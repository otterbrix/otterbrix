#!/usr/bin/env python3
"""Shared row-result comparison helpers for Substrait interop tests."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any


def rows_from_batches(batches) -> list[dict]:
    rows = []
    for batch in batches:
        rows.extend(batch.to_pylist())
    return rows


def row_key(row: dict) -> str:
    return json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def normalize_rows(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=row_key)


def unordered_diff(expected: list[dict], actual: list[dict]) -> dict[str, list[dict]]:
    expected_counter = Counter(row_key(row) for row in expected)
    actual_counter = Counter(row_key(row) for row in actual)

    missing = []
    unexpected = []
    for encoded, count in (expected_counter - actual_counter).items():
        missing.extend(json.loads(encoded) for _ in range(count))
    for encoded, count in (actual_counter - expected_counter).items():
        unexpected.extend(json.loads(encoded) for _ in range(count))
    return {"missing_rows": missing, "unexpected_rows": unexpected}


def first_ordered_mismatch(expected: list[dict], actual: list[dict]) -> dict[str, Any] | None:
    for index, (expected_row, actual_row) in enumerate(zip(expected, actual)):
        if expected_row != actual_row:
            return {"index": index, "expected": expected_row, "actual": actual_row}
    if len(expected) != len(actual):
        return {
            "index": min(len(expected), len(actual)),
            "expected": expected[min(len(expected), len(actual)) :] or None,
            "actual": actual[min(len(expected), len(actual)) :] or None,
        }
    return None


def compare_rows(name: str, expected: list[dict], actual: list[dict], order_sensitive: bool) -> dict[str, Any]:
    if order_sensitive:
        matched = actual == expected
        diff = {"first_mismatch": first_ordered_mismatch(expected, actual)} if not matched else {}
    else:
        matched = normalize_rows(actual) == normalize_rows(expected)
        diff = unordered_diff(expected, actual) if not matched else {}

    return {
        "name": name,
        "status": "passed" if matched else "failed",
        "order_sensitive": order_sensitive,
        "expected_row_count": len(expected),
        "actual_row_count": len(actual),
        "expected_rows": expected,
        "actual_rows": actual,
        **diff,
    }


def require_match(comparison: dict[str, Any], context: str) -> None:
    if comparison["status"] == "passed":
        return
    details = {
        key: comparison[key]
        for key in ("expected_row_count", "actual_row_count", "first_mismatch", "missing_rows", "unexpected_rows")
        if key in comparison
    }
    raise AssertionError(
        f"{comparison['name']}: {context} result mismatch: "
        f"{json.dumps(details, ensure_ascii=False, sort_keys=True)}"
    )


def build_report(kind: str, comparisons: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    failed = [comparison for comparison in comparisons if comparison["status"] != "passed"]
    report = {
        "kind": kind,
        "status": "passed" if not failed else "failed",
        "total": len(comparisons),
        "passed": len(comparisons) - len(failed),
        "failed": len(failed),
        "comparisons": comparisons,
    }
    if extra:
        report.update(extra)
    return report


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
