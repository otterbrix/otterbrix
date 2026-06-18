#!/usr/bin/env python3
"""Verify that Apache DataFusion can deserialize OtterBrix write-node Substrait plans."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from substrait_result_compare import build_report, write_report


SKIP_CODE = 77
DEFAULT_SAMPLES_DIR = Path(__file__).resolve().parent / "substrait_test_samples"


def read_json_samples(directory: Path) -> list[dict[str, Any]]:
    if not directory.is_dir():
        raise AssertionError(f"missing sample directory: {directory}")
    samples = []
    for path in sorted(directory.glob("*.json")):
        with path.open() as file:
            sample = json.load(file)
        sample.setdefault("_sample_file", str(path))
        samples.append(sample)
    if not samples:
        raise AssertionError(f"{directory}: no JSON samples configured")
    return samples


def load_write_cases(samples_dir: Path) -> list[dict[str, Any]]:
    compatible_dir = samples_dir / "datafusion_write_consumer" / "compatible"
    seen = set()
    cases = []
    for case in read_json_samples(compatible_dir):
        name = case.get("name")
        if not name:
            raise AssertionError(f"{case.get('_sample_file')}: write sample needs 'name'")
        if name in seen:
            raise AssertionError(f"duplicate DataFusion write consumer sample name: {name}")
        seen.add(name)
        cases.append(case)
    return cases


def skip_or_fail(message: str, require_consumer: bool) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1 if require_consumer else SKIP_CODE)


def require_module(name: str, require_consumer: bool) -> None:
    if importlib.util.find_spec(name) is None:
        skip_or_fail(f"SKIP: Python module '{name}' is not installed", require_consumer)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--samples-dir", default=DEFAULT_SAMPLES_DIR, type=Path)
    parser.add_argument(
        "--require-consumer",
        action="store_true",
        help="Fail instead of skipping when DataFusion is not installed.",
    )
    return parser.parse_args()


def export_substrait_plans(exporter: Path, output_dir: Path) -> None:
    if not exporter.exists():
        raise RuntimeError(f"exporter binary does not exist: {exporter}")
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(exporter), str(output_dir)], check=True)


def root_input(plan_json: dict[str, Any]) -> dict[str, Any]:
    return plan_json["relations"][0]["root"]["input"]


def require_json_shape(case: dict[str, Any], json_path: Path) -> dict[str, Any]:
    with json_path.open() as file:
        payload = json.load(file)
    input_rel = root_input(payload)
    relation = case["expected_relation"]
    if relation not in input_rel:
        raise AssertionError(f"{case['name']}: expected root input relation {relation!r}, got {sorted(input_rel)}")

    rel = input_rel[relation]
    table = rel.get("namedTable", {}).get("names", [])
    if table != case.get("expected_table", []):
        raise AssertionError(f"{case['name']}: expected table {case.get('expected_table')}, got {table}")

    if relation == "write":
        operation = rel.get("op")
        if operation != case.get("expected_operation"):
            raise AssertionError(f"{case['name']}: expected write op {case.get('expected_operation')}, got {operation}")
    elif relation == "update":
        schema_names = rel.get("tableSchema", {}).get("names", [])
        expected_columns = case.get("expected_updated_columns", [])
        for column in expected_columns:
            if column not in schema_names:
                raise AssertionError(f"{case['name']}: expected updated column {column!r} in schema {schema_names}")
    return {
        "relation": relation,
        "table": table,
        "operation": rel.get("op"),
        "schema_names": rel.get("tableSchema", {}).get("names", []),
    }


def datafusion_roundtrip(binary_path: Path) -> int:
    from datafusion.substrait import Serde

    plan = Serde.deserialize(binary_path)
    encoded = plan.encode()
    if not encoded:
        raise AssertionError(f"{binary_path.name}: DataFusion re-encoded empty Substrait plan")
    # Re-deserialize the DataFusion-owned encoding to verify the external serde can roundtrip it.
    roundtrip_path = binary_path.with_suffix(".datafusion-roundtrip.bin")
    roundtrip_path.write_bytes(encoded)
    Serde.deserialize(roundtrip_path)
    return len(encoded)


def main() -> int:
    args = parse_args()
    require_module("datafusion", args.require_consumer)

    export_dir = args.work_dir / "exports"
    export_substrait_plans(args.exporter, export_dir)

    comparisons = []
    for case in load_write_cases(args.samples_dir):
        name = case["name"]
        try:
            shape = require_json_shape(case, export_dir / f"{name}.json")
            reencoded_size = datafusion_roundtrip(export_dir / f"{name}.bin")
            comparisons.append(
                {
                    "name": name,
                    "status": "passed",
                    "consumer": "datafusion.substrait.Serde",
                    "format": "proto",
                    "reencoded_size_bytes": reencoded_size,
                    **shape,
                }
            )
            print(f"PASS DataFusion deserialized and re-encoded {name}.bin")
        except Exception as exc:  # noqa: BLE001 - report all interop failures uniformly.
            comparisons.append(
                {
                    "name": name,
                    "status": "failed",
                    "consumer": "datafusion.substrait.Serde",
                    "format": "proto",
                    "error": str(exc),
                }
            )
            print(f"FAIL DataFusion write-node serde for {name}.bin: {exc}", file=sys.stderr)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report(
            "datafusion_write_consumer",
            comparisons,
            {
                "compatible_samples": len(comparisons),
            },
        ),
    )
    print(f"wrote comparison report {report_path}")

    failed = [comparison for comparison in comparisons if comparison["status"] != "passed"]
    if failed:
        raise AssertionError(f"DataFusion write consumer failures: {json.dumps(failed, ensure_ascii=False)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
