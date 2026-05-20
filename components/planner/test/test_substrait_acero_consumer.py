#!/usr/bin/env python3
"""Run OtterBrix Substrait exports through Apache Arrow Acero and compare result rows."""

from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

from substrait_result_compare import build_report, compare_rows, require_match, rows_from_batches, write_report


SKIP_CODE = 77
DEFAULT_SAMPLES_DIR = Path(__file__).resolve().parent / "substrait_test_samples"


def read_json_samples(directory: Path) -> list[dict]:
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


def load_consumer_cases(samples_dir: Path) -> tuple[dict[str, list[dict]], set[str]]:
    compatible_dir = samples_dir / "acero_consumer" / "compatible"
    expected_results: dict[str, list[dict]] = {}
    order_sensitive: set[str] = set()
    for case in read_json_samples(compatible_dir):
        name = case.get("name")
        if not name or "expected_rows" not in case:
            raise AssertionError(f"{case.get('_sample_file')}: compatible sample needs 'name' and 'expected_rows'")
        if name in expected_results:
            raise AssertionError(f"duplicate Acero consumer sample name: {name}")
        expected_results[name] = case["expected_rows"]
        if case.get("order_sensitive", False):
            order_sensitive.add(name)

    return expected_results, order_sensitive


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
        help="Fail instead of skipping when PyArrow/Acero is not installed.",
    )
    return parser.parse_args()


def export_substrait_plans(exporter: Path, output_dir: Path) -> None:
    if not exporter.exists():
        raise RuntimeError(f"exporter binary does not exist: {exporter}")
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(exporter), str(output_dir)], check=True)


def make_arrow_tables():
    import pyarrow as pa

    coll = pa.Table.from_pydict({"id": pa.array([3, 1, 2], type=pa.int32())})
    left_table = pa.Table.from_pydict({"left_id": pa.array([10, 20], type=pa.int32())})
    right_table = pa.Table.from_pydict({"right_id": pa.array([100, 200], type=pa.int32())})
    sales = pa.Table.from_pydict(
        {
            "category": pa.array(["a", "a", "b"]),
            "amount": pa.array([10, 20, 5], type=pa.int32()),
        }
    )
    metrics = pa.Table.from_pydict(
        {
            "metric_id": pa.array([1, 2, 3, 4], type=pa.int32()),
            "bucket": pa.array(["a", "a", "b", "b"]),
            "x": pa.array([10, 5, 8, 6], type=pa.int32()),
            "y": pa.array([3, 7, 8, 2], type=pa.int32()),
        }
    )
    return {
        ("coll",): coll,
        ("db", "coll"): coll,
        ("left_table",): left_table,
        ("db", "left_table"): left_table,
        ("right_table",): right_table,
        ("db", "right_table"): right_table,
        ("sales",): sales,
        ("db", "sales"): sales,
        ("metrics",): metrics,
        ("db", "metrics"): metrics,
    }


def make_table_provider():
    tables = make_arrow_tables()

    def table_provider(names, schema):
        key = tuple(names)
        if key not in tables:
            raise KeyError(f"Acero table provider has no table for {key}; expected schema={schema}")
        return tables[key]

    return table_provider


def consume_binary(binary_path: Path) -> list[dict]:
    import pyarrow.substrait as substrait

    reader = substrait.run_query(binary_path.read_bytes(), table_provider=make_table_provider(), use_threads=False)
    return rows_from_batches(reader.read_all().to_batches())


def pyarrow_version() -> str:
    import pyarrow as pa

    return pa.__version__


def main() -> int:
    args = parse_args()
    require_module("pyarrow", args.require_consumer)
    require_module("pyarrow.substrait", args.require_consumer)

    export_dir = args.work_dir / "exports"
    export_substrait_plans(args.exporter, export_dir)

    expected_results, order_sensitive = load_consumer_cases(args.samples_dir)
    compatible_exports = list(expected_results)

    comparisons = []
    for name in compatible_exports:
        actual_rows = consume_binary(export_dir / f"{name}.bin")
        comparison = compare_rows(
            name=name,
            expected=expected_results[name],
            actual=actual_rows,
            order_sensitive=name in order_sensitive,
        )
        comparisons.append(comparison)
        if comparison["status"] == "passed":
            print(f"PASS Apache Arrow Acero executed {name}.bin with expected rows")
        else:
            print(f"FAIL Apache Arrow Acero executed {name}.bin with unexpected rows", file=sys.stderr)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report(
            "acero_consumer",
            comparisons,
            {
                "compatible_samples": len(compatible_exports),
                "pyarrow_version": pyarrow_version(),
            },
        ),
    )
    print(f"wrote comparison report {report_path}")

    for comparison in comparisons:
        require_match(comparison, "Apache Arrow Acero consumer")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
