#!/usr/bin/env python3
"""Run OtterBrix Substrait exports through Apache DataFusion's Substrait consumer."""

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
    compatible_dir = samples_dir / "datafusion_consumer" / "compatible"

    expected_results: dict[str, list[dict]] = {}
    order_sensitive: set[str] = set()
    for case in read_json_samples(compatible_dir):
        name = case.get("name")
        if not name or "expected_rows" not in case:
            raise AssertionError(f"{case.get('_sample_file')}: compatible sample needs 'name' and 'expected_rows'")
        if name in expected_results:
            raise AssertionError(f"duplicate compatible consumer sample name: {name}")
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
    parser.add_argument(
        "--require-consumer",
        action="store_true",
        help="Fail instead of skipping when DataFusion/PyArrow is not installed.",
    )
    parser.add_argument(
        "--samples-dir",
        default=DEFAULT_SAMPLES_DIR,
        type=Path,
        help="Directory with JSON case manifests used by Substrait interop tests.",
    )
    return parser.parse_args()


def export_substrait_plans(exporter: Path, output_dir: Path) -> None:
    if not exporter.exists():
        raise RuntimeError(f"exporter binary does not exist: {exporter}")
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(exporter), str(output_dir)], check=True)


def register_table_aliases(ctx, name: str, batch) -> None:
    for alias in (name, f"db.{name}"):
        try:
            ctx.register_record_batches(alias, [[batch]])
        except Exception as exc:  # noqa: BLE001 - keep trying aliases.
            print(f"WARN: failed to register DataFusion table alias {alias!r}: {exc}", file=sys.stderr)


def make_datafusion_context():
    import pyarrow as pa
    from datafusion import SessionContext

    ctx = SessionContext()
    ctx.sql("CREATE SCHEMA IF NOT EXISTS db").collect()
    register_table_aliases(ctx, "coll", pa.RecordBatch.from_pydict({"id": pa.array([3, 1, 2], type=pa.int32())}))
    register_table_aliases(
        ctx,
        "left_table",
        pa.RecordBatch.from_pydict({"left_id": pa.array([10, 20], type=pa.int32())}),
    )
    register_table_aliases(
        ctx,
        "right_table",
        pa.RecordBatch.from_pydict({"right_id": pa.array([100, 200], type=pa.int32())}),
    )
    register_table_aliases(
        ctx,
        "sales",
        pa.RecordBatch.from_pydict(
            {
                "category": pa.array(["a", "a", "b"]),
                "amount": pa.array([10, 20, 5], type=pa.int32()),
            }
        ),
    )
    register_table_aliases(
        ctx,
        "metrics",
        pa.RecordBatch.from_pydict(
            {
                "metric_id": pa.array([1, 2, 3, 4], type=pa.int32()),
                "bucket": pa.array(["a", "a", "b", "b"]),
                "x": pa.array([10, 5, 8, 6], type=pa.int32()),
                "y": pa.array([3, 7, 8, 2], type=pa.int32()),
            }
        ),
    )
    return ctx


def consume_binary(ctx, binary_path: Path):
    from datafusion.substrait import Consumer, Serde

    plan = Serde.deserialize(binary_path)
    logical_plan = Consumer.from_substrait_plan(ctx, plan)
    return ctx.create_dataframe_from_logical_plan(logical_plan).collect()



def main() -> int:
    args = parse_args()
    require_module("pyarrow", args.require_consumer)
    require_module("datafusion", args.require_consumer)

    export_dir = args.work_dir / "exports"
    export_substrait_plans(args.exporter, export_dir)

    expected_results, order_sensitive = load_consumer_cases(args.samples_dir)
    compatible_exports = list(expected_results)

    comparisons = []
    for name in compatible_exports:
        ctx = make_datafusion_context()
        binary_batches = consume_binary(ctx, export_dir / f"{name}.bin")
        comparison = compare_rows(
            name=name,
            expected=expected_results[name],
            actual=rows_from_batches(binary_batches),
            order_sensitive=name in order_sensitive,
        )
        comparisons.append(comparison)
        if comparison["status"] == "passed":
            print(f"PASS DataFusion executed {name}.bin with expected rows")
        else:
            print(f"FAIL DataFusion executed {name}.bin with unexpected rows", file=sys.stderr)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report(
            "datafusion_consumer",
            comparisons,
            {
                "compatible_samples": len(compatible_exports),
            },
        ),
    )
    print(f"wrote comparison report {report_path}")

    for comparison in comparisons:
        require_match(comparison, "DataFusion consumer")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
