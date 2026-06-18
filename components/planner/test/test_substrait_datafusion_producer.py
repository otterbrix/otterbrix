#!/usr/bin/env python3
"""End-to-end checks for Substrait plans produced by external engines and imported by OtterBrix."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

from substrait_result_compare import build_report, compare_rows, require_match, rows_from_batches, write_report


SKIP_CODE = 77

DEFAULT_SAMPLES_DIR = Path(__file__).resolve().parent / "substrait_test_samples"


def load_producer_cases(samples_dir: Path) -> list[dict]:
    directory = samples_dir / "datafusion_producer"
    if not directory.is_dir():
        raise AssertionError(f"missing sample directory: {directory}")

    cases = []
    seen = set()
    for path in sorted(directory.glob("*.json")):
        with path.open() as file:
            case = json.load(file)
        if not case.get("name") or not case.get("sql") or "expected_rows" not in case:
            raise AssertionError(f"{path}: each sample must contain 'name', 'sql' and 'expected_rows' fields")
        if case["name"] in seen:
            raise AssertionError(f"duplicate DataFusion producer sample name: {case['name']}")
        seen.add(case["name"])
        cases.append(case)

    if not cases:
        raise AssertionError(f"{directory}: no JSON samples configured")
    return cases


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--importer", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument(
        "--require-consumer",
        action="store_true",
        help="Fail instead of skipping when DataFusion/PyArrow is unavailable.",
    )
    parser.add_argument(
        "--samples-dir",
        default=DEFAULT_SAMPLES_DIR,
        type=Path,
        help="Directory with JSON case manifests used by Substrait interop tests.",
    )
    return parser.parse_args()


def load_datafusion(require_consumer: bool):
    try:
        import pyarrow as pa
        from datafusion import SessionContext
        from datafusion.substrait import Producer

        return pa, SessionContext, Producer
    except Exception as exc:  # noqa: BLE001 - optional external producer dependency.
        message = f"DataFusion producer is unavailable: {exc}"
        if require_consumer:
            raise RuntimeError(message) from exc
        print(f"SKIP: {message}", file=sys.stderr)
        raise SystemExit(SKIP_CODE) from exc


def make_large_customers() -> list[dict]:
    regions = ["north", "south", "east", "west"]
    tiers = ["bronze", "silver", "gold"]
    return [
        {
            "customer_id": customer_id,
            "region": regions[customer_id % len(regions)],
            "tier": tiers[customer_id % len(tiers)],
        }
        for customer_id in range(200)
    ]


def make_large_orders() -> list[dict]:
    return [
        {
            "order_id": order_id,
            "customer_id": order_id % 200,
            "amount": ((order_id * 37) % 1000) + 1,
            "quantity": (order_id % 5) + 1,
        }
        for order_id in range(5000)
    ]


def make_datafusion_context(pa, SessionContext):
    ctx = SessionContext()
    tables = {
        "coll": (pa.schema([("id", pa.int32())]), [{"id": 3}, {"id": 1}, {"id": 2}]),
        "limit_table": (pa.schema([("id", pa.int32())]), [{"id": value} for value in range(12)]),
        "left_table": (pa.schema([("id", pa.int32())]), [{"id": 1}, {"id": 2}]),
        "right_table": (pa.schema([("id", pa.int32())]), [{"id": 2}, {"id": 3}]),
        "set_left": (pa.schema([("id", pa.int32())]), [{"id": 1}, {"id": 2}]),
        "set_right": (pa.schema([("id", pa.int32())]), [{"id": 2}, {"id": 3}]),
        "sales": (
            pa.schema([("category", pa.string()), ("amount", pa.int32())]),
            [{"category": "a", "amount": 10}, {"category": "a", "amount": 20}, {"category": "b", "amount": 5}],
        ),
        "large_customers": (
            pa.schema([("customer_id", pa.int32()), ("region", pa.string()), ("tier", pa.string())]),
            make_large_customers(),
        ),
        "large_orders": (
            pa.schema(
                [
                    ("order_id", pa.int32()),
                    ("customer_id", pa.int32()),
                    ("amount", pa.int32()),
                    ("quantity", pa.int32()),
                ]
            ),
            make_large_orders(),
        ),
    }

    for table_name, (schema, rows) in tables.items():
        table = pa.Table.from_pylist(rows, schema=schema)
        ctx.register_record_batches(table_name, [table.to_batches()])
    return ctx


def compare_query_result(case: dict, batches) -> dict:
    return compare_rows(
        name=case["name"],
        expected=case["expected_rows"],
        actual=rows_from_batches(batches),
        order_sensitive=case.get("order_sensitive", False),
    )



def export_datafusion_plans(pa, SessionContext, Producer, output_dir: Path, cases: list[dict]) -> list[dict]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = make_datafusion_context(pa, SessionContext)
    comparisons = []
    for case in cases:
        name = case["name"]
        sql = case["sql"]
        path = output_dir / f"{name}.bin"
        path.unlink(missing_ok=True)
        dataframe = ctx.sql(sql)
        comparison = compare_query_result(case, dataframe.collect())
        comparison["sql"] = sql
        comparisons.append(comparison)
        if comparison["status"] == "passed":
            print(f"PASS DataFusion SQL {name} returned expected rows")
        else:
            print(f"FAIL DataFusion SQL {name} returned unexpected rows", file=sys.stderr)

        plan = Producer.to_substrait_plan(dataframe.logical_plan(), ctx)
        path.write_bytes(plan.encode())
        if not path.exists() or path.stat().st_size == 0:
            raise AssertionError(f"DataFusion did not write a valid Substrait plan: {path}")
        print(f"PASS DataFusion export {name}.bin: {sql}")
    return comparisons


def import_with_otterbrix(importer: Path, export_dir: Path) -> None:
    if not importer.exists():
        raise RuntimeError(f"importer binary does not exist: {importer}")
    subprocess.run([str(importer), str(export_dir)], check=True)


def main() -> int:
    args = parse_args()
    pa, SessionContext, Producer = load_datafusion(args.require_consumer)

    export_dir = args.work_dir / "external-producer-exports"
    cases = load_producer_cases(args.samples_dir)
    comparisons = export_datafusion_plans(pa, SessionContext, Producer, export_dir, cases)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report("datafusion_producer", comparisons, {"samples": len(cases)}),
    )
    print(f"wrote comparison report {report_path}")

    for comparison in comparisons:
        require_match(comparison, "DataFusion SQL")

    import_with_otterbrix(args.importer, export_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
