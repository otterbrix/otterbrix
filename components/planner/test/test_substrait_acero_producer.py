#!/usr/bin/env python3
"""Producer-surface checks for Apache Arrow Acero/PyArrow Substrait serialization.

PyArrow 21 exposes Substrait producers for schemas and bound expressions, but not
for a full Acero execution plan.  This test validates the public producer surface
that exists today and records the full-plan producer availability in the report.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

from substrait_result_compare import build_report, require_match, write_report


SKIP_CODE = 77
DEFAULT_SAMPLES_DIR = Path(__file__).resolve().parent / "substrait_test_samples"


def skip_or_fail(message: str, require_consumer: bool) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1 if require_consumer else SKIP_CODE)


def require_module(name: str, require_consumer: bool) -> None:
    if importlib.util.find_spec(name) is None:
        skip_or_fail(f"SKIP: Python module '{name}' is not installed", require_consumer)


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--samples-dir", default=DEFAULT_SAMPLES_DIR, type=Path)
    parser.add_argument(
        "--require-producer",
        action="store_true",
        help="Fail instead of skipping when PyArrow/Acero producer dependencies are unavailable.",
    )
    return parser.parse_args()


def schemas():
    import pyarrow as pa

    return {
        "schema_coll": pa.schema([("id", pa.int32())]),
        "schema_metrics": pa.schema(
            [
                ("metric_id", pa.int32()),
                ("bucket", pa.string()),
                ("x", pa.int32()),
                ("y", pa.int32()),
            ]
        ),
        "schema_sales": pa.schema([("category", pa.string()), ("amount", pa.int32())]),
        "schema_large_orders": pa.schema(
            [
                ("order_id", pa.int32()),
                ("customer_id", pa.int32()),
                ("amount", pa.int32()),
                ("quantity", pa.int32()),
            ]
        ),
        "schema_join_tables": pa.schema(
            [
                ("left_id", pa.int32()),
                ("right_id", pa.int32()),
                ("left_value", pa.int32()),
                ("right_value", pa.int32()),
            ]
        ),
    }


def expression_cases():
    import pyarrow as pa
    import pyarrow.acero as acero

    metrics_schema = pa.schema(
        [
            ("metric_id", pa.int32()),
            ("bucket", pa.string()),
            ("x", pa.int32()),
            ("y", pa.int32()),
        ]
    )
    sales_schema = pa.schema([("category", pa.string()), ("amount", pa.int32())])
    coll_schema = pa.schema([("id", pa.int32())])

    x = acero.field("x")
    y = acero.field("y")
    amount = acero.field("amount")
    id_field = acero.field("id")
    quantity = acero.field("quantity")

    return {
        "expr_project_math": {
            "schema": metrics_schema,
            "names": ["metric_id", "sum_xy", "delta_xy", "product_xy"],
            "exprs": [acero.field("metric_id"), x + y, x - y, x * y],
        },
        "expr_predicates": {
            "schema": metrics_schema,
            "names": ["x_gt_y", "x_gte_y", "x_lte_y"],
            "exprs": [x > y, x >= y, x <= y],
        },
        "expr_boolean_filters": {
            "schema": metrics_schema,
            "names": ["x_gt_or_lt_y", "x_gte_and_gt_y"],
            "exprs": [(x > y) | (x < y), (x >= y) & (x > y)],
        },
        "expr_sales_amounts": {
            "schema": sales_schema,
            "names": ["category", "amount_double", "amount_squared", "amount_self_gte"],
            "exprs": [acero.field("category"), amount + amount, amount * amount, amount >= amount],
        },
        "expr_coll_projection_sort_key": {
            "schema": coll_schema,
            "names": ["id", "id_plus_id", "id_gt_id"],
            "exprs": [id_field, id_field + id_field, id_field > id_field],
        },
        "expr_nested_arithmetic": {
            "schema": pa.schema(
                [
                    ("order_id", pa.int32()),
                    ("customer_id", pa.int32()),
                    ("amount", pa.int32()),
                    ("quantity", pa.int32()),
                ]
            ),
            "names": ["gross_amount", "amount_plus_quantity", "gross_gt_amount"],
            "exprs": [amount * quantity, amount + quantity, (amount * quantity) > amount],
        },
        "expr_sales_predicates": {
            "schema": sales_schema,
            "names": ["amount_self_gte", "amount_self_lte", "amount_self_lt"],
            "exprs": [amount >= amount, amount <= amount, amount < amount],
        },
    }


def full_plan_producer_available() -> bool:
    import pyarrow.substrait as substrait

    candidate_names = (
        "serialize_plan",
        "to_substrait_plan",
        "to_substrait",
        "serialize_declaration",
        "serialize_exec_plan",
    )
    return any(hasattr(substrait, name) for name in candidate_names)


def check_schema_case(name: str) -> dict:
    import pyarrow.substrait as substrait

    schema = schemas()[name]
    encoded = substrait.serialize_schema(schema)
    decoded = substrait.deserialize_schema(encoded)
    ok = decoded.equals(schema, check_metadata=False)
    result = {
        "name": name,
        "status": "passed" if ok else "failed",
        "serialized_bytes": len(encoded.schema),
        "fields": decoded.names,
    }
    if not ok:
        result["message"] = f"schema mismatch: expected {schema}, actual {decoded}"
    elif result["serialized_bytes"] <= 0:
        result["status"] = "failed"
        result["message"] = "schema producer returned an empty Substrait payload"
    return result


def check_expression_case(name: str) -> dict:
    import pyarrow.substrait as substrait

    case = expression_cases()[name]
    encoded = substrait.serialize_expressions(case["exprs"], case["names"], case["schema"])
    decoded = substrait.deserialize_expressions(encoded)
    decoded_names = list(decoded.expressions)
    ok = decoded.schema.equals(case["schema"], check_metadata=False) and decoded_names == case["names"]
    result = {
        "name": name,
        "status": "passed" if ok else "failed",
        "serialized_bytes": len(encoded),
        "expression_names": decoded_names,
    }
    if not ok:
        result["message"] = (
            f"expression round-trip mismatch: expected names={case['names']} schema={case['schema']}; "
            f"actual names={decoded_names} schema={decoded.schema}"
        )
    elif result["serialized_bytes"] <= 0:
        result["status"] = "failed"
        result["message"] = "expression producer returned an empty Substrait payload"
    return result


def main() -> int:
    args = parse_args()
    require_module("pyarrow", args.require_producer)
    require_module("pyarrow.acero", args.require_producer)
    require_module("pyarrow.substrait", args.require_producer)

    sample_dir = args.samples_dir / "acero_producer"
    samples = read_json_samples(sample_dir)
    args.work_dir.mkdir(parents=True, exist_ok=True)

    schema_cases = schemas()
    expr_cases = expression_cases()
    comparisons = []
    for sample in samples:
        name = sample.get("name")
        kind = sample.get("kind")
        if not name or not kind:
            raise AssertionError(f"{sample.get('_sample_file')}: sample needs 'name' and 'kind'")
        if kind == "schema":
            if name not in schema_cases:
                raise AssertionError(f"unknown Acero schema producer sample: {name}")
            result = check_schema_case(name)
        elif kind == "expressions":
            if name not in expr_cases:
                raise AssertionError(f"unknown Acero expression producer sample: {name}")
            result = check_expression_case(name)
        else:
            raise AssertionError(f"{sample.get('_sample_file')}: unsupported Acero producer sample kind: {kind}")
        comparisons.append(result)
        if result["status"] == "passed":
            print(f"PASS Apache Arrow Acero produced Substrait {kind} sample {name}")
        else:
            print(f"FAIL Apache Arrow Acero produced invalid Substrait {kind} sample {name}", file=sys.stderr)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report(
            "acero_producer",
            comparisons,
            {
                "samples": len(samples),
                "full_plan_producer_available": full_plan_producer_available(),
                "note": "PyArrow exposes schema/expression Substrait producers, not a public full Acero plan producer.",
            },
        ),
    )
    print(f"wrote comparison report {report_path}")

    for comparison in comparisons:
        require_match(comparison, "Apache Arrow Acero producer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
