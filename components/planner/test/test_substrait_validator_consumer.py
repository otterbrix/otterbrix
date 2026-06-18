#!/usr/bin/env python3
"""Run OtterBrix Substrait exports through the independent Substrait validator consumer."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from substrait_result_compare import build_report, write_report


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


def load_validator_cases(samples_dir: Path) -> list[str]:
    compatible_dir = samples_dir / "substrait_validator_consumer" / "compatible"
    names = []
    for case in read_json_samples(compatible_dir):
        name = case.get("name")
        if not name:
            raise AssertionError(f"{case.get('_sample_file')}: validator sample needs 'name'")
        names.append(name)
    return names


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exporter", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--samples-dir", default=DEFAULT_SAMPLES_DIR, type=Path)
    parser.add_argument("--validator", type=Path, default=None)
    parser.add_argument(
        "--require-consumer",
        action="store_true",
        help="Fail instead of skipping when substrait-validator is not installed.",
    )
    return parser.parse_args()


def skip_or_fail(message: str, require_consumer: bool) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1 if require_consumer else SKIP_CODE)


def resolve_validator(path: Path | None, require_consumer: bool) -> Path:
    candidates = []
    if path:
        candidates.append(path)
    env_path = os.environ.get("SUBSTRAIT_VALIDATOR")
    if env_path:
        candidates.append(Path(env_path))
    found = shutil.which("substrait-validator")
    if found:
        candidates.append(Path(found))

    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return candidate

    skip_or_fail("SKIP: substrait-validator executable is not installed", require_consumer)
    raise AssertionError("unreachable")


def export_substrait_plans(exporter: Path, output_dir: Path) -> None:
    if not exporter.exists():
        raise RuntimeError(f"exporter binary does not exist: {exporter}")
    output_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(exporter), str(output_dir)], check=True)


def validate_binary(validator: Path, binary_path: Path) -> dict:
    result = subprocess.run(
        [
            str(validator),
            str(binary_path),
            "--in-type",
            "proto",
            "--out-type",
            "diag",
            "-m",
            "loose",
            "--uri-depth",
            "0",
            "--allow-proto-any",
            "type.googleapis.com/google.protobuf.Struct",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def validator_version(validator: Path) -> dict[str, str]:
    def run(*args: str) -> str:
        result = subprocess.run([str(validator), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        return result.stdout.strip()

    return {
        "validator_version": run("--version"),
        "substrait_version": run("--substrait-version"),
    }


def main() -> int:
    args = parse_args()
    validator = resolve_validator(args.validator, args.require_consumer)
    export_dir = args.work_dir / "exports"
    export_substrait_plans(args.exporter, export_dir)

    comparisons = []
    for name in load_validator_cases(args.samples_dir):
        binary_path = export_dir / f"{name}.bin"
        result = validate_binary(validator, binary_path)
        passed = result["returncode"] == 0
        comparisons.append(
            {
                "name": name,
                "status": "passed" if passed else "failed",
                "consumer": "substrait-validator",
                "format": "proto",
                "expected_returncode": 0,
                "actual_returncode": result["returncode"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            }
        )
        if passed:
            print(f"PASS substrait-validator consumed {name}.bin")
        else:
            print(f"FAIL substrait-validator rejected {name}.bin", file=sys.stderr)

    report_path = args.work_dir / "result_comparison.json"
    write_report(
        report_path,
        build_report(
            "substrait_validator_consumer",
            comparisons,
            {
                "compatible_samples": len(comparisons),
                **validator_version(validator),
            },
        ),
    )
    print(f"wrote comparison report {report_path}")

    failed = [comparison for comparison in comparisons if comparison["status"] != "passed"]
    if failed:
        details = {item["name"]: item["stderr"] or item["stdout"] for item in failed}
        raise AssertionError(f"substrait-validator consumer failures: {json.dumps(details, ensure_ascii=False)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
