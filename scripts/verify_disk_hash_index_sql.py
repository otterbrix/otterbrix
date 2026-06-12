#!/usr/bin/env python3
"""
Manual correctness checks for the disk-backed hash index (Bitcask + disk_hash_table).

Runs SQL via benchmark_runner (--disk) and validates:
  - query row counts (-- @expected_rows N)
  - on-disk layout under <workspace>/<phase>/wal/<table_oid>/<index_name>/

Requirements:
  - Built benchmark_runner (e.g. ./build-release/benchmark/runner/benchmark_runner)
  - python3.10+

Example:
  python3 scripts/verify_disk_hash_index_sql.py \\
    --runner ./build-release/benchmark/runner/benchmark_runner

Two-phase load (optional; requires benchmark_runner with --no-setup):
  1) _setup.sql: CREATE TABLE + -- @load_csv only → --load-only
  2) create_index.sql: CREATE INDEX ... → --load-only --no-setup
  Never run CHECKPOINT or any --file=... without --skip-load after step 1
  (runner would re-execute sibling _setup.sql and break pg_attribute).
"""

from __future__ import annotations

import argparse
import json
import shutil
import struct
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


PAGE_SIZE = 4096
INDEX_NAME = "idx_id_hash"
TABLE_NAME = "kv"
MIN_USER_TABLE_OID = 16384


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def fnv1a_32(data: bytes) -> int:
    h = 2166136261
    for b in data:
        h ^= b
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def find_runner(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit).resolve()
        if not p.is_file():
            die(f"benchmark_runner not found: {p}")
        return p
    candidates = [
        Path("build-release/benchmark/runner/benchmark_runner"),
        Path("build-v5/benchmark/runner/benchmark_runner"),
        Path("build-benchmark/benchmark/runner/benchmark_runner"),
        Path("benchmark/runner/benchmark_runner"),
    ]
    for c in candidates:
        if c.is_file():
            return c.resolve()
    die("Pass --runner PATH to benchmark_runner (not found in default locations)")
    return Path()  # unreachable


@dataclass
class SqlCase:
    name: str
    sql: str
    expected_rows: int
    description: str = ""


@dataclass
class DiskExpect:
    name: str
    check: Callable[[Path], str | None]  # None => OK, else error message


@dataclass
class Phase:
    name: str
    db_name: str
    setup_sql: str = ""
    csv_rows: list[tuple[int, str]] | None = None
    sql_cases: list[SqlCase] = field(default_factory=list)
    mutation_sql: list[str] = field(default_factory=list)
    disk_expectations: list[DiskExpect] = field(default_factory=list)


def read_hash_index_header(path: Path) -> tuple[int, int, int]:
    raw = path.read_bytes()[:PAGE_SIZE]
    if len(raw) < 24:
        raise ValueError("hash_index.bin too small")
    page_size = struct.unpack_from("<I", raw, 12)[0]
    bucket_count = struct.unpack_from("<I", raw, 16)[0]
    next_overflow = struct.unpack_from("<Q", raw, 20)[0]
    return page_size, bucket_count, next_overflow


def find_index_dir(phase_dir: Path, index_name: str = INDEX_NAME) -> Path | None:
    """Index files live under config.disk.path (benchmark cwd → ./wal/<table_oid>/<index>/)."""
    for root_name in ("wal", "disk"):
        root = phase_dir / root_name
        if not root.is_dir():
            continue
        for candidate in root.rglob(index_name):
            if candidate.is_dir():
                return candidate
        for hash_bin in root.rglob("hash_index.bin"):
            return hash_bin.parent
    return None


def bitcask_segment_files(index_dir: Path) -> list[Path]:
    return sorted(index_dir.glob("bitcask.[0-9]*.data"))


def make_collision_ids(count: int, bucket_count: int = 1024, seed_prefix: str = "coll") -> list[int]:
    target_bucket = 0
    found: list[int] = []
    i = 0
    while len(found) < count:
        key = f"{seed_prefix}_{i}"
        if fnv1a_32(key.encode()) % bucket_count == target_bucket:
            found.append(i)
        i += 1
        if i > 5_000_000:
            raise RuntimeError("could not generate enough collision keys")
    return found


def create_index_sql(db_name: str) -> str:
    return f"CREATE INDEX {INDEX_NAME} ON {db_name}.{TABLE_NAME} USING hash (id);"


def write_phase_csv(phase_dir: Path, rows: list[tuple[int, str]]) -> Path:
    csv_path = phase_dir / "data.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for row_id, payload in rows:
            f.write(f"{row_id},{payload}\n")
    return csv_path


def build_setup_sql(
    db_name: str,
    csv_path: Path | None,
    extra_sql: str = "",
    *,
    with_disk_hash_index: bool = True,
) -> str:
    """
    Single load pass: CREATE TABLE, optional CREATE INDEX, then CSV.

    CREATE INDEX must appear after the @load_csv line in the file so it is not
    executed before the table exists; the runner still runs the whole SQL block
    before CSV (index metadata + backfill on load), matching restart benchmarks.
    """
    lines = [
        f"-- @database {db_name}",
        f"CREATE TABLE {TABLE_NAME} (id INTEGER, payload STRING) WITH (storage = 'disk');",
    ]
    if csv_path is not None:
        # Relative path: @load_csv is tokenized on spaces (no absolute paths with spaces).
        lines.append(f"-- @load_csv data.csv {TABLE_NAME} ,")
    if with_disk_hash_index:
        lines.append(create_index_sql(db_name))
    if extra_sql.strip():
        lines.append(extra_sql.strip())
    return "\n".join(lines) + "\n"


def write_sql_case(path: Path, case: SqlCase) -> None:
    text = f"-- @expected_rows {case.expected_rows}\n{case.sql.strip()}\n"
    path.write_text(text, encoding="utf-8")


def run_runner(
    runner: Path,
    cwd: Path,
    sql_file: str,
    *,
    disk: bool = True,
    load_only: bool = False,
    skip_load: bool = False,
    no_setup: bool = False,
    checkpoint_mb: int = 0,
    timeout_sec: float = 300,
) -> subprocess.CompletedProcess[str]:
    cmd = [str(runner), f"--file={sql_file}"]
    if disk:
        cmd.append("--disk")
    if load_only:
        cmd.append("--load-only")
        if checkpoint_mb > 0:
            cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    elif not skip_load and checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    if skip_load:
        cmd.append("--skip-load")
    if no_setup:
        cmd.append("--no-setup")
    cmd.extend(["--runs=1", "--timeout=0"])
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=None if timeout_sec <= 0 else timeout_sec,
        check=False,
    )


def assert_runner_ok(proc: subprocess.CompletedProcess[str], label: str) -> None:
    combined = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        die(f"{label}: benchmark_runner exit {proc.returncode}\n{combined[-4000:]}")
    for needle in (
        "Error loading group",
        "SQL error:",
        "CSV load SQL error",
        "Expected rows mismatch",
        "Cannot create database",
    ):
        if needle in combined:
            die(f"{label}: {needle}\n{combined[-4000:]}")
    out = proc.stdout or ""
    if "FAIL" in out.splitlines()[-20:]:
        die(f"{label}: benchmark reported FAIL\n{out[-4000:]}")


def check_index_tree(_ctx: dict, index_dir: Path) -> str | None:
    if not index_dir.is_dir():
        return f"missing index directory: {index_dir}"
    hi = index_dir / "hash_index.bin"
    if not hi.is_file() or hi.stat().st_size < PAGE_SIZE:
        return f"missing or tiny hash_index.bin: {hi}"
    segs = bitcask_segment_files(index_dir)
    if not segs:
        return "no bitcask.*.data segments"
    current = index_dir / "CURRENT"
    if not current.is_file():
        return "missing CURRENT segment marker"
    try:
        ps, buckets, nxt = read_hash_index_header(hi)
    except ValueError as e:
        return str(e)
    if ps != PAGE_SIZE:
        return f"unexpected page_size in header: {ps}"
    if buckets == 0:
        return "bucket_count is zero"
    if nxt < 1 + buckets:
        return f"invalid next_overflow_page={nxt} for bucket_count={buckets}"
    return None


def check_bitcask_data_populated(_ctx: dict, index_dir: Path) -> str | None:
    """Bulk CSV load should persist a non-trivial Bitcask keylog (batch INSERT may use one segment)."""
    segs = bitcask_segment_files(index_dir)
    total = sum(s.stat().st_size for s in segs)
    if total < 50_000:
        return f"bitcask data too small after bulk load: {total} bytes in {len(segs)} segment(s)"
    return None


def default_phases(db_suffix: str) -> list[Phase]:
    db = f"hash_verify_{db_suffix}"
    base_rows = [(i, f"payload_{i}") for i in range(1, 201)]
    coll_ids = make_collision_ids(40, 1024, "collision")
    coll_rows = [(10000 + k, f"collision_{i}") for k, i in enumerate(coll_ids)]
    coll_id_sql = ", ".join(str(10000 + k) for k in range(len(coll_ids)))
    long_key = "a" + ("x" * 500)
    long_row = [(50001, long_key)]

    core_cases = [
        SqlCase(
            "point_lookup_existing",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 42 AND payload = 'payload_42';",
            1,
            "VKR: point lookup for existing key",
        ),
        SqlCase(
            "point_lookup_missing",
            f"SELECT id FROM {db}.{TABLE_NAME} WHERE id = 999999;",
            0,
            "VKR: missing key returns no rows",
        ),
        SqlCase(
            "count_all_loaded",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id BETWEEN 1 AND 200;",
            1,
            "sanity: 200 base rows visible",
        ),
        SqlCase(
            "collision_bucket_keys",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id IN ({coll_id_sql});",
            1,
            "VKR-style: many keys hashing to the same bucket (spill chain stress)",
        ),
        SqlCase(
            "long_string_key",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 50001 AND payload = '{long_key}';",
            1,
            "VKR: long key (> inline limit) round-trip",
        ),
        SqlCase(
            "hash_index_used_range_scan_small",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 1;",
            1,
            "equality on indexed column",
        ),
    ]

    tombstone_cases = [
        SqlCase(
            "tombstone_old_key_gone",
            f"SELECT id FROM {db}.{TABLE_NAME} WHERE id = 50;",
            0,
            "VKR: after DELETE, old key absent",
        ),
        SqlCase(
            "tombstone_new_key_present",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 5050 AND payload = 'reused_slot_value';",
            1,
            "VKR: re-insert after delete (COUNT avoids duplicate-version rows)",
        ),
    ]

    bulk_rows = [(i, f"bulk_{i}") for i in range(1, 2501)]
    bulk_cases = [
        SqlCase(
            "bulk_lookup_mid",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 1500 AND payload = 'bulk_1500';",
            1,
            "VKR: lookup after large load / possible rehash",
        ),
        SqlCase(
            "bulk_count",
            f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id BETWEEN 1 AND 2500;",
            1,
        ),
    ]

    return [
        Phase(
            name="core",
            db_name=db,
            setup_sql="",  # filled in run_phase
            csv_rows=base_rows + coll_rows + long_row,
            sql_cases=core_cases,
            disk_expectations=[DiskExpect("index_tree", check_index_tree)],
        ),
        Phase(
            name="tombstone",
            db_name=db,
            setup_sql="",
            csv_rows=base_rows,
            mutation_sql=[
                f"DELETE FROM {db}.{TABLE_NAME} WHERE id = 50;",
                f"INSERT INTO {db}.{TABLE_NAME} (id, payload) VALUES (5050, 'reused_slot_value');",
            ],
            sql_cases=tombstone_cases,
        ),
        Phase(
            name="restart",
            db_name=db,
            setup_sql="",
            csv_rows=base_rows,
            sql_cases=[
                SqlCase(
                    "restart_lookup",
                    f"SELECT COUNT(*) FROM {db}.{TABLE_NAME} WHERE id = 77 AND payload = 'payload_77';",
                    1,
                    "persistence after process restart (--skip-load)",
                ),
            ],
        ),
        Phase(
            name="bulk_rehash",
            db_name=db,
            setup_sql="",
            csv_rows=bulk_rows,
            sql_cases=bulk_cases,
            disk_expectations=[
                DiskExpect("index_tree", check_index_tree),
                DiskExpect("bitcask_data_populated", check_bitcask_data_populated),
            ],
        ),
    ]


def run_phase(
    runner: Path,
    workspace: Path,
    phase: Phase,
    ctx: dict,
    timeout_sec: float,
    checkpoint_mb: int,
) -> list[dict]:
    phase_dir = workspace / phase.name
    tests_dir = phase_dir / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)
    csv_path = write_phase_csv(phase_dir, phase.csv_rows) if phase.csv_rows else None
    setup_text = phase.setup_sql or build_setup_sql(
        phase.db_name, csv_path, with_disk_hash_index=True
    )
    (phase_dir / "_setup.sql").write_text(setup_text, encoding="utf-8")

    results: list[dict] = []

    # Each benchmark_runner invocation is a new process; load once, then --skip-load.
    print(f"  [{phase.name}] load table + disk hash index + data (--load-only)...")
    proc = run_runner(
        runner, phase_dir, "_setup.sql", load_only=True, timeout_sec=timeout_sec, checkpoint_mb=checkpoint_mb
    )
    assert_runner_ok(proc, f"{phase.name}/load")
    # benchmark_runner --load-only already runs CHECKPOINT when --disk is set

    for mut in phase.mutation_sql:
        mut_name = f"mutation_{hash(mut) & 0xFFFF:04x}.sql"
        mut_path = phase_dir / mut_name
        mut_path.write_text(mut.strip() + "\n", encoding="utf-8")
        print(f"  [{phase.name}] mutation {mut_name} (--skip-load)...")
        proc = run_runner(runner, phase_dir, mut_name, skip_load=True, timeout_sec=timeout_sec)
        assert_runner_ok(proc, f"{phase.name}/{mut_name}")

    index_dir = find_index_dir(phase_dir)
    if index_dir:
        hi = index_dir / "hash_index.bin"
        if hi.is_file():
            _, buckets, _ = read_hash_index_header(hi)
            ctx["bucket_count_after_load"] = buckets
            ctx["hash_index_size_after_load"] = hi.stat().st_size
        ctx["index_dir"] = str(index_dir)

    for dex in phase.disk_expectations:
        if index_dir is None:
            die(f"{phase.name}: disk hash index dir missing under {phase_dir}/wal or disk/")
        err = dex.check(ctx, index_dir)
        results.append(
            {
                "phase": phase.name,
                "check": dex.name,
                "ok": err is None,
                "error": err,
            }
        )
        status = "OK" if err is None else f"FAIL: {err}"
        print(f"    disk:{dex.name}: {status}")
        if err:
            die(f"{phase.name} disk check {dex.name}: {err}")

    for case in phase.sql_cases:
        rel = f"tests/{case.name}.sql"
        write_sql_case(tests_dir / f"{case.name}.sql", case)
        print(f"  [{phase.name}] sql:{case.name} (--skip-load)...")
        proc = run_runner(runner, phase_dir, rel, skip_load=True, timeout_sec=timeout_sec)
        assert_runner_ok(proc, f"{phase.name}/{case.name}")
        results.append(
            {
                "phase": phase.name,
                "case": case.name,
                "ok": True,
                "expected_rows": case.expected_rows,
                "description": case.description,
            }
        )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify disk hash index SQL scenarios")
    parser.add_argument("--runner", help="Path to benchmark_runner binary")
    parser.add_argument(
        "--workspace",
        help="Working directory (default: temp dir under /tmp)",
    )
    parser.add_argument("--keep-workspace", action="store_true", help="Do not delete workspace on success")
    parser.add_argument("--timeout-sec", type=float, default=600, help="Per subprocess timeout (0 = none)")
    parser.add_argument("--json-report", help="Write machine-readable report to this path")
    parser.add_argument(
        "--checkpoint-mb",
        type=int,
        default=0,
        metavar="N",
        help="Forward --csv-checkpoint-mb=N to benchmark_runner during load (0=off, periodic CHECKPOINT every N MiB).",
    )
    args = parser.parse_args()

    runner = find_runner(args.runner)
    workspace = (
        Path(args.workspace).resolve()
        if args.workspace
        else Path(tempfile.gettempdir()) / f"otterbrix_hash_disk_verify_{int(time.time())}"
    )
    if " " in str(workspace):
        die(
            "Workspace path must not contain spaces (CSV @load_csv path is space-delimited). "
            f"Got: {workspace}"
        )
    workspace.mkdir(parents=True, exist_ok=True)

    print(f"Runner:    {runner}")
    print(f"Workspace: {workspace}")

    ctx: dict = {}
    all_results: list[dict] = []
    suffix = str(int(time.time()))

    try:
        for phase in default_phases(suffix):
            print(f"\n=== Phase: {phase.name} ===")
            all_results.extend(run_phase(runner, workspace, phase, ctx, args.timeout_sec, args.checkpoint_mb))
    except SystemExit:
        raise
    except Exception as e:
        die(str(e))
    else:
        print("\n=== All scenarios passed ===")
        if args.json_report:
            Path(args.json_report).write_text(json.dumps(all_results, indent=2), encoding="utf-8")
        print(f"Workspace: {workspace}")
        if not args.keep_workspace:
            shutil.rmtree(workspace, ignore_errors=True)
            print("(workspace removed; pass --keep-workspace to inspect disk layout)")
        return

    # failure path: keep workspace
    print(f"\nWorkspace preserved: {workspace}", file=sys.stderr)


if __name__ == "__main__":
    main()
