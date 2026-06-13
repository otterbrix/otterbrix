#!/usr/bin/env python3
"""
Controlled experiment: verify disk hash index auto-rehash after bulk insert.

Inserts ROWS unique keys (default 900), checkpoints, then reads hash_index.bin
header. With default_bucket_count=1024 and max_load_factor=0.75, the first
rehash happens after the 769th key. The current implementation uses linear
hashing and grows to entry_count / (max_load_factor * 0.6), so bucket_count is
not rounded to a power of two. For the default 900 rows the expected target is
2000 buckets, not 2048.

Example:
  python3 scripts/verify_hash_index_rehash.py \\
    --runner ./build-release/benchmark/runner/benchmark_runner
"""

from __future__ import annotations

import argparse
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

PAGE_SIZE = 4096
INDEX_NAME = "idx_id_hash"
TABLE_NAME = "kv"
DEFAULT_BUCKET_COUNT = 1024
MAX_LOAD_FACTOR = 0.75
AUTO_REHASH_TARGET_FACTOR = 0.6
TARGET_LOAD_FACTOR = MAX_LOAD_FACTOR * AUTO_REHASH_TARGET_FACTOR
# rehash when entry_count / bucket_count > 0.75  → 769th insert on empty 1024-bucket table
FIRST_REHASH_AT_ENTRIES = int(DEFAULT_BUCKET_COUNT * MAX_LOAD_FACTOR) + 1


def expected_bucket_count_after_rehash(entries: int) -> int:
    """Mirror disk_hash_table_t::maybe_rehash_if_needed_unlocked().

    The C++ code computes:
        target_lf = max_load_factor_ * 0.6
        target_buckets = static_cast<uint32_t>(entry_count_ / target_lf)

    Rehash is incremental linear hashing, so the final bucket count is not
    rounded to a power of two.
    """
    if entries < FIRST_REHASH_AT_ENTRIES:
        return DEFAULT_BUCKET_COUNT
    return max(DEFAULT_BUCKET_COUNT + 1, int(float(entries) / TARGET_LOAD_FACTOR))


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def read_hash_index_header(path: Path) -> tuple[int, int, int, int]:
    raw = path.read_bytes()
    if len(raw) < 24:
        die(f"{path}: file too small ({len(raw)} bytes)")
    page_size = struct.unpack_from("<I", raw, 12)[0]
    bucket_count = struct.unpack_from("<I", raw, 16)[0]
    next_overflow = struct.unpack_from("<Q", raw, 20)[0]
    file_pages = len(raw) // PAGE_SIZE
    return page_size, bucket_count, next_overflow, file_pages


def find_hash_index_bin(work_dir: Path) -> Path | None:
    wal_root = work_dir / "wal"
    if not wal_root.is_dir():
        return None
    matches = sorted(wal_root.rglob("hash_index.bin"))
    if not matches:
        return None
    # Prefer index dir named idx_id_hash if multiple exist.
    for p in matches:
        if p.parent.name == INDEX_NAME:
            return p
    return matches[0]


def write_csv(path: Path, rows: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write("id,payload\n")
        for i in range(1, rows + 1):
            f.write(f"{i},row_{i}\n")


def write_setup(db_name: str) -> str:
    return (
        f"-- @database {db_name}\n"
        f"CREATE TABLE {TABLE_NAME} (id INTEGER, payload STRING) WITH (storage = 'disk');\n"
        f"CREATE INDEX {INDEX_NAME} ON {db_name}.{TABLE_NAME} USING hash (id);\n"
        f"-- @load_csv data.csv {TABLE_NAME} ,\n"
    )


def write_lookup(db_name: str, key: int) -> str:
    return f"-- @expected_rows 1\nSELECT COUNT(*) FROM {db_name}.{TABLE_NAME} WHERE id = {key};\n"


def run_runner(runner: Path, cwd: Path, sql_file: str, *, load_only: bool, checkpoint_mb: int = 0) -> None:
    cmd = [str(runner), f"--file={sql_file}", "--disk", "--runs=1", "--timeout=0"]
    if load_only:
        cmd.append("--load-only")
    if checkpoint_mb > 0:
        cmd.append(f"--csv-checkpoint-mb={checkpoint_mb}")
    proc = subprocess.run(cmd, cwd=str(cwd), text=True, capture_output=True, check=False)
    combined = (proc.stdout or "") + (proc.stderr or "")
    if proc.returncode != 0:
        die(f"benchmark_runner failed (exit {proc.returncode}):\n{combined[-4000:]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify disk hash index auto-rehash (small row count).")
    parser.add_argument("--runner", required=True, help="Path to benchmark_runner")
    parser.add_argument("--rows", type=int, default=900, help="Rows to insert (default 900, triggers rehash)")
    parser.add_argument(
        "--workspace",
        default="",
        help="Working directory (default: temp dir under repo or system temp)",
    )
    parser.add_argument("--keep-workspace", action="store_true")
    parser.add_argument(
        "--checkpoint-mb",
        type=int,
        default=0,
        metavar="N",
        help="Forward --csv-checkpoint-mb=N to benchmark_runner during load (0=off, periodic CHECKPOINT every N MiB).",
    )
    args = parser.parse_args()

    runner = Path(args.runner).resolve()
    if not runner.is_file():
        die(f"runner not found: {runner}")

    if args.rows < FIRST_REHASH_AT_ENTRIES:
        die(
            f"--rows={args.rows} is below first rehash threshold ({FIRST_REHASH_AT_ENTRIES}). "
            f"Use at least {FIRST_REHASH_AT_ENTRIES} rows."
        )

    if args.workspace:
        work_dir = Path(args.workspace).resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
        cleanup = False
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="otterbrix_rehash_verify_"))
        cleanup = not args.keep_workspace

    db_name = f"rehash_bench_{int(time.time())}"
    try:
        write_csv(work_dir / "data.csv", args.rows)
        (work_dir / "_setup.sql").write_text(write_setup(db_name), encoding="utf-8")
        (work_dir / "lookup.sql").write_text(write_lookup(db_name, args.rows // 2), encoding="utf-8")

        print(f"Workspace: {work_dir}")
        expected_bucket_count = expected_bucket_count_after_rehash(args.rows)
        expected_load_factor = args.rows / expected_bucket_count

        print(f"Inserting {args.rows} rows (first auto-rehash expected after entry #{FIRST_REHASH_AT_ENTRIES})")
        print(
            "Expected linear-hashing target: "
            f">={expected_bucket_count} buckets "
            f"(target load factor ~= {TARGET_LOAD_FACTOR:.2f}, "
            "not rounded to a power of two)"
        )
        print("Running load-only (--disk, CHECKPOINT at end)...")
        run_runner(runner, work_dir, "lookup.sql", load_only=True, checkpoint_mb=args.checkpoint_mb)

        hash_bin = find_hash_index_bin(work_dir)
        if hash_bin is None:
            die(f"hash_index.bin not found under {work_dir / 'wal'}")

        page_size, bucket_count, next_overflow, file_pages = read_hash_index_header(hash_bin)
        load_factor = args.rows / bucket_count if bucket_count else float("inf")
        min_pages = 1 + bucket_count

        print(f"\nhash_index.bin: {hash_bin}")
        print(f"  file size:     {hash_bin.stat().st_size:,} bytes ({file_pages} pages)")
        print(f"  page_size:     {page_size}")
        print(f"  bucket_count:  {bucket_count}")
        print(f"  next_overflow: {next_overflow}")
        print(f"  entries (rows): {args.rows}")
        print(f"  load_factor:   {load_factor:.4f}  (must be <= {MAX_LOAD_FACTOR}; target ~= {expected_load_factor:.4f})")

        ok = True
        if bucket_count < expected_bucket_count:
            print(
                f"\nFAIL: bucket_count={bucket_count}, expected >={expected_bucket_count} "
                f"according to current linear-hashing auto-rehash target "
                f"entries / ({MAX_LOAD_FACTOR} * {AUTO_REHASH_TARGET_FACTOR})"
            )
            ok = False
        else:
            print(
                f"\nOK: bucket_count grew to {bucket_count} "
                f"(rehash from {DEFAULT_BUCKET_COUNT} confirmed; expected target >= {expected_bucket_count})"
            )

        if file_pages < min_pages:
            print(f"WARN: file has {file_pages} pages, expected at least {min_pages} (1 header + bucket pages)")

        if load_factor > MAX_LOAD_FACTOR + 0.05:
            print(f"WARN: load_factor {load_factor:.4f} still above {MAX_LOAD_FACTOR} — unexpected after rehash")

        print("\nSanity: point lookup via index...")
        run_runner(runner, work_dir, "lookup.sql", load_only=False, checkpoint_mb=args.checkpoint_mb)

        if not ok:
            die("Rehash verification failed")
        print("Lookup OK. Rehash experiment passed.")
    finally:
        if cleanup:
            import shutil

            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
