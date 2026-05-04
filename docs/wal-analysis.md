# WAL System Analysis

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Critical Bugs](#2-critical-bugs)
3. [Design Issues](#3-design-issues)
4. [Missing Features](#4-missing-features)
5. [Performance Issues](#5-performance-issues)
6. [Error Handling Issues](#6-error-handling-issues)
7. [Replay Correctness Issues](#7-replay-correctness-issues)
8. [Configuration Issues](#8-configuration-issues)
9. [Testing Gaps](#9-testing-gaps)
10. [Summary](#10-summary)

---

## 1. Architecture Overview (CURRENT — to be replaced by Section 11 decisions)

NOTE: This section describes the CURRENT WAL implementation. Section 11
defines the target architecture (W-ACTOR, W-PAGE, W-FORMAT, etc.) that
replaces multi-worker sharding, msgpack serialization, and plain file append.

### Components (current)

```
manager_wal_replicate_t (actor)
    │
    ├── worker_0 (wal_replicate_t actor)
    │       ├── .wal_0_000000 (segment file)
    │       ├── .wal_0_000001
    │       └── id_ (atomic counter, stride=worker_count)
    │
    └── worker_1 (wal_replicate_t actor)
            ├── .wal_1_000000
            ├── .wal_1_000001
            └── id_ (atomic counter, stride=worker_count)

routing: hash(collection_name) % worker_count
```

### Record Format

```
[size: 4 bytes BE] [msgpack payload] [crc32: 4 bytes BE]

PHYSICAL_INSERT: array[9] = [last_crc, wal_id, txn_id, type=10, db, coll, data, row_start, row_count]
PHYSICAL_DELETE:  array[8] = [last_crc, wal_id, txn_id, type=11, db, coll, row_ids, count]
PHYSICAL_UPDATE:  array[9] = [last_crc, wal_id, txn_id, type=12, db, coll, row_ids, data, count]
COMMIT:           array[3] = [last_crc, wal_id, txn_id]
```

### Write Flow

```
executor → send(wal_manager, write_physical_insert, data, txn_id)
    wal_manager → route to worker_N (hash collection name)
        worker_N → pack(msgpack) → write_buffer(file) → return wal_id
executor → send(wal_manager, commit_txn, txn_id)
    wal_manager → send commit to ALL workers
```

### Replay Flow

```
wal_reader_t::read_committed_records(last_checkpoint_wal_id):
    1. Scan ALL segment files for ALL workers
    2. Read all records with id > last_checkpoint_wal_id
    3. Collect committed_txn_ids set (from COMMIT markers)
    4. Filter: keep only records where txn_id in committed_txn_ids (or txn_id=0)
    5. Sort by wal_id
    6. Return for replay
```

### Configuration

```cpp
struct config_wal {
    path: filesystem path                      // default: cwd/wal
    on: bool                                   // default: true
    sync_to_disk: bool                         // default: true
    agent: int                                 // default: 2 (worker count)
    max_segment_size: size_t                   // default: 4MB
};
```

Two implementations: `manager_wal_replicate_t` (active) and
`manager_wal_replicate_empty_t` (no-op when `config.wal.on=false`).

In-memory variant: `wal_replicate_without_disk_t` — inherits wal_replicate_t,
overrides write_buffer() (no-op) and read_buffer() (returns zeros).
Used when `config.sync_to_disk=false`. Records not persisted, lost on restart.

### Segment File Details

**Naming:** `.wal_<WORKER_ID>_<6-DIGIT-SEGMENT-IDX>`

Examples: `.wal_0_000000`, `.wal_0_000001`, `.wal_1_000000`

**File flags:** WRITE | READ | FILE_CREATE, NO_LOCK

**Rotation:** When `file_size + record_size > max_segment_size` (4MB default):
```cpp
rotate_segment_():
    file_.reset()              // close old
    ++current_segment_idx_     // increment
    file_ = open_file(...)     // open new
    // last_crc32_ carries over (CRC chain not reset)
```

**Discovery on startup:**
```
discover_segments_():
    filter files by prefix ".wal_<worker_index>_"
    sort lexicographically (zero-padded = numeric order)
    return vector<path>
```

### WAL ID Allocation

Workers use **stride-based partitioning** to avoid coordination:

```
worker_count = 2:
    worker_0 IDs: 0, 2, 4, 6, 8, ...    (stride = 2)
    worker_1 IDs: 1, 3, 5, 7, 9, ...    (stride = 2)

worker_count = 4:
    worker_0: 0, 4, 8, 12, ...
    worker_1: 1, 5, 9, 13, ...
    worker_2: 2, 6, 10, 14, ...
    worker_3: 3, 7, 11, 15, ...
```

Init on startup: scan all segments for this worker, find max ID, resume from there.
If no segments exist: start at `worker_index`.

### Serialization Details (dto.cpp)

**Size/CRC32 encoding:** 4 bytes big-endian

**COMMIT marker:** msgpack array[3] = `[last_crc32, wal_id, txn_id]`

**PHYSICAL_INSERT:** msgpack array[9]:
```
[0] last_crc32 (uint64)
[1] wal_id (uint64)
[2] txn_id (uint64)
[3] record_type = 10 (uint64)
[4] database (string)
[5] collection (string)
[6] data_chunk (nested msgpack via data_chunk_t::serialize)
[7] row_start (uint64)
[8] row_count (uint64)
```

**PHYSICAL_DELETE:** msgpack array[8]:
```
[0]-[5] same as INSERT
[6] row_ids (nested msgpack array of int64)
[7] count (uint64)
```

**PHYSICAL_UPDATE:** msgpack array[9]:
```
[0]-[5] same as INSERT
[6] row_ids (nested msgpack array of int64)
[7] new_data (nested msgpack via data_chunk_t::serialize)
[8] count (uint64)
```

**CRC32:** Computed via `absl::ComputeCrc32c()` on payload only (not size header).
Each record stores `last_crc32` from previous record for chain integrity.

### Checkpoint Coordination

**WAL_ID file:** `<storage_directory>/WAL_ID` — plain text decimal

```
Checkpoint flow:
    1. manager_disk_t::checkpoint_all(current_wal_id)
       → checkpoint all DISK tables
    2. agent_disk_t::fix_wal_id(current_wal_id)
       → write current_wal_id to WAL_ID file (text, truncate)
    3. manager_wal_replicate_t::truncate_before(checkpoint_wal_id)
       → each worker deletes segments where last_id <= checkpoint_wal_id
       → current segment NEVER deleted

Recovery flow:
    1. Read WAL_ID file → last_checkpoint_wal_id
    2. wal_reader_t::read_committed_records(last_checkpoint_wal_id)
       → scan all segments for all workers
       → collect records with id > last_checkpoint_wal_id
       → filter by committed txn_ids
       → sort by wal_id
    3. Replay records
```

### Transaction Lifecycle in WAL

```
executor::execute_plan():
    txn = txn_manager.begin_transaction(session)

    // Execute DML operators → produce data

    // Write physical records (BEFORE disk commit)
    wal_id = co_await send(wal, write_physical_insert, db, coll, data, row_start, count, txn.id)
    // or write_physical_delete / write_physical_update

    // Commit in-memory state
    commit_id = txn_manager.commit(session)

    // Commit to disk storage
    co_await send(disk, storage_commit_append, ctx, commit_id, row_start, count)

    // Write COMMIT marker (AFTER disk commit)
    co_await send(wal, commit_txn, txn.id)

    // On ABORT: no COMMIT marker → replay will exclude these records
```

**Critical observation:** WAL physical records are written BEFORE disk commit,
but COMMIT marker is written AFTER disk commit. This means:
- Crash after physical write but before COMMIT → records excluded from replay (correct)
- Crash after COMMIT marker → records included in replay (correct)
- But: no fsync means NEITHER physical records NOR commit marker may reach disk

### File Handle Capabilities

```cpp
file_handle_t interface:
    read(buffer, size)                  // sequential read
    read(buffer, size, location)        // positional read
    write(buffer, size)                 // sequential write (used by WAL)
    write(buffer, size, location)       // positional write (used by WAL_ID)
    sync()                              // fsync — EXISTS but NEVER called by WAL
    truncate(new_size)                  // truncate file — used by WAL_ID, NOT by WAL
    seek(position)                      // set position (WAL: seek to EOF on open)
    file_size()                         // get file size
```

### Existing Test Coverage

| Test | What it tests | What it misses |
|---|---|---|
| physical_insert_write_and_read | Single INSERT write+read | Multi-record, ID verification |
| physical_delete_write_and_read | DELETE with row_ids | Out-of-order IDs, duplicates |
| physical_update_write_and_read | UPDATE with row_ids + data | Data chunk content verification |
| commit_marker_write_and_read | COMMIT marker txn_id | Chain of commits, txn recovery |
| corrupted_record_detected | CRC corruption detection | Records after corruption |
| mixed_valid_corrupt_records | Corruption prevents commit | Recovery after skipping corrupt |

### Executor DML → WAL Flow (exact ordering)

```
executor::execute_plan() (executor.cpp:161-314):

  Step 1: txn = txn_manager.begin_transaction(session)          [sync, local]

  Step 2: result = co_await execute_sub_plan_(plan)              [async, operators run]
           → operators produce: wal_insert_data, wal_row_ids,
             wal_update_data, append_row_start, append_row_count

  Step 3: WAL physical write (BEFORE disk commit):
    INSERT: co_await send(wal, write_physical_insert,
                session, db, coll, data, row_start, row_count, txn_id)
    DELETE: co_await send(wal, write_physical_delete,
                session, db, coll, row_ids, count, txn_id)
    UPDATE: co_await send(wal, write_physical_update,
                session, db, coll, row_ids, data, count, txn_id)

  Step 4: disk flush (fire-and-forget):
    send(disk, flush, session, wal_id)  → pending, not awaited

  Step 5: commit_id = txn_manager.commit(session)                [sync, local]

  Step 6: disk commit (AFTER txn commit):
    INSERT: co_await send(disk, storage_commit_append, ctx, commit_id, row_start, count)
    DELETE: co_await send(disk, storage_commit_delete, ctx, commit_id)

  Step 7: index commit:
    INSERT: co_await send(index, commit_insert, ctx, commit_id)
    DELETE: co_await send(index, commit_delete, ctx, commit_id)

  Step 8: WAL COMMIT marker (LAST step):
    co_await send(wal, commit_txn, session, txn_id)

  ABORT path (on error, Steps 3-8 skipped):
    co_await send(disk, storage_revert_append, ctx)
    co_await send(index, revert_insert, ctx)
    txn_manager.abort(session)
    → NO WAL notification on abort
    → Orphan physical records remain in WAL but without COMMIT marker
    → Replay excludes them (correct)
```

**Critical ordering:**
- WAL physical write (Step 3) BEFORE disk commit (Step 6)
- WAL COMMIT marker (Step 8) AFTER disk commit (Step 6)
- This means: crash after Step 3 but before Step 8 → records in WAL but no COMMIT → excluded from replay
- BUT: no fsync at any step → everything may be lost

### Dispatcher CHECKPOINT → WAL Flow

```
dispatcher::execute_plan() case checkpoint_t (dispatcher.cpp:348-375):

  Step 1: co_await send(index, flush_all_indexes, session)

  Step 2: wal_max_id = co_await send(wal, current_wal_id, session)
          → manager queries all workers: max(worker_0.id_, worker_1.id_, ...)

  Step 3: checkpoint_wal_id = co_await send(disk, checkpoint_all, session, wal_max_id)
          → disk: checkpoint all DISK tables
          → disk: fix_wal_id(wal_max_id) → write to WAL_ID file (text)
          → returns: wal_max_id (or 0 if IN_MEMORY tables exist)

  Step 4: co_await send(wal, truncate_before, session, checkpoint_wal_id)
          → manager sends to ALL workers
          → each worker: discover segments, delete where last_id <= checkpoint_wal_id
          → current segment NEVER deleted
```

### Manager → Worker Routing

```
manager_wal_replicate_t routing:

  write_physical_insert(db, coll, ...):
      idx = hash(db + "." + coll) % dispatchers_.size()
      co_await send(dispatchers_[idx], write_physical_insert, ...)
      return wal_id

  write_physical_delete/update: same routing

  commit_txn(txn_id):
      for EACH worker (ALL workers, not just one):
          co_await send(dispatchers_[i], commit_txn, txn_id)
      return last_id
      // Reason: physical records may be on ANY worker (different collections)
      // COMMIT must be visible to ALL workers for replay correctness

  current_wal_id():
      max_id = 0
      for EACH worker:
          max_id = max(max_id, dispatchers_[i]->current_id())  // sync atomic read
      return max_id

  truncate_before(checkpoint_id):
      for EACH worker:
          co_await send(dispatchers_[i], truncate_before, checkpoint_id)

  load(wal_id):
      all_records = []
      for EACH worker:
          records = co_await send(dispatchers_[i], load, wal_id)
          all_records.append(records)
      sort(all_records, by: record.id ascending)
      return all_records
```

### Worker Internal Write Flow

```
wal_replicate_t::write_physical_insert():
    1. next_id(id_, worker_count_)     // id_ += worker_count (stride)
    2. last_crc32_ = pack_physical_insert(buffer, resource, last_crc32_, id_,
                                          txn_id, db, coll, data, row_start, row_count)
    3. write_buffer(buffer)
         if file_size + buffer.size > max_segment_size:
             rotate_segment_()          // close old, open new
         file_->write(buffer.data(), buffer.size())  // NO fsync
    4. co_return id_

wal_replicate_t::commit_txn():
    1. next_id(id_, worker_count_)     // id_ += worker_count
    2. last_crc32_ = pack_commit_marker(buffer, last_crc32_, id_, txn_id)
    3. write_buffer(buffer)            // same path as above
    4. co_return id_
```

### WAL Reader Standalone Flow

```
wal_reader_t::read_committed_records(after_id):

    Pass 1: Read all records from all workers
        committed_txn_ids = set()
        all_physical_records = []

        for each wal_file in all worker segment files:
            while not EOF:
                record = read_wal_record(file, offset)
                if record.is_corrupt: break (stop this file)
                if record.id <= after_id: skip
                if record.is_commit_marker():
                    committed_txn_ids.insert(record.transaction_id)
                if record.is_physical():
                    all_physical_records.push(record)

    Pass 2: Filter by committed
        committed_records = []
        for each record in all_physical_records:
            if record.txn_id == 0 OR committed_txn_ids.contains(record.txn_id):
                committed_records.push(record)

    Pass 3: Sort by WAL ID
        sort(committed_records, by: id ascending)

    return committed_records
```

### Empty Manager Variant

```
manager_wal_replicate_empty_t:
    All methods return no-op/empty:
    - load() → empty vector
    - commit_txn() → id_t{0}
    - truncate_before() → void (no-op)
    - current_wal_id() → id_t{0}
    - write_physical_insert/delete/update() → id_t{0}

    Used when config.wal.on = false
    No workers spawned, no files created, zero overhead
```

### In-Memory Worker Variant

```
wal_replicate_without_disk_t (inherits wal_replicate_t):
    Overrides:
    - write_buffer() → no-op (data discarded)
    - read_buffer() → returns zeros
    - load() → empty vector

    Preserves from parent:
    - id_ counter (incremented normally)
    - last_crc32_ chain (computed but not persisted)
    - commit_txn() calls pack + write_buffer (no-op write)

    Used when config.sync_to_disk = false
    Workers created but no files. Data lost on restart.
    IDs still advance to maintain consistent numbering.
```

### Key Constants

| Constant | Value | Source |
|---|---|---|
| Default WAL path | `cwd/wal` | configuration.hpp |
| Default sync_to_disk | true | configuration.hpp |
| Default worker count (agent) | 2 | configuration.hpp |
| Default max_segment_size | 4MB (4 * 1024 * 1024) | configuration.hpp |
| Segment name format | `.wal_{worker}_{idx:06d}` | wal.cpp:19 |
| Size field | 4 bytes big-endian | dto.cpp:22 |
| CRC32 field | 4 bytes big-endian | dto.cpp:15 |
| CRC32 algorithm | absl::ComputeCrc32c | dto.cpp:35 |
| WAL_ID file | `<storage_dir>/WAL_ID` (plain text decimal) | disk.cpp:19 |
| COMMIT array size | 3 elements | dto.cpp:42 |
| INSERT array size | 9 elements | dto.cpp:59 |
| DELETE array size | 8 elements | dto.cpp:85 |
| UPDATE array size | 9 elements | dto.cpp:110 |
| File flags (write) | WRITE \| READ \| FILE_CREATE | wal.cpp:64 |
| File flags (read) | READ | wal.cpp:182 |
| File lock | NO_LOCK | wal.cpp:65 |
| ID stride | worker_count | wal.cpp:363 |

---

## 2. Critical Bugs

### 2.1 No fsync — Data Loss on Crash

**Severity: CRITICAL**
**File: services/wal/wal.cpp:123-128**

```cpp
void wal_replicate_t::write_buffer(buffer_t& buffer) {
    if (file_->file_size() + buffer.size() > config_.max_segment_size) {
        rotate_segment_();
    }
    file_->write(buffer.data(), buffer.size());
    // NO fsync() called!
}
```

WAL never calls `file_->sync()` after writes. Data written stays in OS page cache
and is lost on crash, power failure, or kernel panic. The file handle interface
has `.sync()` method but it is never invoked.

The `config_.sync_to_disk` flag exists but is only used to select between
disk-backed and in-memory workers — NOT for actual fsync control.

**Impact:** Complete data loss of any records written since last OS buffer flush.
WAL provides zero durability guarantee.

**Potential fixes:**
- A) fsync after every write_buffer() — safest, worst performance
- B) fsync only on commit_txn() — good balance (group commit)
- C) fsync periodic (timer) — configurable latency vs durability tradeoff
- D) Consumer-configurable: per-write / per-commit / periodic / never

### 2.2 Segment Rotation Not Atomic

**Severity: CRITICAL**
**File: services/wal/wal.cpp:423-432**

```cpp
void wal_replicate_t::rotate_segment_() {
    file_.reset();              // step 1: close old file
    ++current_segment_idx_;     // step 2: increment counter
    file_ = open_file(...);     // step 3: open new file
}
```

Crash between step 1 and step 3: old file closed, new not opened, `file_` is null.
Next write after recovery will fail or write to wrong segment.

**Impact:** Data loss or corruption during segment rotation.

**Potential fixes:**
- A) Open new file before closing old: `new_file = open(); old_file.close(); file_ = new_file;`
- B) Write rotation marker to old segment before closing
- C) Use temp filename + atomic rename

### 2.3 Partial Record After Crash Not Detected

**Severity: HIGH**
**File: services/wal/wal.cpp:137-149, services/wal/wal_reader.cpp**

After crash mid-write:
1. Size header written but payload incomplete
2. Reader reads truncated payload
3. CRC mismatch → record marked corrupt → reading stops for this segment
4. All records AFTER corruption in same segment are lost silently

No attempt to:
- Truncate file to last valid record on startup
- Skip corrupted record and try to recover subsequent records
- Report how many records were lost

**Impact:** Silent data loss — records after corruption point are discarded.

**Potential fixes:**
- A) On startup: scan to find last valid record, truncate file at that point
- B) Try to skip corrupted records by scanning for next valid size header
- C) Report corruption count and lost record range

### 2.4 Unvalidated Segment Index Parsing

**Severity: HIGH**
**File: services/wal/wal.cpp:50-59**

```cpp
auto last_underscore = last_name.rfind('_');
current_segment_idx_ = std::stoi(last_name.substr(last_underscore + 1));
```

Uses `rfind('_')` — returns LAST underscore. Segment filename `.wal_0_000001`
has two underscores. If worker_index has multiple digits (`.wal_10_5`),
parsing is correct. But if filename is malformed, `std::stoi()` throws
uncaught exception.

**Impact:** Crash on startup with malformed WAL filenames.

**Potential fixes:**
- A) Use proper prefix parsing: `".wal_" + worker_index + "_"` then extract remainder
- B) Wrap in try-catch, log error, skip malformed files
- C) Validate filename format with regex before parsing

---

## 3. Design Issues

### 3.1 No Crash Safety (No Write-Ahead Guarantee)

**Severity: CRITICAL**
**File: entire WAL subsystem**

Standard WAL contract: data is durable **before** application state changes.
Current otterbrix WAL:
1. write_buffer() → data in OS buffer (NOT durable)
2. Return WAL ID → application continues
3. fsync never called → data may never reach disk

The name "Write-Ahead Log" implies writes are ahead of (before) data changes.
Without fsync, writes may be behind or lost entirely.

**Potential fixes:**
- Implement proper WAL protocol: write → fsync → return → allow state change
- At minimum: fsync on commit_txn (group commit pattern)

### 3.2 CRC Chain Not Verified Across Segments on Startup

**Severity: MEDIUM**
**File: services/wal/wal.cpp:99, 431**

`last_crc32_` carries over during segment rotation (correct for writes).
But on startup `init_id()` scans segments independently and does not validate
that the CRC chain is unbroken across segment boundaries. A flipped bit
between segments would go undetected.

**Potential fixes:**
- On startup: read last record of each segment, verify last_crc32 matches first record of next segment
- Log warning if chain is broken

### 3.3 File Append Without Truncation After Crash

**Severity: HIGH**
**File: services/wal/wal.cpp:61-68**

On startup, file is opened and seeked to EOF. If last write was partial
(crash mid-write), EOF includes the partial data. Next write appends
after the partial data, creating an unreadable file.

```
File before crash: [valid_record_1] [partial_record_2_half_written]
After restart:     [valid_record_1] [partial_record_2_garbage] [new_record_3]
```

Reader will fail on partial_record_2 and never reach new_record_3.

**Potential fixes:**
- Scan file on startup, find last valid record offset, truncate to that offset

---

## 4. Missing Features

### 4.1 No DDL Logging

**Severity: HIGH**
**File: services/wal/record.hpp (enum wal_record_type)**

Only PHYSICAL_INSERT/DELETE/UPDATE + COMMIT are supported. DDL operations
(CREATE TABLE, ALTER TABLE, DROP TABLE) are not logged. Schema changes
are lost on crash between DDL execution and checkpoint.

Note: This is addressed in the catalog migration document (W4 decision).

### 4.2 No Transaction Rollback Markers

**Severity: MEDIUM**
**File: services/wal/record.hpp**

Only COMMIT markers exist. Aborted transactions leave orphan DML records
in WAL that are never explicitly marked as aborted. Replay correctly
excludes them (not in committed set), but:
- Orphan records waste WAL space
- No way to distinguish "not yet committed" from "explicitly aborted"
- Auditing impossible

**Potential fix:** Add ROLLBACK record type.

### 4.3 No WAL Archival

**Severity: MEDIUM**
**File: services/wal/wal.cpp:331-352 (truncate_before)**

Old segments are deleted permanently. No option to archive to cold storage
for point-in-time recovery or compliance.

**Potential fix:** Add archive callback before deletion.

### 4.4 No Compression

**Severity: LOW**
**File: services/wal/dto.cpp**

Records are serialized with msgpack without compression. Large data_chunk
objects (INSERT with megabytes of data) bloat WAL files.

**Potential fix:** Optional Zstd or LZ4 compression in serialization layer.

---

## 5. Performance Issues

### 5.1 No Write Batching

**Severity: MEDIUM**
**File: services/wal/wal.cpp:123-128**

Each record triggers a separate `file_->write()` syscall. No write combining
or buffering. For high-throughput scenarios (millions of ops/sec), syscall
overhead dominates.

**Potential fix:** Accumulate records in memory buffer, flush when buffer full or on commit.

### 5.2 Worker Sharding May Be Imbalanced

**Severity: MEDIUM**
**File: services/wal/manager_wal_replicate.cpp:98-101**

Hash-based routing: `hash(collection_name) % worker_count`. No control over
distribution. Hot collections hash to same worker creating bottleneck.

**Potential fix:** Consistent hashing, or round-robin for single-collection workloads.

### 5.3 Lock Contention in Manager

**Severity: MEDIUM**
**File: services/wal/manager_wal_replicate.cpp (spin_lock)**

Manager uses spin_lock that guards worker dispatch and pending future polling.
Under high throughput, multiple callers contend on this lock.

**Potential fix:** Lock-free queue per worker, or partition callers to workers without lock.

### 5.4 Small Default Segment Size (4MB)

**Severity: LOW**
**File: components/configuration/configuration.hpp:21**

4MB segments cause frequent rotation under load. Each rotation = file close +
file create + seek overhead. Standard databases use 16MB-256MB segments.

**Potential fix:** Default 64-256MB, configurable.

---

## 6. Error Handling Issues

### 6.1 Disk Full Not Handled

**Severity: HIGH**
**File: services/wal/wal.cpp:123-128**

`file_->write()` return value is not checked. On disk full, write silently
fails. WAL ID is incremented. Caller thinks data is persisted.

**Potential fix:** Check return value, propagate error to caller.

### 6.2 Corrupted Segment Stops All Replay for That Worker

**Severity: MEDIUM**
**File: services/wal/wal_reader.cpp:42-98**

On corrupt record: `break` inner loop, skip rest of segment. All valid records
after corruption in same segment are lost.

**Potential fix:** Try to recover by scanning for next valid record header.

### 6.3 Worker Crash Not Detected

**Severity: HIGH**
**File: services/wal/wal.hpp, wal.cpp**

If worker actor throws exception in write handler, exception is caught by
actor framework. No retry, no fallback, no notification to caller that
write failed. WAL ID already incremented.

**Potential fix:** Wrap write methods, retry with backoff, or propagate error to future.

---

## 7. Replay Correctness Issues

### 7.1 Row ID Stability

**Severity: MEDIUM**
**File: services/wal/record.hpp**

DELETE/UPDATE records use physical row_ids (int64_t offsets). If table state
differs from original (rows added/deleted by other means), replay produces
wrong results.

**Potential fix:** Use logical row identifiers or validate row contents before replay.

### 7.2 Transaction Ordering Across Workers

**Severity: MEDIUM**
**File: services/wal/manager_wal_replicate.cpp:121-140**

Records from different workers are sorted by WAL ID for replay. WAL IDs are
partitioned: worker 0 gets even IDs, worker 1 gets odd. Sorting produces
correct interleaving.

But: if transaction T1 writes to worker 0 (ID=2) then worker 1 (ID=3),
and crash happens after worker 0 write but before worker 1, replay sees
partial transaction. COMMIT marker not written → excluded. Correct behavior.

However, if two transactions write to same worker and commit order differs
from write order, replay order may differ from original commit order.

**Potential fix:** Use global commit timestamp, not per-worker WAL IDs, for ordering.

---

## 8. Configuration Issues

### 8.1 No Configuration Validation

**Severity: MEDIUM**
**File: services/wal/manager_wal_replicate.cpp, services/wal/wal.cpp**

No validation of:
- path (permissions, existence)
- agent count (negative, zero, too high)
- max_segment_size (zero, overflow)
- sync_to_disk + read-only filesystem

**Potential fix:** Add `config.validate()` with meaningful error messages.

### 8.2 Worker Count Not Adjustable After Creation

**Severity: LOW**
**File: services/wal/manager_wal_replicate.cpp**

Changing worker count requires recreating WAL. Old segments are not
redistributed. Collection-to-worker mapping changes.

**Potential fix:** Store worker count in WAL metadata, detect changes on startup.

---

## 9. Testing Gaps

### 9.1 No Crash Recovery Tests

**Severity: CRITICAL**

Missing: fork process, write records, kill mid-write, restart, verify recovery.
This is the fundamental WAL test.

### 9.2 No Concurrent Write Tests

**Severity: HIGH**

Missing: multi-worker stress test with concurrent writes from multiple collections.

### 9.3 No Replay Correctness Test

**Severity: HIGH**

Missing: write data → checkpoint → write more → crash → replay → verify state matches pre-crash.

### 9.4 No Segment Rotation Tests

**Severity: MEDIUM**

Missing: records spanning rotation boundary, exact-size records, oversized records.

### 9.5 No CRC Chain Validation Test

**Severity: MEDIUM**

Missing: verify CRC chain across multiple records and segments.

---

## 10. Summary

### By Severity

| Severity | Count | Issues |
|----------|-------|--------|
| CRITICAL | 4 | No fsync (#2.1), non-atomic rotation (#2.2), no crash safety (#3.1), no crash tests (#9.1) |
| HIGH | 7 | Partial record (#2.3), segment parsing (#2.4), file truncation (#3.3), no DDL logging (#4.1), disk full (#6.1), worker crash (#6.3), no replay test (#9.3) |
| MEDIUM | 11 | CRC chain (#3.2), no rollback markers (#4.2), no archival (#4.3), write batching (#5.1), sharding (#5.2), lock contention (#5.3), corruption stops replay (#6.2), row ID stability (#7.1), txn ordering (#7.2), no config validation (#8.1), segment rotation test (#9.4) |
| LOW | 3 | No compression (#4.4), segment size (#5.4), worker count (#8.2) |

### Top 5 Priorities

1. **Add fsync** (at minimum on commit_txn) — without this, WAL is not a WAL
2. **Truncate corrupted suffix on startup** — prevent cascading data loss
3. **Make segment rotation atomic** — prevent corruption during rotation
4. **Check write() return values** — detect disk full
5. **Add crash recovery tests** — verify the above fixes actually work

### Problem → Decision Mapping

| Problem | Section | Resolved by | How |
|---|---|---|---|
| No fsync (#2.1) | 2.1 | **W-SYNC** | Per-table FULL/NORMAL/OFF, fsync on commit for FULL |
| Non-atomic rotation (#2.2) | 2.2 | **W-PAGE** | Page-based writes, no segment rotation gap (always full pages) |
| Partial record after crash (#2.3) | 2.3 | **W-CRC** | CRC chain verification at startup, truncate at first break |
| Segment index parsing (#2.4) | 2.4 | **W-ACTOR** | Per-database files with known naming, no worker index parsing |
| No crash safety (#3.1) | 3.1 | **W-SYNC + W-PAGE** | fsync + page-aligned writes |
| CRC chain not verified (#3.2) | 3.2 | **W-CRC** | Explicit chain verification at startup |
| File append without truncation (#3.3) | 3.3 | **W-CRC** | Truncate to last valid record on startup |
| No DDL logging (#4.1) | 4.1 | **catalog migration W4** | DDL = DML on system tables, WAL records same as DML (see catalog doc) |
| No rollback markers (#4.2) | 4.2 | Not addressed | Orphan records excluded by committed txn filter (correct behavior) |
| No archival (#4.3) | 4.3 | Not addressed | Future work |
| No compression (#4.4) | 4.4 | Not addressed | Low priority |
| No write batching (#5.1) | 5.1 | **W-PAGE** | Write buffer accumulates records, flush full pages |
| Sharding imbalance (#5.2) | 5.2 | **W-ACTOR** | Per-database (no hash sharding) |
| Lock contention (#5.3) | 5.3 | **W-ACTOR** | Single actor per database (no shared lock) |
| Segment size 4MB (#5.4) | 5.4 | **W-PAGE** | Page-based, configurable segment size |
| Disk full not handled (#6.1) | 6.1 | Not addressed | Needs write() return check (orthogonal to format) |
| Corruption stops replay (#6.2) | 6.2 | **W-CORRUPT** | STOP-A: replay up to corruption, start with warning |
| Worker crash (#6.3) | 6.3 | **W-ACTOR** | Single actor per database, simpler error handling |
| Row ID stability (#7.1) | 7.1 | Not addressed | Fundamental design (physical row IDs) |
| Txn ordering (#7.2) | 7.2 | **W-ACTOR** | Single ordered stream per database |
| No config validation (#8.1) | 8.1 | Not addressed | Orthogonal |
| Worker count hardcoded (#8.2) | 8.2 | **W-ACTOR** | No worker count (one actor per database) |
| No crash recovery tests (#9.1) | 9.1 | Needed | Must be written |
| No concurrent write tests (#9.2) | 9.2 | Needed | Multi-executor → single WAL actor |
| No replay correctness tests (#9.3) | 9.3 | Needed | Must be written |

### Not Addressed (acceptable)

| Problem | Why acceptable |
|---|---|
| No rollback markers (#4.2) | Uncommitted records excluded by commit filter. No data loss. Waste minimal. |
| No archival (#4.3) | Future work for PITR. Page structure (W-PAGE) enables binary search needed for archival. |
| No compression (#4.4) | Low priority. Raw binary already compact. Can add page-level compression later. |
| Disk full (#6.1) | Orthogonal to WAL format. Needs write() return check in wal_write_buffer_t::flush_page(). |
| Row ID stability (#7.1) | Fundamental design. Would require logical row IDs (major change). |
| Config validation (#8.1) | Orthogonal. Add validate() method separately. |

---

## 11. Decisions for WAL Rewrite

Reference: PostgreSQL (filtered for embedded), DuckDB, SQLite WAL.

### Decision Registry

| ID | Decision | Rationale |
|---|---|---|
| W-SYNC | Per-table sync mode (FULL/NORMAL/OFF) | Framework consumers choose durability per table via config/code |
| W-CORRUPT | STOP-A on corruption | Replay up to corruption, start with warning (PostgreSQL/DuckDB approach) |
| W-TORN | Backup prev checkpoint per table | rename(current→prev), checkpoint, fsync, delete(prev). WAL replay covers gap |
| W-ACTOR | Per-database WAL actors inside manager | Manager routes by database name to per-db actors. Executor sees one address (manager). Isolation, replication-ready |
| W-PAGE | Page-structured WAL (4096-byte pages) | Alignment (O_DIRECT/io_uring ready) + buffering (batch writes) + seek (binary search for replication/PITR) |
| W-FORMAT | Raw binary records (no msgpack) | Fixed header + column-native payload. Matches disk format |
| W-MSGPACK | Remove msgpack from WAL only | WAL uses raw binary. msgpack kept in conanfile.py (used by b_plus_tree, types, serialization) |
| W-CRC | CRC chain verification at startup | Verify last_crc32 chain, truncate at first break |

### W-SYNC: Per-Table Sync Mode

```cpp
enum class wal_sync_mode : uint8_t {
    OFF = 0,      // no fsync ever — max performance
    NORMAL = 1,   // fsync at checkpoint only — periodic durability
    FULL = 2      // fsync at every commit — full durability
};
```

Configured per table via code/config (not SQL):
```cpp
create_storage_with_columns("payments", columns, {.wal_sync = sync_mode::FULL});
create_storage_with_columns("logs", columns, {.wal_sync = sync_mode::OFF});
```

Effective sync per transaction = max(all participating tables).
Executor tracks effective_sync, passes to WAL manager on commit_txn:
`commit_txn(session, txn_id, effective_sync_mode, database_name)`
Manager routes to per-database WAL actor (W-ACTOR).

Changes: ~30 lines (enum + field in storage + field in context + if-check in commit).

### W-CORRUPT: STOP-A on Corruption

```
WAL replay encounters corrupted record:
    1. Stop replay at corruption point
    2. System starts with data up to that point
    3. Log WARNING with record number and byte offset
    4. No refuse-to-start, no degraded mode

Rationale: checkpoint data = source of truth (always consistent).
WAL after checkpoint = best-effort recovery. Same as PostgreSQL and DuckDB.
```

### W-TORN: Backup Previous Checkpoint

```
Per-table checkpoint:
    1. rename(table.otbx → table.otbx.prev)   // O(1) atomic backup
    2. table.checkpoint()                       // write new file
    3. fsync(table.otbx)                        // ensure on disk
    4. delete(table.otbx.prev)                  // cleanup

Recovery if table.otbx corrupted:
    → fallback to table.otbx.prev
    → WAL replay from prev checkpoint point

WAL retention: keep records from min(prev_checkpoint_wal_id) across all tables in database (see Styk 4)
    truncate_before(prev_checkpoint_wal_id) not current
```

### W-ACTOR: Per-Database WAL Actor

```
database "payments" → wal_actor_payments → .wal_payments_*
database "analytics" → wal_actor_analytics → .wal_analytics_*

Replaces: global multi-worker sharding (hash % worker_count)

Benefits:
    - Different databases don't block each other
    - FULL fsync on "payments" doesn't stall "analytics" writes
    - Replication per-database (single ordered stream)
    - PITR per-database
    - No fan-out commit (one actor per transaction)
    - No merge-sort on load (single ordered file per database)

Single-database case: equivalent to single WAL actor (S1)
```

### W-PAGE: Page-Structured WAL with Alignment and Buffering

All three I/O optimizations in one design: page structure (seek), alignment
(O_DIRECT/io_uring ready), and write buffering (batch syscalls + group commit).

#### WAL File Layout

```
[File Header: PAGE_SIZE bytes]     // magic, version, page_size, database
[Page 0: PAGE_SIZE bytes]          // page_header + packed records + padding
[Page 1: PAGE_SIZE bytes]
...

PAGE_SIZE = 4096 bytes (configurable: 4096, 8192, 16384)
```

#### File Header (first page of each segment)

```
[magic: 4 bytes]                   // "OWAL" = 0x4C41574F
[version: 2 bytes]                 // 1
[page_size: 4 bytes]               // 4096
[segment_index: 4 bytes]
[database_name_len: 2 bytes]
[database_name: N bytes]
[created_timestamp: 8 bytes]
[padding to PAGE_SIZE]
```

#### Page Header (32 bytes)

```
[page_lsn: 8 bytes LE]            // wal_id of first record in page
[page_end_lsn: 8 bytes LE]        // wal_id of last record in page
[num_records: 4 bytes LE]          // complete records in page
[data_size: 4 bytes LE]            // bytes used (excluding header + padding)
[flags: 2 bytes LE]                // PARTIAL_RECORD_START/CONT/END
[checksum: 4 bytes LE]             // CRC32 of entire page (header + data + padding)
[reserved: 2 bytes]

Usable data per page: PAGE_SIZE - 32 = 4064 bytes
Overhead: 0.8% (32/4096)
```

#### Large Records Spanning Pages

```
Record > 4064 bytes:

Page N:   [header: flags=PARTIAL_START] [record_header + payload_part_1] [pad]
Page N+1: [header: flags=PARTIAL_CONT]  [payload_part_2]                [pad]
Page N+2: [header: flags=PARTIAL_END]   [payload_part_3] [next_record]  [pad]
```

#### Write Buffer (in-memory, aligned)

```cpp
class wal_write_buffer_t {
    alignas(4096) char current_page_[PAGE_SIZE];  // O_DIRECT ready
    size_t current_offset_{PAGE_HEADER_SIZE};
    uint32_t num_records_{0};
    uint64_t page_lsn_{0};
    uint64_t page_end_lsn_{0};

    void append_record(data, size, wal_id):
        if fits in current page:
            memcpy(current_page_ + offset, data, size)
        elif fits in fresh page:
            flush_page()           // write full aligned page to disk
            start_new_page()
            memcpy(...)
        else:
            flush spanning pages   // PARTIAL flags

    void flush_page():
        fill page header (lsn, end_lsn, num_records, data_size)
        zero padding
        compute page checksum
        file_->write(current_page_, PAGE_SIZE)  // always full page
};
```

#### Binary Search (O(log N) seek for replication/PITR)

```
find_page(target_lsn):
    lo = 1, hi = file_size / PAGE_SIZE - 1    // skip file header
    while lo < hi:
        mid = (lo + hi) / 2
        read page header at mid * PAGE_SIZE (32 bytes)
        if page.page_lsn > target_lsn: hi = mid
        else: lo = mid + 1
    // linear scan within page lo
```

#### O_DIRECT / io_uring Compatibility

```
O_DIRECT requirements:
    ✓ Buffer aligned: alignas(4096) current_page_
    ✓ Size aligned: always write PAGE_SIZE bytes
    ✓ Offset aligned: pages at multiples of PAGE_SIZE

io_uring batch:
    io_uring_prep_write(sqe, fd, page_buffer, PAGE_SIZE, offset)
    io_uring_submit(&ring)
    // Multiple pages in one kernel submission
```

#### Key Properties

| Property | Value |
|---|---|
| Page size | 4096 (configurable) |
| Page header | 32 bytes |
| Usable per page | 4064 bytes |
| Overhead | 0.8% |
| Alignment | O_DIRECT ready |
| Seek | Binary search O(log N) by page_lsn |
| Checksum | Per-page CRC32 + per-record CRC32 chain |
| Large records | Spanning pages with PARTIAL flags |
| Write unit | Always full pages |
| Group commit | Natural — records accumulate in buffer between flushes |

#### Page Checksum Failure Behavior

```
On read, if page checksum fails:
    1. Page considered corrupt → W-CORRUPT (STOP-A) applies
    2. Stop replay at this page
    3. All records in this page and subsequent pages NOT replayed
    4. Recovery uses checkpoint + WAL up to previous valid page
    5. Log WARNING with page number and byte offset
```

#### Spanning Records and truncate_before

```
Record R spans pages N and N+1 (PARTIAL_START + PARTIAL_END):

    truncate_before(checkpoint_wal_id):
        If checkpoint_wal_id falls within spanning record R:
            → Entire record R discarded (cannot partially truncate)
            → Page N deleted (PARTIAL_START, no complete record)
            → Page N+1: if contains other complete records after R, kept

        If both pages N and N+1 have page_end_lsn <= checkpoint_wal_id:
            → Both pages deleted (safe, all records covered by checkpoint)
```

### W-FORMAT: Raw Binary WAL Record

Replaces msgpack serialization. Matches disk format (raw binary blocks).
Records are packed inside pages (see W-PAGE above).

#### Record Layout

```
WAL Record:
[size: 4 bytes LE]              // total payload size (excluding size and crc fields)
[last_crc32: 4 bytes LE]        // CRC chain from previous record
[wal_id: 8 bytes LE]            // monotonic WAL ID
[txn_id: 8 bytes LE]            // transaction ID
[record_type: 1 byte]           // COMMIT=1, INSERT=10, DELETE=11, UPDATE=12
[database_len: 2 bytes LE]      // 0 for COMMIT
[collection_len: 2 bytes LE]    // 0 for COMMIT
[row_start: 8 bytes LE]         // INSERT: first row position
[row_count: 8 bytes LE]         // number of rows affected
[payload_size: 4 bytes LE]      // data_chunk or row_ids byte size
[database: database_len bytes]
[collection: collection_len bytes]
[payload: payload_size bytes]
[crc32: 4 bytes LE]             // CRC of bytes between size and crc32

COMMIT record (compact):
[size: 4][last_crc32: 4][wal_id: 8][txn_id: 8][record_type: 1][crc32: 4]
= 29 bytes
```

#### Data Chunk Payload (for INSERT/UPDATE)

```
[num_columns: 2 bytes LE]
[num_rows: 4 bytes LE]
[null_mask_size: 4 bytes LE]          // 0 if no nulls
[null_mask: null_mask_size bytes]     // bitmask, 1 bit per row per column

For each column:
    [physical_type: 1 byte]
    [data_size: 4 bytes LE]
    [data: data_size bytes]

    Fixed-size (BIGINT, DOUBLE, INT32, BOOL, etc.):
        data = raw memcpy of column buffer (num_rows × type_size)

    Variable-size (STRING):
        data = [offsets: (num_rows+1) × 4 bytes LE] + [string_data]
        string[i] = data[offset[i] .. offset[i+1]]
```

#### Row IDs Payload (for DELETE)

```
[raw int64_t array: row_count × 8 bytes LE]
// Direct memcpy from pmr::vector<int64_t>
```

#### Update Payload (for UPDATE)

```
[row_ids_size: 4 bytes LE]
[row_ids: row_ids_size bytes]          // raw int64_t array
[data_chunk: remaining bytes]          // same format as INSERT payload
```

### Comparison: msgpack vs raw binary

| Aspect | msgpack (current) | Raw binary (new) |
|---|---|---|
| COMMIT record | ~30 bytes (3-elem array + tags) | 29 bytes (fixed) |
| INSERT header | ~20 bytes (tags + array headers) | 49 bytes (fixed, but no tags) |
| Per uint64 field | 9 bytes (1 tag + 8 value) | 8 bytes (value only) |
| Per string | 1-5 header + data | 2 length + data |
| data_chunk | msgpack nested arrays, per-value tags | memcpy column buffers |
| Row IDs | msgpack array, per-element pack loop | memcpy int64 array |
| Serialization speed | ~10-100μs (msgpack pack per field) | ~1-10μs (memcpy) |
| Deserialization speed | ~10-100μs (msgpack unpack per field) | ~1-5μs (pointer cast + memcpy) |
| Dependencies | msgpack-cxx library | None |
| data_chunk 100 rows × 5 BIGINT cols | msgpack: ~5KB (tags per value) | raw: ~4KB (memcpy) |

### Integration with Catalog Migration

#### Styk 1: pg_catalog = "pg_system" database

System tables live in special "pg_system" database with its own WAL actor.
DDL writes to system WAL. DML writes to per-user-database WAL.

```
WAL actors:
    wal_actor_pg_system → system tables (pg_class, pg_attribute, ...)
    wal_actor_mydb      → user data (mydb.users, mydb.orders, ...)

DDL (CREATE TABLE mydb.users):
    executor → send(wal_manager, write_physical_insert, "pg_system", "pg_class", ...)
    executor → send(wal_manager, commit_txn, txn_id, sync_mode, "pg_system")

DML (INSERT INTO mydb.users):
    executor → send(wal_manager, write_physical_insert, "mydb", "users", ...)
    executor → send(wal_manager, commit_txn, txn_id, sync_mode, "mydb")
```

No distributed commit. DDL transaction = system WAL only. DML transaction = database WAL only.

#### Styk 3: D4 lazy load + WAL recovery

User WAL replay creates storage on demand from pg_class schema.
System WAL replayed BEFORE user WAL (pg_class available).

```
Recovery:
    1. Load 9 system tables from checkpoint (sync)
    2. Replay system WAL → pg_class, pg_attribute restored
    3. Start schedulers
    4. Replay user WAL per database:
       for each record in wal_mydb:
           if not storages_.contains(record.collection):
               schema = scan pg_attribute WHERE attrelid = oid
               storages_[name] = create_empty_storage(schema)
           apply record to storage
```

#### Styk 4: WAL retention per-database

Retention = min(prev_checkpoint_wal_id) across ALL tables in database.

```
checkpoint_all():
    for each table in database:
        table.prev_checkpoint_wal_id = table.checkpoint_wal_id
        table.checkpoint_wal_id = current_wal_id

    min_prev = min(table.prev_checkpoint_wal_id for all tables)
    send(wal_actor, truncate_before, min_prev)
```

#### Styk 6: WAL manager routes internally

Executor and dispatcher see ONE wal_address (manager). Manager routes by database.
No changes to executor, dispatcher, sync(), or pipeline::context_t.

```
manager_wal_replicate_t:
    wal_actors_ = {
        "pg_system": wal_system_actor,
        "mydb": wal_mydb_actor,
    }

    write_physical_insert(session, db, coll, data, txn_id):
        actor = wal_actors_[db]
        co_await send(actor, write_physical_insert, ...)

    commit_txn(session, txn_id, sync_mode, database):
        actor = wal_actors_[database]
        co_await send(actor, commit_txn, ...)

    create_database_wal(database_name):
        actor = spawn<wal_replicate_t>(database_name)
        wal_actors_[database_name] = actor

    drop_database_wal(database_name):
        wal_actors_.erase(database_name)
```

Executor sends to wal_manager (same address as today).
Manager routes to per-database actor (internal).
sync() passes one wal_address (manager). No change.

### Files to Change

| File | Change |
|---|---|
| **NEW** services/wal/wal_page.hpp | Page structure: header, buffer, flush, read, binary search |
| **NEW** services/wal/wal_binary_format.hpp | Raw binary record serializer/deserializer (replaces dto) |
| services/wal/dto.hpp | DELETE (replaced by wal_binary_format) |
| services/wal/dto.cpp | DELETE (replaced by wal_binary_format) |
| services/wal/wal.hpp | Per-database actor, wal_write_buffer_t, fsync in commit_txn |
| services/wal/wal.cpp | Page-based write/read, CRC chain verification at startup, truncate at corruption |
| services/wal/wal_reader.hpp | Page-based reader, binary search by LSN |
| services/wal/wal_reader.cpp | Rewrite for page format |
| services/wal/record.hpp | Keep struct, verify enum values match W-FORMAT |
| services/wal/manager_wal_replicate.hpp | Route by database name (not hash), create/drop database WAL actors |
| services/wal/manager_wal_replicate.cpp | Per-database actor lifecycle, routing logic |
| services/wal/base.hpp | Add page constants (PAGE_SIZE, HEADER_SIZE, MAGIC) |
| components/vector/data_chunk.hpp | Add serialize_binary / deserialize_binary |
| components/vector/data_chunk.cpp | Column-native serialization (memcpy fixed, offset+data variable) |
| services/disk/manager_disk.cpp | Backup prev checkpoint (rename/fsync/delete per table), WAL retention min(prev) |
| components/table/table_storage_t | Add wal_sync_mode field, prev_checkpoint_wal_id |
| services/collection/context_storage.hpp | Add wal_sync_mode field |
| services/collection/executor.cpp | Track effective_sync, pass database name to commit_txn |
| components/configuration/configuration.hpp | Add wal_sync_mode enum, page_size config |
