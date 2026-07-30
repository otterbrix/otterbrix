# .otbx Storage Format — Improvement Roadmap

Status: proposal
Scope: the on-disk representation only (block layer, segment serialization,
checkpoint protocol). The in-memory table layer (row groups, vectors, MVCC),
the WAL, the actor ownership model and the pg_catalog stay as they are.

---

## 1. Current state (facts)

The format is a per-table single-file block store, architecturally a DuckDB-style
storage engine sliced Postgres-style into one file per relation:

- **File layout**: `${disk.path}/${db_oid}/${tbl_oid}/table.otbx`
  (`services/disk/manager_disk_storage.cpp`), sidecars `table.otbx.wal_id`
  (last-checkpointed WAL id) and `table.otbx.prev` (torn-checkpoint backup)
  (`services/disk/agent_disk.cpp`, checkpoint_inner).
- **Headers**: `main_header_t` (magic `OTBX`, `CURRENT_VERSION = 1`, unused
  `flags`) + **double** `database_header_t` with an `iteration` counter; loader
  picks the newer valid copy. Data blocks start at `3 * SECTOR_SIZE`
  (`components/table/storage/single_file_block_manager.hpp`).
- **Blocks**: 256 KB (`DEFAULT_BLOCK_ALLOC_SIZE`), 8-byte header holding a
  CRC32-C checksum per block; persistent free-list serialized into the metadata
  chain; metadata manager splits a block into 64 sub-blocks of ~4 KB linked by
  explicit next-pointers (`metadata_manager.*`, `metadata_writer.*`).
- **Table data**: row groups → `column_data_t` → `column_segment_t` backed by
  `(block_id, offset, segment_size)`; validity stored as separate BIT segments;
  struct/list/array as child columns. Checkpoint serializes
  `persistent_column_data_t`: `data_pointer_t{row_start, tuple_count, block_id,
  offset, compression, segment_size}`, whole-column stats (v2 field) and
  per-segment stats (v3 field) — the versioned-field pattern already works
  (`persistent_column_data.cpp`, `data_pointer.cpp`).
- **Checkpoint**: per-table, incremental via `partial_block_manager_t`
  (segments < 80% of a block are packed together), two-fsync commit
  (data+metadata → fsync → header → fsync), plus a **full-file `.prev` copy**
  before every checkpoint as a second torn-write protection
  (`agent_disk.cpp:checkpoint_inner`, `manager_disk.cpp:table_storage_t::checkpoint`).
- **MVCC**: memory-only. `row_version_manager_t` has no serialize path; the
  checkpoint is *MVCC-gated* — it refuses while any version stamp is above the
  publish watermark, because the file stores no version metadata
  (`agent_disk.cpp`, compact gate). Recovery = load file + WAL tail replay.
- **Deletes on disk**: `row_group_pointer_t` already serializes a
  `deletes_pointers` field and `row_group_t` carries `deletes_pointers_` /
  `deletes_is_loaded_` scaffolding, but `write_to_disk` never populates it.
- **Compression**: `compression_type` enum (UNCOMPRESSED, CONSTANT, RLE,
  BITPACKING, DICTIONARY, VALIDITY_UNCOMPRESSED) exists and every
  `data_pointer_t` carries a `compression:u8`. **Three lightweight codecs are
  implemented end-to-end** — CONSTANT, RLE and DICTIONARY: the write side
  analyzes and packs in `column_checkpoint_state.cpp` (`~236–292`), the
  read/fetch/predicate side lives in `column_segment.cpp` (landed in #462).
  **Not yet implemented: generic block compression (ZSTD) and BITPACKING** —
  the remaining enum slots that would cover general numeric / arbitrary data.
- **Row group size**: effective default is `DEFAULT_VECTOR_CAPACITY = 1024`
  rows (the execution-vector size), configurable via the `collection_t` ctor.

### Known gaps, in one line each

1. No *generic* compression (ZSTD) or BITPACKING at rest — three lightweight
   codecs (CONSTANT/RLE/DICTIONARY) already land; the general-purpose and
   bit-packed cases are the gap.
2. Deletes and version metadata are not persisted → checkpoint is MVCC-gated,
   vacuum is coupled to checkpointing.
3. Row groups are execution-sized (1024), not storage-sized.
4. `.prev` full-file copy makes checkpoint cost O(file), not O(dirty).
5. Per-segment statistics are persisted but scan pruning over them is not
   guaranteed end-to-end; no bloom filters for cold key lookups.
6. `flags` in the main header are unused — no feature-bit discipline yet.
7. The file never shrinks (free blocks are reused, trailing space not
   truncated).
8. Predicate pushdown is skipped on compressed segments: `check_predicate`
   returns `true` unconditionally for RLE/DICTIONARY (`column_segment.cpp:1099`),
   so those segments are always fully scanned+filtered — the zone-map /
   predicate-skip win is lost exactly where a codec was applied (couples to
   gap 5).

---

## 2. Goals / non-goals

**Goals**
- Cut disk footprint 3–10× (compression).
- Make checkpoint cost proportional to dirty data, not file size.
- Decouple vacuum from checkpoint; survive restart without replaying deletes.
- Keep every existing `.otbx` readable at every step (no stop-the-world
  migrations).

**Non-goals**
- Replacing the format with Parquet/ORC/Arrow IPC/MergeTree — evaluated and
  rejected for the base store (see §6); interop is provided at the SQL boundary
  instead.
- Persisting in-flight (uncommitted) MVCC state.
- Multi-writer files. One agent per file stays an invariant.

---

## 3. Staged plan

Each stage is an independent PR with red-first tests; old files remain readable
after every stage.

### Stage 0 — format versioning discipline (~1 day)

- Use `main_header_t.flags` as **feature bits**:
  `bit0 = compressed_segments`, `bit1 = persisted_deletes`,
  `bit2 = version_metadata`, rest reserved.
- Policy: *always read older files; write new fields only under a flag*. Extend
  the `persistent_column_data` v2/v3 versioned-field pattern to the header.
- `validate()` rejects unknown flag bits with a clear error (today an unknown
  future file would be misread).
- Tests: old-file read; unknown-flag file → clean error.

### Stage 1 — compression + storage-sized row groups (~1–2 weeks)

Priority note: CONSTANT/RLE/DICTIONARY already exist (§1). The neutral VLDB
survey *An Empirical Evaluation of Columnar Storage Formats*
(Zeng/Pavlo/McKinney, PVLDB 17) finds the highest-value at-rest wins come from
**lightweight encodings, not heavyweight block compression** — general block
compression is often a net loss on NVMe (up to ~4.2× scan overhead) and decode
speed matters more than ratio. So finish the lightweight codecs first and treat
ZSTD as an optional, last step.

**1a. Finish the lightweight codecs + compressed-segment pushdown** (bit0).
- Add **BITPACKING + FrameOfReference** for integers (the missing enum slot),
  then **FSST** for strings and **ALP** for floats. Reference implementations
  exist in DuckDB / FastLanes / BtrBlocks (MIT/Apache).
- Fix predicate pushdown on compressed segments (gap 8): teach
  `check_predicate` to prune RLE/DICTIONARY segments instead of returning
  `true`, so zone-map / predicate skipping survives compression.
- Write path already sets `compression` per segment; add `uncompressed_size`
  to `data_pointer_t` (versioned field) where a codec needs it.
- Tests (red-first, one PR per codec): checkpoint→load roundtrip equality;
  reading a legacy uncompressed file; a mixed-codec file (incremental
  checkpoint leaves old segments in the old encoding next to new ones); a
  predicate-pruning counter test on a compressed segment.

**1b. Row group size 64–128k rows for disk tables.**
- The parameter already exists (`collection_t(row_group_size)`); raise the
  default for DISK relkinds only.
- Watch: cold-load granularity grows (a row group is the lazy-load unit);
  `initialize_scan_with_offset` geometry is documented geometry-agnostic — add
  a test pinning that with a non-1024 group size.
- Perf smoke: scan throughput, checkpoint time, memory of a single group load.

**1c. Generic ZSTD per segment (optional, last).**
- Only after the lightweight codecs: a whole-segment ZSTD pass for segments the
  lightweight codecs don't shrink well. Write path compresses before
  `partial_block_manager` placement (`data_pointer_t` gains `uncompressed_size`);
  read path decompresses in `block_handle_t::load()` into the buffer pool —
  in-memory representation stays uncompressed (pins, eviction, scans unchanged).
- Gate on the VLDB caveat: benchmark scan throughput on NVMe before enabling by
  default; it may stay opt-in per column.
- zstd is already in the dependency graph (verify via conan graph; it rides in
  with existing packages).
- Tests (red-first): roundtrip equality; a mixed file (ZSTD next to lightweight
  and uncompressed segments).

### Stage 2 — shadow paging; retire `.prev` (~1 week)

Today every checkpoint starts with a full `copy_file(.otbx → .prev)` — the
single most expensive and least incremental step.

- Invariant to establish: checkpoint writes modified segments **only into free
  blocks** (never in place); blocks of the previous version are returned to the
  free list **only after** the header swap. With the double-header `iteration`
  protocol this makes the previous file version self-consistent up to the swap —
  the `.prev` copy and its restore logic become redundant.
- Audit the one potential violator: `partial_block_manager` /
  `mark_as_modified` block reuse within a checkpoint window.
- Remove: the `.prev` copy, `.prev` restore in recovery
  (`manager_disk_io.cpp`), keep a read-only migration path for files that still
  have a `.prev` sibling.
- Tests (red-first): kill between fsync #1 and fsync #2 → previous version
  loads (re-target the existing torn-checkpoint tests from `.prev` to the
  header swap); block-reuse audit test (no block referenced by the old header
  is rewritten before swap).

### Stage 3 — persisted delete vectors (~1 week, bit1)

The format slot already exists (`row_group_pointer_t.deletes_pointers`).

- Serialize **committed** deletes from `row_version_manager_t` in
  `row_group_t::write_to_disk`; load them into the version manager on
  `load_from_disk`.
- Encoding: persist deletes as **deletion vectors (roaring bitmaps)** — the
  industry-standard metadata-only delete (Lance, Delta, Iceberg v2): one bitmap
  per row group, rewritten on checkpoint rather than in-place tombstones. This
  fits the incremental-checkpoint model (a changed bitmap is O(deletes), not
  O(rows)).
- Effect: checkpoint no longer needs deletes fully compacted away; vacuum
  becomes an independent background concern.
- Tests (red-first): insert → delete → commit → checkpoint → restart → rows
  invisible **without WAL replay** (today this scenario only holds via the
  compact gate); mixed old/new file.

### Stage 4 — pruning and cold-lookup structures (~1–2 weeks)

- **Zone maps end-to-end**: per-segment min/max stats are already persisted
  (v3); guarantee the scan path (`initialize_scan` + filter) actually skips
  segments by them, with a pinning test (scan counters).
- **Bloom filters per column** (optional, sidecar in the metadata chain, under
  a feature bit): targets cold `scan_by_keys` / FK checks, which today are full
  slice scans.
- Later, borrowed from MergeTree: a sparse index over a declared sort key
  (granule marks), if/when tables get an ORDER BY key.

### Stage 5 — version metadata; ungate the checkpoint (strategic, after 3)

- Persist a commit-id horizon per row group (bit2), later versioned row ranges.
- Effect: checkpoint stops refusing under live load (no more MVCC gate
  deferrals), recovery replays a shorter WAL tail.
- This is the deepest format change; do it on top of stages 0–3 discipline.

### Parallel track — interop without format changes

`COPY TO/FROM PARQUET` at the SQL boundary gives debugging/exchange/analytics
interop for near-zero format risk. Independent of all stages above.

---

## 4. Compatibility & migration policy

- Every stage keeps the reader able to open all previous minor layouts
  (feature bits + versioned fields).
- Writers emit new features only when the corresponding stage is enabled;
  a freshly checkpointed table naturally upgrades its file.
- No offline migration tool is required: the first post-upgrade checkpoint
  rewrites dirty segments in the new form; older segments read fine as-is.
- Downgrade story: a file with unknown feature bits fails loudly (stage 0);
  documentation notes which release introduced which bit.

## 5. Effort / impact summary

| Stage | What | Impact | Effort | Risk |
|---|---|---|---|---|
| 0 | feature bits + read-old policy | enables everything | S | low |
| 1a | finish lightweight codecs (bitpack/FoR/FSST/ALP) + compressed-segment pushdown | disk ×3–10, scan speed | S–M | low |
| 1b | 64–128k row groups | scans, compression ratio, metadata size | S | low-med (load granularity) |
| 2 | shadow paging, drop `.prev` | checkpoint O(dirty), −code | M | med (block-reuse audit) |
| 3 | persisted deletes (roaring bitmaps) | vacuum ⊥ checkpoint, faster restart | M | med |
| 1c | ZSTD per segment (optional) | further disk on residual segments | S–M | low-med (NVMe scan overhead) |
| 4 | zone maps + bloom | cold lookups | M | low |
| 5 | version metadata | checkpoint under load | L | high (deep change) |

Recommended order: **0 → 1a → 1b → 2 → 3 → 4 → (1c optional, interleaved) → 5**.
Stages 0–1 deliver most of the visible value. Per the VLDB evidence, 1c (ZSTD)
is optional and may stay opt-in — the lightweight codecs in 1a carry most of
the win.

## 6. Alternatives considered (and why rejected for the base store)

- **Parquet / ORC / Arrow IPC as the checkpoint format**: fits the
  "immutable snapshot + WAL tail" model, but loses checkpoint incrementality
  (`partial_block_manager`), adds a heavy dependency (Arrow C++ for Parquet) or
  narrows interop (ORC), and forces a row-group/file layout redesign. Kept as
  the *interop boundary* (`COPY TO/FROM PARQUET`) instead of the base format.
- **ClickHouse MergeTree (chDB / embedding)**: no standalone format library —
  reusing it means embedding the whole engine; the immutable-parts +
  async-mutations model contradicts the HTAP core (row-level MVCC, WAL-first
  statement atomicity, UNIQUE enforcement). Borrow ideas only: sparse index
  granules, Delta/Gorilla-style codecs, partition minmax.
- **RocksDB as the block layer**: brings its own WAL/snapshots we do not need,
  removes direct buffer-pool control, poor columnar scan locality.
- **Embedding DuckDB wholesale**: replaces half the DBMS, not the disk
  representation; unstable internal APIs; strategic identity question, not a
  format decision.
- **Newer research/industry file formats (Lance, Nimble, Vortex, BtrBlocks,
  FastLanes)**: all are immutable, scan-optimized analytics artifacts with no
  row-level MVCC / WAL / UNIQUE / in-place mutation *at the format layer* —
  mutation is always delegated to a table/versioning layer above the file. The
  most mature new ones (Vortex, Lance) are Rust with a C-ABI/FFI boundary that
  forfeits the direct buffer-pool ownership this engine requires; BtrBlocks and
  FastLanes are codec libraries, not formats — worth borrowing from (Stage 1a),
  not adopting. The vendorable-storage-only + columnar + in-place-incremental
  quadrant is empty, which is why this custom format exists. Corroborated by the
  neutral VLDB survey *An Empirical Evaluation of Columnar Storage Formats*
  (Zeng/Pavlo/McKinney, PVLDB 17), whose conclusion — lightweight encodings over
  heavyweight block compression — shaped the Stage 1 ordering above.

## 7. Invariants that must hold at every stage

1. One agent-writer per file (actor ownership); no file locks introduced.
2. WAL-first ordering for appends is untouched; recovery = file + WAL tail.
3. In-memory representation (vectors, row groups, pins) unchanged — only the
   at-rest encoding evolves.
4. Every stage lands red-first: a failing test demonstrating the gap precedes
   the implementation (project rule 18).
5. Old files readable forever; unknown future files fail loudly.
