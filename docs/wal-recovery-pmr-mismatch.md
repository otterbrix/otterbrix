# WAL-recovery PMR resource mismatch: investigating the SIGABRT in the Debug build

**Date:** 2026-06-03
**Branch:** `upgrade/actor-zeta-1.2.0` (HEAD = `c0d5185f`)
**Status:** root cause established, minimal fix proposed (not applied)

## Symptom

A fresh Debug build from scratch → 3 integration tests fail with SIGABRT,
and each crash kills the entire `test_otterbrix` binary (out of ~87 cases, ~19 get executed):

- `integration::cpp::test_wal_pool::update_wal_recovery`
- `integration::cpp::test_wal_pool::sql_dml_full_cycle`
- `integration::cpp::test_persistence::wal_recovery_dml_full_cycle`

```
Assertion failed: (resource_ == other.resource_),
function operator=, file validation.cpp, line 44.
```

Common pattern: phase 2 of the test — "restart and verify WAL replayed UPDATE".
Only the **UPDATE** replay fails; insert/delete replay passes.

## Stack (lldb, `-k "bt"`)

```
base_spaces.cpp:233   WAL-replay lambda → direct_update_sync(oid, row_ids, *r->physical_data)
  └ manager_disk_storage.cpp:122   manager_disk_t::direct_update_sync
    └ agent_disk.cpp:285           entry->storage->update(ids_vec, new_data)
      └ data_table.cpp:418         updates_slice.slice(data, ...)   ← updates_slice on the agent's resource_
        └ vector.cpp:230/168/181   vector_t::slice → reference → reinterpret
          └ validation.cpp:44      validity_mask_t::operator= → assert(resource_ == other.resource_)
```

## Root cause

A conflict of **pmr allocators**, not concurrency (everything is on a single main thread,
inside the `base_otterbrix_t` constructor during WAL replay):

- `r->physical_data` is deserialized from the WAL on the **system resource** (`&resource`
  from `base_spaces.cpp`);
- the storage's `data_table_t` lives on the **agent's resource** (`agent->resource()`);
- `data_table_t::update` does `updates_slice.slice(data, ...)` — a zero-copy
  reference to foreign vectors, and `validity_mask_t::operator=` requires the
  resources to match.

Why insert replay does not fail: `storage->append()` internally **copies** the data
(row_groups → column segments), whereas `update()` **slices**. The exact failing
combination is "foreign resource × slicing update".

## Why it surfaced only now ("the code didn't change, yet it started crashing")

1. **The assert exists only in Debug.** `assert(resource_ == other.resource_)`
   is compiled only without `NDEBUG`. Moreover, in an NDEBUG build this code is
   **functionally safe**: `operator=` still performs a deep-copy of the mask
   onto *its own* `resource_` (validation.cpp:50,
   `std::make_shared<validity_data_t>(resource_, other.validity_mask_, count_)`).
   That is, the resource mismatch is latent — without the assert it does not
   manifest in any way.

2. **Building from scratch changed the configuration.** There was no old build
   directory (it was configured anew); conan `--build=missing` at 22:31 built the
   **first Debug binary of actor-zeta** in the cache (`~/.conan2/p/b/actor55eb6382ee9f1`) —
   all previous packages were Release only (gnu17). This indirectly indicates
   that earlier runs (including the "769/771 PASS" in the HEAD commit message of
   `c0d5185f`, made the same day at 22:12) ran with a different build profile,
   where asserts are disabled.

3. **The mismatch itself is old** — it appeared with the mega-commit `cad41ea9`
   (actor-zeta 1.2.0 + Pure MVCC): WAL replay is deserialized on the system
   resource, while the storage lives on the agent's resource (`direct_update_sync`
   has existed as a path since "Disk for table" #462). The combination "Debug
   asserts + update replay" was simply executed for the first time in this
   configuration.

**Proof limitation:** the old build directory was deleted along with its
`Testing/` log, so the profile of the previous green run is reconstructed
indirectly (from the contents of the conan cache). The mechanism is nonetheless
fully explained: the build configuration changed, not the code.

## Hypotheses checked and rejected

| Hypothesis | Verdict |
|---|---|
| Regression in the latest commits (`c0d5185f`, `724ad7ff`) | No — none of them touch agent_disk/replay/vector resources |
| The actor-zeta dependency drifted (a "moving 1.2.0") | No — the cache has exactly one recipe revision of 1.2.0 (from 2026-05-28); the large source diff turned out to be a comparison of 1.1.1 ↔ 1.2.0 |
| A race between actors | No — the crash is on a single thread before the schedulers start |

## Minimal fix (agent boundary)

`agent_disk.cpp`, `direct_update_sync`, before `storage->update()`:

```cpp
// new_data is deserialized on the WAL-replay resource; the storage lives on the
// agent's resource. update() slices (zero-copy refs), so we materialize a local
// copy on resource() — the same boundary rule as for ids_vec above.
components::vector::data_chunk_t local(resource(), new_data.types(), new_data.size());
new_data.copy(local, 0);   // deep copy: vector_ops::copy per column
entry->storage->update(ids_vec, local);
```

Why this is correct (verified against the code):

- `data_chunk_t::copy()` (data_chunk.cpp:172) — element-wise copy via
  `vector_ops::copy`: validity via `copy_indexing` (bit-level `set` /
  `slice_in_place` on its own buffer), strings are re-allocated into the
  target's string buffer on its resource. There are no resource asserts on this path.
- The ctor `data_chunk_t(resource, types, capacity)` creates FLAT vectors,
  `count_ = 0` → the preconditions of `copy()` are satisfied.
- `partial_copy()` is **not suitable** — internally it slices (zero-copy), and the
  vectors stay on the source's resource (vector.cpp:61).
- `direct_append_sync` / `direct_delete_sync` need no changes: append copies
  internally, delete builds `ids_vec` already on `resource()`.

Cost: one deep copy of the chunk per UPDATE WAL record — only on the recovery
path (bootstrap); the hot path is unaffected.

## Relation to the async-first refactoring

Moving replay onto a mailbox/queue (see `async-first-refactor.md`) **does not fix
this bug by itself**: the message carries the same `data_chunk_t` with the same
pointer to the foreign allocator. The queue solves threads and ownership;
materialization at the receiver's pmr boundary is needed in both architectures —
the minimal fix is not thrown away, it becomes part of the agent-boundary contract.

## Open tails

- The fix is not applied (awaiting a decision).
- After the fix, there may be a next hidden bug further along the same tests
  (the recovery tests failed on the first assert, so the subsequent code was not
  executed) — the criterion of truth: rebuild + run `test_wal_pool::*` and
  `test_persistence::*`.
- Separately observed: a hang of the background full-suite run (the log froze on a
  DML insert, the process had to be killed) — a likely connection with the known
  cooperative flakiness (see async-first-refactor.md, "multithread bug"),
  requires separate reproduction.
