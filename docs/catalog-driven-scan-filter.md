# Catalog-Driven Scan Filter — Architectural Note

> **Status (2026-05-13)**: deferred. Documented after a session attempt to
> wire pg_computed_column tombstone filtering directly into `transfer_scan`
> caused a suite-wide regression (74 → 63 cases enumerated, mid-run abort).

## Problem

For `relkind='g'` (dynamic-schema) tables, `ALTER TABLE … DROP COLUMN`
writes a tombstone row to `pg_computed_column` (`attrefcount=0`,
`attversion=max+1`) but does **not** remove the physical column from the
row-group storage. The comment at
`components/physical_plan/operators/operator_computed_field_unregister.cpp:121-129`
spells out why: an immediate `storage::drop_column` call here broke the
re-INSERT path (`dynamic_schema_re_add_after_drop`) — `row_group::append`
crashes with a column-count mismatch because `drop_column` does not fully
reset row-group state mid-pipeline. Physical compaction is therefore
deferred to `operator_vacuum_t` (Phase 7.5b).

Until VACUUM runs, the row-group still has the dropped column. `SELECT *`
issues a `transfer_scan → storage_scan` and gets back a chunk whose
`data` vector includes the tombstoned column. The aggregate above it has
no explicit projections (SELECT * in `transform_select.cpp:204-207` is a
`break;`), so the dropped column leaks through to the cursor. Tests
`dynamic_schema_drop_column` and
`dynamic_schema_drop_then_readd_preserves_old_data` fail on exactly this
leak.

## Goal

After `DROP COLUMN b` on a dynamic-schema table, `SELECT *` must omit
`b` from the result chunk without depending on VACUUM having run.

## What was tried — and why it broke

```cpp
// transfer_scan::await_async_and_resume — after storage_scan
auto data = co_await storage_scan(...);

// Read pg_class via read_rows_by_key for relkind.
// If relkind == 'g':
//   read pg_computed_column via read_rows_by_key (filter by relid)
//   compute live names: max-version per attname where attrefcount > 0
//   filter data->data to only keep columns whose alias is live
```

Implementation in `transfer_scan.cpp` ~100 LOC. Build succeeded. Runtime:
suite enumeration stopped at 63 cases (was 74), mid-run abort. The
failure mode was not isolated — multiple tests broke, including ones
that don't hit `DROP COLUMN` at all.

### Why a "simple async read inside the operator" is not simple

1. **Pipeline-actor reentrancy**. `transfer_scan` already issues one
   `actor_zeta::send` (storage_scan). Adding two more (`read_rows_by_key`
   on `pg_class`, then on `pg_computed_column`) means the operator's
   coroutine suspends and resumes three times. Each resume is dispatched
   through the executor's `await_async_and_resume` loop; downstream
   operators in the same `operator_sequence` see a chunk whose column
   count changed under them after the first resume. Sort/group/match
   operators were built assuming `transfer_scan` emits a chunk whose
   schema matches `s->columns()` at construction time. Changing the
   chunk's column count post-scan invalidates that assumption — index
   maps, projection slots, and type bindings constructed at
   `create_plan_match` / `create_plan_aggregate` time are wrong.

2. **No "live columns" handle reaches the planner**. `create_plan_match`,
   `create_plan_aggregate`, etc. read `node->table_oid()` and instantiate
   operators with the *physical* column shape (the storage's
   `s->columns()`). They have no knowledge that some of those columns
   are tombstoned. Even if `transfer_scan` filters at the scan boundary,
   downstream operators were constructed against the unfiltered shape
   and assume positional access by column index.

3. **`pg_class` lookup keyed by name, not by oid**. The existing
   `manager_disk_t::resolve_table` API takes `(namespace_oid, name)`,
   not `table_oid`. Inside `transfer_scan` we have `table_oid_`. Adding
   a `read_rows_by_key(pg_class, "oid", table_oid_)` works but is a
   sequential scan over `pg_class` — every SELECT on every table pays
   this cost.

4. **'r' tables also need filtering** (`pg_attribute.attisdropped`),
   so the filter is not relkind='g'-only — and the lookup for 'r' uses
   a different system table than for 'g'. Two code paths inside the
   scan.

## Three architectural paths considered

### Path A — Filter inside `transfer_scan` (the tried approach)

Pros:
- Local; no changes to `create_plan_*` / planner.
- One file touched.

Cons:
- Pipeline reentrancy (see #1 above) — broke the suite.
- Downstream operators see unexpected shape (see #2).
- Sequential `pg_class` scan on every SELECT (see #3).
- Two code paths inside `transfer_scan` (see #4).

Verdict: blocked. Each cons item is itself a fix project.

### Path B — Plumb resolved-table metadata through the plan tree (full Step 3)

The Phase 13 plan calls for `operator_resolve_table_t` to stamp full
table metadata (including the live-column list) onto the
`node_catalog_resolve_table_t` logical node. The planner reads that
metadata when constructing `transfer_scan` / aggregate / match operators
and builds them with the *live-only* column shape from the start.

Pros:
- Single source of truth; downstream operators are consistent.
- No mid-pipeline shape change.
- Reuses the `operator_resolve_table_t` Pass-1 mechanism already wired
  up in this session (commits `16da88a`, `e5c5a76`).

Cons:
- Requires `node_catalog_resolve_table_t` to carry a column list
  (currently it only has `namespace_oid` + `table_oid`).
- Requires `operator_resolve_table_t` to populate that column list
  (currently emits a data_chunk but doesn't write to the logical node).
- Requires `create_plan_*` to read the live column list from the
  resolve node (via `plan_resolve_index_t`) and project accordingly.
- Requires the dispatcher's Pass-1 / two-pass execution to be properly
  wired (currently Pass 1 is documented and reverted; see commit
  `16da88a` notes).

Verdict: this is the architecturally correct path. ~150-300 LOC across
`node_catalog_resolve_table.{hpp,cpp}`, `operator_resolve_table.cpp`,
`create_plan_aggregate.cpp`, `create_plan_match.cpp`, dispatcher Pass 1.
Multi-session.

### Path C — Physical compaction at DROP time (re-attempted Phase 11.G)

Have `operator_computed_field_unregister_t` call
`storage::compact_relkind_g_storage` (or a synchronous equivalent)
immediately after the tombstone is written, so the physical column is
gone before any subsequent SELECT. The current code defers this to
`operator_vacuum_t`.

Pros:
- Physical truth in storage matches catalog truth.
- No downstream filtering needed.
- `transfer_scan` stays simple.

Cons:
- `dynamic_schema_re_add_after_drop` crashed when Phase 11.G tried this
  (see comment in `operator_computed_field_unregister.cpp:121-129`).
  `row_group::append` failed with column-count mismatch because
  `drop_column` does not fully reset row-group state when called
  mid-pipeline.
- Would need to fix the underlying `row_group` reset bug first.

Verdict: blocked on a deeper storage-layer fix to `row_group::drop_column`
mid-pipeline semantics.

## Recommendation

Path B. It's the path the Phase 13 plan
(`/Users/kotbegemot/.claude/plans/parallel-petting-haven.md`) already
endorses (the "extend node_catalog_resolve_table_t with resolved
metadata storage" task #193 in this session's task list, deferred from
the architectural infrastructure landing in commit `16da88a`).
The minimum to land:

1. Add `columns: std::vector<resolved_column_info_t>` field to
   `node_catalog_resolve_table_t` (resolved metadata, including
   `attname` and an "alive" flag).
2. Extend `operator_resolve_table_t::await_async_and_resume` to populate
   that field after the pg_computed_column / pg_attribute scan
   (the operator already does the scan to build its output chunk;
   reuse the same data).
3. Add a `plan_resolve_index_t::live_columns_for(table_oid)` helper.
4. In `create_plan_aggregate.cpp` / `create_plan_match.cpp`, when
   constructing `transfer_scan` for a table with a stamped resolve node,
   pass a `projection` argument (vector of column indices to keep) so
   the scan emits a pre-filtered chunk.
5. Wire dispatcher Pass 1 (currently commented-out in `dispatcher.cpp`
   per commit `16da88a` notes) so the resolve node is populated before
   the planner builds the physical plan.

## Tests this unblocks

- `integration::cpp::test_sql_features::dynamic_schema_drop_column`
- `integration::cpp::test_sql_features::dynamic_schema_drop_then_readd_preserves_old_data`

## Out of scope for this note

- Tombstone semantics on `relkind='r'` tables — pg_attribute has an
  `attisdropped` column with the same role; Path B should cover both
  via the same `live_columns` mechanism.
- VACUUM / compaction physical-removal timing.
- Index handling on tombstoned columns (separate Phase 7 deferred
  problem, see `docs/phase7-deferred-items.md` §7.6).
