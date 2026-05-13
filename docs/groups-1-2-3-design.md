# Design: closing the remaining 4 tests (Groups 1, 2, 3)

> **State**: `70/74` после Group 4 (commit `0312190`). Этот документ —
> архитектурный design для Groups 3 / 1 / 2. Group 4 был test-fix only;
> остальные требуют real architecture work, поэтому документируем design
> до implementation.

---

## Group 3 — `test_collection::sql::index` (CREATE INDEX on relkind='g')

### Current state

- `validate_logical_plan.cpp:2147` rejects ALL CREATE INDEX on relkind='g'.
- The test's `create index before insert` expects this rejection.
- The test's `create_index` after INSERT expects success.
- `operator_create_index_backfill_t` exists (lines 32-112 in
  `components/physical_plan/operators/operator_create_index_backfill.cpp`)
  but is never reached because validate rejects first.

### Problem space

```
CREATE INDEX path:
  parser → transformer → validate (REJECT here for 'g') → planner → executor
                              │
                              └── currently terminal for 'g' tables
```

After guard relax, control reaches the planner → executor → backfill operator.
But empirically: SELECT through the just-created index returns 0 rows
(test_collection_sql.cpp:542 fails with 0 == 9).

### Two-pronged design

#### Part A — Validate guard relaxation

Reject only when columns are not yet registered:

```cpp
if (tbl_idx && tbl_idx->relkind == 'g' && tbl_idx->columns.empty()) {
    return error_t{...};  // "INSERT data first to register schema"
}
```

This unblocks the "post-INSERT CREATE INDEX" path while keeping the
"empty 'g' table CREATE INDEX" rejection that
`create index before insert` exercises.

**Risk**: low. The empty-columns check preserves the existing rejection
for the bare-CREATE-TABLE → CREATE INDEX sequence. Subsequent type
evolution invalidating the index is a documented residual (see
`docs/phase7-deferred-items.md` §7.6).

#### Part B — Backfill verification (lldb-pinpoint required)

`operator_create_index_backfill_t::await_async_and_resume`:

```cpp
1. send register_collection (idempotent)
2. send create_index → id_index
3. if disk_address != empty:
     send storage_total_rows  // ← Q1: returns >0 for in-memory 'g'?
     if total_rows > 0:
       send storage_scan_segment  // ← Q2: returns chunk with the rows?
       if scan_data:
         send insert_rows  // ← Q3: index_scan can find them after?
4. flip pg_index.indisvalid → true
```

Three unknowns to verify with lldb + fprintf(stderr) traces:

- **Q1**: Does `storage_total_rows` return >0 for an in-memory relkind='g'
  table after 100 rows were INSERTed? If 0, `manager_disk_storage.cpp:240`
  `s->total_rows()` is the path — need to check if in-memory storage's
  counter tracks 'g' inserts.

- **Q2**: Does `storage_scan_segment(table_oid, 0, total_rows)` return
  the expected chunk? If empty, scan_segment doesn't cover in-memory 'g'
  rows.

- **Q3**: Does subsequent `manager_index_t::search` find the inserted rows?
  Possible MVCC visibility issue: CREATE INDEX is a separate txn from
  the INSERTs, and index entries might be tagged with the CREATE INDEX
  txn's commit_id; subsequent SELECT in a fresh txn might not see them.

**Investigation path**:
1. Patch backfill operator with `fprintf(stderr, ...)` at each step.
2. Build + run `sql::index` test.
3. Observe which step short-circuits (total_rows=0, scan empty, or search empty).
4. Fix the identified gap.

### Acceptance

- `test_collection::sql::index` passes end-to-end (lines 519, 525, 542).
- No regression in other CREATE INDEX tests
  (`test_index::base`, `index already exist`, `delete_and_update`,
  `no_type base check`).

### Estimate

~3-5 hours, ~50 LOC.

---

## Group 1 — `dynamic_schema_drop_column` family

> **Status (2026-05-13)**: attempted transfer_scan inline filter via
> `read_rows_by_key`. lldb-probe revealed deeper MVCC issue:
>
> ```
> [TS] table_oid=16385 col_count=3        (storage has a, b, c physically)
> [TS] relkind='g' pc_rows=1              (pg_class scan ok)
> [TS] cc_rows=3                          (pg_computed_column has 3 rows)
> [TS] latest.size=3 live_names.size=3
> [TS]   a ver=0 rc=1
> [TS]   b ver=0 rc=1                     (LIVE! tombstone NOT visible)
> [TS]   c ver=0 rc=1
> ```
>
> After `ALTER DROP COLUMN b`, `operator_computed_field_unregister_t`
> writes a tombstone row (ver=max+1, rc=0) via `append_pg_catalog_row`
> → `ctx->pg_catalog_appends` → dispatcher's `storage_commit_appends`.
> The tombstone IS persisted, but `manager_disk_t::read_rows_by_key`
> on `pg_computed_column` filtered by `relid==table_oid` from a
> fresh SELECT txn sees only the 3 original ver=0 rows, NOT the
> ver=1 tombstone.
>
> **Two possibilities** (lldb-probe required to pinpoint):
> 1. Tombstone's `commit_id` is higher than SELECT's `start_time` —
>    SELECT can't see it (MVCC isolation).
> 2. `storage_commit_appends` for ALTER's tombstone didn't fire
>    or didn't update the row's MVCC tag correctly.
>
> Implication: Path B (in-operator filter via pg_computed_column scan)
> can't see the tombstone either. The fix must either:
> - Use `manager_disk_t::resolve_table`-style sync `inline_scan`
>   (sees ALL physical rows, not MVCC-filtered).
> - OR fix the underlying MVCC visibility of pg_computed_column
>   tombstones from subsequent txns.
>
> This is beyond Group 1's documented scope. Defer to follow-up
> session with focused MVCC investigation.


### Current state

- After `ALTER TABLE … DROP COLUMN b`, `transform_select.cpp:204-207`
  emits `break;` for `SELECT *` (no explicit projections).
- `transfer_scan` returns whatever `storage_scan` emits, which includes
  tombstoned column `b` (row_group physically keeps it until VACUUM —
  see `operator_computed_field_unregister.cpp:121-129`).
- The aggregate above passes the chunk through unfiltered.

### Architecture target

```
SQL: SELECT * FROM foo
       │
       ▼
TRANSFORMER:
   aggregate(foo) ← SELECT * leaves group/expressions empty
       │
       ▼
DISPATCHER auto-wrap (Phase 13):
   sequence_t
     ├── catalog_resolve_table_t (foo)   ← resolves columns
     └── aggregate(foo)
       │
       ▼
PASS 1 (executor runs only resolve children):
   operator_resolve_table_t — scans pg_class + pg_computed_column
     • stamps namespace_oid + table_oid on resolve node (DONE — 16da88a)
     • stamps live columns list (THIS DESIGN)
       │
       ▼
PASS 2 (validate / enrich / planner / exec):
   create_plan_aggregate reads live_columns from resolve node
     • builds transfer_scan with a column projection mask
     • mask = positions of live columns in storage's physical column order
       │
       ▼
   operator_transfer_scan applies mask when emitting chunks:
     • storage_scan returns full chunk
     • scan operator drops columns whose position is NOT in mask
       │
       ▼
   AGGREGATE / cursor: only live columns visible
```

### Changes required

#### Step 1 — `node_catalog_resolve_table_t` extension

`components/logical_plan/node_catalog_resolve_table.{hpp,cpp}`:

```cpp
struct resolved_column_info_t {
    std::string attname;
    catalog::oid_t attoid{INVALID_OID};
    components::types::complex_logical_type type;
    bool live{true};
};

class node_catalog_resolve_table_t {
    // existing fields...

    const std::vector<resolved_column_info_t>& columns() const noexcept;
    void set_columns(std::vector<resolved_column_info_t> cols) noexcept;

private:
    std::vector<resolved_column_info_t> columns_;
};
```

#### Step 2 — `operator_resolve_table_t` populates `columns_`

`components/physical_plan/operators/operator_resolve_table.cpp`:
in `await_async_and_resume`, after computing the columns list for the
output chunk (which the operator already does), ALSO call
`target_node_->set_columns(...)` with the live column metadata.

#### Step 3 — `plan_resolve_index_t::live_columns_for`

`services/dispatcher/validate_logical_plan.cpp` (or new header):

```cpp
struct plan_resolve_index_t {
    // existing maps...
    std::unordered_map<catalog::oid_t,
                       std::vector<resolved_column_info_t>>
        columns_by_table_oid;
};

inline const std::vector<resolved_column_info_t>*
live_columns_for(catalog::oid_t table_oid) {
    const auto* idx = active_plan_resolve_index();
    if (!idx) return nullptr;
    auto it = idx->columns_by_table_oid.find(table_oid);
    return it == idx->columns_by_table_oid.end() ? nullptr : &it->second;
}
```

`gather_plan_resolve_index` extended to populate `columns_by_table_oid`
from each `node_catalog_resolve_table_t`'s `columns()`.

#### Step 4 — `create_plan_aggregate.cpp` / `create_plan_match.cpp`

When constructing `transfer_scan` for a table, look up
`live_columns_for(table_oid)`. If non-null, compute a projection mask
(vector of indices into the storage's physical column order) and pass
to `transfer_scan`'s constructor.

`transfer_scan` extended:

```cpp
class transfer_scan : public read_only_operator_t {
public:
    transfer_scan(memory_resource*, oid_t, limit_t,
                  std::vector<int> projection = {});  // NEW
    // ...
};

actor_zeta::unique_future<void> transfer_scan::await_async_and_resume(ctx) {
    auto data = co_await storage_scan(...);
    if (data && !projection_.empty()) {
        // Build a filtered chunk: keep only data->data[projection_[i]] for each i.
        // Preserves projection order (== logical column order from resolve).
    }
    output_ = ...;
}
```

#### Step 5 — Dispatcher Pass 1 activation

Currently the auto-wrap + Pass 1 prototype is commented out in
`services/dispatcher/dispatcher.cpp` (see commit `16da88a` notes —
prototype broke `operator_insert` 6/50 rows in a re-entry case).

Re-enable Pass 1 such that:
- Detect transformer-emitted or auto-synthesized
  `sequence_t(catalog_resolve_*, consumer)` wrap.
- Run a focused executor invocation for only the resolve children.
- After completion, the catalog_resolve nodes have stamped metadata
  (oids + columns).
- Then proceed with normal Pass 2 (validate / enrich / planner / exec
  of the unwrapped consumer or full wrap).

**Critical risk** (from session experience): Pass 1 must NOT interfere
with operator_insert's chunk processing. The previous prototype
re-entered the executor and broke partial column matching in
storage_append's positional fallback. Investigation suggests Pass 1
should use a SEPARATE session / txn or be inline-resolved without going
back through the executor.

### Acceptance

- `dynamic_schema_drop_column` passes (line 1655).
- `dynamic_schema_drop_then_readd_preserves_old_data` passes.
- All currently-passing tests stay passing (especially:
  `dynamic_schema_basic_flow`, `dynamic_schema_type_evolution_multistep`,
  every relkind='r' SELECT).

### Estimate

~1-2 days. Critical path is Step 5 (dispatcher Pass 1) which has known
landmines.

---

## Group 2 — `dynamic_schema_union`

### Current state

`transform_select.cpp:103` enters with `SelectStmt::op = SETOP_UNION`
(or INTERSECT/EXCEPT), `targetList = nullptr`. `fca8eed` added a guard
that throws `runtime_error` for this case → caught by
`wrapper_dispatcher::execute_sql` → `sql_parse_error` cursor. Test
expects success → still fails.

### Architecture target

```
SQL: SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2
       │
       ▼
PARSER produces:
   SelectStmt {op=SETOP_UNION, all=true, larg=SelectStmt(t1), rarg=SelectStmt(t2)}
       │
       ▼
TRANSFORMER:
   transform_select detects op != SETOP_NONE:
     left  = transform_select(larg)
     right = transform_select(rarg)
     return make_node_union(resource, left, right, all=true)
       │
       ▼
node_union_t (NEW):
   • children[0] = left aggregate
   • children[1] = right aggregate
   • all_ : bool  (UNION vs UNION ALL)
       │
       ▼
PLANNER:
   walk_ddl falls through default case, recurses into both children
   (no special rewrite). Returns node_union_t unchanged.
       │
       ▼
PHYSICAL_PLAN_GENERATOR:
   create_plan_union (NEW) → operator_union_t with two children.
       │
       ▼
EXECUTOR:
   operator_union_t (NEW)::await_async_and_resume:
     • runs left child operator chain (reuses existing executor mechanics)
     • runs right child operator chain
     • concatenates output chunks
     • if NOT all_: dedup via hash set on row content
     • emits combined chunk
```

### Changes required

#### Step 1 — `node_union_t`

`components/logical_plan/node_union.{hpp,cpp}` (NEW):

```cpp
class node_union_t : public node_t {
public:
    node_union_t(memory_resource*, bool all);
    bool all() const noexcept { return all_; }

private:
    bool all_;
};
```

#### Step 2 — `transform_select` SETOP dispatch

`components/sql/transformer/impl/transform_select.cpp`:

```cpp
if (node.op != SETOP_NONE) {
    if (node.op != SETOP_UNION) {
        throw runtime_error("INTERSECT/EXCEPT not supported");
    }
    auto left  = transform_select(*node.larg, params);
    auto right = transform_select(*node.rarg, params);
    auto union_node = make_node_union(resource_, node.all);
    union_node->append_child(left);
    union_node->append_child(right);
    return union_node;
}
```

#### Step 3 — `operator_union_t`

`components/physical_plan/operators/operator_union.{hpp,cpp}` (NEW).

#### Step 4 — `create_plan_union.{hpp,cpp}`

`components/physical_plan_generator/impl/create_plan_union.{hpp,cpp}` (NEW)
+ dispatch case в `create_plan.cpp`.

#### Step 5 — Schema reconciliation

For UNION ALL, output schema = left's schema (column types from left,
positional match with right). For UNION (distinct), need to verify
left's and right's types are compatible.

For the test, left and right both SELECT `a, b` from 'g' tables with
the same registered columns → trivially compatible.

### Acceptance

- `dynamic_schema_union` passes.
- INTERSECT / EXCEPT continue to throw clean parse error
  (out of scope today).

### Estimate

~3-5 days. Most LOC.

---

## Recommended order (unchanged)

1. **Group 3** — небольшой scope, может разблокировать pattern для остальных.
2. **Group 1** — большой architectural но building на уже committed
   Phase 13 infrastructure (16da88a).
3. **Group 2** — самый изолированный, можно делать в параллель.

## Risks across all groups

- **Pass 1 activation** (shared between Group 1 and possibly Group 3):
  prior attempt regressed suite 74 → 63. Need either inline catalog
  read (bypassing executor re-entry) or separate session for Pass 1.
- **Test interactions**: changes to scan/projection (Group 1) affect
  every SELECT path. Risk of regressing currently-passing tests.

## What this document does NOT cover

- Implementation tasks (these are designs; coding is a separate task).
- Performance considerations (catalog reads on every scan have cost).
- VACUUM / physical compaction interactions.
