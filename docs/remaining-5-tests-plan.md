# План — финальные failing тесты

> **State (2026-05-13)**: 70/74 (94.6%) — Group 4 закрыта в `0312190`.
> Все архитектурные коммиты сессии стабильны (`16da88a` → `0312190`).
> Этот документ — план как добить оставшиеся 4, с приоритетом, scope
> estimates и lldb-проверенными корнями.

## Status

- ✅ **Group 4** (`test_collection::insert`) — closed via `0312190`.
  Test updated к PostgreSQL DEFAULT semantics.
- ⏳ **Group 3** (`test_collection::sql::index`) — next.
- ⏳ **Group 1** (`dynamic_schema_drop_column` + `drop_then_readd`).
- ⏳ **Group 2** (`dynamic_schema_union`).

## Текущее состояние

```
test cases: 74 | 69 passed | 5 failed
assertions: 3839 | 3834 passed | 5 failed
```

5 failing:
1. `test_collection::insert` (`partial_insert` default-vs-NULL)
2. `test_collection::sql::index` (relkind='g' guard + backfill)
3. `test_sql_features::dynamic_schema_drop_column` (SELECT * leak)
4. `test_sql_features::dynamic_schema_drop_then_readd_preserves_old_data` (same family)
5. `test_sql_features::dynamic_schema_union` (UNION ALL not lowered)

## Группировка по подходу

### Группа 1 — Architectural (Phase 13 Step 3 full)
**Tests**: 3, 4 (dynamic_schema_drop_column, drop_then_readd).
**Scope**: ~150-300 LOC across 5+ files.
**Doc**: `docs/catalog-driven-scan-filter.md` (Path B).

### Группа 2 — Feature gap (требует impl)
**Tests**: 5 (dynamic_schema_union).
**Scope**: ~200-400 LOC — full SETOP_UNION / INTERSECT / EXCEPT lowering.

### Группа 3 — Multi-layer cooperation
**Test**: 2 (sql::index).
**Scope**: ~50 LOC guard relax + ~50 LOC backfill в operator_create_index_backfill.

### Группа 4 — Semantic decision
**Test**: 1 (test_collection::insert).
**Scope**: ~5 LOC код + изменения тестов / convention review.

---

## Plan: Group 4 (test_collection::insert) — START HERE

**Why first**: smallest LOC, clearest finding. Resolution unblocks test count.

### Root cause (already pinpointed)
`storage_append` в `manager_disk_storage.cpp:373-378` всегда заполняет missing
колонку дефолтом если он есть. test_collection::insert::"partial_insert/value_defaults"
ждёт NULL для nullable+default+omitted. 4 теста persistence
(`disk_partial_insert`, `default_application_in_session`,
`partial_insert_consistent_wal_recovery`, `partial_insert_two_columns_wal`)
ждут default behavior.

### Concrete steps
1. **Tab-review** test convention:
   - Open all 5 tests with conflict.
   - Decide: PostgreSQL behavior (DEFAULT applies для omitted) vs Mongo/JSON
     behavior (NULL для omitted unless DEFAULT keyword).
   - Document in `docs/insert-default-semantics.md`.

2. If PostgreSQL — update `test_collection::insert` line 262-263 to expect default.

3. If Mongo — update 4 persistence tests, keep current insert test, change
   storage_append code:
   ```cpp
   if (table_columns[t].is_not_null() && table_columns[t].has_default_value()) {
       // fill default
   } else {
       // set_all_invalid
   }
   ```

### Verification
- Run test_collection::insert + all 4 persistence tests.
- Total impact: ±0 tests (one or the other resolves).

### Effort
~30 min discussion + ~10 LOC.

---

## Plan: Group 3 (test_collection::sql::index)

### Root cause (lldb-confirmed)
Two gaps:
- **Validate guard** rejects CREATE INDEX on relkind='g' →
  `validate_logical_plan.cpp:2147`. Pre-existed in baseline.
- **Backfill operator** (`operator_create_index_backfill_t`) likely not invoked
  when guard rejects OR fails to populate index for in-memory storage.
  Earlier lldb hits showed `index_scan::await_async_and_resume` line 42
  (the empty-result branch) — meaning `ctx->index_address` is empty OR
  `manager_index_t::search` returned 0 rows.

### Concrete steps
1. **Relax guard**:
   ```cpp
   if (tbl_idx && tbl_idx->relkind == 'g' && tbl_idx->columns.empty()) {
       return error;  // ony reject if no schema yet
   }
   ```
   This unblocks CREATE INDEX after INSERT registered the schema.

2. **Debug backfill**: add fprintf(stderr, ...) traces в
   `operator_create_index_backfill_t::await_async_and_resume` lines 32, 66, 74.
   Verify operator is hit AND `total_rows > 0` AND `insert_rows` is called.

3. **If `total_rows == 0`** — investigate `manager_disk_t::storage_total_rows`
   for IN_MEMORY relkind='g' storage. May need to count via table_storage's
   row_group iterator instead of metadata.

4. **If insert_rows but search returns 0** — investigate
   `manager_index_t::search` для txn visibility. Index entries may have
   different visibility flag than data rows.

### Risks
- Relaxing guard breaks `dynamic_schema_*` tests that expect CREATE INDEX
  to fail on 'g' (none found in scan, low risk).
- Backfill side-effect: if existing 'r' tests work via different path,
  changes may regress.

### Effort
~2-3 hours debug + ~50 LOC.

---

## Plan: Group 1 (dynamic_schema_drop_column / drop_then_readd)

### Root cause (documented)
См. `docs/catalog-driven-scan-filter.md`. Path B рекомендация:
plan-tree metadata через `node_catalog_resolve_table_t.columns` field +
plan_resolve_index_t exposed live_columns + create_plan_aggregate /
create_plan_match construct operators с live-only projection.

### Concrete steps (from doc)
1. Add `columns: std::vector<resolved_column_info_t>` field to
   `node_catalog_resolve_table_t` + getter/setter.
2. Extend `operator_resolve_table_t::await_async_and_resume` чтобы populate
   the new field (reuse existing scan, just write to back-pointer).
3. Add `plan_resolve_index_t::live_columns_for(table_oid) -> vector<string>`
   helper в `validate_logical_plan.cpp`.
4. В `create_plan_aggregate.cpp` / `create_plan_match.cpp` после resolve
   read live_columns from idx, pass projection mask to `transfer_scan`.
5. Wire dispatcher Pass 1 (currently commented out in `dispatcher.cpp` —
   see `16da88a` notes) чтобы run resolve operators **before** validate
   so plan_resolve_index_t has data when `create_plan_*` runs.

### Risks
- **Pass 1 activation** previously broke `operator_insert` (74→63 mid-run).
  Need careful executor re-entry handling — separate pre-work.
- Adding column list field changes node memory layout; ensure existing
  tests still pass.

### Effort
~1-2 days, multiple files. Each step ~30-60 LOC.

---

## Plan: Group 2 (dynamic_schema_union)

### Root cause (lldb-confirmed)
`transform_select.cpp:137` deref'd null `targetList` для SETOP_UNION — fixed
in `fca8eed` to return clean error. But test EXPECTS UNION ALL to work.

### Concrete steps
1. Add SETOP_UNION handling в `transform_select`:
   ```cpp
   if (node.op != SETOP_NONE) {
       auto left = transform_select(*node.larg, params);
       auto right = transform_select(*node.rarg, params);
       // Combine via new node type or sequence of aggregates
       return make_node_union(resource_, left, right, node.all);
   }
   ```

2. Add new logical plan node `node_union_t` (or reuse `node_sequence_t`
   with custom output combine).

3. Add physical plan generator `create_plan_union.cpp` →
   `operator_union_t` that runs both children, concatenates output chunks.

4. INTERSECT / EXCEPT — similar shape, deduplicate semantics differ.

### Risks
- Schema reconciliation: left/right SELECT may have different column types.
  PostgreSQL requires compatible types; otterbrix needs to define rule.
- DISTINCT vs ALL handling: UNION dedups, UNION ALL doesn't.

### Effort
~3-5 days, multiple files. ~200-400 LOC.

---

## Recommended order

1. **Group 4** (semantic decision) — fastest. Trade 1 test for 4 OR keep
   current state. Either way, count stable.
2. **Group 3** (sql::index) — moderate. Unblocks 1 test.
3. **Group 1** (drop_column family) — large architectural. Unblocks 2 tests.
4. **Group 2** (UNION ALL) — largest. Unblocks 1 test.

Total to reach 74/74: ~1-2 weeks focused work.

## Out of scope decisions

- **Do not** attempt all 4 groups in single session — Group 1's Pass 1
  activation alone broke suite 74→63 in this session.
- **Do not** revert `9c6ae84` Path B documentation — it captures lessons.
- **Do not** relax `dynamic_schema_*` test expectations to "just pass" —
  they document genuine product behavior gaps.

## Файлы для каждой Group

| Group | Files |
|---|---|
| 4 | `services/disk/manager_disk_storage.cpp`, test files |
| 3 | `services/dispatcher/validate_logical_plan.cpp`, `operator_create_index_backfill.cpp` |
| 1 | `components/logical_plan/node_catalog_resolve_table.{hpp,cpp}`, `operator_resolve_table.cpp`, `validate_logical_plan.cpp`, `create_plan_aggregate.cpp`, `create_plan_match.cpp`, `dispatcher.cpp` |
| 2 | `transform_select.cpp`, new `node_union.{hpp,cpp}`, new `create_plan_union.{cpp,hpp}`, new `operator_union.{cpp,hpp}` |
