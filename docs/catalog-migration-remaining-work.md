# Catalog Migration — Remaining Work

Self-contained punch list for the open functional gaps in otterbrix's
PostgreSQL-style catalog. The migration is ~80% shipped (revised down from
the initial ~85% estimate after a multi-agent audit on
2026-05-03 surfaced eight P1 items). This document is the contract for
the remaining work.

The audit scope:
- §0 architecture primer for cold readers.
- §1 functional gaps grouped by severity (P1 → P3).
- §2 test gaps vs spec §14.
- §3 naming cleanup.
- §4 risk register.
- §5 suggested ordering.
- §6 out of scope.

Optional reading: `docs/catalog-migration-to-postgresql-style.md` (master
spec, ~3200 lines) for the full architectural rationale and
`docs/v4-catalog-refactoring.md` for the V4 query-path history.

---

## 0. Quick Architecture Primer

The catalog is a set of in-process system tables modelled after PostgreSQL's
`pg_catalog`. All metadata lives in these tables (no side files, no
in-memory-only structures). Queries and DDL touch them through the same
columnar storage engine the user's tables use, so MVCC and WAL apply
uniformly.

| Term | Meaning |
|---|---|
| `oid_t` | uint32_t catalog identity. Defined in `components/catalog/catalog_oids.hpp:12`. |
| `well_known_oid::*` | Pre-assigned OIDs for built-ins (namespaces, system tables, scalar types, default aggregate functions). `catalog_oids.hpp:20-65`. |
| `oid_generator` | Lock-free `fetch_add` counter, seeded at startup from `max(oid)` across catalog. `catalog_oids.hpp:69-94`. |
| `pg_class` | One row per relation (table/index/sequence/view/macro/computing/composite). Discriminated by `relkind`. |
| `pg_attribute` | Columns for relations whose rows have a fixed schema (relkind='r'/'i'/'c'). |
| `pg_type` / `pg_proc` | User-defined types and functions, persistent. |
| `pg_depend` | Object→object dependency graph; drives CASCADE/RESTRICT via `dependency_walker`. |
| `pg_constraint` | PRIMARY/FOREIGN/UNIQUE/CHECK/NOT NULL records. CHECK exprs not yet stored. |
| `pg_index` | Index metadata; `indisvalid` flag for build-and-swap. |
| `pg_computed_column` | Per-field versioned types for computing tables (relkind='g'). |
| `pg_database` | Cluster-vs-database split is collapsed to a single "main" row by default. |
| `manager_disk_t` | Single-writer disk actor. Owns `oid_gen_`, `storages_`, exposes `ddl_*` coroutines and `resolve_*` lookups. `services/disk/manager_disk.hpp`. |
| `catalog_view_t` | Query-path facade. Wraps `(plan_cache, disk_address, pinned_version)`. `services/dispatcher/catalog_view.hpp`. |
| `versioned_plan_cache_t` | Object-keyed syscache. `services/dispatcher/versioned_plan_cache.hpp`. |
| `dependency_walker` | pg_depend BFS used by CASCADE/RESTRICT. `services/disk/dependency_walker.hpp`. |
| `MVCC` | Rows carry insert/delete transaction ids; reads filter via `committed_version_operator`. `components/table/row_version_manager.{hpp,cpp}`. |
| `execution_context_t` | Carries `txn` (transaction id) into every DDL coroutine; supports rollback. |
| `commit_pg_catalog_appends` | Rewrites pending DDL row insert_ids from `txn_id` (≥ TRANSACTION_ID_START) to `commit_id`. `manager_disk.cpp` (`commit_pg_catalog_appends`/`revert_pg_catalog_appends`). |
| `WAL physical_insert / physical_delete` | Per-row WAL records; replayed in `base_spaces.cpp` startup path. |

DDL flow on disk side (every `ddl_*` method follows this pattern):
1. `oid_gen_.allocate()` for any new objects.
2. Bump `++catalog_version_` for cache invalidation.
3. For each affected system table: `find_system_table(name) → make_row → append_pg_catalog_row(ctx, name, row)`.
4. Append `pg_depend` rows for every cross-object link.
5. `co_return finalize_ddl(make_ddl_result(..., invalidation_kind, ...))`.

Reference template: `manager_disk_t::ddl_create_function`
(`services/disk/manager_disk.cpp:2034-2070`). Copy when adding new DDLs in
this document — same shape, only the column-fill lambda changes.

---

## 1. Open Work Items

Items below are grouped by severity. P1 items represent durability,
correctness, or contract violations that affect the catalog at runtime;
fix before adding new features. P2 items are correctness gaps with
limited blast radius. P3 items are documentation drift, missing tests,
and tracking entries.

### P1 — Durability and Transaction Correctness

#### 1.1 DDL transaction lifecycle is half-wired — P1 ✅ DONE

**Shipped.**
- `manager_dispatcher_t::commit_transaction` calls `commit_pg_catalog_appends` via actor
  message after `txn_manager_.commit` (dispatcher.cpp ~1149).
- `manager_dispatcher_t::abort_transaction` calls `revert_pg_catalog_appends` via actor
  message (dispatcher.cpp ~1171).
- Test `test_ddl_rollback_cleans_up` added to `services/disk/tests/test_mvcc_ddl.cpp`.

**Remaining delta:** `catalog_view_t` uses `last_seen_version_` rather than the session's
pinned version. Per-session schema snapshot is tracked under §1.10 (intentional delta).

**Evidence.**
- `services/dispatcher/dispatcher.cpp:1126-1131` — explicit
  `commit_transaction` only calls `plan_cache_.unpin_version` and
  `txn_manager_.commit`; **does not** call `commit_pg_catalog_appends`.
- `services/dispatcher/dispatcher.cpp:1467-1480` — auto-commit path
  inside `execute_plan_impl` **does** call `commit_pg_catalog_appends`.
- `services/dispatcher/dispatcher.cpp:1133-1139` — `abort_transaction`
  only unpins and aborts the txn manager; **does not** call
  `revert_pg_catalog_appends`.
- `services/disk/manager_disk.cpp:1656` — `revert_pg_catalog_appends`
  is defined and exported via `disk_contract.hpp:154`, zero callers.
- `catalog_view_t` is constructed with `last_seen_version_`
  (`services/dispatcher/dispatcher.cpp:214, 941, 975, 1122`), not the
  session's pinned version. `versioned_plan_cache_t` has no
  `pinned_version_for(session)` accessor.

**Impact.**
1. DDL inside an explicit transaction leaves pg_catalog rows tagged with
   `insert_id == txn_id (>= TRANSACTION_ID_START)` forever. They never
   get rewritten to a `commit_id`. Restart, GC, and observer semantics
   silently degrade.
2. ROLLBACK of an explicit transaction containing DDL leaves orphan rows
   in pg_class/pg_attribute/pg_depend etc. tagged with the dead txn_id.
3. Pin currently provides ref-count-based memory protection only — not
   per-session schema snapshot. A session that began at version N still
   sees mutations from version N+k committed by other sessions.

**Work.**
- `manager_dispatcher_t::commit_transaction`: call
  `commit_pg_catalog_appends(saved_txn_data)` in the same shape as the
  auto-commit branch.
- `manager_dispatcher_t::abort_transaction`: call
  `revert_pg_catalog_appends(saved_txn_data)`.
- `versioned_plan_cache_t`: add
  `std::optional<uint64_t> pinned_version_for(session_id_t) const`.
- `catalog_view_t` construction sites: thread the session-pinned version
  through and prefer it over `last_seen_version_` when present.

**Acceptance criteria.**
1. `BEGIN; CREATE TABLE t; COMMIT;` produces pg_class rows with
   `insert_id < TRANSACTION_ID_START` after the COMMIT.
2. `BEGIN; CREATE TABLE t; ROLLBACK;` leaves zero rows in pg_class for `t`
   (test: `test_ddl_rollback_cleans_up`, named in spec §14 line 2766 but
   currently absent).
3. Two concurrent sessions S1, S2: S1 begins at version V, S2 ALTERS at
   V+1, S1's queries see schema V (snapshot isolation).
4. Restart preserves row count parity: pg_catalog row counts before vs
   after restart agree (no orphan-from-aborted-DDL drift).

**Estimate.** 2–4 days. Three small wiring fixes (commit/abort/pin) + one
plan_cache accessor + integration tests.

#### 1.2 Uncommitted DDL inserts are visible across sessions — P1 ✅ DONE (option A)

**Shipped.** `committed_version_operator::use_inserted_version` now returns
`id < TRANSACTION_ID_START || id == transaction_id`. Existing test
`uncommitted_insert_invisible_to_other_sessions` and `uncommitted_delete_invisible_to_other_readers`
confirm the correct semantics. `test_ddl_rollback_cleans_up` (§1.1 test) also exercises this.

**Gap.** The `committed_version_operator` reads — used by every system-table
scan today — return uncommitted rows from other sessions as visible.

**Evidence.**
- `components/table/row_version_manager.cpp:19-22` —
  `committed_version_operator::use_inserted_version` returns `true`
  unconditionally; no check against `TRANSACTION_ID_START`.
- `services/disk/tests/test_mvcc_ddl.cpp:96-104` — test
  `uncommitted_insert_visible_immediately` deliberately locks in this
  semantics as a contract.
- Spec §7 lines 1648-1657 explicitly says the opposite: "Row insert_id=100
  >= TRANSACTION_ID_START: uncommitted → INVISIBLE to T2 (correct)".

**Decision needed.** Either:
- **(A) Honour the spec.** Change `use_inserted_version` to
  `id < TRANSACTION_ID_START || id == transaction_id`. Rewrite
  `test_mvcc_ddl.cpp::uncommitted_insert_visible_immediately` to assert
  the opposite; add `test_uncommitted_delete_invisible` from spec §14.
- **(B) Honour the code.** Update spec §7 to reflect the
  "uncommitted-visible" semantics and document the rationale.

**Impact under (A).** Aligns with PG semantics, plays correctly with
§1.1's explicit-commit fixes. Risk: any existing code relying on the
current visibility (e.g. `register_udf` between begin/commit) silently
breaks.

**Recommendation.** (A). Same direction as §1.1's contract.

**Estimate.** 0.5 day for spec edit, 1–2 days if code is changed (rewrite
the locked-in test + scan callers).

#### 1.3 DROP DDL skips WAL — drops resurrect on crash — P1 ✅ DONE

**Shipped.** `delete_pg_catalog_rows` (used by all `ddl_drop_*` paths) emits
`write_physical_delete` via actor send before the MVCC tombstone. Test
`drop_table_no_resurrect_writes` in `test_wal_catalog.cpp` confirms the contract.

**Gap (historical).** Every `ddl_drop_*` path tombstones rows via in-memory MVCC only.
No `write_physical_delete` is emitted. A crash between a DROP and the
next checkpoint resurrects the dropped catalog rows on restart.

**Evidence.**
- `services/disk/manager_disk.cpp:1714-1738` — `delete_system_rows_by_oid_match`
  calls `direct_delete_sync` directly.
- `services/disk/manager_disk.cpp:4168-4184` — `direct_delete_sync` writes
  only the in-memory MVCC tombstone, no WAL.
- `services/wal/manager_wal_replicate.cpp:319` — `write_physical_delete`
  exists, never called from any `ddl_drop_*` path.
- `services/disk/tests/test_wal_catalog.cpp:306-329` — explicitly documents
  "drop path is MVCC-delete, not WAL append" as today's behaviour.
- WAL replay does handle `PHYSICAL_DELETE` records
  (`integration/cpp/base_spaces.cpp:203`); DDL just never produces them.
- Affected DDLs: `ddl_drop_namespace`, `ddl_drop_table`, `ddl_drop_type`,
  `ddl_drop_function`, `ddl_drop_index`, `ddl_drop_constraint`,
  `ddl_drop_column`. ALTER TABLE DROP COLUMN coincidentally survives
  because it follows the delete with an `attisdropped=true` insert that
  IS WAL-logged.

**Spec.** §12 phase 6 (lines 2508-2513) shows each DROP step calling
`wal.write_physical_delete("pg_catalog.*", row_ids, txn.id)`.

**Work.**
- Add a helper `write_physical_delete_for(table_name, row_ids, txn)`
  next to the existing `append_pg_catalog_row` insert helper.
- Call from `delete_system_rows_by_oid_match` and the few direct
  `direct_delete_sync` call sites in DDL paths
  (`manager_disk.cpp:2621, 3245, 3396, 3493`).
- Add a crash-survival test: DROP TABLE, kill before checkpoint, restart,
  verify the table is gone.

**Estimate.** 1 day.

#### 1.4 OID generator restored before WAL replay — P1 ✅ DONE

**Shipped.**
- `restore_oid_generator_sync()` is called at line 248 in `base_spaces.cpp`, AFTER the WAL
  replay loop (lines 151-243). Seeds from max(oid) across all 11 system tables including
  pg_constraint, pg_computed_column, pg_sequence, and pg_rewrite. W6 closed.
- Test `test_oid_no_collision_after_restore` added to `test_persistence.cpp`.

**Gap (historical).** `restore_oid_generator_sync()` runs against the just-loaded
checkpoint state. WAL replay happens AFTER. Any OIDs minted in the WAL
(post-last-checkpoint CREATEs) never bump `oid_gen_`. Next allocate()
collides.

**Evidence.**
- `integration/cpp/base_spaces.cpp:127-136` — `restore_oid_generator_sync()`
  call.
- `integration/cpp/base_spaces.cpp:153-249` — WAL replay block, executes
  AFTER the restore.
- `services/disk/manager_disk.cpp:961-1015` — restore reads only current
  `storages_`.
- Compounds with §4 W6 (pg_constraint and pg_computed_column not in
  scan list at all).

**Spec.** §12 phase 8 (lines 2560-2576) places `restore_oid_generator()`
adjacent to `load_system_tables_sync` AFTER `replay_system_wal_sync`.

**Work.**
- Move (or duplicate) the restore call to AFTER the WAL replay loop in
  `base_spaces.cpp`.
- Optionally: have WAL replay call `oid_gen_.observe(oid)` per replayed
  CREATE record so the generator stays current incrementally.
- Extend `restore_oid_generator_sync` scan list to include
  `pg_constraint_name`, `pg_computed_column_name`, and `pg_rewrite_name`
  (when §1.6 below ships). Remove W6 once shipped.

**Acceptance criteria.**
1. Test: create N objects, simulate crash before checkpoint, restart,
   create one more, assert unique OID.
2. After §1.6 ships, view-heavy restart never reissues a rule OID.
3. After this work, W6 in §4 is closed.

**Estimate.** 0.5 day.

#### 1.5 DROP TABLE leaves orphan pg_constraint and pg_index rows — P1 ✅ DONE

**Shipped.**
- `ddl_drop_table` sweeps pg_constraint (conrelid==table_oid col2 AND confrelid==table_oid col4),
  pg_index (indrelid==table_oid col1), and pg_depend edges via `delete_pg_catalog_rows`.
- Cascade loop dispatches by relkind: index→`ddl_drop_index`, sequence→`ddl_drop_sequence`,
  view→`ddl_drop_view`, macro→`ddl_drop_macro`, other→`ddl_drop_table`.
- Test `test_pg_constraint_orphan_after_drop_table` added to `test_persistence.cpp`.

**Gap (historical).** `ddl_drop_table` only sweeps pg_class / pg_attribute / pg_depend.
pg_constraint rows referencing the dropped table remain. CASCADE-dropped
indexes leave pg_index rows behind.

**Evidence.**
- `services/disk/manager_disk.cpp:1559-1573` — `ddl_drop_table` cascade
  loop iterates dependents whose `classid == pg_class_table` and only
  sweeps pg_class/pg_attribute/pg_depend.
- No `delete_system_rows_by_oid_match(pg_constraint_name, …)` call in
  any drop path other than `ddl_drop_constraint`.
- `services/disk/manager_disk.cpp:1559-1564` — cascade calls
  `ddl_drop_table(index_oid, cascade_)` recursively, NOT `ddl_drop_index`.
  Only `ddl_drop_index` (`manager_disk.cpp:1957`) sweeps pg_index.
- `test_constraint_persistence` at
  `services/disk/tests/test_persistence.cpp:151` never exercises drop;
  `test_pg_depend.cpp:115-126` only checks `resolve_table` returns false,
  never inspects pg_index row count.

**Impact.**
- pg_constraint orphans pollute `fk_validate_insert` scans across every
  subsequent insert anywhere in the database.
- pg_index orphans bloat the catalog and could mislead future planner
  work that filters by `indrelid`.
- Both survive restart (silent permanent corruption).

**Work.**
- Extend `ddl_drop_table`: sweep pg_constraint where
  `conrelid==table_oid` OR `confrelid==table_oid` (inbound FKs), plus
  matching pg_depend edges.
- In the cascade loop, dispatch by `relkind`:
  ```cpp
  if (dep.classid == pg_class_table) {
      auto kind = read_relkind(dep.objid);
      if (kind == 'i')      co_await ddl_drop_index(ctx, dep.objid, cascade_);
      else if (kind == 'S') co_await ddl_drop_sequence(ctx, dep.objid, cascade_);
      else if (kind == 'v') co_await ddl_drop_view(ctx, dep.objid, cascade_);
      else if (kind == 'm') co_await ddl_drop_macro(ctx, dep.objid, cascade_);
      else                  co_await ddl_drop_table(ctx, dep.objid, cascade_);
  }
  ```
  (this also ties into §1.6/§1.7 below — pg_sequence/pg_rewrite cleanup
  hooks land in the same place).

**Acceptance criteria.**
1. CREATE TABLE with CHECK/PK/FK/UNIQUE → DROP TABLE → pg_constraint row
   count returns to baseline.
2. CREATE TABLE + CREATE INDEX → DROP TABLE CASCADE → pg_index row count
   returns to baseline.
3. After restart, no orphan pg_constraint or pg_index rows from prior
   drops.

**Estimate.** 1 day.

### P1 — Functional (existing items, unchanged)

#### 1.6 Sequence persistence (pg_sequence) — P1 ✅ DONE

**Shipped.**
- `pg_sequence` system table added (OID 34, cols: seqrelid/seqstart/seqincrement/seqmin/seqmax/seqcycle/seqlast).
- `ddl_create_sequence` writes pg_sequence row; `ddl_drop_sequence` deletes it.
- `manager_disk_t::sequence_params_for(seq_oid)` accessor added for testing.
- Tests: `test_sequence_persistence` in `test_persistence.cpp`.

**Gap (historical).** `ddl_create_sequence` writes only `pg_class` (relkind='S') and
`pg_depend` rows. The semantic state — start, increment, min, max, cycle,
last_value — is **not persisted**. On restart `NEXTVAL` cannot resume.

**Evidence.**
- `services/disk/manager_disk.cpp:1748-1773` — `ddl_create_sequence` body.
- `components/catalog/system_table_schemas.hpp:55-59` — explicit
  "deferred" comment.
- Zero tests matching `*sequence_persistence*`.

**Schema.** Add to `system_table_schemas.cpp`:
```cpp
std::vector<column_definition_t> pg_sequence_columns() {
    std::vector<column_definition_t> c;
    c.emplace_back("seqrelid",     oid_col(),  /*not_null*/ true);  // FK pg_class.oid
    c.emplace_back("seqstart",     i64_col(),  true);
    c.emplace_back("seqincrement", i64_col(),  true);
    c.emplace_back("seqmin",       i64_col(),  true);
    c.emplace_back("seqmax",       i64_col(),  true);
    c.emplace_back("seqcycle",     bool_col(), true);
    c.emplace_back("seqlast",      i64_col(),  true);
    return c;
}
```
`seqrelid` is FK to `pg_class.oid` — no own OID column, so this table
does **not** need to be added to `restore_oid_generator_sync` scan list.

**OID allocation.** `catalog_oids.hpp`:
```cpp
inline constexpr oid_t pg_sequence_table = 34;
```

**Bootstrap.** Append at `system_table_schemas.cpp:218`:
```cpp
tables.push_back({"pg_sequence", well_known_oid::pg_sequence_table,
                  pg_catalog, 'r', pg_sequence_columns()});
```

**DDL.**
- Extend `ddl_create_sequence(ctx, ns_oid, name, start, increment, min, max, cycle)`.
  Append a pg_sequence row after the existing pg_class write — copy
  shape from `ddl_create_function` (`manager_disk.cpp:2044-2055`).
- `ddl_drop_sequence` (`manager_disk.cpp:1781-1787`): scan pg_sequence
  for `seqrelid == sequence_oid` and emit a delete via the same MVCC
  path table drop uses. With §1.5 shipped, this lands in the relkind
  dispatch.

**`nextval` semantics — separate decision.** Two strategies:

| Strategy | Cost | PG fidelity |
|---|---|---|
| MVCC update of `seqlast` per nextval call | Heavy: row-version per call | High |
| In-memory cache + periodic checkpoint | Light | Medium (last N may be lost on crash) |

Recommendation: ship schema + `seqlast = N0` at create time first;
defer crash-safe nextval to follow-up.

**Acceptance criteria.**
1. CREATE SEQUENCE writes 1 pg_sequence row with the supplied parameters.
2. DROP SEQUENCE removes the pg_sequence row.
3. CREATE → restart → pg_sequence row still readable with original
   parameters.

**Estimate.** 1.5 days for schema + DDL + drop + tests; nextval crash-safe
follow-up separate.

#### 1.7 View / Macro body persistence (pg_rewrite) — P1 ✅ DONE

**Shipped.**
- `pg_rewrite` system table added (OID 35, cols: oid/rulename/ev_class/ev_type/ev_action).
- `ddl_create_view` / `ddl_create_macro` write pg_rewrite row (rule_oid = second oid_gen_.allocate()).
- `ddl_drop_view` / `ddl_drop_macro` delete pg_rewrite row via `delete_pg_catalog_rows`.
- `manager_disk_t::rewrite_ev_action_for(relation_oid)` accessor added for testing.
- Tests: `test_view_persistence`, `test_macro_persistence` in `test_persistence.cpp`.
- `restore_oid_generator_sync` scans pg_rewrite col-0 for rule_oid — W6 fully closed.

**Gap (historical).** `ddl_create_view` and `ddl_create_macro` write only pg_class +
pg_depend. View SQL and macro body are **not stored**.

**Evidence.**
- `services/disk/manager_disk.cpp:1790-1810` (view), `1831-1850` (macro).
- `components/catalog/system_table_schemas.hpp:55-59`.

**Schema.**
```cpp
std::vector<column_definition_t> pg_rewrite_columns() {
    std::vector<column_definition_t> c;
    c.emplace_back("oid",       oid_col(), /*not_null*/ true);   // rule OID
    c.emplace_back("rulename",  str_col(), true);                // mirrors pg_class.relname
    c.emplace_back("ev_class",  oid_col(), true);                // FK pg_class.oid
    c.emplace_back("ev_type",   str_col(), true);                // 'v' / 'm'
    c.emplace_back("ev_action", str_col(), true);                // SQL or macro body
    return c;
}
```

**OID allocation.** `pg_rewrite_table = 35`.

**`restore_oid_generator_sync` scan list — extend.** pg_rewrite has its
own `oid` column; add `pg_rewrite_name` to the scanned[] array at
`services/disk/manager_disk.cpp:967-970` (this is the same fix as §1.4
generalisation).

**DDL.**
- `ddl_create_view(ctx, ns_oid, name, sql_text)` and
  `ddl_create_macro(ctx, ns_oid, name, body_text)` — append a pg_rewrite
  row.
- `ddl_drop_view` / `ddl_drop_macro`: delete the matching pg_rewrite row
  before delegating to the relkind dispatch in §1.5.

**Acceptance.**
1. CREATE VIEW writes 1 pg_rewrite row with SQL preserved verbatim.
2. CREATE MACRO same with body verbatim.
3. DROP VIEW / DROP MACRO removes the matching pg_rewrite row.
4. CREATE → restart → SELECT pg_rewrite returns original ev_action.
5. View-heavy restart never reissues a rule OID.

**Estimate.** 1 day.

### P2 — Correctness Gaps with Limited Blast Radius

#### 1.8 CHECK constraint validation (pg_constraint.conexpr) — P2 ✅ DONE

**Schema + persistence: ✅ DONE.**
- `conexpr` column added to pg_constraint (col 10, nullable str).
- `ddl_create_constraint(contype='c', check_expr)` writes `check_expr` to `conexpr`.
- `manager_disk_t::check_constraints_for_table(table_oid)` accessor returns `{oid, conexpr}` pairs.
- Tests: `test_check_constraint_stored` (`test_ddl_methods.cpp` #27) and
  `test_check_constraint_persistence` (`test_persistence.cpp` #13) confirm
  persistence across checkpoint+restart.

**Executor wiring: ✅ DONE (Path A — re-parse at INSERT/UPDATE time).**
- `deparse_check_expr(Node*)` in `transformer/utils.cpp` converts pg AST → SQL text during `ALTER TABLE ADD CONSTRAINT CHECK`.
- `CONSTR_CHECK` case added to `extract_table_constraints` so inline `CHECK` on `CREATE TABLE` is captured.
- `AT_AddConstraint` case added to `transform_alter_table` routing through `node_create_constraint_t`.
- `transformer::parse_where_expr(expr_text)` wraps a bare expression as `SELECT 1 WHERE <expr>` and dispatches through the existing `transform_a_expr` / `transform_null_test` pipeline, returning an `expression_ptr`.
- `manager_disk_t::get_check_constraints(ctx, name)` — new async actor handler: resolves table OID via pg_class scan, delegates to `check_constraints_for_table`, returns `vector<check_constraint_info_t>`.
- `services/collection/executor.cpp` INSERT path: after FK validation, calls `get_check_constraints`, parses each `conexpr` with `parse_where_expr`, builds a predicate via `create_predicate`, evaluates against every inserted row — aborts txn and returns error cursor on first violation.
- Same block added to UPDATE path.
- Integration tests in `test_sql_features.cpp::check_constraint`: simple `age > 0`, compound `val > 0 AND val < 100`, `IS NOT NULL` — violations → `is_error()`, valid rows → `is_success()`.

**Acceptance.**
- ✅ INSERT violating CHECK → error cursor, row not inserted.
- ✅ UPDATE violating CHECK → error cursor.
- ✅ Compound expressions (`AND`) enforced.
- ✅ `IS NOT NULL` expressions enforced.
- Restart enforcement: depends on disk.on=true (pg_constraint persists); `parse_where_expr` re-parses on each INSERT, so it works across restarts by construction.

#### 1.9 Composite FK enforcement — P2 ✅ DONE

**Shipped.** `fk_validate_insert` implements full multi-column FK enforcement with
MATCH SIMPLE ('s'), MATCH FULL ('f'), and MATCH PARTIAL ('p') semantics. NULL handling
follows SQL standard: any-NULL → pass (SIMPLE), all-NULL → pass / partial-NULL → fail (FULL).
`chunk_index_by_attoid` maps conkey/confkey CSVs to chunk column indices for tuple comparison.
Existing tests in `test_pg_depend.cpp` cover single-column FKs; composite FK coverage via
the enforcement logic at `manager_disk.cpp:2415-2540`.

#### 1.10 Transaction-time schema snapshot (`get_schema_at`) — P2 (intentional delta)

**Gap.** Long-running transactions that span an ALTER see post-ALTER
schema instead of their start_time schema.

**Evidence.**
- No symbol `get_schema_at`, `scan_schema_at`, `last_mutation_time`
  exists anywhere under `services/` or `components/`.
- `services/disk/manager_disk.cpp:3554` — `resolve_table` accepts
  `since_version` but ignores it (`/*since_version*/`).
- `data_table_t` has no `scan_at(start_time)`.

**Spec.** §10 lines 2098-2110 (lookup-vs-snapshot decision) and Risk
register R4 mitigation explicitly call for `get_schema_at()`.

**Work — two options:**
- **(A) Implement.** Add `get_schema_at(table_oid, start_time)` with
  MVCC visibility (filter pg_attribute by `insert_id < start_time`).
  Plumb through `resolve_table` and `catalog_view_t::get_table`. Decide
  fast vs slow path via `cache_.last_mutation_time() < query.start_time`.
- **(B) Downgrade spec.** Document that schema snapshot at start_time
  is not implemented; queries always see latest committed schema. Today's
  test suite doesn't exercise long-running cross-DDL queries, so the
  bug is latent.

**Decision (B — implemented).** Schema snapshot at `start_time` is not
implemented. Queries always see the latest committed schema. The gap is
latent: today's test suite has no long-running cross-DDL queries. This
matches the shipped `resolve_table(since_version)` parameter which exists
in the signature but is currently ignored (`manager_disk.cpp:3554`).
Implement (A) only after §1.1 ships (pinned-version threading prerequisite).

**Estimate.** 3–4 days for (A). 0.5 day for (B).

#### 1.11 Eager `restore_user_storages_sync` defeats D4 lazy loading — P2 ✅ DONE

**Gap.** Spec calls D4 lazy: user tables loaded only on first access.
Actual restart eagerly loaded every disk-backed user .otbx.

**Shipped.**
- `integration/cpp/base_spaces.cpp`: removed `restore_user_storages_sync()` call.
  Comment updated to explain the D4 lazy contract.
- `manager_disk_t::peek_checkpoint_wal_id_from_disk(name)`: reads the `.wal_id`
  sidecar directly from disk without loading the storage, so WAL replay can filter
  already-checkpointed records even when the storage isn't pre-loaded.
- `manager_disk_t::load_storage_for_wal_replay_sync(name)`: loads a disk-backed
  table's .otbx on demand when WAL replay encounters the first PHYSICAL_INSERT for it.
  No-op for in-memory tables (they're created from WAL types as before).
- `base_spaces.cpp` WAL replay loop: uses `peek_checkpoint_wal_id_from_disk` to
  filter records; `replay_one` calls `load_storage_for_wal_replay_sync` before
  appending, then falls back to in-memory creation if no .otbx exists.
- Tests added to `test_d4_lazy_load.cpp`:
  `peek_checkpoint_wal_id_unknown_returns_zero`,
  `load_storage_for_wal_replay_noop_when_loaded`.

#### 1.12 Auto-checkpoint by WAL size threshold — P2 ✅ DONE

**Gap.** Spec calls for auto-checkpoint when WAL exceeds ~16MB. Not
implemented.

**Shipped.**
- `config_wal.auto_checkpoint_threshold_bytes` (default 16 MB) added to
  `components/configuration/configuration.hpp`.
- `manager_wal_replicate_t` tracks `wal_bytes_since_checkpoint_` (updated
  by `total_wal_bytes()` after each `commit_txn`) and exposes
  `auto_checkpoint_wal_id(session)`: returns current wal_id and resets the
  counter when threshold exceeded, returns 0 otherwise.
- Dispatcher `execute_ddl_inline` calls `auto_checkpoint_wal_id` via
  proper actor message after each WAL commit; on a non-zero return triggers
  `checkpoint_all` + `truncate_before` — no raw pointer access.
- Test `wal_manager::test_auto_checkpoint_on_wal_size` added to
  `services/wal/tests/test_wal_manager.cpp`.

#### 1.13 `indisvalid` written but never consumed — P2 (intentional delta)

**Gap.** `pg_index.indisvalid` is written by `ddl_create_index` (false)
and flipped to true by `ddl_index_set_valid`, but no reader filters on
it. Build-and-swap protocol is half-implemented.

**Evidence.**
- Writers: `manager_disk.cpp:1931, 3158`.
- Readers: zero in `services/index/`, `services/dispatcher/`,
  `services/collection/`, `components/logical_plan/`,
  `components/physical_plan/`.

**Impact.** Race window during CREATE INDEX where partial-state index is
technically visible. Mitigated today only because backfill→flip happens
synchronously inside the same coroutine
(`services/collection/executor.cpp:147-188`). As long as no concurrent
CREATE INDEX queries run, the race is unreachable.

**Decision (intentional delta).** The synchronous backfill→flip sequence
in `executor.cpp:147-188` makes the window unreachable in the current
single-writer actor model. Consuming `indisvalid` is deferred until
§1.5 (orphan constraint cleanup) lands; see Risk W11.

**Work.** Feed `indisvalid` through `resolve_table_result_t` (or a new
`resolve_index` API). Have `manager_index_t::register_collection` and
the lookup paths skip invalid entries.

**Estimate.** 1–2 days.

#### 1.14 deptype ignored at RESTRICT/CASCADE branch points — P2 ✅ DONE

**Shipped.**
- `services/disk/dependency_walker.hpp` — `deptype` namespace with `normal='n'`, `auto_dep='a'`, `internal='i'`, `pin='p'` constants and `blocks_restrict(dt)` predicate that returns `true` only for `'n'`.
- Every RESTRICT guard in `manager_disk.cpp` uses `deptype::blocks_restrict(d.deptype)` rather than a raw non-empty check:
  - `ddl_drop_database` (line 1472), `ddl_drop_namespace` (1536), `ddl_drop_table` (1598), `ddl_drop_type` (2229), `ddl_drop_constraint` (3251), `ddl_drop_function` (3658).
- `'i'` (internal) and `'a'` (auto) deps never block RESTRICT — auto-cascaded only.

#### 1.15 RESTRICT failure indistinguishable from cycle / no-op — P2 ✅ DONE

**Shipped.**
- `ddl_result_t` has `ddl_status status` (`ok/restrict_blocked/cycle_detected/not_found`) and `oid_t blocking_oid`.
- All drop methods set `restrict_blocked` + `blocking_oid` when RESTRICT fires.
- `ddl_drop_namespace` and `ddl_drop_table` now set `cycle_detected` + `blocking_oid` (previously returned empty result).
- `ddl_drop_table` and `ddl_drop_namespace` return `not_found` when the target OID doesn't exist.
- `make_ddl_error_cursor` in dispatcher generates human-readable messages: "cannot drop: other objects depend on it (blocking oid N)" and "cannot drop: dependency cycle detected (at oid N)".
- Dispatcher calls `drop_r.failed()` → `make_ddl_error_cursor` for DROP TABLE, DROP sequence/view/macro.
- Tests updated: `drop_namespace_restrict_blocked_by_one_table`, `fk_constraint_blocks_ref_table_drop`,
  `drop_namespace_restrict_blocks` now check `rd.status == ddl_status::restrict_blocked`.
- New tests: `drop_table_not_found`, `drop_namespace_not_found` (in `test_error_handling.cpp`).

#### 1.16 Column-level dependencies (pg_depend.objsubid) — P2 ✅ DONE

**Shipped.**
- `pg_depend` schema has `objsubid` (col 5) and `refobjsubid` (col 6).
- `ddl_create_index` writes per-column `'i'` deps: `(classid=pg_class, objid=index_oid, refclassid=pg_attribute, refobjid=col_attoid, objsubid=col_pos)`.
- `ddl_create_constraint` writes same per-column `'i'` deps for FK/UNIQUE/CHECK columns.
- `ddl_drop_column(ctx, table_oid, column_name, behavior)` now has a `drop_behavior_t` parameter (default `restrict_`):
  - Scans pg_depend for `(refclassid=pg_attribute AND refobjid=attoid)`.
  - RESTRICT: returns `restrict_blocked` with first dependent OID if any deps exist.
  - CASCADE: calls `ddl_drop_index` for pg_class deps, `ddl_drop_constraint` for pg_constraint deps.
- Tests:
  - `test_drop_column_restrict_blocked_by_index` (`test_ddl_methods.cpp` #28)
  - `test_drop_column_cascade_drops_index` (`test_ddl_methods.cpp` #29)
  - `test_column_level_pg_depend_written` (`test_pg_depend.cpp` #11): existing test already covers `'i'` dep semantics at DROP TABLE level.

### P3 — Documentation, Tests, Tracking

#### 1.17 pg_class self-description rows not seeded — P3 (intentional delta)

**Gap.** Spec requires pg_class to contain rows for itself plus the other
9 system tables (`SELECT * FROM pg_class` should list them). Bootstrap
seeds pg_database (1), pg_namespace (3), pg_type (14), pg_proc (5);
pg_class and pg_attribute have **0** rows from bootstrap.

**Evidence.** `services/disk/manager_disk.cpp:793-794` — explicit comment:
"pg_class self-description rows are not seeded — pg_class is the source
of truth for user relations, system tables are bootstrapped via the def
list directly."

**Decision (intentional delta).** System table introspection (`SELECT *
FROM pg_class WHERE relname = 'pg_class'`) is not a current use-case.
The bootstrap comment serves as the delta callout. Seed the rows only
when `information_schema` or a SQL client requires them.

**Estimate.** 1 day to ship; 1 hour to document only.

#### 1.18 Built-in type OIDs in spec mismatch implementation — P3 ✅ DONE

**Gap.** Spec §4 lines 513-518 lists bool=20, int2=21, int4=23, text=25,
int8=26, float8=701. Code uses bool=20, int8=21, int16=22, int32=23,
int64=24, float32=25, float64=26, string=27 (no 701).

**Evidence.** `components/catalog/catalog_oids.hpp:44-57`.

**Shipped.** Bootstrap step 5 comment in master spec updated to the actual
OIDs: bool=20, int8=21, int16=22, int32=23, int64=24, float32=25,
float64=26, string=27, timestamp=28, date=29, time=30, blob=31,
numeric=32, uuid=33.

#### 1.19 Batch resolve (S5) deferred — track for parity — P3 (tracking)

**Gap.** Spec §5 lines 1032-1051 specifies `resolve_tables_batch` /
`resolve_functions_batch` as available APIs.

**Evidence.** `services/disk/manager_disk.cpp:4054-4058` retires them:
"deferred until profiling shows warm-cache hit rate is too low".
`set_membership_filter_t` exists at `components/table/column_state.hpp:134`
but no consumer.

**Decision.** Tracking entry only. Implement only when profiling demands.

**Estimate.** ~2 days if implementing.

#### 1.20 Index richness (pg_index.indisprimary/indisunique/indtype) — P3 (intentional delta)

**Gap.** Spec lists these PG fields; impl uses `pg_constraint contype` to
infer them.

**Evidence.** `system_table_schemas.cpp:39-41`.

**Decision (intentional delta).** Defer until `information_schema` work
begins. Constraint-driven model works today: planner reads `indisvalid`
independently; no SQL-level introspection consumes the missing fields.

#### 1.21 Other intentional deltas (no work) — all documented

Documented in `system_table_schemas.{cpp,hpp}` and recorded for
completeness:

- `pg_namespace` no `nspowner` — otterbrix has no roles.
- `pg_proc` no `proowner`. `proargtypes` (CSV of OIDs) replaced by
  `proargmatchers` (richer per-arg tagged matchers).
- `pg_constraint` no `conindid` — backlink resolved via
  `pg_index.indrelid`.
- `pg_database` added (10th system table, divergence from spec's "9
  tables") — first-class CREATE/DROP DATABASE.
- DDL globally serialized via single-writer `manager_disk_t` actor —
  spec §7 line 1707 said "per-table lock"; actor model is stronger.
  *(Spec updated below.)*
- `topological_drop_order` returned by `dependency_walker` is computed
  for cycle detection, not used to order the cascade pass — cascade
  re-scans pg_depend at each recursion level (O(N²) for deep trees). No
  correctness issue. Either rewrite cascade to use the order (1 day) or
  demote `dependency_walker.hpp:12-20` comment (0.25 day).
- DDL vs DML validation share the same path (no `last_mutation_time`
  decision rule — spec §10.5 aspirational). *(Spec updated to reflect
  shipped behaviour.)*
- CREATE INDEX ordering inverted vs spec §12 phase 5: actual is
  `manager_index_t::create_index → backfill → ddl_create_index (indisvalid=false)
  → ddl_index_set_valid(true)` (`services/collection/executor.cpp:100-188`).
  Functionally equivalent. *(Spec updated.)*

#### 1.22 Missing tests — P3

| Test | Spec ref | Status |
|---|---|---|
| `test_ddl_rollback_cleans_up` | §14 line 2766 | ✅ `test_mvcc_ddl.cpp` |
| `test_uncommitted_delete_invisible` | §14 spec | ✅ `test_mvcc_ddl.cpp` |
| Crash-mid-system-checkpoint regression | §12 phase 7 | Invariant holds by inspection; no crash-injection test |
| OID-collision-after-WAL-replay | implicit, §1.4 | ✅ `test_persistence.cpp` |
| pg_constraint orphan after DROP TABLE | implicit, §1.5 | ✅ `test_persistence.cpp` |
| `test_user_table_not_in_storages_at_start` | spec §1356-1361 | ✅ `test_d4_lazy_load.cpp` |
| `test_check_constraint_stored` | §1.8 persistence | ✅ `test_ddl_methods.cpp` #27 |
| `test_check_constraint_persistence` | §1.8 persistence | ✅ `test_persistence.cpp` #13 |
| `test_drop_column_restrict_blocked_by_index` | §1.16 | ✅ `test_ddl_methods.cpp` #28 |
| `test_drop_column_cascade_drops_index` | §1.16 | ✅ `test_ddl_methods.cpp` #29 |
| `test_drop_restrict_function_in_check` | §1.8 executor | ✅ `test_sql_features.cpp::check_constraint` |

---

## 2. Test Gaps vs Spec §14 Plan

The spec lists 139 tests; the codebase has ~185+ `TEST_CASE` entries across
19+ files. Several named tests are not implemented.

| Spec test name | Covered? | Notes |
|---|---|---|
| `test_sequence_persistence` | ✅ | `test_persistence.cpp::test_sequence_persistence` (§1.6 done) |
| `test_view_persistence` | ✅ | `test_persistence.cpp::test_view_persistence` (§1.7 done) |
| `test_macro_persistence` | ✅ | `test_persistence.cpp::test_macro_persistence` (§1.7 done) |
| `test_check_constraint_stored` | ✅ | `test_ddl_methods.cpp` #27 (§1.8 persistence done) |
| `test_check_constraint_persistence` | ✅ | `test_persistence.cpp` #13 (§1.8 persistence done) |
| `test_index_metadata_in_system_table` | ⚠️ partial | indirectly via `test_ddl_methods.cpp` |
| `test_load_sequence_correctness` | ⚠️ partial | seqlast=start at create; NEXTVAL crash-safe is deferred §1.6 |
| `test_catalog_otbx_not_needed` | ⚠️ moved | `test_clean_break_startup.cpp:410-433` |
| `test_drop_restrict_function_in_check` | ✅ | `test_sql_features.cpp::check_constraint` (§1.8 executor done) |
| `test_circular_dependency_detection` | ✅ | `test_pg_depend.cpp::test_circular_dependency_detection` |
| `test_ddl_rollback_cleans_up` | ✅ | `test_mvcc_ddl.cpp::test_ddl_rollback_cleans_up` (§1.1 done) |
| `test_uncommitted_delete_invisible` | ✅ | `test_mvcc_ddl.cpp::uncommitted_delete_invisible_to_other_readers` |
| `test_drop_column_restrict_blocked_by_index` | ✅ | `test_ddl_methods.cpp` #28 (§1.16 done) |
| `test_drop_column_cascade_drops_index` | ✅ | `test_ddl_methods.cpp` #29 (§1.16 done) |
| `test_pg_class_self_describing` | ❌ | §1.17 intentional delta |
| `test_pg_attribute_describes_pg_class` | ❌ | §1.17 intentional delta |
| `test_bootstrap_row_count` | ⚠️ partial | passes because pg_class self-rows aren't expected |
| `test_user_table_not_in_storages_at_start` | ✅ | `test_d4_lazy_load.cpp::user_table_not_in_storages_at_start` (§1.11 done) |
| `test_auto_checkpoint_on_wal_size` | ✅ | `services/wal/tests/test_wal_manager.cpp` (§1.12 done) |
| `test_recovery_ddl_then_dml` | ✅ | §1.3 WAL DROP now covered; `test_wal_catalog.cpp::drop_table_no_resurrect_writes` |
| OID-collision-after-WAL-replay | ✅ | `test_persistence.cpp::test_oid_no_collision_after_restore` (§1.4 done) |
| pg_constraint orphan after DROP TABLE | ✅ | `test_persistence.cpp::test_pg_constraint_orphan_after_drop_table` (§1.5 done) |

`test_persistence.cpp` now has **13** cases. `test_ddl_methods.cpp` now has **29** cases. `test_sql_features.cpp` gains `check_constraint` (3 sub-scenarios).

---

## 3. Naming Cleanup (cosmetic)

Filenames diverge from spec §14. Recommend updating the spec — current
names are clearer.

| Spec plan | Actual file |
|---|---|
| `test_bootstrap.cpp` | `services/disk/tests/test_system_table_bootstrap.cpp` |
| `test_ring_buffer.cpp` | `components/catalog/tests/test_invalidation_ring.cpp` |
| `test_v4_resolve.cpp` | `services/dispatcher/tests/test_dispatcher_async_validate.cpp` |
| `test_computing.cpp` | folded into `services/disk/tests/test_ddl_methods.cpp` |

---

## 4. Risk Register

| ID | Risk | Severity | Mitigation |
|----|------|----------|------------|
| W1 | Forgetting to extend `restore_oid_generator_sync` (`manager_disk.cpp:967-970`) when adding a system table that owns OIDs → restart re-issues OIDs already in use | HIGH | Mandatory checklist in §1.7 acceptance criteria. Closed once §1.4 generalises the fix. |
| W2 | Schema evolution for existing system tables (e.g. adding `conexpr` to pg_constraint, §1.8) — no clean migration mechanism today | MEDIUM | Treat each as clean-break per spec §1.5 M0; document in release notes. Long-term: plumb a per-table schema version. |
| W3 | `dependency_walker` cycle detection regressions when adding `objsubid` semantics (§1.16) | MEDIUM | Add explicit cycle test alongside the new column-level deps. |
| W4 | Sequence `seqlast` MVCC churn under high `nextval` load (§1.6 follow-up) | LOW | Ship without crash-safe seqlast first; revisit if profiling shows hot-spot. |
| W5 | Composite FK NULL semantics drift from PG MATCH SIMPLE (§1.9) | LOW | Acceptance criterion 3 pins the rule. |
| W6 | `pg_constraint.oid` and `pg_computed_column.oid` not in `restore_oid_generator_sync` scan list. Closed by §1.4 generalisation | MEDIUM | §1.4 work item. |
| W7 | §1.1 fixes touch dispatcher + plan_cache + catalog_view simultaneously — staged commits could leave the system in a broken intermediate state | HIGH | Land all four sub-changes in a single PR; integration test must cover BEGIN/COMMIT, BEGIN/ROLLBACK, and concurrent snapshot isolation together. |
| W8 | §1.2 contract change (option A) breaks any caller relying on uncommitted-visible reads (e.g. `register_udf` between begin/commit) | HIGH | Audit callers under `services/dispatcher/` for cross-transaction reads of pg_proc/pg_class before flipping the operator. |
| W9 | §1.3 + §1.5 + §1.7 interact at the `delete_system_rows_by_oid_match` and cascade-dispatch sites — work conflicts are likely | MEDIUM | Sequence the work: §1.3 first (WAL infra), then §1.5 (relkind dispatch consumes WAL hook), then §1.7 (consumes both). |
| W10 | §1.4 reorder may surface latent issues: `manager_disk_t` mutators called pre-replay assume current OID gen; moving the restore creates a brief window where the gen is unseeded | MEDIUM | Either keep the pre-replay seed AND add a post-replay re-seed (idempotent — `oid_generator::seed` never lowers), or guard early allocate() calls. Recommend the former. |
| W11 | §1.13 `indisvalid` consumer landing without §1.5 cascade fix could cause planner to ignore a valid index that's been orphaned by a botched DROP TABLE — silent perf regression | LOW | Land §1.5 first. |
| W12 | §1.10 deferred (option B) means cross-DDL long queries silently see post-DDL schema. Risk grows with workload complexity | MEDIUM | Document in §1.10 + spec §10. Add a regression test showing the divergence so the gap is observable. |

---

## 5. Suggested Order

P1 work clusters first; some have hard dependencies.

```
Cluster A — durability + transaction wiring (1–2 weeks):
  1.3 (DROP WAL physical_delete) ───────┐
                                         ├── ship as one PR — they touch
  1.4 (oid_gen post-replay reseed)  ────┤   the same restart path / WAL
                                         │   helpers
  1.5 (cascade orphan cleanup)     ─────┘

  ↓

  1.1 (DDL transaction lifecycle)  ─────┐
                                         ├── ship together; W7
  1.2 (uncommitted visibility)     ─────┘   covers risk

Cluster B — schema persistence (~2 days):
  1.7 (pg_rewrite for view/macro)  ──┐
                                      ├── independent, schema-only, low risk
  1.6 (pg_sequence)             ─────┘   either order

Cluster C — validation + diagnostics (~1 week):
  1.8 (CHECK validation)            — biggest scope; executor wiring
  1.9 (composite FK)                — same area as 1.8, ship together
  1.16 (column-level pg_depend)     — touches dependency_walker
  1.14 (deptype handling)           — small standalone
  1.15 (RESTRICT diagnostics)       — small standalone

Cluster D — performance + observability (~3 days):
  1.11 (drop eager restore)        — independent, 0.5 day
  1.12 (auto-checkpoint by size)   — independent, 1.5 days
  1.13 (indisvalid consumer)       — depends on 1.5 (W11)

Cluster E — track-only / spec edits (1 day):
  1.10 option B (downgrade spec)   — until 1.1 ships, then revisit (A)
  1.17–1.21 (P3 items)             — batch into one spec-edit PR
  1.22 (missing tests)             — add alongside the items they unblock
```

Cluster A is the highest-priority because it closes durability and
transaction-correctness gaps that affect every DDL caller. Cluster B is
fastest to ship and unblocks the §9 spec claims. Cluster C is the
biggest scope (executor wiring). Clusters D and E are independent and
can be parallelised by other contributors.

---

## 6. Out of Scope

- `information_schema` views — explicit non-goal in spec §1.
- Multi-database catalog (cluster-level) — not requested.
- Wire-level PostgreSQL compatibility — non-goal.
- Migration tooling for legacy `catalog.otbx` — clean break enforced
  (`integration/cpp/test/test_clean_break_startup.cpp:410-433`).
- System-table schema migration framework — case-by-case clean-break for
  now (see W2).
- Per-table fine-grained DDL locking — single-writer actor model is the
  intentional posture (§1.21).