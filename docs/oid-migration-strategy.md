# OID Migration Strategy

**Status**: design proposal, not yet started.
**Created**: 2026-05-09 (post Phase 0-7 + cleanup).
**Owner**: TBD.
**Related**: task #132 (storage_commit_append crash for wrapped relkind='g' INSERT), task #133 to be created for this work.

Long-term direction: **`collection_full_name_t` (database/schema/collection strings) → `pg_class.oid` (single 32-bit OID)** as the canonical identifier for storage operations. `collection_full_name_t` retained only at the SQL/user boundary.

---

## 1. Why migrate

### 1.1 The triggering bug (Phase 6 debug, 2026-05-09)

While debugging Phase 7 dynamic-schema tests, we found that for `relkind='g'` INSERT wrapped into `sequence_t(insert, computed_field_register)`, the executor's commit-side block at `services/collection/executor.cpp:210` reads `logical_plan->collection_full_name()` which is **empty** (sequence_t is constructed with `{}`). Result: storage MVCC tags never flipped → SELECT in another session returns 0 rows.

Naive fallback to `result.dml_collection` (which `operator_insert` correctly populates) caused **SIGABRT after 5 test cases**. Root-cause investigation revealed that `collection_full_name_t` has **3 different shapes** in the codebase:

| Source | Shape |
|---|---|
| `wrapper_dispatcher::create_collection` (C++ API, 2-arg ctor) | `{database="testdatabase", schema="", collection="testcollection"}` |
| SQL parser via `rangevar_to_collection` | `{database="", schema="testdatabase", collection="testcollection"}` |
| `operator_insert::name_` (from logical_plan->collection_full_name()) | varies — depends on how the upstream plan was built |

`storage_append` accepts both because it normalizes during lookup. But **WAL `commit_txn`** (executor.cpp:280) takes `coll_name.database` directly — empty string for SQL-form → spawns a phantom WAL worker → cascading state corruption → crash 5 tests later.

### 1.2 Class of bugs eliminated by OID

A single `oid_t` (32-bit unsigned, allocated by `oid_gen_` in disk-actor) cannot have 3 shapes. Equality is `==`. Hashing is identity. There is no "schema vs database" ambiguity.

Other latent bugs that disappear:
- Case-sensitivity mismatches in collection names (different parts of code lowercase, others don't)
- Whitespace/encoding in identifiers
- Collection rename: today `collection_full_name_t` becomes stale; OID is stable across renames
- Multi-database joins where database names collide
- `unordered_map<collection_full_name_t, ...>` hash collisions (3 strings vs 1 oid)

---

## 2. Current state

### 2.1 Where `collection_full_name_t` is used

```bash
grep -rn "collection_full_name_t" components/ services/ | wc -l
# ~600 occurrences, ~80 files
```

Major surfaces:
- **Disk storage**: `manager_disk_t::storages_` keyed by `collection_full_name_t`. All `storage_*` actor methods take `collection_full_name_t` via `execution_context_t::name`.
- **WAL**: workers keyed by `database_name_t` (a slice of cfn). Records carry full strings.
- **Index**: `manager_index_t` keyed by cfn.
- **Catalog**: `pg_class` row carries `(oid, relname, relnamespace)`. Storage and disk-side maps re-derive `collection_full_name_t` from these strings.
- **SQL transformer / planner**: produces `node_*` with cfn fields.
- **Logical plan nodes**: `node_t::collection_` is `const collection_full_name_t`.
- **Physical operators**: each DML op stores `name_` cfn.
- **execution_context_t**: passes cfn into actor sends.
- **resolved_table_t / resolved_namespace_t**: catalog_view results use cfn.

### 2.2 OID infrastructure (already exists)

Phase 0-7 introduced `pg_class.oid` as the unique table identifier. Allocated via `manager_disk_t::allocate_oids_batch`. Persisted in `pg_class` rows. Read by `resolve_table` and `catalog_view_t`.

Lookup map in disk-actor (already used by some operators):
- Each `storage_t` is created with cfn key BUT ALSO assigned a `pg_class.oid` during CREATE TABLE.
- `manager_disk_resolve.cpp` already has `oid → table_full_name` lookup helpers.

So OIDs already coexist with cfn. We need to flip the *primary* key from cfn to oid.

### 2.3 What pg_class oids look like

- 32-bit unsigned (`oid_t = std::uint32_t`)
- Allocated monotonically by `oid_gen_` per disk-actor
- `INVALID_OID = 0`
- Range 1-15 reserved for `well_known_oid::*` (system tables)
- User tables get oids ≥ 16
- Stable across restarts (oid_gen state persisted via `restore_oid_generator_sync`)

---

## 3. Target state

### 3.1 Storage layer

```cpp
class manager_disk_t : public actor_zeta::basic_actor<manager_disk_t> {
    // BEFORE: keyed by cfn
    // std::unordered_map<collection_full_name_t, std::unique_ptr<collection_storage_entry_t>, hash> storages_;

    // AFTER: keyed by oid
    std::unordered_map<oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;

    // Public storage_* API takes oid:
    unique_future<std::pair<uint64_t, uint64_t>> storage_append(execution_context_t ctx,
                                                                  oid_t table_oid,
                                                                  std::unique_ptr<data_chunk_t> data);
    unique_future<void> storage_commit_append(execution_context_t ctx,
                                                oid_t table_oid,
                                                uint64_t commit_id,
                                                int64_t row_start, uint64_t count);
    // ... etc
};
```

`execution_context_t::name` becomes `execution_context_t::table_oid`. cfn no longer in the hot path.

### 3.2 WAL

WAL physical records currently encode database_name + collection_name as strings. After migration: encode `oid_t` (4 bytes vs avg 30 bytes for strings). Format change → migration tool needed for existing DBs (or accept fresh-start only).

`wal_worker` keyed by oid (or by `database_oid`, since WAL workers are per-database currently).

### 3.3 Index

`manager_index_t::engines_` keyed by `oid_t`. Same pattern as disk.

### 3.4 Catalog API

`catalog_view_t` and `resolve_table` continue producing `resolved_table_t` with `oid` field (already there). Downstream consumers read `tbl->oid` instead of `tbl->name`.

`pg_attribute`, `pg_computed_column`, `pg_constraint`, `pg_index`, `pg_depend` already use oids in their schemas — no changes needed there. They just become the canonical reference, no dual lookup.

### 3.5 SQL boundary

`node_*_t::collection_` becomes `node_*_t::table_oid_` for resolved nodes. Pre-resolution stage (parser → enrich) still uses cfn (because user typed strings); enrich resolves cfn → oid and stamps the node.

`logical_plan::node_t::collection_` removed; replaced with `oid_t table_oid_` (for nodes that target a specific table). For wrapper nodes like `sequence_t`, the field is unused → the original wrap bug disappears.

### 3.6 Persistence / WAL recovery

Bootstrap re-reads pg_class on startup, rebuilds `oid → cfn` map, repopulates `storages_` map keyed by oid. Old strings-keyed WAL records get translated through the rebuilt map. New WAL records emit oids directly.

---

## 4. Migration plan (phased)

### Phase 8.A — OID-aware storage API (additive)

Add new `storage_*_by_oid` actor methods in parallel with existing cfn-based ones. New code uses oid path. Existing code keeps working.

**Files**: `services/disk/manager_disk.{hpp,cpp}`, `disk_contract.hpp`, +new methods (~250 LOC).

**Effort**: 3-5 days.

### Phase 8.B — Operators migrate to oid

DML operators (insert/update/delete) and DDL operators carry `oid_t table_oid` instead of `cfn`. They call `storage_*_by_oid` methods. Pre-resolution in dispatcher/enrich stamps oid on the node.

**Files**: 16 physical operators, dispatcher INSERT path, enrich_logical_plan, planner.

**Effort**: 1 week.

### Phase 8.C — execution_context_t carries oid

Field rename: `execution_context_t::name` → `execution_context_t::table_oid`. All call sites updated. `name` removed.

**Files**: ~80 files (every `exec_ctx{...}` construction).

**Effort**: 3 days (mostly mechanical search-replace + minor structural).

### Phase 8.D — Index migrates

`manager_index_t::engines_` re-keyed. `index::insert_rows`, `commit_insert`, etc. take oid.

**Files**: `services/index/`, ~10 files.

**Effort**: 3-5 days.

### Phase 8.E — WAL format migration

WAL records encode oid instead of strings. Recovery handles both old (string-keyed) and new (oid-keyed) records during transition. Eventually drop string support after all DBs migrated.

**Files**: `services/wal/`, format definitions.

**Effort**: 1 week + migration testing.

### Phase 8.F — Catalog/resolve hot path simplification

`catalog_view_t` cache keyed by oid. `resolve_table` returns oid as primary. cfn becomes derived/optional.

**Files**: `services/dispatcher/catalog_view.{cpp,hpp}`, `services/disk/manager_disk_resolve.cpp`.

**Effort**: 3 days.

### Phase 8.G — Logical plan node refactor

`node_t::collection_` removed. Replaced with `oid_t table_oid_` for nodes that target tables. Wrapper nodes (sequence_t, check_constraint, etc.) don't carry table identity — that lives on inner DML node.

This **automatically fixes** task #132 since the bug was wrapper having empty collection.

**Files**: `components/logical_plan/`, ~50 files.

**Effort**: 1 week.

### Phase 8.H — Cleanup [COMPLETE]

Remove cfn-based storage_* API, cfn fields from operators, schema-specific normalization code (`rangevar_to_collection` shape divergence), etc.

**Files**: many; deletion-only.

**Effort**: 3-5 days.

**Status**: complete. cfn↔oid bridge helpers (8.E), rangevar normalization fallback (8.A), and `scan_by_table_oid` thin alias (8.H) all removed. `collection_full_name_t::unique_identifier` retained as parser-only field (4-part SQL identifier `uuid.db.schema.table`); `schema` retained at SQL boundary. Type rename to `qualified_name_t` deferred to Phase 9+ (~312 callsites, not sed-able atomically).

### Total estimate: ~5-6 weeks for one engineer.

### Migration status: COMPLETE (Phase 8 + Phase 9)

Phases 8.A through 8.H landed across the docs/catalog-migration-postgresql-style branch. Build is green; integration tests at baseline (16/36 seed 42, 29/58 seed 100). Deferred items tracked in §12.4.

### Phase 9 — column identity + node_with_cfn_t cleanup [COMPLETE 2026-05-10]

Follow-up to Phase 8 — removes residual string-as-routing on column level and the `node_with_cfn_t` intermediate base.

- **9.A.0**: `attoid_` fields + accessors on `node_alter_column_rename` / `node_computed_field_unregister`; enrich resolves attname → attoid.
- **9.B**: operators (`alter_column_drop/rename`, `computed_field_unregister`) switched to attoid keyed reads / filter-by-attoid in callback. attname-as-routing eliminated from all operator sites.
- **9.G**: removed string duplicates `column_names_` (node_create_index), `columns_` / `ref_columns_` (node_create_constraint). Display via attoid lookup or keys().
- **9.W**: `node_with_cfn_t` intermediate base removed; virtual `collection_full_name() / database_name() / collection_name()` accessors removed from `node_t`. Each derived node carries role-named string field (`relname_`, `dbname_`, `viewname_`, `seqname_`, `indexname_`, etc.). 31 nodes migrated atomically.
- **9.H**: final audit — all four grep invariants pass (0 hits in production code; only descriptive comments confirming removal). Build green; 5-seed regression matches Phase 8.H baseline.

**LOC delta**: ~+220 across ~50 files (logical_plan, physical_plan/operators, dispatcher/{enrich,validate}_logical_plan, planner, catalog/ddl_metadata_builder, sql/transformer/utils).

**Deferred to Phase 10+**: full removal of `collection_full_name_t` from `components/logical_plan/` (256 ctor-parameter / comment hits remain; non-blocking — fields already split into role-named fields inside ctors). Type rename to `qualified_name_t` still deferred (~312 callsites).

---

## 5. Risks

### 5.1 WAL backward compatibility

Existing on-disk WAL files have string-keyed records. Skip-this-DB strategy (require fresh init) is simplest but loses prod data. Compatibility shim doubles complexity.

**Mitigation**: phase 8.E does dual-format support; deprecation tool announces 6-month window.

### 5.2 OID stability across crashes

`oid_gen_` state must persist (`restore_oid_generator_sync` already handles this). If oid generation resets or skips, in-flight references break.

**Mitigation**: `oid_gen_` persistence is already battle-tested by Phase 0-7 work. Add invariant test "oid never repeats across restart".

### 5.3 Cross-database / cross-namespace queries

If user references a collection by FQN across databases, dispatcher needs to resolve cfn → oid before any operation. Currently resolution is per-execute_plan via catalog_view. Should still work; just cfn → oid happens earlier, oid is then carried.

### 5.4 Debugging / log readability

`storage_commit_append for oid 47` is less readable than `for testdatabase.testcollection`. Solution: `manager_disk_t::log_table_name(oid)` helper that does on-the-fly lookup for trace messages.

### 5.5 Test infrastructure

~600 occurrences of `collection_full_name_t` across tests. Most can stay (test fixtures construct cfn, then resolve to oid for production paths). But helpers need new oid-based variants.

### 5.6 Phase 8.G logical plan refactor — biggest risk

Changes node_t base class. Breaks every node constructor. Mechanical churn but bug-prone.

**Mitigation**: do as a separate dedicated PR, no other concurrent work.

---

## 6. Connection to current Phase 7 issues

### Task #132 (storage_commit_append for wrapped relkind='g' INSERT)

This task disappears in **Phase 8.G** when `node_t::collection_` is removed. wrapper sequence_t doesn't claim a table identity; the inner DML node carries oid; executor reads `result.dml_table_oid` (renamed from `dml_collection`) which was always set correctly by `operator_insert`.

**Decision for now**: don't try to fix #132 with cfn band-aid. Let it be a known limitation pending Phase 8 work. Document the failing test scenarios in #132's notes.

### Task #105 (DROP DATABASE cascade)

Already verified to work; cfn → oid migration is invisible to it (operator_dynamic_cascade_delete already uses oid via pg_depend).

### Task #98 (MVCC visibility)

Independent of cfn vs oid. Stays as-is.

---

## 7. Code anchors

### High-touch files for Phase 8

| File | Phase | Why |
|---|---|---|
| `services/disk/manager_disk.hpp` | 8.A | storages_ map re-keyed |
| `services/disk/manager_disk_storage.cpp` | 8.A | storage_* impls |
| `services/disk/disk_contract.hpp` | 8.A | actor message contract |
| `components/context/execution_context.hpp` | 8.C | name → table_oid field |
| 16 `physical_plan/operators/operator_*.cpp` | 8.B | each DML/DDL op |
| `services/dispatcher/dispatcher.cpp` | 8.B | INSERT path wrap, enrich |
| `services/dispatcher/enrich_logical_plan.cpp` | 8.B | resolve cfn → oid earlier |
| `components/planner/planner.cpp` | 8.B | rewrite_* functions |
| `services/index/manager_index.{hpp,cpp}` | 8.D | engines_ map |
| `services/wal/wal_records.hpp` | 8.E | format change |
| `services/wal/manager_wal_replicate.cpp` | 8.E | encode/decode |
| `services/dispatcher/catalog_view.{hpp,cpp}` | 8.F | tbl_cache_ key |
| `services/disk/manager_disk_resolve.cpp` | 8.F | resolve_table return type |
| `components/logical_plan/node.hpp` | 8.G | base class refactor |
| `components/logical_plan/node_*.{hpp,cpp}` | 8.G | each node type |

### Stable surfaces (no changes)

- pg_attribute schema (already oid-based)
- pg_computed_column schema (oid-based)
- pg_depend (oid-based)
- pg_constraint (oid-based for FK refs)
- pg_index (oid-based)
- attribute lookup machinery in resolve_table

### Wrapper-API entrypoints (cfn at boundary, OK)

- `wrapper_dispatcher_t::create_collection(session, db, coll)` — user passes strings; dispatcher resolves to oid internally.
- SQL parser → `rangevar_to_collection(...)` → cfn → enrich resolves to oid before any storage call.

---

## 8. Trigger to start Phase 8

Conditions:
1. Phase 6 build/test stabilizes — at least 60/73 tests pass without architectural fix
2. User confirms commitment to OID direction (this doc captures the proposal)
3. Time budget allocated (5-6 weeks)
4. Decision on WAL format migration policy (fresh-init vs dual-format shim)

Recommend: open task #133 with this doc as reference, schedule Phase 8 after a stabilization period (1-2 weeks of fixing remaining Phase 6 test failures with cfn-based band-aids).

---

## 9. Open questions

1. **Schema field**: `collection_full_name_t` has `unique_identifier`, `database`, `schema`, `collection`. After migration, do we keep schema as a SQL-level concept? Postgres has schemas; our usage seems to mix them with database names.
2. **Multi-database WAL**: one WAL per database currently. With oid keying, should it be one WAL period (oid namespace global)? Affects recovery and replication.
3. **Cross-namespace JOIN**: queries that touch tables in different namespaces — resolve order matters. Doable today; needs explicit testing post-migration.
4. **System tables**: pg_catalog.* tables themselves have oids (well_known_oid::*). They're already oid-keyed in pg_class. Migration applies to them too — but no migration is needed since they're recreated on bootstrap.

---

## 10. Cross-references

- `docs/phase7-deferred-items.md` — Phase 7 follow-ups (some overlap with 8.G)
- `docs/phase7-design-decisions.md` — design decisions log (#75-#118)
- `~/.claude/plans/parallel-petting-haven.md` — master Phase 0-6 plan (historical)
- task #132 — concrete bug that motivates this doc
- task #133 — to be created when Phase 8 starts
- `services/disk/manager_disk_resolve.cpp:76+` — resolve_table dual-mode (relkind r/g) — already reads oids
- `components/catalog/system_table_schemas.cpp` — all 12 catalog tables, oid-keyed

---

## 11. Phase 8.A-D Implementation Detail

Эта секция расписывает внутренности первых четырёх фаз миграции (`8.A` storage API → `8.D` index) до уровня сигнатур и LOC-оценок. Цель — дать инженеру, который возьмёт `task #133`, прямую дорожную карту, не требующую повторного чтения исходников.

Общий принцип переходного периода: **additive, then flip**. Для каждого слоя мы сначала добавляем `_by_oid`-вариант рядом со старым cfn-вариантом, переводим вызовы один за другим, и только потом удаляем cfn-сигнатуры. Это позволяет держать дерево зелёным на каждом коммите и не требует "большого взрыва".

### 11.1 Phase 8.A — Storage API additive

Все методы определены в `services/disk/manager_disk.hpp:123-453` и продублированы в `services/disk/disk_contract.hpp:33-217`. Реализация — в `services/disk/manager_disk_storage.cpp` (для `storage_*`) и `manager_disk_ddl.cpp` (для DDL).

**Стратегия первой итерации**: каждый `_by_oid`-метод транслирует `oid → cfn` через приватный helper (см. ниже) и пробрасывает на существующий cfn-impl. Это сохраняет семантику и поведение MVCC бит-в-бит, но переносит точку трансляции с call-site внутрь disk-actor.

#### Lookup helper

```cpp
// manager_disk.hpp (private)
const collection_full_name_t* cfn_for_oid(components::catalog::oid_t oid) const noexcept;
```

Реализация — линейный скан `storages_` (или новая мапа `oid_to_cfn_`, заполняемая в `create_storage_*_sync` и `drop_storage`). Размер мапы = число таблиц, обновляется только при DDL → стоимость поиска O(log N) или O(1). LOC: ~25.

После Phase 8.F мапа становится первичной и cfn-форма уезжает в одноразовый dump для логов через `log_table_name(oid)` (см. секцию 5.4).

#### Полная таблица методов

LOC-оценки — на пару (header signature + impl-обёртка), не считая возможной гомогенизации тестов.

| Метод | Текущая сигнатура (cfn) | Предлагаемый `_by_oid` вариант | LOC |
|---|---|---|---|
| `storage_append` | `storage_append(execution_context_t ctx, std::unique_ptr<data_chunk_t> data)` — cfn пробрасывается через `ctx.name` | `storage_append_by_oid(execution_context_t ctx, oid_t table_oid, std::unique_ptr<data_chunk_t> data)`; ctx.name перестаёт быть load-bearing | 8 |
| `storage_update` | `storage_update(ctx, vector_t row_ids, unique_ptr<data_chunk_t> data)` | `storage_update_by_oid(ctx, oid_t table_oid, row_ids, data)` | 8 |
| `storage_delete_rows` | `storage_delete_rows(ctx, vector_t row_ids, uint64_t count)` | `storage_delete_rows_by_oid(ctx, oid_t table_oid, row_ids, count)` | 8 |
| `storage_commit_append` | `storage_commit_append(ctx, uint64_t commit_id, int64_t row_start, uint64_t count)` | `storage_commit_append_by_oid(ctx, oid_t table_oid, commit_id, row_start, count)` | 8 |
| `storage_revert_append` | `storage_revert_append(ctx, int64_t row_start, uint64_t count)` | `storage_revert_append_by_oid(ctx, oid_t table_oid, row_start, count)` | 6 |
| `storage_commit_delete` | `storage_commit_delete(ctx, uint64_t commit_id)` | `storage_commit_delete_by_oid(ctx, oid_t table_oid, commit_id)` | 6 |
| `storage_commit_appends` (batched) | `storage_commit_appends(ctx, commit_id, vector<pg_catalog_append_range_t> ranges)` — каждый range уже содержит cfn внутри | `storage_commit_appends_by_oid(ctx, commit_id, vector<oid_append_range_t> ranges)`; новый `oid_append_range_t { oid_t table_oid; int64_t row_start; uint64_t count; }` | 18 (включая новую структуру) |
| `storage_commit_deletes` (batched) | `storage_commit_deletes(ctx, commit_id, set<collection_full_name_t> tables)` | `storage_commit_deletes_by_oid(ctx, commit_id, set<oid_t> tables)` | 8 |
| `storage_revert_appends` (batched) | `storage_revert_appends(ctx, vector<pg_catalog_append_range_t> ranges)` | `storage_revert_appends_by_oid(ctx, vector<oid_append_range_t> ranges)` | 8 |
| `storage_scan` | `storage_scan(session, cfn name, unique_ptr<table_filter_t> filter, int limit, transaction_data txn)` | `storage_scan_by_oid(session, oid_t table_oid, filter, limit, txn)` | 8 |
| `storage_fetch` | `storage_fetch(session, cfn name, vector_t row_ids, uint64_t count)` | `storage_fetch_by_oid(session, oid_t table_oid, row_ids, count)` | 8 |
| `storage_types` | `storage_types(session, cfn name)` | `storage_types_by_oid(session, oid_t table_oid)` | 6 |
| `storage_total_rows` | `storage_total_rows(session, cfn name)` | `storage_total_rows_by_oid(session, oid_t table_oid)` | 6 |
| `storage_scan_segment` | `storage_scan_segment(session, cfn name, int64_t start, uint64_t count)` | `storage_scan_segment_by_oid(session, oid_t table_oid, start, count)` | 6 |
| `create_storage` (and `_with_columns`, `_disk`) | `create_storage(session, cfn name)` etc. | **Стайт**: cfn остаётся на boundary (имя таблицы — пользовательское). После create мы регистрируем oid → cfn в lookup-мапе. Подпись не меняется. Optional: вернуть `oid_t` создаваемой таблицы (сейчас она аллоцируется upstream через `allocate_oids_batch`). | 0 (no-change) |
| `drop_storage` | `drop_storage(session, cfn name)` | `drop_storage_by_oid(session, oid_t table_oid)` — снимает запись из oid-мапы и вызывает существующий cfn-вариант | 8 |
| `flush` | `flush(session, wal::id_t wal_id)` | no oid в сигнатуре — глобальная операция; не трогаем | 0 |
| `maybe_cleanup` | `maybe_cleanup(ctx, uint64_t lowest_active_start_time)` | глобальная — не трогаем (`ctx.table_oid` не используется) | 0 |
| `vacuum_all` | `vacuum_all(session, uint64_t lowest_active_start_time)` | глобальная — не трогаем | 0 |
| `cleanup_all_versions` (index, для симметрии) | `cleanup_all_versions(session, uint64_t lowest_active)` | глобальная — не трогаем | 0 |

Итого по 8.A: ~140 LOC новых сигнатур + ~80 LOC `cfn_for_oid` helper, регистрация oid в `create_storage_*_sync`, тесты. Полный бюджет ~250 LOC, как указано в секции 4.

**Замечание про commit-batched API**: `pg_catalog_append_range_t` (определена в `components/context/pg_catalog_swap.hpp`) сегодня хранит `{collection_full_name_t collection, int64_t row_start, uint64_t count}`. Новая `oid_append_range_t` либо заменяет её целиком (рискованно, ловит много call-sites сразу), либо живёт параллельно (предпочтительно — как `pg_catalog_append_range_oid_t`). После Phase 8.G старая структура удаляется.

### 11.2 Phase 8.B — Operator migration

~16 физических операторов сейчас держат `collection_full_name_t name_` и используют его при сборке `execution_context_t exec_ctx{ctx->session, ctx->txn, name_}` для отправки на disk-actor (паттерн виден в `components/physical_plan/operators/operator_insert.cpp:36`).

Цель: каждый оператор должен носить `oid_t table_oid_` вместо (или рядом с) `name_`. Конструктор принимает oid; `await_async_and_resume` строит `exec_ctx` с oid и зовёт `_by_oid`-методы.

Для DDL-операторов, которые **уже** работают только с oid (`operator_alter_column_*`, `operator_dynamic_cascade_delete_t`, `operator_drop_index_t`, `operator_computed_field_*`), Phase 8.B не делает почти ничего — только меняет `exec_ctx.name` → `exec_ctx.table_oid` после Phase 8.C.

| Файл | Текущее `name_` поле | Предлагаемый `table_oid_` | Конструктор: было → стало | `await_async_and_resume` изменения | LOC |
|---|---|---|---|---|---|
| `operator_insert.cpp/.hpp` | `collection_full_name_t name_` | `oid_t table_oid_` | `(resource, log, cfn)` → `(resource, log, oid_t)` | `exec_ctx{...,table_oid_}`, `storage_append_by_oid`, `index::insert_rows_by_oid` | 30 |
| `operator_update.cpp/.hpp` | `collection_full_name_t name_` (+ updates_, expr_, upsert_) | `oid_t table_oid_` | `(resource, log, cfn, updates, upsert, expr)` → `(resource, log, oid_t, updates, upsert, expr)` | symmetric `_by_oid` | 30 |
| `operator_delete.cpp/.hpp` | `collection_full_name_t name_` (+ expression_) | `oid_t table_oid_` | `(resource, log, cfn, expr)` → `(resource, log, oid_t, expr)` | symmetric | 25 |
| `operator_create_collection.cpp/.hpp` | `collection_full_name_t collection_` (+ columns_, is_disk_, catalog_writes_) | **keeps** `cfn collection_` (нужно для disk-side `create_storage` API, см. 11.1 — это boundary) **AND** добавляет `oid_t table_oid_` (для последующей регистрации oid в lookup-мапе и записи в pg_class) | `(... cfn, columns, is_disk, writes)` → `(... cfn, columns, is_disk, writes, oid_t table_oid)` | minor — pg_class row уже содержит oid, просто пробрасываем его в `create_storage` для немедленной oid-регистрации | 15 |
| `operator_alter_column_add.cpp/.hpp` | already `oid_t table_oid_` + `column_definition_t column_` | unchanged | unchanged (Phase 7 уже сделал) | minor (через 8.C) | 5 |
| `operator_alter_column_drop.cpp/.hpp` | already `oid_t table_oid_, namespace_oid_, attoid_` + name + behavior | unchanged | unchanged | minor (через 8.C) | 5 |
| `operator_alter_column_rename.cpp/.hpp` | already `oid_t table_oid_` + names | unchanged | unchanged | minor | 5 |
| `operator_check_constraint.cpp/.hpp` | no name field — оператор работает над input-чанком в pipeline-памяти | unchanged | unchanged | unchanged | 0 |
| `operator_fk_check.cpp/.hpp` | `catalog::fk_info_t fk_` (где `fk_info_t` уже хранит `parent_table_oid` + child column names) | unchanged | unchanged (oid уже в `fk_info_t`) | вызов `disk.scan_by_table_oid` уже oid-keyed | 0 |
| `operator_fk_cascade.cpp/.hpp` | `catalog::fk_info_t fk_` | unchanged | unchanged | child-table cascade-delete пока ходит через cfn → переводим на `storage_delete_rows_by_oid` через `fk_.child_table_oid` | 15 |
| `operator_create_index_metadata.cpp/.hpp` | `vector<pair<cfn, data_chunk_t>> catalog_writes_` (cfn — это pg_class/pg_index/pg_depend — system tables; их oid'ы — well-known) | unchanged (system-table cfn — стабильны и валидны) | unchanged | 0 |
| `operator_create_index_backfill.cpp/.hpp` | `cfn collection_` + index_name_ + ... + `oid_t table_oid_, index_oid_` | **keeps cfn** для index-actor `register_collection` API, постепенно переедет на oid в Phase 8.D | unchanged до 8.D | в 8.D перевод на `register_collection_by_oid(table_oid)` | 10 (в 8.D) |
| `operator_drop_index.cpp/.hpp` | `cfn collection_, index_name_, vector<catalog_delete_t>` | в 8.D → oid_t table_oid_ + index_oid | в 8.D переедет index-actor вызов | 10 (в 8.D) |
| `operator_dynamic_cascade_delete.cpp/.hpp` | already oid-only (`seed_classid, seed_objid, behavior`) | unchanged | unchanged | 0 |
| `operator_computed_field_register.cpp/.hpp` | already `oid_t table_oid_` + columns_ | unchanged | unchanged | 0 |
| `operator_computed_field_unregister.cpp/.hpp` | already `oid_t table_oid_` + column_name_ | unchanged | unchanged | 0 |
| `operator_get_schema.cpp/.hpp` | `vector<pair<string, string>> ids_` — pre-resolution stage | unchanged (resolve внутри ходит через `resolve_namespace`/`resolve_table` который оба oid-aware) | unchanged | 0 |
| `operator_register_udf.cpp/.hpp` | function_ + executor_register_fn_ — нет таблицы | unchanged | unchanged | 0 |
| `operator_unregister_udf.cpp/.hpp` | function_name_ + inputs_ — нет таблицы | unchanged | unchanged | 0 |
| `operator_vacuum.cpp/.hpp` | глобальный | unchanged основа; внутри в шаге 5b есть итерация по pg_class → имена таблиц нужно оставить cfn для `compact_relkind_g_storage`, но `rebuild_indexes/storage_total_rows/storage_scan_segment/insert_rows` переедут на `_by_oid` | вызовы перепишутся на oid | 25 |
| `operator_checkpoint.cpp/.hpp` | глобальный | unchanged | unchanged | 0 |
| `operator_primitive_write.cpp/.hpp` | `cfn catalog_table_, data_chunk_t row_` — это pg_catalog таблица (system, well-known oids) | можно перевести на well-known oid (`well_known_oid::PG_CLASS` etc.) либо оставить cfn до Phase 8.G | предпочтительно: добавить параллельный `operator_primitive_write_by_oid` вариант, использовать его для системных таблиц (там oid'ы well-known) | 20 |
| `operator_primitive_delete.cpp/.hpp` | `cfn catalog_table_, oid_col_idx_, target_oid_` | symmetric — well-known catalog_table oid | symmetric | 20 |

Итого 8.B: ~210 LOC (включая консервативный запас на тестовые fixtures).

**Замечание про конструкторы**: в Phase 8.B мы НЕ удаляем cfn-варианты конструкторов. Каждый оператор временно имеет два конструктора — `(... cfn ...)` и `(... oid ...)`. Старый позволяет dispatcher/planner-у строить операторы пока миграция этих uppermost слоёв не завершена в 8.G. Удаление в 8.H.

### 11.3 Phase 8.C — execution_context_t migration

Текущий `components/context/execution_context.hpp:9-13`:

```cpp
struct execution_context_t {
    session::session_id_t session;
    table::transaction_data txn{0, 0};
    collection_full_name_t name;
};
```

После миграции:

```cpp
struct execution_context_t {
    session::session_id_t session;
    table::transaction_data txn{0, 0};
    components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
};
```

#### Подход к миграции: атомарный switch — НЕТ. Дополнительное поле + переход — ДА

Промежуточная форма (1-2 коммита):

```cpp
struct execution_context_t {
    session::session_id_t session;
    table::transaction_data txn{0, 0};
    collection_full_name_t name;       // legacy, set during transition
    components::catalog::oid_t table_oid{components::catalog::INVALID_OID};  // new, primary post-8.A
};
```

В этой форме каждый call-site `exec_ctx{ctx->session, ctx->txn, name_}` обновляется до `exec_ctx{ctx->session, ctx->txn, name_, table_oid_}` — minor mechanical change. Disk-actor читает `ctx.table_oid` в `_by_oid`-методах и `ctx.name` в legacy-методах. Когда все call-sites переведены и legacy-методы удалены (Phase 8.H), поле `name` удаляется.

#### Compatibility helpers

В переходный период (после 8.A, во время 8.B):

```cpp
// components/context/execution_context.hpp
inline execution_context_t make_exec_ctx(session_id_t s, table::transaction_data t,
                                         collection_full_name_t cfn,
                                         components::catalog::oid_t oid) {
    return {s, t, std::move(cfn), oid};
}
```

Если call-site знает только cfn (старый код), а нужен oid → опционально `oid_t = INVALID_OID` ⇒ `_by_oid`-метод бросает invariant violation. Либо disk-actor сам резолвит cfn → oid через приватный helper и логирует deprecation warn. Предпочтительный путь — второй (мягкая деградация).

#### Затрагиваемые файлы

`grep -rn "execution_context_t" components/ services/ | wc -l` — на момент написания ~80 файлов. Большая часть — `exec_ctx{...}`-конструкции в DDL-операторах и dispatcher path. Смена структуры → пересборка всех TU. Одного коммита достаточно (mechanical).

LOC: ~80 файлов × 1-3 строки = 100-200 LOC изменений (преимущественно diff-only — добавление поля `table_oid` в инициализатор).

### 11.4 Phase 8.D — Index migration

Текущий `services/index/manager_index.hpp:128-129`:

```cpp
std::pmr::unordered_map<collection_full_name_t, components::index::index_engine_ptr,
                        collection_name_hash> engines_;
```

Цель:

```cpp
std::pmr::unordered_map<components::catalog::oid_t,
                        components::index::index_engine_ptr> engines_;
```

Хеш — identity (default `std::hash<uint32_t>`), без коллизий формы cfn (см. секцию 1.1).

#### Затрагиваемые actor messages

| Метод | Текущая сигнатура | Предлагаемый `_by_oid` вариант | LOC |
|---|---|---|---|
| `register_collection` | `register_collection(session, cfn name)` | `register_collection_by_oid(session, oid_t table_oid)` — engine-entry заводится по oid, метаданные (имя для логов) опционально подгружаются по запросу | 12 |
| `unregister_collection` | `unregister_collection(session, cfn name)` | `unregister_collection_by_oid(session, oid_t table_oid)` | 6 |
| `insert_rows` | `insert_rows(ctx, unique_ptr<data_chunk_t> data, uint64_t start_row_id, uint64_t count)` | `insert_rows_by_oid(ctx, oid_t table_oid, data, start_row_id, count)` — engine lookup по oid из ctx.table_oid | 8 |
| `delete_rows` | `delete_rows(ctx, data, vector<int64_t> row_ids)` | `delete_rows_by_oid(ctx, oid_t table_oid, data, row_ids)` | 8 |
| `update_rows` | `update_rows(ctx, old_data, new_data, row_ids, new_start_row_id)` | `update_rows_by_oid(ctx, oid_t table_oid, old_data, new_data, row_ids, new_start_row_id)` | 8 |
| `commit_insert` | `commit_insert(ctx, uint64_t commit_id)` | `commit_insert_by_oid(ctx, oid_t table_oid, commit_id)` | 6 |
| `commit_delete` | `commit_delete(ctx, uint64_t commit_id)` | `commit_delete_by_oid(ctx, oid_t table_oid, commit_id)` | 6 |
| `revert_insert` | `revert_insert(ctx)` | `revert_insert_by_oid(ctx, oid_t table_oid)` | 6 |
| `cleanup_all_versions` | `cleanup_all_versions(session, uint64_t lowest_active)` | глобальный — не меняем | 0 |
| `rebuild_indexes` | `rebuild_indexes(session, cfn name)` | `rebuild_indexes_by_oid(session, oid_t table_oid)` | 6 |
| `create_index` | `create_index(session, cfn name, index_name_t, keys, index_type)` | `create_index_by_oid(session, oid_t table_oid, index_name_t, keys, index_type)` | 12 |
| `drop_index` | `drop_index(session, cfn name, index_name_t)` | `drop_index_by_oid(session, oid_t table_oid, index_name_t)` | 8 |
| `search` | `search(session, cfn name, keys, value, compare_type, start_time, txn_id)` | `search_by_oid(session, oid_t table_oid, keys, value, compare_type, start_time, txn_id)` | 12 |
| `has_index` | `has_index(session, cfn name, index_name_t)` | `has_index_by_oid(session, oid_t table_oid, index_name_t)` | 6 |
| `flush_all_indexes` | `flush_all_indexes(session)` | глобальный — не меняем | 0 |
| `get_indexed_keys` | `get_indexed_keys(session, cfn name)` | `get_indexed_keys_by_oid(session, oid_t table_oid)` | 6 |

Итого 8.D: ~110 LOC новых сигнатур + ~30 LOC `cfn_to_oid` helper для index-actor (зеркалит disk-actor) + ~60 LOC изменений в operator_create_index_backfill_t / operator_drop_index_t / operator_insert/update/delete для перехода на `_by_oid`. Бюджет ~200 LOC, что согласовано с секцией 4 (3-5 дней).

#### Регистрация oid в index-actor

Сегодня `register_collection` вызывается из dispatcher после `create_storage`. После 8.A oid уже зарегистрирован в disk-actor. Index-actor должен иметь свою копию `oid → cfn` мапы либо запрашивать у disk-actor через resolve. Простейший путь — продублировать (мапа маленькая, обновляется только на DDL).

Важно: index-actor сегодня держит **одну** запись на cfn, но `index_engine_t` внутри может содержать несколько индексов на колонки. После oid-keying ничего не меняется — `index_engine_ptr` остаётся per-table, oid просто заменяет cfn как ключ.

---

LOC summary by phase (Phase 8.A-D only):

| Phase | LOC budget | Files touched | Days |
|---|---|---|---|
| 8.A storage API | ~250 | 4 (manager_disk.hpp/cpp, disk_contract.hpp, manager_disk_storage.cpp) | 3-5 |
| 8.B operators | ~210 | ~16 | 5-7 |
| 8.C exec_ctx | ~150 | ~80 (mostly trivial) | 2-3 |
| 8.D index | ~200 | ~10 | 3-5 |

Phases run mostly in series (8.A blocks 8.B, 8.C is concurrent with 8.B, 8.D after 8.A is enough). Total Phase 8.A-D budget: ~810 LOC of new code, ~3 weeks of one engineer.

---

## 12. Phase 8.E-H Implementation Detail

This section deepens the four most-uncertain phases — WAL format, catalog hot path, logical_plan refactor, and cleanup — to the level §11 reaches for 8.A-D. It is written against the code as it stands on `docs/catalog-migration-postgresql-style` (post P5.3 / P6.build) and references concrete files + line ranges where decisions land.

### 12.1 Phase 8.E — WAL format migration

#### 12.1.1 Where the format lives today

WAL on-disk format is concentrated in two files:

- `services/wal/record.hpp` — in-memory `record_t` (decoded form). Carries `collection_full_name_t collection_name` plus the type-specific physical_data / physical_row_ids / row_start / row_count.
- `services/wal/wal_binary.{hpp,cpp}` — encode/decode. The byte layout is documented inline at `wal_binary.cpp:46-66` and reproduced below verbatim.

There is **no separate `wal_records.hpp`**; record format is `record.hpp` + the encode/decode pair. The page-level container (`wal_page_*`) is format-agnostic — it just frames length-prefixed CRCed blobs and is unaffected by this migration.

Replay path: `services/wal/wal_reader.cpp::read_database_segments` does a 2-pass committed-txn filter, then returns `std::vector<record_t>` to bootstrap. Recovery in `services/disk/manager_disk.cpp` (and `base_spaces.cpp`) iterates these and replays via in-memory cfn lookups.

#### 12.1.2 Current physical record layout

DML records (`PHYSICAL_INSERT=10`, `PHYSICAL_DELETE=11`, `PHYSICAL_UPDATE=12`) share one envelope, defined in `wal_binary.cpp:46-66`:

```
[size:4]
[last_crc32:4]
[wal_id:8]
[txn_id:8]
[record_type:1]
[db_len:2]
[coll_len:2]
[row_start:8]
[row_count:8]
[payload_size:4]
[database: db_len]            <-- variable-length
[collection: coll_len]        <-- variable-length
[payload: payload_size]
[crc32:4]
```

Fixed-header constant: `DML_FIXED_HEADER = 45 bytes` (line 68-78). Total fixed cost per DML record is **45 + 4 (size) + 4 (trailing crc) = 53 bytes**, plus `db_len + coll_len` strings, plus payload.

Per-payload bodies:
- `PHYSICAL_INSERT`: payload = `serialize_binary(data_chunk)`. row_count = chunk size.
- `PHYSICAL_DELETE`: payload = raw `int64_t[count]` of row_ids. row_start = 0.
- `PHYSICAL_UPDATE`: payload = `[row_ids_size:4][row_ids][serialize_binary(new_data)]`. row_start = 0.

`COMMIT=1` records are 29 bytes flat — no strings, no payload.

The decoder is `decode_record` (`wal_binary.cpp:287-419`) — reads the fixed header, then `db_len + coll_len` bytes into `rec.collection_name.database` / `rec.collection_name.collection`. Note: `unique_identifier` and `schema` are **never** written by the encoder and **never** read by the decoder — only `database` + `collection` round-trip. This is consistent with all production WAL having shape `{db, "", coll}`. (Confirmed by inspection — only `.database` / `.collection` are touched in `wal_binary.cpp:131-133, 366-369`.)

#### 12.1.3 Storage savings estimate

Typical realistic identifiers in the integration tests:

| Field | Bytes |
|---|---|
| `database` ("testdatabase") | 12 |
| `collection` ("testcollection") | 14 |
| db_len + coll_len fields | 4 |
| **Total identifier overhead** | **30 bytes** |

After migration the same record carries one `oid_t` (4 bytes) instead of `db_len + coll_len + database + collection` (4 + 12 + 14 = 30 bytes for this fixture). **Savings: 26 bytes per DML record** (~13% per typical small-INSERT record, ~50% of total record bytes for delete-heavy workloads where payload is just an 8-byte row_id).

For multi-MB recovery scans, the proportional speedup is ≈ same as bytes-saved % — CRC time is linear in body size.

#### 12.1.4 Proposed v2 layout

Two new record types, allocated outside the current 1/10/11/12 range so a v1 decoder seeing a v2 byte fails CRC-cleanly rather than misparsing:

```cpp
enum class wal_record_type : uint8_t {
    COMMIT              = 1,
    PHYSICAL_INSERT     = 10,   // v1 (cfn-keyed, deprecated)
    PHYSICAL_DELETE     = 11,   // v1
    PHYSICAL_UPDATE     = 12,   // v1
    PHYSICAL_INSERT_V2  = 110,  // v2 (oid-keyed)
    PHYSICAL_DELETE_V2  = 111,
    PHYSICAL_UPDATE_V2  = 112,
};
```

V2 envelope:

```
[size:4]
[last_crc32:4]
[wal_id:8]
[txn_id:8]
[record_type:1]              <-- 110/111/112
[table_oid:4]                <-- replaces db_len+coll_len+database+collection
[row_start:8]
[row_count:8]
[payload_size:4]
[payload: payload_size]
[crc32:4]
```

V2 fixed header: `DML_V2_FIXED_HEADER = 4 + 8 + 8 + 1 + 4 + 8 + 8 + 4 = 45 bytes` — same constant as v1 by coincidence (the 4-byte table_oid replaces 2+2 length fields plus the typical 26 bytes of strings). Identical fixed-header size makes the writer trivially branchable and lets us share bounds-check arithmetic.

#### 12.1.5 Backward-compatibility design

The reader (`decode_record` and its callers) gains a v1-vs-v2 dispatch on `record_type`:

```cpp
record_t decode_record(...) {
    // ... read size, crc, header common fields ...
    switch (rec.record_type) {
        case COMMIT:                 return rec;
        case PHYSICAL_INSERT: case PHYSICAL_DELETE: case PHYSICAL_UPDATE:
            decode_dml_v1(rec, ptr, body_size);  // current behavior, populates rec.collection_name
            break;
        case PHYSICAL_INSERT_V2: case PHYSICAL_DELETE_V2: case PHYSICAL_UPDATE_V2:
            decode_dml_v2(rec, ptr, body_size);  // populates rec.table_oid
            break;
        default:
            rec.is_corrupt = true;
    }
}
```

`record_t` (in `record.hpp:18-42`) gains `oid_t table_oid{INVALID_OID}` alongside the existing `collection_name`. Exactly one is populated per record (mutually exclusive based on type).

During recovery (`manager_disk_t::recover_from_wal` and bootstrap):

1. After `pg_class` storage is populated (already happens before WAL replay — pg_catalog tables come up first), build a transient `cfn → oid` lookup table.
2. For each replayed record:
   - v2: `dispatch_to_oid_storage(rec.table_oid, ...)` — direct hit on `storages_[oid]`.
   - v1: `oid = lookup_or_invalid(rec.collection_name)`. If found, dispatch as if v2. If not found → log warning and skip (table was dropped post-WAL-write before checkpoint; semantically equivalent to today's behavior of failing to find the storage_t).

Writer side: behind a `wal_format_v2` config flag (default off in 8.E first patch, default on in 8.E final patch, hard-coded after 8.H). When v2 is on, `encode_insert/delete/update` take `oid_t` instead of `database`/`collection`. The contract methods (`manager_wal_replicate_t::write_physical_insert/delete/update` at `manager_wal_replicate.cpp:318+, 354+, 388+`) get oid-taking overloads; existing callers stay on the cfn path during 8.E, migrate during 8.F-G as upstream operators flip to oid.

#### 12.1.6 Migration tool

Standalone CLI (`tools/wal_migrate_v1_to_v2`, ~200 LOC):

```
$ wal_migrate_v1_to_v2 --wal-dir /var/lib/otterbrix/wal \
                       --catalog-dir /var/lib/otterbrix/data \
                       [--dry-run]
```

Behavior:

1. Open the catalog directory's `pg_class` storage read-only — this needs only the on-disk page format, no actor system. Build a complete `(database, collection) → oid` map. Database name comes from the WAL subdirectory we're about to process; collection comes from `pg_class.relname`.
2. For each segment file `wal_<db>_NNNNNN`:
   - Stream-decode every record via `decode_record`.
   - For COMMIT: re-encode unchanged.
   - For DML v1: look up oid; if found, re-encode as v2; if missing, drop record + log warning.
   - For DML v2: pass through (idempotent — supports re-runs / partially-migrated dirs).
3. Write to `wal_<db>_NNNNNN.v2` next to the original. The CRC chain is fully recomputed since record sizes change (and so does `last_crc32`). Atomic rename + remove of the original after a verify pass.

Tool is intentionally offline-only — runs while otterbrix is stopped. Online migration would require coordinating with active wal workers and isn't worth the complexity given the deprecation window.

Deprecation timeline: 6 months of dual-format reader support (v1 path stays in `decode_record`). At 6m: warn on every v1 record. At 12m: remove v1 decoder, error out on v1 records → tool becomes mandatory before upgrade.

### 12.2 Phase 8.F — Catalog hot path

#### 12.2.1 Current cache structure

`catalog_view_t` is constructed fresh per `execute_plan` (per task #75 — see `catalog_view.hpp:1-7` rationale). Caches in `catalog_view.hpp:101-105`:

```cpp
std::unordered_map<std::string, resolved_namespace_t> ns_cache_;       // key: nspname
std::unordered_map<std::string, resolved_table_t>     tbl_cache_;      // key: "<ns_oid>|<tblname>"
std::unordered_map<std::string, resolved_type_t>      type_cache_;     // key: "<ns_oid>|<typname>"
std::unordered_map<oid_t, std::vector<resolved_fk_t>> fk_outgoing_;    // already oid-keyed
std::unordered_map<oid_t, std::vector<resolved_fk_t>> fk_referencing_; // already oid-keyed
```

The composite-string keys are built by `make_oid_name_key` (`catalog_view.cpp:27-34`): `"<oid_int>|<name>"`. The header comment notes "to avoid custom hash functors" — cheap to write, but still a string allocation per probe.

Resolve flow today (per `catalog_view.cpp::get_table` and `manager_disk_resolve.cpp::resolve_table:35-184`):

```
caller (validate / enrich)
  → catalog_view.get_table(ctx, ns_oid, name)
    → tbl_cache_.find("ns_oid|name")
    → on miss: actor send → manager_disk_t::resolve_table(ns_oid, name)
                              ├─ scan pg_class for (relnamespace==ns_oid, relname==name)
                              ├─ extract oid + relkind
                              └─ enumerate columns from pg_attribute (or pg_computed_column for relkind='g')
    → store in tbl_cache_, return resolved_table_t* with .oid filled in
```

`resolved_table_t` already has `.oid` (`resolved_objects.hpp:36`). Same for `resolved_namespace_t.oid` (line 17). The downstream consumers — `enrich_logical_plan`, `validate_logical_plan`, dispatcher INSERT/UPDATE/DELETE paths — currently use `tbl->name` (and rebuild a cfn from `ns->name + "" + tbl->name`) for storage calls. **The oid is already populated; it's just unused at the storage boundary.**

#### 12.2.2 Proposed change

Two-axis change:

**(a) Cache keying.** Once oid is the storage primary key, the natural `tbl_cache_` key is also `oid` — but `get_table(ns_oid, name)` is name-keyed by the SQL parser's contract (a user typed `FROM ns.tbl`). Solution: keep the name-keyed `tbl_cache_` (it's a bounded-lifetime per-execute_plan cache; the string key cost is negligible), but **also** add an `oid → resolved_table_t*` secondary index for the post-enrich consumers that already have an oid:

```cpp
// catalog_view.hpp additions
std::unordered_map<oid_t, resolved_table_t*> tbl_by_oid_;  // points into tbl_cache_

const resolved_table_t* try_get_table_by_oid(oid_t) const noexcept;
```

Populated in `get_table` after `tbl_cache_.emplace`: `tbl_by_oid_[payload.oid] = &it->second;`. This is what the planner / executor will use once Phase 8.G nodes carry `table_oid`.

**(b) Namespace cache stays name-keyed.** `resolve_namespace` is the entry point — the user types a namespace name in `FROM x.y`. Once enrich resolves it once per execute_plan, downstream code uses `ns->oid` directly and never looks up by name again. No change needed.

#### 12.2.3 Resolve flow after migration

```
parser produces:  collection_full_name_t cfn = {db_or_ns, "", collname}
  → enrich_logical_plan(plan, catalog_view):
       ns = catalog_view.get_namespace(cfn.database)        -> ns->oid
       tbl = catalog_view.get_table(ctx, ns->oid, cfn.collection) -> tbl->oid
       node->set_table_oid(tbl->oid)                        // Phase 8.G stamp
  → planner / physical_plan_generator: reads node.table_oid, never re-resolves
  → executor → disk: storage_*_by_oid(tbl_oid, ...)
```

The single name→oid resolution per query, performed in enrich, replaces today's "every operator re-derives cfn from `tbl->name`" pattern. This is what the existing `resolve_table_result_t.oid` field was always intended to enable; 8.F is just plumbing.

#### 12.2.4 Downstream consumers

Files that currently read `tbl->name` (or reconstruct cfn from `ns->name + tbl->name`) and switch to `tbl->oid` after 8.F:

- `services/dispatcher/enrich_logical_plan.cpp` — already calls `get_table` and could trivially stamp oid (precedent: `node_alter_table_t` and `node_create_collection_t` already get oid stamped).
- `services/dispatcher/dispatcher.cpp` — INSERT path constructs a `collection_full_name_t` ~6 places; each becomes `oid_t`.
- `components/planner/planner.cpp` — `rewrite_*` functions read `node->collection_full_name()`; flip to `node->table_oid()`.
- `services/collection/executor.cpp:210` (the line behind task #132) reads `logical_plan->collection_full_name()`; becomes `logical_plan->table_oid()`. The wrapped-sequence_t bug disappears because sequence_t never stamps an oid; executor reads `result.dml_table_oid` from inner DML node.

Estimated 8.F surface: ~20 callsites in dispatcher + ~10 in planner + small additions in `catalog_view.{hpp,cpp}`. ~3 days as scoped in §4.

### 12.3 Phase 8.G — Logical plan node refactor

#### 12.3.1 Current state

`components/logical_plan/node.hpp:19-64` defines `node_t` with:

```cpp
class node_t : public boost::intrusive_ref_counter<node_t> {
public:
    node_t(memory_resource*, node_type, const collection_full_name_t& collection);
    const collection_full_name_t& collection_full_name() const;
    const database_name_t& database_name() const;
    const collection_name_t& collection_name() const;
    // ...
protected:
    const collection_full_name_t collection_;   // <-- const, set in ctor
    // ...
};
```

`collection_` is **immutable after construction**. Every node — including wrappers like `node_sequence_t` (`node_sequence.hpp:9-16`) and DDL-on-database like `node_create_database_t` — passes either a real cfn or a default-constructed empty cfn.

This is the structural cause of task #132: `node_sequence_t::node_sequence_t(resource)` calls `node_t(resource, type, {})` with empty cfn. When a wrapper sequence is the top of the plan, `executor.cpp:210` reads that empty cfn → `commit_txn` gets `database=""` → spawns phantom WAL worker → crash.

#### 12.3.2 Proposed change (incremental)

Two steps, both fitting §4's 1-week budget for 8.G:

**Step 1 — additive `table_oid_` on `node_t`.** Add a non-const `oid_t table_oid_{INVALID_OID}` field plus accessors:

```cpp
class node_t : ... {
public:
    components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
    void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }
protected:
    components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
    const collection_full_name_t collection_;  // kept; populated by parser
};
```

Enrich (Phase 8.F) stamps `table_oid_` for every node where it's meaningful. Consumers prefer `table_oid_` when set, fall back to `collection_` only inside the parser → enrich window. All existing constructors stay valid → no churn yet.

Two precedents already exist in the codebase:
- `node_alter_table_t` already has `table_oid_` + `set_table_oid` (`node_alter_table.hpp:65-66, 81`).
- `node_create_collection_t` already has `namespace_oid_` + `set_namespace_oid` (`node_create_collection.hpp:35-36, 45`).

Step 1 generalizes that pattern to the base class.

**Step 2 — atomic switch (the actual deprecation).** Once all enrich/planner/executor consumers prefer `table_oid_` (Step 1 + 8.F + 8.G partial), do a sweep to remove `collection_` reads from resolved-stage code paths. Keep the field around (still non-const-removal not viable yet), but mark it deprecated for resolved nodes via comment. Eventually delete in 8.H once it's confirmed every path either resolves or doesn't need it.

A tempting alternative — `using table_id_t = std::variant<oid_t, collection_full_name_t>;` — adds complexity without clear gain. The two-fields-with-precedence shape is closer to how the rest of the catalog already works (cf. `node_alter_table_t`).

#### 12.3.3 `node_sequence_t` and other identity-less wrappers

`node_sequence_t` (`node_sequence.hpp`) never claims a table identity — it's an ordered list of children, each of which may target its own table. After 8.G:

```cpp
node_sequence_t::node_sequence_t(memory_resource* r)
    : node_t(r, node_type::sequence_t, {})
{
    // table_oid_ stays INVALID_OID; that's correct. Executor reads table_oid
    // from the active inner child (already done correctly today via
    // result.dml_collection — just becomes result.dml_table_oid).
}
```

Same pattern applies to: `node_checkpoint_t`, `node_create_database_t` (database is named in `collection_.database`, no table involved), `node_abort_transaction_t`, `node_commit_transaction_t`. None need `table_oid_`.

#### 12.3.4 Per-category change list

| Category | Nodes | After enrich, table_oid_ … |
|---|---|---|
| **DML** | `node_insert_t`, `node_update_t`, `node_delete_t`, `node_aggregate_t`, `node_match_t`, `node_join_t`*, `node_primitive_write_t`, `node_primitive_delete_t` | **REQUIRED** — disk storage targeted by oid |
| **DDL on table** | `node_alter_table_t` (already), `node_drop_collection_t`, `node_create_constraint_t`, `node_create_index_t`, `node_drop_index_t`, `node_check_constraint_t`, `node_fk_check_t`, `node_fk_cascade_t`, `node_dynamic_cascade_delete_t`, `node_alter_column_add_t`, `node_alter_column_drop_t`, `node_alter_column_rename_t`, `node_computed_field_register_t`, `node_computed_field_unregister_t` | **REQUIRED** post-enrich |
| **DDL creating table** | `node_create_collection_t` (already has `namespace_oid_`), `node_create_view_t`, `node_create_macro_t`, `node_create_sequence_t`, `node_create_type_t` | INVALID_OID (table doesn't exist yet); allocated by planner during execute |
| **DDL on db/ns** | `node_create_database_t`, `node_drop_database_t` | always INVALID_OID |
| **Wrappers / control** | `node_sequence_t`, `node_checkpoint_t`, `node_commit_transaction_t`, `node_abort_transaction_t`, `node_get_schema_t`, `node_vacuum_t` | always INVALID_OID — task #132 fixed structurally |
| **Query-tree internal** | `node_group_t`, `node_having_t`, `node_sort_t`, `node_limit_t`, `node_data_t`, `node_function_t` | INVALID_OID (no table identity; their parent has it) |
| **UDF / type / macro** | `node_register_udf_t`, `node_unregister_udf_t`, `node_drop_macro_t`, `node_drop_sequence_t`, `node_drop_type_t`, `node_drop_view_t` | INVALID_OID for db/macro-targeting; oid for table-targeting (e.g. `node_drop_view` if backed by relkind='v' pg_class entry) |

\* `node_join_t` joins multiple tables — oid is the *primary* table; secondary tables resolved through children.

#### 12.3.5 Touch list

`components/logical_plan/` has 47 `node_*.hpp` files (count via `ls components/logical_plan/node_*.hpp | wc -l`) plus the base `node.hpp`. Step 1 (additive field) touches only `node.hpp` and `node.cpp` — 1 file pair.

Step 2 (consumer flips) touches every node's planner/dispatcher consumer. Search-and-replace targets:

- `grep -rn "->collection_full_name()" components/ services/` — ~80 hits → flip to `->table_oid()` where the post-enrich path is taken.
- `grep -rn "->database_name()\|->collection_name()" components/ services/` — ~120 hits → most stay (debug logging, error messages); a minority need oid plumbing.
- ~20 enrich-stamping additions (one per node category that needs `table_oid_`).

Total ~50 source files modified, in line with §4's 8.G scope.

### 12.4 Phase 8.H — Cleanup [COMPLETE]

Items deletable once 8.A-G have been stable for one release cycle. All items below have been executed except where marked DEFERRED.

#### 12.4.1 cfn-based storage_* methods

In `services/disk/disk_contract.hpp:127-175`, the cfn-shaped contract entries (the inventory used to size deletion):

| Method | Replacement |
|---|---|
| `storage_types(session, cfn)` | `storage_types_by_oid(session, oid)` |
| `storage_total_rows(session, cfn)` | `storage_total_rows_by_oid(session, oid)` |
| `storage_scan(session, cfn, filter, limit, txn)` | `storage_scan_by_oid(...)` |
| `storage_fetch(session, cfn, row_ids, count)` | `storage_fetch_by_oid(...)` |
| `storage_scan_segment(session, cfn, start, count)` | `storage_scan_segment_by_oid(...)` |
| `storage_append(ctx, data)` (cfn lives in `ctx.name`) | `storage_append_by_oid(ctx, oid, data)` then drop `ctx.name` |
| `storage_update(ctx, row_ids, data)` | likewise |
| `storage_delete_rows(ctx, row_ids, count)` | likewise |
| `storage_commit_append(ctx, commit_id, row_start, count)` | likewise |
| `storage_revert_append(ctx, row_start, count)` | likewise |
| `storage_commit_delete(ctx, commit_id)` | likewise |
| `storage_commit_appends(ctx, commit_id, ranges)` | rewrite range type to oid-keyed |
| `storage_commit_deletes(ctx, commit_id, std::set<cfn>)` | `std::set<oid_t>` |
| `storage_revert_appends(ctx, ranges)` | likewise |
| `scan_by_key(ctx, cfn, ...)` (already paired with `scan_by_table_oid` at `manager_disk_resolve.cpp:513`) | drop in favor of oid variant |

That's **~15 contract entries** to remove. Each removal trims `dispatch_traits` (line 177-217) and a corresponding implementation in `manager_disk_storage.cpp` / `manager_disk_resolve.cpp`. The compile-time `dispatch_traits` decline is the integration test for "all callers gone".

#### 12.4.2 `collection_full_name_t` shrinkage [PARTIALLY DONE in Phase 9.W; full type removal DEFERRED to Phase 10+]

`components/base/collection_full_name.hpp:1-71` — the type currently has 4 string fields (`unique_identifier`, `database`, `schema`, `collection`) plus three constructors and three operators. WAL encoding round-trips only `database` + `collection` (§12.1.2 confirmed by grep), so `unique_identifier` and `schema` are not load-bearing on the disk path.

After 8.H:
- `unique_identifier` removed (never written by any encoder, only read in equality/hash; ~10 LOC across `.hpp` + every consumer).
- `schema` retained iff `pg_namespace.nspname` semantics surface to users beyond mapping to the disk-side cfn shape. If the SQL boundary keeps schema-qualified names, the field migrates to a parser-only type. Either way, the 3-shape bug from §1.1 collapses to one shape (`{db, coll}`) once schema is no longer a storage key dimension.
- Or — drop the type entirely from the storage path; keep only at the SQL boundary as a tuple of `(db_name, coll_name)`. The struct stays for parser-stage code; gets renamed to something like `qualified_name_t` to clarify it's no longer the storage key.

#### 12.4.3 `rangevar_to_collection` shape normalization [DONE in 8.A]

`components/sql/transformer/utils.hpp:41` — `rangevar_to_collection(RangeVar*)` produces the SQL-form shape `{database="", schema="testdatabase", collection="testcollection"}`. The C++ wrapper API (`wrapper_dispatcher::create_collection`) produces a different shape `{database="testdatabase", schema="", collection="testcollection"}`. `manager_disk_resolve.cpp:656-658` normalizes between them with a try-this-then-that fallback:

```cpp
collection_full_name_t full{ns_name, "", tbl_name};
if (storages_.find(full) == storages_.end())
    full = {"", ns_name, tbl_name};
```

After 8.H this whole normalization vanishes — there is no storage map keyed by cfn, so cfn shape stops mattering. ~50 LOC across `manager_disk_resolve.cpp`, `manager_disk_storage.cpp`, and a few dispatcher helpers can be deleted.

#### 12.4.4 Dispatcher resolution paths

`services/dispatcher/dispatcher.cpp` currently has multiple paths that re-resolve cfn → storage_t at the dispatch point (defensive, post-task #132 era). After 8.G these aren't needed:

- "resolve and re-stamp before INSERT" path (a few hundred LOC).
- Some `enrich_logical_plan` post-processing that synthesizes a cfn from pg_class results (the `tbl->name + ns->name → cfn` reconstruction).
- `services/dispatcher/catalog_view.cpp:213-216` — the constants `pg_constraint_coll`, `pg_attribute_coll`, `pg_class_coll`, `pg_namespace_coll` (cfn literals for catalog tables). Replace with `well_known_oid::*` references once `read_rows_by_key` has an oid variant.

Estimated ~200-300 LOC.

#### 12.4.5 Helpers that derive cfn from pg_class [DONE]

Once `storage_*_by_oid` is the only API, helpers like `manager_disk_t::scan_by_table_oid` (`manager_disk_resolve.cpp:612-661`) — which today does `oid → pg_class → pg_namespace → cfn → scan_by_key(cfn)` — collapse to a single `storages_.find(oid)`. The 50-line oid→cfn round-trip in that function evaporates.

**Status (Phase 8.H)**: `scan_by_table_oid` removed from `manager_disk_resolve.cpp`, `manager_disk.hpp`, `manager_disk.cpp` (msg_id table + dispatch case), `disk_contract.hpp` (declaration + dispatch_traits). Two callers (`operator_fk_check.cpp`, `operator_fk_cascade.cpp`) updated to call `scan_by_key` directly. cfn↔oid bridge helpers (`resolve_oid_for_cfn_local`, `resolve_cfn_for_oid_local`, `lookup_oid_by_cfn`) were already removed in Phase 8.E.

#### 12.4.6 LOC estimate

| Category | Approx LOC removed |
|---|---|
| Disk contract methods + dispatch_traits | 100-150 |
| Disk storage method bodies (cfn variants) | 200-300 |
| `collection_full_name_t` shrinkage + callers | 50-100 |
| `rangevar_to_collection` normalization | 50 |
| Dispatcher resolve/restamp paths | 200-300 |
| `scan_by_table_oid`-style oid→cfn helpers | 50 |
| **Total** | **~650-950** |

Within the 500-800 LOC target band; the upper bound depends on how aggressively `collection_full_name_t` is pared back vs. retained at the SQL boundary.

---

## 13. Risks, Testing, Performance

This section deepens section 5 (risks), defines a phase-by-phase testing strategy, quantifies expected performance impact, and resolves the open questions in section 9.

### 13.1 Detailed risk register

#### 13.1.1 Risks restated from section 5

**R1 — WAL backward compatibility (5.1)**
- *Failure scenario*: User upgrades binary; on first start WAL replay encounters records keyed by `(database_name, collection_name)` strings. New replay code expects `oid_t`. Replay aborts → instance fails to start → user-visible: "service won't come up after upgrade", potential data loss if user runs `--force-fresh`.
- *Detection*: WAL recovery cross-version test (write WAL with v1, replay with v2). Plus a checksum-tagged "WAL record format version" byte at record head that v2 reader inspects before deserializing.
- *Mitigation*: Phase 8.E adds dual-format reader. Records carry a 1-byte format tag (`0x01` = string-keyed, `0x02` = oid-keyed). v2 reader translates string→oid via `pg_class` rebuilt from snapshot. After 6-month deprecation window, drop v1 reader.
- *Residual risk*: Low. A pg_class rebuild that misses an entry would orphan WAL records. Bootstrap rebuild test (13.2) covers this.

**R2 — OID stability across crashes (5.2)**
- *Failure scenario*: `oid_gen_` resets to 16 (start of user range) after crash → next CREATE TABLE allocates an oid that already names a dropped-but-WAL-pending table → WAL replay routes new INSERTs to wrong storage.
- *Detection*: invariant test "oid never repeats across restart" — start instance, create 100 tables, capture oids, kill -9, restart, create 100 more, assert no overlap.
- *Mitigation*: `restore_oid_generator_sync` already persists generator high-water mark in `pg_class`-adjacent metadata. Add post-restore assertion `oid_gen_.next() > max(pg_class.oid)` at startup.
- *Residual risk*: Very low. The persistence is already exercised by Phase 0-7 tests.

**R3 — Cross-database / cross-namespace queries (5.3)**
- *Failure scenario*: SELECT references `db_a.public.t1 JOIN db_b.public.t2`. Enrich resolves cfn→oid by querying current-database catalog only. Cross-database oid lookup misses → "table not found" surfaced incorrectly.
- *Detection*: integration test with two databases and a cross-DB join (currently unsupported but worth a stub test that asserts the diagnostic).
- *Mitigation*: `catalog_view_t::resolve_cfn_to_oid` consults the correct database's catalog (oid namespace today is per-database; section 13.4.2 keeps it that way).
- *Residual risk*: Low for now since cross-DB JOIN is not a supported feature. Becomes higher if/when it ships.

**R4 — Debugging / log readability (5.4)**
- *Failure scenario*: Production trace shows `storage_commit_append for oid 47 failed`; on-call engineer cannot map oid→name without a running instance.
- *Detection*: code review during 8.A — every log site that emitted cfn must call the new helper.
- *Mitigation*: `manager_disk_t::log_table_name(oid)` returns `"db.schema.table (oid 47)"` via cached lookup. All `LOG_*` macros wrap oid args in this helper.
- *Residual risk*: Negligible. Trace ergonomics, not correctness.

**R5 — Test infrastructure churn (5.5)**
- *Failure scenario*: A test fixture constructs cfn but production code expects oid → silent skip or false-pass (test passes because both paths return "0 rows").
- *Detection*: deliberate negative tests — feed a cfn into oid-only API, expect compile error (after 8.H removes cfn API). Pre-8.H, expect runtime assertion.
- *Mitigation*: shared helper `test::resolve_oid(cfn)` used by all tests.
- *Residual risk*: Low; mostly mechanical.

**R6 — Phase 8.G logical plan refactor (5.6)**
- *Failure scenario*: `node_t::collection_` removal misses a derived class → silent default-constructed `oid_t = INVALID_OID` (0) reaches storage → `storages_.find(0)` succeeds against pg_class oid 0 (which is itself well-known) → corruption of system catalog.
- *Detection*: assertion in `storage_*_by_oid` — `assert(oid != INVALID_OID && oid not in well_known_oid::* unless caller is bootstrap)`.
- *Mitigation*: do 8.G as a single dedicated PR; freeze concurrent work; `oid_t` constructor removed in favor of `oid_t::make(uint32_t)` to surface every default-construction site at compile time.
- *Residual risk*: Medium during the PR; low after merge.

#### 13.1.2 New risks

**R7 — Multi-stage / rolling upgrade (mixed v1/v2 binaries)**
- *Failure scenario*: A deployment with N replicas is rolling-upgraded. Replicas A,B (v1) and C (v2) replay the same WAL stream. v1 emits string-keyed records; v2 emits oid-keyed records; A reading C's records fails with "unknown record format" → A diverges from C → split brain.
- *Detection*: cross-version replay test — generate WAL with v2, replay with v1 (in addition to v1→v2 already in 13.2). Both directions must either succeed or fail-closed (refuse to start, don't half-replay).
- *Mitigation*: v1 readers do not exist in the codebase yet; otterbrix is single-process today. We recommend: (a) declare "no rolling upgrades across 8.E boundary"; (b) add an explicit `wal_format_min_supported = 0x01` field at WAL header so a v1 binary refuses to open a WAL containing 0x02 records, surfacing the upgrade-order error early.
- *Residual risk*: Low if the otterbrix deployment model stays single-process. Becomes Medium if/when replicas ship.

**R8 — OID overflow (32-bit space exhaustion)**
- *Failure scenario*: `oid_t = std::uint32_t`; max 4,294,967,295. Allocation is monotonic in `oid_gen_` and never reused (even on DROP TABLE — that's how pg_depend stays consistent). At 1k DDL/sec sustained = 4.3M sec ≈ 50 days. At 1 DDL/sec ≈ 136 years. Realistic upper bound: a workload that programmatically creates+drops temp tables thousands of times a second hits the wall in a few months.
- *Detection*: counter monitor — emit `oid_gen_.next()` to metrics. Alarm at 80% of UINT32_MAX (≈3.4B).
- *Mitigation*:
  - *Short-term*: document the bound; warn users running churning DDL workloads.
  - *Long-term*: migration path to `oid_t = std::uint64_t`. Cost: WAL format bump, pg_class.oid column widens, all storage maps re-keyed (but transparently — `unordered_map<uint32_t, X>` → `unordered_map<uint64_t, X>` is mechanical). Defer until a workload reaches 50% of UINT32_MAX or replication ships (whichever comes first).
- *Residual risk*: Very low for foreseeable workloads. Higher for synthetic stress.

**R9 — Cross-instance OID collision (replication / federation)**
- *Failure scenario*: Two otterbrix instances each allocate user oid 16 for their first user table. Replication ships logical records from instance A to B → record "INSERT into oid 16" lands at the wrong table on B.
- *Detection*: integration test once replication exists — no test possible today.
- *Mitigation*: oids must become per-instance scoped (every record carries `(instance_id, oid)`) or globally unique (high-bits assigned per instance, low-bits local — i.e., the Snowflake pattern). Decision deferred until replication design starts. The 64-bit migration (R8) opens room for an instance prefix.
- *Residual risk*: N/A today (no replication). Document as a precondition to replication design.

**R10 — Catalog import/export tooling (pg_dump-style)**
- *Failure scenario*: User exports schema+data with a future `otterbrix_dump` tool from instance A; imports to instance B. Records reference oid 47 (table `users` on A); on B, oid 47 is `orders`. Import silently routes user data into orders.
- *Detection*: import roundtrip test — dump+restore, run schema-equivalence + row-count check.
- *Mitigation*: dump format must encode `pg_class` mapping `(oid → relname, relnamespace)` alongside data. Import translates source-oids → target-oids using fresh allocation on B, rewriting referenced oids during data load. Foreign keys, indexes, dependencies (pg_depend) all need translation.
- *Residual risk*: N/A today (no pg_dump). Document as a precondition to dump tool design. Add a test stub now that asserts "import without translation map fails closed".

### 13.2 Testing strategy

#### Per-phase test matrix

| Phase | Unit | Integration | Compat | Stress | Negative |
|---|---|---|---|---|---|
| 8.A storage API | `storage_append_by_oid_basic`, `storage_commit_append_by_oid_basic`, `storage_remove_by_oid_basic` | INSERT via dispatcher → new oid path → SELECT roundtrip | None (additive) | 1M append_by_oid, p50/p99 latency | invalid oid (INVALID_OID, unknown oid, oid in pg_class but no storage) |
| 8.B operators | each operator with stamped oid (insert/update/delete/scan/join) | full DML pipeline, oid stamped at enrich, reaches operator | parser→enrich→operator chain still works for cfn-shaped tests | 10k operator constructions/sec | wrapped sequence_t with empty inner — must error at enrich, not crash |
| 8.C execution_context | exec_ctx oid plumbing | every actor send carries oid | None (rename only) | None | exec_ctx with INVALID_OID rejected at receive |
| 8.D index | `index::insert_rows_by_oid` etc. | INSERT triggers index update via oid | rebuild from disk: re-keyed engines_ map matches pg_class | 100k indexed inserts | index for nonexistent oid |
| 8.E WAL | record encode/decode (v1, v2, v1→v2 translate) | full crash-recovery cycle: write v2, kill, replay v2 | **WAL recovery cross-version**: write WAL with v1 binary checkout, replay with v2 HEAD | 1M record replay timing | v1 record with unknown collection name; v2 record with oid not in pg_class |
| 8.F catalog/resolve | `catalog_view_t::resolve_cfn_to_oid` cache | enrich → resolve → operator end-to-end | catalog rebuild from snapshot then WAL replay | 10k concurrent resolves | cfn that doesn't exist; cfn collision across databases |
| 8.G logical plan | each node type construction with oid | parse → plan → execute with oid-only nodes | None (final form) | plan construction throughput | wrapper node without inner DML target |
| 8.H cleanup | None (deletion only) | full regression suite (existing 73 tests) | None | None | compile-error tests (cfn API gone — must not link) |

#### Specific named tests

- **`wal_recovery_cross_version_test`** (8.E): build v1 binary at tag `pre-phase-8`, write WAL with 1k INSERTs spanning 12 system tables and 5 user tables, kill. Build v2 HEAD, point at the same WAL dir, start, run SELECT against every table. Assert row counts match pre-kill.
- **`bootstrap_from_empty_test`** (every phase): start with empty `pg_class`. Bootstrap creates all 12 system tables. Assert each gets the correct `well_known_oid::*` value (pg_class=1, pg_attribute=2, pg_namespace=3, etc.). Assert `oid_gen_.next() == 16` after bootstrap (first user oid).
- **`concurrent_create_table_test`** (8.A or earlier — pre-existing, but expand): N=64 threads each issuing CREATE TABLE in parallel. Assert all 64 get distinct oids, all in `pg_class`, no gaps, `oid_gen_` final value = 16+64. Catches `oid_gen_` thread-safety regressions.
- **`oid_overflow_simulation_test`** (offline): synthetic test that fast-forwards `oid_gen_` to UINT32_MAX-100, runs 100 CREATE TABLE, asserts the 101st fails cleanly with a typed error (not assert-abort). Skipped in CI by default.
- **`mixed_format_wal_test`** (8.E): WAL containing both v1 and v2 records (manually constructed). Replay must produce equivalent state.

#### Test ownership

- 8.A-D: per-phase author writes unit + integration tests as part of the PR.
- 8.E: dedicated WAL-test PR before any production roll-forward.
- 8.G: refactor PR includes the wrapped-sequence_t negative test (closes task #132).
- 8.H: no new tests; existing test suite is the contract.

### 13.3 Performance impact

#### Hash and lookup cost

- *Today*: `unordered_map<collection_full_name_t, X>::find(cfn)`. cfn = 3 std::strings, ~30 bytes hashed via boost::hash_combine over each string. ~30 ns per lookup measured (Phase 6 profile, M2 Pro).
- *Post-8.A*: `unordered_map<uint32_t, X>::find(oid)`. 4 bytes hashed via identity. ~5 ns per lookup expected (6× speedup on the lookup itself).
- *Hot path frequency*: every storage_* call (∼4 per INSERT row in MVCC-tagged path); every index lookup; every catalog_view resolve.

Estimated end-to-end speedup of paths dominated by these lookups: **3-5%** on INSERT-heavy workloads (lookup is not the bottleneck — disk and MVCC tag flip dominate). On pure metadata churn (CREATE/DROP TABLE bursts), **15-25%** plausible.

#### Memory

- *cfn key*: ~80 bytes per entry (3 std::string headers + small-string-optimized inline + alignment). For 10k tables: ~800 KB just in keys, plus hash buckets.
- *oid key*: 4 bytes per entry. For 10k tables: ~40 KB. **Savings ~750 KB** in `storages_` alone, multiplied across every per-table map (index, WAL workers, catalog cache).

#### Cache friendliness

- oid lives in 4 bytes — fits 16 oids per 64-byte cache line. Iteration over storages_ becomes streamable.
- cfn keys force pointer chase to heap-allocated string data (when SSO doesn't apply, which is most production names). Random L1/L2 misses per lookup.

#### WAL bandwidth

- *Per-record savings*: `database_name` (avg 12 bytes incl. length prefix) + `collection_name` (avg 18 bytes) ≈ **25 bytes saved per record**, replaced by 4-byte oid (net ~21 bytes saved).
- *Throughput*: at 100k records/sec sustained: **~2.1 MB/s WAL write savings**. Over a day: ~180 GB less WAL written. Significant for replication bandwidth too.
- *Replay*: faster proportionally (less IO, less deserialization work).

#### Negative impact: enrich-time resolve

- Each query now requires `cfn → oid` resolution at enrich. Today this is a `catalog_view_t::tbl_cache_` lookup (already O(1)), but for *uncached* cfns it's a `disk_actor` round-trip (~50 µs).
- *Mitigation*: catalog_view caches resolved oid for the duration of `execute_plan`. First reference pays the round-trip; subsequent references in the same plan are free.
- *Residual cost*: cold-cache first query against a freshly-restarted instance pays one round-trip per distinct table referenced. Acceptable.

#### Benchmarking plan

Establish baseline numbers (current `main`) before starting 8.A. Re-run after 8.D, 8.G, 8.H.

| Benchmark | Metric | Tool |
|---|---|---|
| 1k INSERT/sec sustained, 1 table, 1M rows | p50 / p99 latency, WAL bytes/sec | existing integration harness + `iostat` |
| SELECT roundtrip against 100-table schema (random table per query) | queries/sec, lookup ns | new microbench in `services/collection/benches/` |
| DDL CREATE TABLE burst, 10k tables in 60 sec | tables/sec, oid_gen_ contention | new bench |
| Mixed OLTP (insert + select + occasional update), 8 threads, 5 min | throughput/thread, p99 | existing or new |
| WAL replay of 10M-record log | replay sec, MB/sec | offline tool |

Acceptance criteria for the migration: **no regression > 2%** on any workload; **expected gains ≥ 5%** on INSERT-heavy and ≥ 15% on DDL-bursty workloads.

### 13.4 Resolution of open questions (section 9)

#### 13.4.1 Schema field

- *Recommendation*: **Keep schema as a SQL-level concept** but resolve `namespace_oid` at enrich time. `collection_full_name_t::schema` becomes a string derived from `pg_namespace.nspname` only at the user-facing boundary (parser output, error messages, system-table SELECT projections).
- *Reasoning*: PostgreSQL parity is valuable for SQL portability. Internally, oid-only is cleaner. The schema string is a presentation concern, not a routing key.
- *Alternative considered*: drop schema entirely, treat database-namespace pairs as the only level. Rejected because existing tests and SQL test suites assume `schema.table` qualification.
- *Defer-able?* No — must be settled at 8.B (when nodes get oid-stamped). The decision is "oid for `namespace_oid`, string for display."

#### 13.4.2 Multi-database WAL

- *Recommendation*: **Stay per-database for now**; revisit when sharding or replication lands. WAL workers keyed by `database_oid` (resolved once at startup from `pg_namespace`/database catalog).
- *Reasoning*: per-database WAL preserves crash-recovery isolation between databases. Single global WAL would simplify the "global oid namespace" question but complicate per-database recovery and DROP DATABASE atomicity (currently each WAL is independent and can be wiped per-DB).
- *Alternative considered*: single global WAL with database_oid in every record. Rejected — DROP DATABASE would have to scan and skip records by oid; recovery would be all-or-nothing across databases.
- *Defer-able?* Yes. Marked as a precondition revisit when sharding/replication design opens.

#### 13.4.3 Cross-namespace JOIN

- *Recommendation*: **Enrich resolves all referenced cfns to oids before plan generation**; plan and operators carry only oids; storage layer needs no special handling.
- *Reasoning*: namespace boundaries are a parser/catalog concern. Once resolved, oids are flat. Storage layer is namespace-agnostic.
- *Alternative considered*: deferred resolution at execute time. Rejected — would scatter cfn through hot path and re-introduce the shape divergence this migration is designed to eliminate.
- *Defer-able?* No — settled by the design of 8.B.

#### 13.4.4 System tables

- *Recommendation*: **No migration needed**. `well_known_oid::*` constants are already oid-keyed in `pg_class`. Bootstrap creates them with these oids deterministically.
- *Reasoning*: section 8 of this doc and `components/catalog/system_table_schemas.cpp` show the 12 catalog tables already use well-known oids; the migration applies to user tables.
- *Alternative considered*: re-allocate system table oids at runtime. Rejected — well-known oids are referenced by code (constants); changing them is a binary-incompatible change.
- *Defer-able?* Question is already resolved. Just document in 8.A that bootstrap path uses `well_known_oid::*` literals, not `oid_gen_.next()`.

### 13.5 Decision matrix

| Decision | Recommendation | Effort | Risk | Defer-able? |
|---|---|---|---|---|
| Phase 8 timing | After Phase 6 stable (60+/73 tests passing) | n/a | n/a | Yes |
| WAL backward compat policy | Dual-format reader (v1+v2) for one deprecation window | 1 week (subset of 8.E) | Medium — ser/de bugs are silent | No (must land in 8.E) |
| 64-bit OID widening (R8) | Defer; revisit at 50% UINT32_MAX or replication start | 1 week (mechanical, format bump) | Low | Yes |
| Schema field retention | Keep as SQL/display concept; namespace_oid internally | 0 (already structured this way) | Low | No (settled at 8.B) |
| Multi-database WAL global vs per-DB | Per-database (status quo) | 0 today | Low | Yes (revisit at sharding) |
| Cross-namespace JOIN handling | Resolve at enrich, oid-only downstream | included in 8.B | Low | No |
| System table migration | None needed; bootstrap uses well_known_oid::* | 0 | Negligible | Already resolved |
| Wrapped-sequence_t fix (task #132) | Resolved by 8.G; do not band-aid earlier | included in 8.G | Low (fix is structural) | No (#132 stays open until 8.G) |
| Rolling-upgrade support (R7) | Declare unsupported across 8.E boundary; format-tag refuses to half-open | 1 day (header tag) | Low for single-process; Medium when replicas ship | Yes (replicas not shipped) |
| Cross-instance OID scoping (R9) | Defer; precondition for replication | n/a | N/A today | Yes |
| Dump/restore translation (R10) | Defer; precondition for dump tool | n/a | N/A today | Yes |
| Phase 8.G as dedicated PR | Required; freeze concurrent work | included in 8.G estimate | High during PR; Low after | No |
| Benchmark gates | No regression > 2%; expected gain ≥ 5% INSERT, ≥ 15% DDL | 2 days harness | Low | No (gating criterion) |