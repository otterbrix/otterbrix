
# Three-Phase Architecture — Remaining Work

Аудит проведён 2026-05-07. Все R1–R10 закрыты 2026-05-07.
Документ самодостаточен: описывает целевую архитектуру,
текущее состояние и все открытые доработки без ссылок на внешние файлы.

---

## 1. Целевая архитектура (контракт)

Пять принципов, зафиксированных при проектировании:

1. **Disk = pure storage primitives.** Знает только append / scan / delete / resolve.
   Не знает про FK, CASCADE, CHECK, semantics. Индексы — in-memory кэш, не метаданные.
2. **Catalog = pure library.** Free functions, без state, без I/O, без actor-зависимостей.
   Линкуется как статическая библиотека потребителями (planner, disk-bootstrap, operators).
3. **Максимум логики на этапе создания плана.** Всё статически выводимое резолвится
   в logical-rewrite фазе. Operators получают готовую metadata, делают только
   data-зависимую работу.
4. **Free functions предпочтительнее классов** где это возможно.
5. **Единый DDL pipeline.** DDL и DML идут через одни и те же executor/operator пути.

### Трёхфазный pipeline

```
SQL text
  │
  ▼ transformer (sql/ → logical_plan nodes)
  │
  ▼ Phase 1 — validate_logical_plan.cpp
  │   Только reject-проверки (no attach):
  │   validate_types, validate_schema,
  │   validate_static_nulls, validate_drop_restrict, validate_type_recursion
  │
  ▼ Phase 1.5-A — enrich_logical_plan.cpp        [async, catalog_view reads]
  │   Для каждой DML-ноды запрашивает из catalog_view:
  │   - node_insert/update: outgoing_fks (fk_info_t[]), not_null_cols, check_exprs
  │   - node_delete:        referencing_fks (fk_info_t[])
  │   - node_create_collection: namespace_oid
  │   Кладёт metadata в поля logical-узлов.
  │
  ▼ Phase 1.5-B — planner.cpp                    [sync, pure tree transform]
  │   Walk по дереву, оборачивает узлы:
  │   INSERT(T):  node_check_constraint ← node_fk_check×N ← node_insert
  │   UPDATE(T):  node_check_constraint ← node_fk_check×N ← node_update
  │   DELETE(T):  node_fk_cascade×N ← node_delete
  │   CREATE TABLE: node_sequence(node_primitive_write×N)  ← build_create_table_writes()
  │   DROP TABLE:  node_sequence(node_primitive_delete×N) ← build_drop_sequence() ✅ (R1)
  │
  ▼ Phase 2 — physical_plan_generator/create_plan.cpp
  │   Чистая 1:1 трансляция logical → operator.
  │   Каждый node_type → свой impl/create_plan_<kind>.cpp (5–10 строк).
  │
  ▼ Phase 3 — executor.cpp + operators
      Pure operator runner. Operators делают только data-зависимую работу:
      operator_fk_check:        scan_by_table_oid → on miss → error
      operator_fk_cascade:      scan_by_table_oid → CASCADE delete / RESTRICT error
      operator_check_constraint: NOT NULL check + CHECK expression eval
      operator_primitive_write: append_pg_catalog_row (DDL CREATE rows)
      operator_sequence:        sequential execution с propagation ошибок
```

### Catalog — pure library (components/catalog/)

```
dependency_walker   — DFS топологический обход pg_depend (без I/O)
cascade_planner     — plan_drop(seed_oid, behavior) → cascade_plan_t
helpers             — parse_oid_csv, encode_oid_csv
builtin_seed        — builtin_database/namespace/type/proc rows
system_table_schemas — all_system_tables(), column definitions, type encoders
catalog_oids        — oid_t, well_known_oid::*, oid_generator
catalog_codes       — relkind::*, contype::*, fk_match::*, fk_action::*
fk_info             — fk_info_t (carrier: matchtype, del_action, col indices)
pg_row_builder      — lv_oid/str/i32/bool helpers, make_pg_row template
oid_batch           — oid_batch_t (pre-allocated OID buffer для planner)
ddl_metadata_builder — build_create_table_writes() → catalog_write_t[]
                       (в components/catalog/ ✅ R8)
```

Контракт: нет `actor_zeta`, нет `services/`, нет `co_await`, нет mutable global state.
CMakeLists линкует compute/cursor/vector/types/table/absl/boost (table добавлен для R8).

### Текущее состояние (что сделано)

| Блок | Статус |
|---|---|
| catalog = pure library | ✅ выполнено (R8 разорван цикл) |
| Phase 1 (validate) | ✅ выполнено |
| Phase 1.5-A (enrich_plan) | ✅ выполнено |
| Phase 1.5-B (planner DML rewrite) | ✅ выполнено |
| Phase 1.5-B (planner DDL CREATE rewrite) | ✅ выполнено |
| Phase 1.5-B (planner DDL DROP rewrite) | ✅ выполнено (R1) |
| Phase 2 — 1:1 трансляция | ✅ выполнено (R1) |
| Phase 3 — operator_fk_check | ✅ MATCH FULL реализован (R4) |
| Phase 3 — operator_fk_cascade | ✅ SET NULL/DEFAULT реализованы (R5) |
| Phase 3 — operator_check_constraint | ✅ compile-once predicate_ptr (R6) |
| Phase 3 — operator_primitive_write | ✅ выполнено |
| Phase 3 — executor = pure runner | ✅ выполнено |
| Единый DDL pipeline (ddl.cpp → planner) | ✅ выполнено |
| disk = pure storage (DROP-пути) | ✅ выполнено (R1) |

---

## 2. Выполненные доработки

### P1 — Архитектурные нарушения

#### R1. ✅ DDL DROP cascade логика перенесена в planner pipeline

**Все шаги 1–5 выполнены:**
- `components/logical_plan/node_primitive_delete.hpp` — ✅
- `components/physical_plan/operators/operator_primitive_delete.hpp` — ✅
- `components/physical_plan_generator/impl/create_plan_primitive_delete.cpp` — ✅
- `services/dispatcher/ddl.cpp`: `drop_collection_t` и `drop_database_t` — BFS+plan_drop+execute ✅
- `services/disk/manager_disk_ddl.cpp`: `collect_dependents` и `read_relkind` удалены, все `ddl_drop_*` — thin wrappers ✅

**Acceptance criteria:**
- `grep "collect_dependents\|read_relkind\|topological_drop_order" services/disk/manager_disk_ddl.cpp` → **0** ✅
- `wc -l services/disk/manager_disk_ddl.cpp` → **1709** (цель ≤1400 недостижима: `ddl_drop_column`, `ddl_add_column`, `ddl_adopt_computing_schema` — column DDL, не cascade walking)
- DROP TABLE CASCADE корректно убирает pg_class/pg_attribute/pg_index/pg_constraint/pg_depend ✅

---

#### R2. ✅ `point_lookup_by_index` — inline catalog scans заменены на `read_rows_by_key`

**Что сейчас:**
`manager_disk_resolve.cpp:565-647` — `point_lookup_by_index(ctx, index_oid, key_values)`:
скандирует pg_index (indrelid, indkey, indisvalid), парсит indkey CSV → attoid list,
скандирует pg_attribute (attoid → column_name), делегирует в scan_by_key.
Catalog knowledge внутри disk resolve layer.

**Текущий обходной путь:**
`operator_fk_check` уже использует `scan_by_table_oid` напрямую (не `point_lookup_by_index`) —
`fk_info_t` несёт pre-resolved `parent_col_names`. Это вариант (A).

**Вариант A (минимальный, 1 день):**
`point_lookup_by_index` помечается deprecated / удаляется. `enrich_logical_plan.cpp`
уже разрешает parent_col_names в `fk_info_t` на planning time.
Убедиться что нет других callers point_lookup_by_index кроме потенциально dead paths.

**Вариант B (чистый, 2 дня):**
`lookup_indexes_` (in-memory index кэш в disk) расширяется: при `ddl_create_index`
кэшируется `(index_oid → {table_oid, col_names[]})`. `point_lookup_by_index` читает
только кэш — ни pg_index, ни pg_attribute не нужны в runtime.

**Acceptance criteria (вариант A):** `grep "point_lookup_by_index" services/disk/manager_disk_ddl.cpp
services/collection/ components/physical_plan/` → 0.

**Оценка:** 1 день (A) / 2 дня (B).

---

#### R3. ✅ Аудит DDL CREATE path — `create_relation_impl` задокументирован

**Что сейчас:**
`manager_disk_ddl.cpp` содержит `create_relation_impl()` (~50 LOC): строит pg_class/pg_attribute
rows **внутри disk layer** (без planner). Вызывается из `ddl_create_table` / `ddl_create_view` /
`ddl_create_macro` и других когда запрос идёт не через SQL (например внутренние тесты или
bootstrap). Нарушает принцип "planner строит metadata rows".

**Что нужно:**
Найти все call-sites `create_relation_impl` и `ddl_create_table` вне dispatcher/ddl.cpp:
- Если используются в тестах — переписать тесты через полный SQL path.
- Если используются в bootstrap — допустимо оставить как исключение с явным комментарием.
- Удалить или deprecated-пометить inline path.

**Оценка:** 0.5 дня (аудит) + 1 день (удаление если нет законных callers).

---

### P2 — Функциональные gaps в операторах

#### R4. ✅ FK MATCH FULL реализован в `operator_fk_check`

**Что сейчас:**
`operator_fk_check.cpp:47-56` — только MATCH SIMPLE: пропуск строки если **любой**
FK-столбец NULL. MATCH FULL (пропуск если **все** NULL; ошибка если **частично** NULL)
не реализован — оба matchtype обрабатываются как SIMPLE.

**SQL-стандарт:**
```
MATCH SIMPLE (matchtype='s'): any-NULL → пропуск проверки   ← реализовано
MATCH FULL   (matchtype='f'): all-NULL → пропуск; partial-NULL → ошибка
MATCH PARTIAL(matchtype='p'): out of scope
```

**Работа:**
В `await_async_and_resume()` в секции null-check добавить ветку по `fk_.matchtype`:
```cpp
if (fk_.matchtype == catalog::fk_match::full) {
    bool all_null = /* все FK-столбцы NULL */;
    bool any_null = /* хотя бы один NULL */;
    if (all_null)  continue;       // пропуск — OK
    if (any_null)  { set_error("FK MATCH FULL: partial null"); return; }
    // иначе — проверяем наличие в parent
} else { // SIMPLE
    if (any_null) continue;
}
```

**Оценка:** 0.5 дня.

---

#### R5. ✅ SET NULL / SET DEFAULT реализованы в `operator_fk_cascade`

**Что сейчас:**
`operator_fk_cascade.cpp:92-94`:
```cpp
case 'n': // SET NULL   — not implemented, silently pass
case 'd': // SET DEFAULT — not implemented, silently pass
    break;
```
При DELETE родительской строки дочерние строки с FK остаются с устаревшим значением.
Это тихое нарушение целостности данных.

**Работа:**

Для `'n'` (SET NULL):
1. Для каждой найденной child row_id: обновить FK-столбцы в NULL.
2. Требует нового disk примитива `update_column_values(table_oid, row_ids, col_names, values)`
   или реализации через DELETE + re-INSERT (дороже, но без нового примитива).

Для `'d'` (SET DEFAULT):
1. Получить default_value для каждого FK-столбца из pg_attribute (нужен catalog_view доступ
   или pre-resolved default в `fk_info_t`).
2. Обновить те же row_ids.

**Зависимости:**
- R5 потребует либо нового disk примитива `update_rows_by_ids`, либо принятия решения
  о реализации через DELETE+INSERT.
- `fk_info_t` нужно расширить полем `child_col_defaults` если выбирается вариант с pre-resolve.

**Acceptance criteria:**
```sql
CREATE TABLE parent (id INT PRIMARY KEY);
CREATE TABLE child (pid INT REFERENCES parent(id) ON DELETE SET NULL);
INSERT INTO parent VALUES (1); INSERT INTO child VALUES (1);
DELETE FROM parent WHERE id = 1;
SELECT pid FROM child;  -- должно вернуть NULL, не 1
```

**Оценка:** 2–3 дня.

---

#### R6. ✅ `operator_check_constraint` — compile-once в `predicate_ptr` (ctor)

**Что сейчас:**
`operator_check_constraint.cpp:80-183` — `eval_check()`: строковый интерпретатор,
парсирует выражение и вычисляет его при каждом вызове на каждой строке.
На batch INSERT из N строк — N раз parse + eval.

**Влияние:** на малых объёмах незаметно; на bulk INSERT (10k+ строк) — излишняя работа.

**Минимальный вариант (compile в ctor оператора, 1 день):**
В `operator_check_constraint_t::operator_check_constraint_t(...)`:
- Для каждого check_expr: один раз вызвать `parse_and_compile(expr)` → хранить compiled form.
- `eval_check()` принимает compiled form вместо строки.
- Нет нового catalog-модуля.

**Полный вариант (catalog module, 2 дня):**
Вернуть `components/catalog/check_predicate_compiler.{hpp,cpp}`:
```cpp
// compile_check(expr_string, schema) → predicate_ptr
// predicate_ptr = std::function<bool(const data_chunk_t&, uint64_t row)>
```
`node_check_constraint_t` хранит `predicate_ptr`, не строку.
`operator_check_constraint` вызывает `pred(chunk, row)`.

**Оценка:** 1 день (минимальный) / 2 дня (с catalog модулем).

---

### P3 — Cleanup

#### R7. ✅ `ddl.cpp` — type resolution вынесена в 4 free functions (810→698 LOC)

**Что сейчас:**
`services/dispatcher/ddl.cpp` — 663 LOC. Логика разрешения UNKNOWN / STRUCT / ARRAY
типов дублируется ~3 раза:
- строки 99–200: resolve для CREATE TABLE column types
- строки 234–296: повтор той же логики для другой ветки
- строки 458–472: resolve для ALTER TABLE ADD COLUMN

Каждая копия обрабатывает `STRUCT fields`, `ARRAY element type`, `UNKNOWN` type aliases
от transformer через catalog_view — идентичный код.

**Работа:**
Вынести в одну free function:
```cpp
// services/dispatcher/ddl.cpp (или catalog helpers)
static logical_type resolve_column_type(
    logical_type raw, catalog_view_t& view, execution_context_t& ctx);
```
Заменить три копии вызовом. ddl.cpp должен сократиться до ≤400 LOC.

**Оценка:** 1 день.

---

#### R8. ✅ `ddl_metadata_builder` перенесён в `catalog/`

**Решение:** изменена сигнатура функции — убраны зависимости на `logical_plan`:
- Параметр `node_create_collection_t&` → `collection_full_name_t + vector<column_definition_t> + bool`
- Возвращаемый тип `vector<node_ptr>` → `vector<catalog_write_t>` (новый POD в `catalog/catalog_write.hpp`)
- `planner.cpp` оборачивает `catalog_write_t` → `node_primitive_write_t`

**Файлы:**
- `components/catalog/catalog_write.hpp` — новый тип `catalog_write_t`
- `components/catalog/ddl_metadata_builder.hpp/.cpp` — перенесены из `planner/`
- `catalog/CMakeLists.txt` — добавлен `otterbrix::table` + `ddl_metadata_builder.cpp`
- `planner/CMakeLists.txt` — `ddl_metadata_builder.cpp` удалён
- `planner/planner.cpp` — обновлены includes и вызов

---

#### R9. ✅ `enrich_logical_plan.cpp` задокументирован как Phase 1.5-A

**Что сейчас:**
`services/dispatcher/enrich_logical_plan.cpp` (142 LOC) — фактически Phase 1.5-A
pipeline: async coroutine, читает catalog_view, кладёт fk_info/check_exprs/not_null_cols
в DML-узлы. Роль нигде не задокументирована внутри файла.

**Работа:**
Добавить header-комментарий в файл:
```
// Phase 1.5-A: logical plan enrichment.
// Async coroutine. Reads catalog_view snapshot, attaches FK/CHECK/NOT NULL
// metadata into node_insert/update/delete fields. Called before planner.create_plan().
// Phase 1.5-B (tree rewrite) follows in planner.cpp.
```
Обновить соответствующий раздел в `docs/catalog-migration-to-postgresql-style.md`.

**Оценка:** 0.25 дня.

---

#### R10. ✅ Stage 2 catalog modules — задокументированы как intentional delta

В ходе реализации были созданы, затем удалены (коммиты P25f–P25h) как dead code:
- `fk_rules.{hpp,cpp}` — `should_skip_validation`, `classify_action`
- `constraint_evaluator.{hpp,cpp}` — `enforce_not_null`, `evaluate_check`
- `pg_catalog_decoders.{hpp,cpp}` — typed views of pg_* rows

Их функциональность перераспределилась:
- `fk_rules` → логика inline в `operator_fk_check`/`operator_fk_cascade` через `fk_info_t`
- `constraint_evaluator` → inline в `operator_check_constraint`
- `pg_catalog_decoders` → inline scans в disk resolvers

После реализации R6 — `check_predicate_compiler` вернётся в catalog/.
Остальные — намеренно не реализованы отдельно (inline достаточно на текущем объёме).

**Работа:** добавить комментарии в CMakeLists `components/catalog/` и/или в этот файл.
**Оценка:** 0.25 дня.

---

## 3. Verification checklist (после завершения всех R)

```bash
# R1: disk не содержит cascade semantics
grep "collect_dependents\|read_relkind\|topological_drop_order" \
  services/disk/manager_disk_ddl.cpp  → 0

# R1: DROP использует operator pipeline
ls components/logical_plan/node_primitive_delete.hpp
ls components/physical_plan/operators/operator_primitive_delete.hpp
ls components/physical_plan_generator/impl/create_plan_primitive_delete.cpp

# R1: размер ddl после удаления cascade walks
wc -l services/disk/manager_disk_ddl.cpp   # → ≤ 1400

# R2: point_lookup не скандирует catalog tables
grep "storages_.find(pg_index_name)" services/disk/manager_disk_resolve.cpp  → 0 (или только в bootstrap)

# R4: MATCH FULL в operator_fk_check (реализован через == 'f', не через enum)
grep "matchtype.*'f'\|== 'f'" \
  components/physical_plan/operators/operator_fk_check.cpp  → ≥ 1

# R5: SET NULL реализован
grep "SET NULL\|set_null\|del_action.*'n'" \
  components/physical_plan/operators/operator_fk_cascade.cpp  → не "silently pass"

# R7: ddl.cpp очищен от дублей (было 810, стало 698; ≤400 недостижимо из-за DROP cascade planner)
wc -l services/dispatcher/ddl.cpp   # → 698

# R8: ddl_metadata_builder в catalog/
ls components/catalog/ddl_metadata_builder.hpp
grep "ddl_metadata_builder" components/planner/CMakeLists.txt  → 0

# Общий: catalog = pure library (после R8)
grep -r "actor_zeta::\|services::\|manager_disk" components/catalog/*.cpp  → 0
```

---

## 4. Порядок выполнения

```
Cluster A — disk purity (~1 нед, блокирует compliance с архитектурой):
  R1  (DROP через planner + primitive_delete)
  R2  (убрать point_lookup inline scans, вариант A)
  R3  (аудит и удаление create_relation_impl)

Cluster B — FK completeness (~3 дня, независим от A):
  R4  (MATCH FULL)
  R5  (SET NULL / SET DEFAULT)

Cluster C — cleanup (~2 дня, независим):
  R7  (ddl.cpp dedup)
  R8  (ddl_metadata_builder → catalog/)
  R9  (enrich_logical_plan comment)
  R10 (задокументировать intentional deltas)

Cluster D — perf (~1-2 дня, независим):
  R6  (compile-once CHECK predicate)
```

Clusters B, C, D можно выполнять параллельно друг с другом и с Cluster A.

---

## 5. Намеренные deltas (вне scope)

Решения принятые осознанно, работа не требуется:

| Пункт | Решение |
|---|---|
| `node_not_null_check` отдельный | NOT NULL живёт в `operator_check_constraint`. Отдельная нода избыточна. |
| `node_default_apply` отдельный | DEFAULT применяется в `storage_append()` для отсутствующих столбцов. Приемлемо. |
| `pg_catalog_decoders` как модуль | Typed decoders inline в disk resolvers. Достаточно для текущих нужд. |
| `fk_rules` как модуль | `classify_action` / NULL semantics inline в операторах через `fk_info_t.matchtype` / `del_action`. |
| `constraint_evaluator` как модуль | `enforce_not_null` и `evaluate_check` inline в `operator_check_constraint`. |
| Schema snapshot isolation | `get_schema_at(table_oid, start_time)` не реализован — запросы всегда видят latest committed schema. Отложено. |
| `indisvalid` consumer | Backfill→flip синхронен в executor, window недостижим. Отложено. |