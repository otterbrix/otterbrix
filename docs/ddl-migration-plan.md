# DDL Migration Plan: disk → catalog + dispatcher

## Цель

Убрать логику построения каталожных строк из `manager_disk_t::ddl_*` методов.
После миграции disk-слой занимается только хранением (WAL, MVCC, scan).
Логика "какие строки писать для DDL-операции" живёт в `components/catalog/ddl_metadata_builder`.

---

## Паттерн (уже реализован для CREATE TABLE)

### 1. `ddl_metadata_builder` — строит строки

```cpp
// components/catalog/ddl_metadata_builder.hpp
std::vector<catalog_write_t>
build_create_table_writes(
    std::pmr::memory_resource*                     resource,
    const collection_full_name_t&                  collection,
    const std::vector<table::column_definition_t>& columns,
    bool                                            is_disk_storage,
    oid_t                                           namespace_oid,
    oid_batch_t&                                    oid_batch);
```

Возвращает `vector<catalog_write_t>` — пары `{table, data_chunk_t}`.

### 2. Dispatcher — оборачивает в `node_primitive_write_t`, исполняет

```cpp
// В execute_ddl, case node_type::sequence_t:
} else if (child->type() == node_type::primitive_write_t) {
    auto* pw = static_cast<node_primitive_write_t*>(child.get());
    auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                      &disk::manager_disk_t::append_pg_catalog_row,
                                      ddl_ctx, pw->catalog_table(), std::move(pw->row()));
    co_await std::move(wf);
} else if (child->type() == node_type::primitive_delete_t) {
    auto* pd = static_cast<node_primitive_delete_t*>(child.get());
    auto [_d, df] = actor_zeta::send(dctx.disk_address,
                                      &disk::manager_disk_t::delete_pg_catalog_rows,
                                      ddl_ctx, pd->catalog_table(),
                                      pd->oid_col_idx(), pd->target_oid());
    co_await std::move(df);
}
```

### 3. Disk — только примитивы

- `append_pg_catalog_row(ctx, table, chunk)` — WAL + MVCC insert
- `delete_pg_catalog_rows(ctx, table, col_idx, oid)` — WAL + MVCC delete
- `commit_pg_catalog_appends(ctx, commit_id)` — commit + `rebuild_lookup_indexes()`

---

## Часть 1: Удаление методов из `manager_disk_t`

### Удаляем полностью — 24 метода

Из `services/disk/manager_disk_ddl.cpp` и объявлений в `services/disk/manager_disk.hpp`:

| Метод | hpp строка | cpp строка | Причина удаления |
|---|---|---|---|
| `create_relation_impl` (private) | 663 | 10 | internal helper, заменяется `build_create_table_writes` |
| `ddl_create_database` | 339 | 128 | только пишет pg_database |
| `ddl_create_namespace` | 340 | 145 | пишет pg_namespace + eager cache (покрывается `rebuild_lookup_indexes`) |
| `ddl_drop_namespace` | 341 | 166 | чистит cache (покрывается `rebuild_lookup_indexes`) |
| `ddl_create_table` | 344 | 202 | тонкая обёртка над `create_relation_impl` |
| `ddl_drop_table` | 349 | 211 | чистит cache (покрывается `rebuild_lookup_indexes`) |
| `ddl_create_sequence` | 378 | 427 | пишет pg_class + pg_sequence + pg_depend |
| `ddl_drop_sequence` | 386 | 478 | делегирует в `delete_pg_catalog_rows` + `ddl_drop_table` |
| `ddl_create_view` | 389 | 490 | пишет pg_class + pg_rewrite + pg_depend |
| `ddl_drop_view` | 393 | 536 | делегирует в `delete_pg_catalog_rows` + `ddl_drop_table` |
| `ddl_create_macro` | 396 | 546 | пишет pg_class + pg_rewrite + pg_depend |
| `ddl_drop_macro` | 400 | 592 | делегирует в `delete_pg_catalog_rows` + `ddl_drop_table` |
| `ddl_create_index` | 406 | 602 | пишет pg_class + pg_index + pg_depend |
| `ddl_drop_index` | 411 | 703 | делегирует в `delete_pg_catalog_rows` + `ddl_drop_table` |
| `ddl_create_type` | 461 | 714 | пишет pg_type + pg_depend |
| `ddl_drop_type` | 465 | 747 | RESTRICT check + `delete_pg_catalog_rows` |
| `ddl_create_function` | 473 | 779 | пишет pg_proc + pg_depend |
| `ddl_drop_function` | 480 | 1332 | RESTRICT check + `delete_pg_catalog_rows` |
| `ddl_create_constraint` | 445 | 820 | пишет pg_constraint + pg_depend |
| `ddl_drop_constraint` | 456 | 909 | RESTRICT check + `delete_pg_catalog_rows` |
| `ddl_drop_column` | 429 | 1091 | tombstone pg_attribute + cascade |
| `ddl_rename_column` | 433 | 1209 | delete + re-insert pg_attribute |
| `ddl_index_set_valid` | 417 | 1279 | delete + re-insert pg_index с новым `indisvalid` |
| `ddl_create_computing_table` | 365 | 1485 | тонкая обёртка над `create_relation_impl` с `relkind='g'` |

### Остаются — 4 метода (in-memory storage side effects)

| Метод | hpp строка | cpp строка | Причина |
|---|---|---|---|
| `ddl_add_column` | 426 | 944 | `storage->add_column()` обновляет in-memory схему загруженной таблицы |
| `ddl_adopt_computing_schema` | 352 | 1364 | сложный rebuild in-memory schema для computing tables |
| `ddl_computed_append` | 368 | 1497 | ref-count management в `pg_computed_column` |
| `ddl_computed_drop` | 372 | 1601 | ref-count management в `pg_computed_column` |

---

## Часть 2: Добавление в `ddl_metadata_builder`

Файлы: `components/catalog/ddl_metadata_builder.hpp` / `.cpp`

Все функции следуют одному паттерну — возвращают `std::vector<catalog_write_t>`.

### 2.1 `build_create_namespace_writes`

```cpp
std::vector<catalog_write_t>
build_create_namespace_writes(
    std::pmr::memory_resource* resource,
    const std::string&          name,
    oid_t                       namespace_oid);  // из oid_batch.allocate()
```

Пишет:
- 1 строку → `pg_namespace` (`oid`, `nspname`)

### 2.2 `build_create_sequence_writes`

```cpp
std::vector<catalog_write_t>
build_create_sequence_writes(
    std::pmr::memory_resource* resource,
    const std::string&          name,
    oid_t                       namespace_oid,
    oid_t                       seq_oid,         // из oid_batch.allocate()
    std::int64_t                start,
    std::int64_t                increment,
    std::int64_t                min_value,
    std::int64_t                max_value,
    bool                        cycle);
```

Пишет:
- 1 строку → `pg_class` (`oid`, `relname`, `relnamespace`, `relkind='S'`, `relstoragemode`)
- 1 строку → `pg_sequence` (`seqrelid`, `seqstart`, `seqincrement`, `seqmin`, `seqmax`, `seqcycle`, `seqlast`)
- 1 строку → `pg_depend` (sequence → namespace, deptype=`'n'`)

### 2.3 `build_create_view_writes`

```cpp
std::vector<catalog_write_t>
build_create_view_writes(
    std::pmr::memory_resource* resource,
    const std::string&          name,
    oid_t                       namespace_oid,
    oid_t                       view_oid,        // из oid_batch.allocate()
    oid_t                       rule_oid,        // из oid_batch.allocate()
    const std::string&          body_sql);
```

Пишет:
- 1 строку → `pg_class` (`relkind='v'`)
- 1 строку → `pg_rewrite` (`oid=rule_oid`, `rulename`, `ev_class=view_oid`, `ev_type='v'`, `ev_action=body_sql`)
- 1 строку → `pg_depend` (view → namespace, `'n'`)

### 2.4 `build_create_macro_writes`

```cpp
std::vector<catalog_write_t>
build_create_macro_writes(
    std::pmr::memory_resource* resource,
    const std::string&          name,
    oid_t                       namespace_oid,
    oid_t                       macro_oid,
    oid_t                       rule_oid,
    const std::string&          body_sql);
```

Пишет:
- 1 строку → `pg_class` (`relkind='m'`)
- 1 строку → `pg_rewrite` (`ev_type='m'`)
- 1 строку → `pg_depend` (macro → namespace, `'n'`)

### 2.5 `build_create_index_writes`

```cpp
std::vector<catalog_write_t>
build_create_index_writes(
    std::pmr::memory_resource*              resource,
    const std::string&                       index_name,
    oid_t                                    namespace_oid,
    oid_t                                    table_oid,
    oid_t                                    index_oid,          // из oid_batch.allocate()
    const std::vector<std::string>&          column_names,
    const std::vector<oid_t>&               column_attoids);    // attoid каждой колонки
```

Пишет:
- 1 строку → `pg_class` (`relkind='i'`)
- 1 строку → `pg_index` (`indexrelid=index_oid`, `indrelid=table_oid`, `indkey=CSV(attoids)`, `indisvalid=false`)
- 1 строку → `pg_depend` (index → table, `'a'`)
- N строк → `pg_depend` (index → column attoid, `'i'`, per column)

### 2.6 `build_create_type_writes`

```cpp
std::vector<catalog_write_t>
build_create_type_writes(
    std::pmr::memory_resource* resource,
    const std::string&          type_name,
    oid_t                       namespace_oid,
    oid_t                       type_oid,
    const std::string&          type_spec);  // пустая строка для builtin
```

Пишет:
- 1 строку → `pg_type` (`oid`, `typname`, `typnamespace`, `typdefspec`)
- 1 строку → `pg_depend` (type → namespace, `'n'`)

### 2.7 `build_create_function_writes`

```cpp
std::vector<catalog_write_t>
build_create_function_writes(
    std::pmr::memory_resource* resource,
    const std::string&          function_name,
    oid_t                       namespace_oid,
    oid_t                       fn_oid,
    std::int32_t                pronargs,
    std::int64_t                prouid,
    const std::string&          proargmatchers,
    const std::string&          prorettype);
```

Пишет:
- 1 строку → `pg_proc` (`oid`, `proname`, `pronamespace`, `pronargs`, `prouid`, `proargmatchers`, `prorettype`)
- 1 строку → `pg_depend` (function → namespace, `'n'`)

### 2.8 `build_create_constraint_writes`

```cpp
std::vector<catalog_write_t>
build_create_constraint_writes(
    std::pmr::memory_resource*   resource,
    const std::string&            constraint_name,
    oid_t                         table_oid,
    oid_t                         constraint_oid,
    char                          contype,           // 'f', 'c', 'u', 'p'
    oid_t                         ref_table_oid,     // INVALID_OID для не-FK
    const std::vector<oid_t>&    fk_column_attoids,
    const std::vector<oid_t>&    ref_column_attoids,
    char                          fk_matchtype,
    char                          fk_del_action,
    char                          fk_upd_action,
    const std::string&            check_expr);
```

Пишет:
- 1 строку → `pg_constraint`
- 1 строку → `pg_depend` (constraint → table, `'i'`)
- N строк → `pg_depend` (constraint → fk_column, `'i'`, per column)
- если FK: 1 строку → `pg_depend` (constraint → ref_table, `'n'`)

---

## Часть 3: Изменения в dispatcher и executor

### 3.1 `services/dispatcher/ddl.cpp`

#### `create_database_t` (сейчас: `ddl_create_namespace`)

```
Было:  ddl_ctx → disk.ddl_create_namespace(...)
Стало: writes = build_create_namespace_writes(...)
       для каждого write → disk.append_pg_catalog_row(...)
```

OID нужен заранее → добавить в `estimate_ddl_oid_count` для `create_database_t`: **1 OID**.

#### `create_sequence_t` (сейчас: `ddl_create_sequence`)

```
Было:  disk.ddl_create_sequence(...)
Стало: oids = allocate_oids_batch(2)  // seq_oid
       writes = build_create_sequence_writes(...)
       для каждого write → disk.append_pg_catalog_row(...)
```

#### `create_view_t` (сейчас: `ddl_create_view`)

```
Стало: oids = allocate_oids_batch(2)  // view_oid + rule_oid
       writes = build_create_view_writes(...)
```

#### `create_macro_t` (сейчас: `ddl_create_macro`)

Аналогично `create_view_t`.

#### `drop_sequence_t / drop_view_t / drop_macro_t`

`build_drop_sequence` уже обрабатывает `kPgSequence` и `kPgRewrite` в ветке `pg_class_table`.
Переписать на использование существующего `build_drop_sequence` без изменений:

```
Было:  disk.ddl_drop_sequence/view/macro(...)
Стало: plan = cascade_planner(pg_class_table, rel_oid, cascade_)
       drop_seq = build_drop_sequence(resource, plan)  // уже готово
       для каждого шага → disk.delete_pg_catalog_rows(...)
```

#### `alter_table_t / add_column` (сейчас: `ddl_add_column`)

```
Стало: 1. pre-scan через disk.read_rows_by_key(pg_attribute, {attrelid}, {table_oid})
             → вычислить next_attnum = max(attnum) + 1
       2. oid = allocate_oids_batch(1)
       3. writes = build_add_column_writes(resource, table_oid, col, next_attnum, attoid, ns_oid)
       4. для каждого write → disk.append_pg_catalog_row(...)
       5. disk.ddl_add_column(...)  // только storage->add_column() side effect
          (ddl_add_column упрощается до одного вызова add_column на storage)
```

Или: `ddl_add_column` переименовать в `update_storage_add_column(table_oid, column)`.

#### `alter_table_t / drop_column` (сейчас: `ddl_drop_column`)

```
Стало: 1. pre-scan pg_attribute → найти attoid, attnum, atttypid, typspec, defspec
       2. pre-scan pg_depend → найти зависимые индексы и констрейнты
       3. CASCADE: build_drop_sequence для каждого зависимого объекта
       4. delete_pg_catalog_rows(pg_attribute, attoid_col=0, attoid)
       5. writes = build_drop_column_tombstone(...)  // новая строка с attisdropped=true
       6. append_pg_catalog_row(pg_attribute, tombstone_chunk)
```

#### `alter_table_t / rename_column` (сейчас: `ddl_rename_column`)

```
Стало: 1. pre-scan pg_attribute → прочитать текущую строку (attoid, attnum, и т.д.)
       2. delete_pg_catalog_rows(pg_attribute, col=0, attoid)
       3. write = build_pg_attribute_row(..., new_name)  // новая строка
       4. append_pg_catalog_row(pg_attribute, chunk)
```

#### `create_constraint_t` (сейчас: `ddl_create_constraint`)

```
Стало: oid = allocate_oids_batch(1)
       writes = build_create_constraint_writes(...)
       для каждого write → disk.append_pg_catalog_row(...)
```

### 3.2 `services/dispatcher/dispatcher.cpp`

#### CREATE TYPE (сейчас: `ddl_create_type`, строка 456)

```
Стало: oid = allocate_oids_batch(1)
       writes = build_create_type_writes(...)
       для каждого write → disk.append_pg_catalog_row(...)
```

#### DROP TYPE (сейчас: `ddl_drop_type`, строка 508)

```
Стало: 1. RESTRICT check: read_rows_by_key(pg_depend, {refclassid, refobjid}, {...})
       2. Если blocked → вернуть ошибку
       3. delete_pg_catalog_rows(pg_type, col=0, type_oid)
       4. delete_pg_catalog_rows(pg_depend, col=1, type_oid)
       5. delete_pg_catalog_rows(pg_depend, col=3, type_oid)
```

#### CREATE FUNCTION (сейчас: `ddl_create_function`, строка 897)

```
Стало: oid = allocate_oids_batch(1)
       writes = build_create_function_writes(...)
       для каждого write → disk.append_pg_catalog_row(...)
```

#### DROP FUNCTION (сейчас: `ddl_drop_function`, строка 960)

Аналогично DROP TYPE.

### 3.3 `services/collection/executor.cpp`

#### CREATE INDEX (сейчас: `ddl_create_index`, строка 214)

```
Стало: 1. pre-scan pg_attribute(attrelid=table_oid) → построить map name→attoid
       2. oids = allocate_oids_batch(1)  // index_oid
       3. writes = build_create_index_writes(...)
       4. для каждого write → disk.append_pg_catalog_row(...)
       5. disk.ddl_index_set_valid(index_oid, true) после backfill
          (упростить до: delete old pg_index row + insert new с indisvalid=true)
```

#### DROP INDEX (сейчас: `ddl_drop_index`, строка 271)

```
Стало: build_drop_sequence уже содержит шаг для pg_index (col 0).
       Использовать существующую CASCADE логику через cascade_planner.
```

#### `ddl_index_set_valid` (строка 225)

```
Стало: 1. read_rows_by_key(pg_index, {indexrelid}, {index_oid}) → читаем indrelid + indkey
       2. delete_pg_catalog_rows(pg_index, col=0, index_oid)
       3. write = build_pg_index_row(index_oid, indrelid, indkey, valid=true)
       4. append_pg_catalog_row(pg_index, chunk)
```

---

## Часть 4: Вспомогательные функции в `ddl_metadata_builder`

### 4.1 Row-builders для update-операций

Для `rename_column`, `index_set_valid`, `drop_column` tombstone:

```cpp
// Строит одну строку pg_attribute
vector::data_chunk_t build_pg_attribute_row(
    std::pmr::memory_resource* resource,
    oid_t attoid, oid_t table_oid, const std::string& name,
    oid_t atttypid, std::int32_t attnum,
    bool not_null, bool has_default, bool is_dropped,
    const std::string& typspec, const std::string& defspec);

// Строит одну строку pg_index
vector::data_chunk_t build_pg_index_row(
    std::pmr::memory_resource* resource,
    oid_t index_oid, oid_t indrelid,
    const std::string& indkey, bool indisvalid);
```

### 4.2 Добавить `lv_i64` в `pg_row_builder.hpp`

**Проблема:** `lv_i64` определён только в `services/disk/manager_disk_impl.hpp`
(namespace `services::disk::detail`) — недоступен из `components/catalog/`.

`build_create_sequence_writes` требует `lv_i64` для полей `seqstart`, `seqincrement`,
`seqmin`, `seqmax`, `seqlast`.

**Решение:** добавить в `components/catalog/pg_row_builder.hpp`:

```cpp
inline components::types::logical_value_t
lv_i64(std::pmr::memory_resource* r, std::int64_t v) {
    return components::types::logical_value_t(r, v);
}
```

### 4.3 Реализовать `estimate_ddl_oid_count`

**Проблема:** сейчас возвращает `0` для всех узлов — `allocate_oids_batch` не вызывается.

```cpp
// services/dispatcher/ddl.cpp
std::size_t estimate_ddl_oid_count(const node_ptr& node) {
    switch (node->type()) {
        case node_type::create_database_t:   return 1;  // ns_oid
        case node_type::create_sequence_t:   return 1;  // seq_oid
        case node_type::create_view_t:       return 2;  // view_oid + rule_oid
        case node_type::create_macro_t:      return 2;  // macro_oid + rule_oid
        case node_type::create_constraint_t: return 1;  // constraint_oid
        case node_type::create_index_t:      return 1;  // index_oid (executor path)
        default:                             return 0;
    }
}
```

### 4.4 `allocate_oids_batch` в `executor.cpp`

**Проблема:** executor не вызывает `allocate_oids_batch` — сейчас `ddl_create_index`
делает это внутри disk-метода.

После миграции executor должен получить OID заранее:
```
oids = co_await disk.allocate_oids_batch(1)  // index_oid
writes = build_create_index_writes(..., oids[0], ...)
```

---

## Итоговая последовательность работ

1. **`ddl_metadata_builder`**: добавить 8 `build_*_writes()` функций + 2 row-builder'а
2. **`ddl.cpp`**: переписать 9 операций (`CREATE DATABASE`, `SEQUENCE`, `VIEW`, `MACRO`, `DROP SEQ/VIEW/MACRO`, `ADD/DROP/RENAME COLUMN`, `CREATE CONSTRAINT`)
3. **`dispatcher.cpp`**: переписать 4 операции (`CREATE/DROP TYPE`, `CREATE/DROP FUNCTION`)
4. **`executor.cpp`**: переписать 3 операции (`CREATE INDEX`, `DROP INDEX`, `index_set_valid`)
5. **`manager_disk.hpp` + `manager_disk_ddl.cpp`**: удалить 24 метода
6. **`estimate_ddl_oid_count`**: реализовать корректный подсчёт OID для каждого типа узла

---

## Что НЕ меняется

- `append_pg_catalog_row` — остаётся в disk (`disk_contract.hpp:164`)
- `delete_pg_catalog_rows` — остаётся в disk (`disk_contract.hpp:170`)
- `commit_pg_catalog_appends` + `rebuild_lookup_indexes` — остаются в disk (`disk_contract.hpp:155`)
- `allocate_oids_batch` — остаётся в disk (`disk_contract.hpp:161`)
- `resolve_*` методы — остаются в disk полностью
- `ddl_add_column`, `ddl_adopt_computing_schema`, `ddl_computed_append`, `ddl_computed_drop` — остаются в disk (упрощаются, но не удаляются)

---

## Статус верификации (2026-05-07)

Все утверждения документа проверены по исходным файлам. Статус:

| Раздел | Статус | Примечание |
|---|---|---|
| Часть 1: 24 метода на удаление | ПОДТВЕРЖДЕНО | Все присутствуют в обоих файлах |
| Часть 1: 4 метода остаются | ПОДТВЕРЖДЕНО | Все присутствуют в обоих файлах |
| Часть 2: `pg_row_builder.hpp` хелперы | ПОДТВЕРЖДЕНО | `lv_oid`, `lv_str`, `lv_i32`, `lv_bool`, `make_pg_row` — все есть |
| Часть 2: `lv_i64` отсутствует в catalog | ПОДТВЕРЖДЕНО | Только в `services::disk::detail` (manager_disk_impl.hpp) |
| Часть 2: `catalog_write_t` структура | ПОДТВЕРЖДЕНО | `{table, row}` |
| Часть 2: `oid_batch_t.allocate()` | ПОДТВЕРЖДЕНО | `oids[next++]` с assert |
| Часть 3: номера строк в `ddl.cpp` | ПОДТВЕРЖДЕНО | Все 13 символов — точные совпадения |
| Часть 3: номера строк в `dispatcher.cpp` | ПОДТВЕРЖДЕНО | Все 5 символов — точные совпадения |
| Часть 3: `build_drop_sequence` для `kPgSequence`/`kPgRewrite` | ПОДТВЕРЖДЕНО | Строки 67-68 в ddl.cpp |
| Часть 4: номера строк в `executor.cpp` | ПОДТВЕРЖДЕНО | ddl_create_index:214, set_valid:225, drop_index:271 |
| Часть 4: `allocate_oids_batch` отсутствует в executor | ПОДТВЕРЖДЕНО | Ни одного вызова |
| Часть 4: примитивы в `disk_contract.hpp` | ПОДТВЕРЖДЕНО | Строки 155/161/164/170 |
| Часть 4: `node_type` enum значения | ПОДТВЕРЖДЕНО | Все 6 значений в `logical_plan/forward.hpp` |
| Часть 4: методы `node_primitive_write/delete_t` | ПОДТВЕРЖДЕНО | write: `catalog_table()`, `row()`; delete: `catalog_table()`, `oid_col_idx()`, `target_oid()` |
| Исправление: счётчик методов | ИСПРАВЛЕНО | Было "23", стало "24" (включает `ddl_create_computing_table`) |