# Bug: CREATE TABLE — таблица невидима для следующего INSERT

**Ветка:** `docs/catalog-migration-postgresql-style`  
**Симптом:** после успешного `CREATE TABLE` следующий `INSERT` падает с ошибкой `collection does not exist`

---

## Воспроизведение

```sql
CREATE TABLE TestDatabase.test_table (id INTEGER, val TEXT);
INSERT INTO TestDatabase.test_table VALUES (1, 'hello');
-- ошибка: collection does not exist
```

6 интеграционных тестов падают по этой причине.

---

## Цепочка вызовов (как должно работать)

```
SQL
 └─► logical plan: create_collection_t
      └─► planner (DDL): sequence_t(create_collection_t, primitive_write×N)
           └─► physical plan generator: operator_create_collection_t
                └─► executor → await_async_and_resume
                     ├─► disk::create_storage_with_columns   (физическое хранилище)
                     ├─► index::register_collection           (индекс-менеджер)
                     ├─► disk::append_pg_catalog_row          (pg_class)
                     ├─► disk::append_pg_catalog_row          (pg_attribute × N)
                     └─► disk::append_pg_catalog_row          (pg_depend)
```

---

## Корневая причина

### Как `INSERT` находит таблицу

При обработке `INSERT` вызывается `disk::resolve_table`:

```cpp
// manager_disk_resolve.cpp:39
if (auto it = table_to_oid_.find(ns_table_key_t{namespace_oid, name});
    it != table_to_oid_.end()) {
    out.found = true;
    ...
}
```

`table_to_oid_` — это in-memory хэш-карта (namespace_oid, table_name) → (table_oid, relkind).  
Она **не читается с диска при каждом запросе** — это кэш, который нужно явно обновлять.

### Когда `table_to_oid_` обновляется

`table_to_oid_` заполняется только через `rebuild_lookup_indexes()`:

```
rebuild_lookup_indexes() вызывается из:
  ├─ manager_disk_bootstrap.cpp  — при старте / рестарте
  ├─ commit_pg_catalog_appends() — после коммита DDL-транзакции
  └─ ddl_create_table() и аналоги — старые DDL методы (удалены в рамках миграции)
```

### Где обрывается цепочка

`append_pg_catalog_row` с `txn_id == 0`:

```cpp
// manager_disk_ddl.cpp
const auto start_row = direct_append_sync(name, row, ctx.txn);
if (ctx.txn.transaction_id != 0 && count > 0) {        // ← txn_id=0: пропускаем
    pending_pg_catalog_appends_[txn_id].push_back(...);
}
// rebuild_lookup_indexes() здесь НЕ вызывается
```

`operator_create_collection_t` работает с `txn_id=0` (CREATE TABLE не входит в `needs_ddl_txn`).  
Строки пишутся в `pg_class` и `pg_attribute`, но `table_to_oid_` не обновляется.  
Следующий `resolve_table` не находит таблицу → `"collection does not exist"`.

---

## Затронутые файлы

| Файл | Роль |
|---|---|
| `services/disk/manager_disk_ddl.cpp` | `append_pg_catalog_row` — пишет строки, но не rebuild |
| `services/disk/manager_disk_resolve.cpp` | `resolve_table` — читает `table_to_oid_` |
| `services/disk/manager_disk_bootstrap.cpp` | `rebuild_lookup_indexes` — обновляет `table_to_oid_` |
| `components/physical_plan/operators/operator_create_collection.cpp` | оператор пишет строки через диск-актор |
| `services/dispatcher/dispatcher.cpp` | `needs_ddl_txn` не включает `create_collection_t` |

---

## Варианты исправления

### Вариант 1 — Rebuild в `append_pg_catalog_row` при `txn_id=0`

Добавить вызов `rebuild_lookup_indexes()` когда строка коммитится немедленно (txn=0):

```cpp
if (ctx.txn.transaction_id != 0 && count > 0) {
    pending_pg_catalog_appends_[txn_id].push_back(...);
} else {
    rebuild_lookup_indexes(); // txn=0: строка уже committed → обновить индекс
}
```

| | |
|---|---|
| ✓ | Нет новых методов, оператор не меняется |
| ✓ | Работает автоматически для любого вызывающего |
| ✗ | `rebuild_lookup_indexes()` вызывается N+2 раз (по одному на каждую строку pg_class + pg_attribute×N + pg_depend) — полный скан pg_class и pg_namespace при каждом вызове |
| ✗ | Побочные эффекты: другие вызывающие `append_pg_catalog_row` с txn=0 тоже получат rebuild |

---

### Вариант 2 — Убрать ранний выход в `commit_pg_catalog_appends` при `txn_id=0`

Изменить `commit_pg_catalog_appends` чтобы при `txn_id=0` он просто вызывал `rebuild_lookup_indexes`:

```cpp
if (txn_id == 0) {
    rebuild_lookup_indexes(); // ← вместо co_return
    co_return;
}
```

Оператор в конце вызывает `commit_pg_catalog_appends({session, {0,0}, {}}, 0)` через диск-актор.

| | |
|---|---|
| ✓ | Нет нового метода — переиспользуем существующий actor-message |
| ✓ | Один rebuild в конце (не N+2) |
| ✓ | Оператор явно контролирует момент обновления |
| ✗ | Изменяем семантику существующего метода (txn_id=0 раньше был no-op) |
| ✗ | Нужно убедиться что нет других вызывающих которые полагаются на no-op при txn=0 |

---

### Вариант 3 — Дать CREATE TABLE настоящий DDL txn

Добавить `create_collection_t` в `needs_ddl_txn` в dispatcher. Тогда:
- `txn_id != 0` → `append_pg_catalog_row` трекает строки в `pending_pg_catalog_appends_`
- После executor → dispatcher вызывает `commit_pg_catalog_appends(txn_id, commit_id)` → rebuild

| | |
|---|---|
| ✓ | Использует уже существующий механизм транзакций без изменения disk-методов |
| ✓ | Один rebuild, MVCC корректен, возможен rollback при сбое |
| ✓ | Единообразно с другими DDL (DROP TABLE, CREATE SEQUENCE и т.д.) |
| ✗ | Изменения в dispatcher: `needs_ddl_txn` + success-handler |
| ✗ | Commit логика (flush + WAL + txn_manager.commit) живёт в dispatcher, не в операторе |

---

## Рекомендация

**Вариант 2** — минимальное изменение, один rebuild, использует существующий actor-message, логика остаётся в операторе (executor → disk).  
**Вариант 3** — архитектурно правильнее (транзакционная атомарность), но требует изменений в dispatcher.