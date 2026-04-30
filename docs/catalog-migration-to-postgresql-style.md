# Catalog Migration to PostgreSQL-Style Architecture

## Table of Contents

1. [Overview](#1-overview)
2. [Current Architecture](#2-current-architecture)
3. [Target Architecture (B3: All-Async)](#3-target-architecture-b3-all-async-with-optimizations)
4. [Phase 0: OID System](#4-phase-0-oid-system)
5. [Phase 1: System Catalog Tables](#5-phase-1-system-catalog-tables)
6. [Phase 2: Versioned Plan Cache](#6-phase-2-versioned-plan-cache)
7. [Phase 3: Transactional DDL via MVCC](#7-phase-3-transactional-ddl-via-mvcc)
8. [Phase 4: Dependency Tracking (pg_depend)](#8-phase-4-dependency-tracking-pg_depend)
9. [Phase 5: Full Migration](#9-phase-5-full-migration)
10. [Query Validation and Resolution Path](#10-query-validation-and-resolution-path)
11. [Error Handling](#11-error-handling)
12. [End-to-End Flow](#12-end-to-end-flow)
13. [Risk Registry](#13-risk-registry)
14. [Complete Test Plan (139 tests)](#14-complete-test-plan)

---

## 1. Overview

This document describes the migration of the otterbrix catalog system from custom
in-memory data structures (versioned tries, hash maps) to a PostgreSQL-style
architecture where metadata is stored in system tables, queried with MVCC, and
cached in a syscache layer.

### Goals

- Transactional DDL (CREATE/ALTER/DROP are MVCC transactions)
- Self-describing catalog (system tables describe themselves)
- Uniform OID-based identification for all database objects
- Dependency tracking (CASCADE/RESTRICT)
- Schema snapshot isolation (queries see schema as-of their start_time)
- Persistent types, functions, constraints (currently lost on restart)

### Non-Goals

- Multi-process shared memory (otterbrix is single-process, actor-based)
- Full PostgreSQL wire protocol compatibility
- Information schema views (future work)

---

## 2. Current Architecture

### Catalog Component (`components/catalog/`)

```
catalog (entry point)
    |
    +-- namespace_storage
    |       |
    |       +-- versioned_trie<table_namespace_t, namespace_info>
    |       |       namespace_info {
    |       |           map<string, table_metadata> tables;
    |       |           map<string, computed_schema> computing;
    |       |       }
    |       |
    |       +-- unordered_map<string, complex_logical_type> registered_types_
    |       +-- unordered_map<string, vector<registered_func_id>> registered_functions_
    |
    +-- transaction_list (active DDL transactions)
    +-- transaction system:
            transaction_scope -> metadata_transaction -> schema_diff / metadata_diff
```

### Identification

| Entity     | Current ID Type                    | Persistence |
|------------|------------------------------------|-------------|
| Table      | `table_id` (string namespace+name) | Name-based  |
| Namespace  | `table_namespace_t` (string vector)| Name-based  |
| Column     | `field_id_t` (uint64, sequential)  | In schema   |
| Type       | string alias                       | Not persisted |
| Function   | `function_uid` (size_t, auto-inc)  | Not persisted |
| Block      | uint64_t                           | Sequential  |
| Row        | int64_t (positional)               | Positional  |

### Persistence

- `catalog.otbx` binary file stores tables, sequences, views, macros per database
- Custom serialization via `catalog_storage_t` (MAGIC=0x5842544F, VERSION=3)
- `indexes_METADATA` msgpack file stores index definitions (name, type, keys, collection)
- Index data stored in per-index B-tree directories: `<db>/<collection>/<index_name>/`
- Types and functions are NOT persisted (lost on restart)
- Constraints are NOT fully persisted (only PRIMARY KEY columns; CHECK, UNIQUE, FK, DEFAULT lost)
- Sequences, views, macros are loaded but not registered in catalog

### What Gets Replaced

All legacy persistence formats are replaced by system catalog tables:

| Legacy Format | Replaced By | Status |
|---|---|---|
| `catalog.otbx` (binary, `catalog_storage_t`) | pg_class + pg_attribute + pg_type + pg_proc | Full replacement |
| `indexes_METADATA` (msgpack, `manager_index_t`) | pg_class (relkind='i') + pg_index | Full replacement |
| `catalog_table_entry_t.primary_key_columns` | pg_constraint (contype='p') | Full replacement |
| `node_create_index_t` serialization | pg_index rows | Full replacement |

### Callers

| Component                        | Usage                                        |
|----------------------------------|----------------------------------------------|
| `services/dispatcher`            | Primary consumer: DDL, DML validation, type/function resolution |
| `services/dispatcher/validate_logical_plan` | Schema validation, function resolution |
| `integration/cpp/base_spaces`    | Catalog initialization from disk              |
| `services/disk`                  | Catalog entry persistence                     |
| `components/planner`             | Unused catalog parameters (legacy)            |

---

## 3. Target Architecture (B3: All-Async with Optimizations)

### Design Decisions

All architectural decisions were evaluated and chosen based on two criteria:
**correctness for framework consumers** and **performance**.

Complete decision registry:

| Label | Decision | Section |
|---|---|---|
| B3 | All-async actor messages, plan cache in dispatcher | Section 3 |
| D4 | Eager catalog (9 system tables at startup) + lazy user storage | Section 5 |
| W4 | DDL logged in WAL, batch commit, checkpoint separate | Section 5 |
| V4 | Coroutine lazy resolve — per-item co_await on cache miss | Section 5, 10 |
| S5 | Batch resolve as additional method (IN filter) | Section 5 |
| O4 | manager_disk_t allocates all OIDs | Section 5 |
| C1 | pg_computed_column — 9th system table for computing tables | Section 5 |
| M0 | No migration — clean break from old format | Section 9 |
| — | Per-object invalidation events (Decision 1 below) | Section 3 |
| — | Pull-based ring buffer for multi-dispatcher (Decision 2 below) | Section 3 |
| — | indisvalid column in pg_index (Decision 3 below) | Section 3 |
| — | Per-transaction versioned cache with alias dedup + lazy GC (Decision 4 below) | Section 3, 6 |

#### Decision 1: Catalog Version Tracking — Per-Object Invalidation Events

DDL operations return a list of per-object invalidation events (oid + object type).
No global version counter — fine-grained invalidation avoids false cache resets.
CREATE TYPE does not invalidate table cache. DROP TABLE does not invalidate function cache.

#### Decision 2: Multi-Dispatcher Coherency — Pull-Based Ring Buffer

manager_disk_t maintains a ring buffer of invalidation events with monotonic version.
Dispatchers request missed events via resolve_catalog(last_seen_version).
No broadcast, no fan-out, no dispatcher registration. Fully compatible with actor-zeta
(no weak refs, no multicast needed). If dispatcher falls behind ring buffer capacity —
full cache reset (same as PostgreSQL sinval overflow).

#### Decision 3: Index Metadata — indisvalid Column in pg_index

CREATE INDEX: INSERT pg_index with indisvalid=false, build btree, UPDATE indisvalid=true.
Planner only uses indexes where indisvalid=true. DML writes to index during build
via indisready flag. Proven PostgreSQL approach. No window of inconsistency between
pg_index and manager_index_t. Crash recovery: startup cleans up indisvalid=false entries.

Index metadata (which columns are indexed) comes from pg_index via resolve_catalog.
Index runtime operations (search, insert_rows) remain in manager_index_t.

#### Decision 4: Snapshot Isolation — Versioned Cache in Dispatcher

Dispatcher stores multiple versions of resolved catalog data, keyed by catalog_version.
begin_transaction() captures current catalog_version (0 roundtrips).
execute_plan() uses cached version for that transaction (0 roundtrips on hit).
Older versions retained while transactions using them are active.
GC: remove versions where version < min(active_transaction_versions).
Does NOT depend on Phase 3 MVCC DDL — works from Phase 1.

### Access Paths

```
SQL PATH (user queries to system tables):
  SELECT * FROM pg_class WHERE relname = 'users'
    -> parser -> planner -> executor
    -> full_scan operator -> actor_zeta::send(ctx->disk_address, storage_scan)
    -> manager_disk_t -> data_table_t::scan()
    (identical to user table scan)

INTERNAL PATH (V4: coroutine lazy resolve, PostgreSQL-style):
  dispatcher::execute_plan():
    co_await validate_types(cache, disk_address, plan)
    co_await validate_schema(cache, disk_address, plan, params)
      |
      |  Inside validate (coroutine):
      |    encounter table "users":
      |      cache hit?  -> use cached schema              -- 0 roundtrips
      |      cache miss? -> co_await send(disk, resolve_table, "users")
      |                     add to cache, use result        -- 1 roundtrip
      |    encounter function ">":
      |      cache hit?  -> use cached function             -- 0 roundtrips
      |      cache miss? -> co_await send(disk, resolve_function, ">", args)
      |                     add to cache, use result        -- 1 roundtrip
      |
      |  No collect_dependencies phase — resolve on demand as AST is walked.
      |  Cache fills incrementally — warm cache = 0 roundtrips.
```

### Component Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  DISPATCHER ACTOR                                                │
│                                                                   │
│  cache_versions_ = {                                              │
│      version_42: { resolved schemas, functions, types },          │
│      version_43: { resolved schemas, functions, types },          │
│  }                                                                │
│  active_txns_ = { txn_A: version_42, txn_B: version_43 }        │
│  last_seen_version_ = 43                                          │
│                                                                   │
│  begin_transaction():                                             │
│      txn.catalog_version = last_seen_version_  -- 0 roundtrips    │
│                                                                   │
│  execute_plan():                                                  │
│      co_await validate_types(cache, disk_address, plan)           │
│      co_await validate_schema(cache, disk_address, plan, params)  │
│        encounter table → cache hit? sync. miss? co_await resolve  │
│        encounter function → cache hit? sync. miss? co_await resolve│
│        No pre-collect. Resolve on demand as AST walked.           │
│        Cache fills incrementally. Warm = 0 roundtrips.            │
│                                                                   │
│  commit/abort_transaction():                                      │
│      GC: remove cache_versions_ < min(active_txns_.values())     │
│                                                                   │
│  DDL flow (DuckDB-style: DDL through executor, same as DML):      │
│      dispatcher creates logical plan for DDL                      │
│      co_await send(executor, execute_ddl_plan, plan)              │
│        executor: co_await send(disk, ddl_create_table, ...)       │
│        executor: co_await send(wal, write_physical_insert, ...)   │
│        executor: co_await send(wal, commit_txn, txn_id)           │
│        executor: co_await send(disk, create_storage, ...)         │
│        executor: co_await send(index, register_collection, ...)   │
│      response has: result + events + new_version                  │
│      process_events(response.events)                              │
│      last_seen_version_ = response.new_version                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │ actor messages (co_await)
┌───────────────────────────▼─────────────────────────────────────┐
│  EXECUTOR ACTOR (unified DDL + DML pipeline)                     │
│                                                                   │
│  DDL:  send(disk, ddl_*) + send(wal, write_*) + send(wal, commit)│
│  DML:  send(disk, storage_*) + send(wal, write_*) + send(wal, commit)│
│  Same WAL writer, same txn_manager, same abort/commit path       │
│  CREATE INDEX: already owned by executor (no change from current)│
└───────────────────────────┬─────────────────────────────────────┘
                            │ actor messages
┌───────────────────────────▼─────────────────────────────────────┐
│  MANAGER_DISK_T ACTOR                                            │
│                                                                   │
│  invalidation_ring_buffer_ (fixed size, monotonic version)       │
│      [v=40: {TABLE_DROPPED, oid=100}]                            │
│      [v=41: {TYPE_CREATED, oid=200}]                             │
│      [v=42: {TABLE_CREATED, oid=300}, {ATTR_CREATED, oid=301}]  │
│      [v=43: {INDEX_CREATED, oid=400}]                            │
│      ...                                                          │
│                                                                   │
│  Per-item resolve (V4): resolve_table, resolve_function, etc.    │
│  Batch resolve (S5): resolve_tables_batch (additional)            │
│  All scans synchronous inside handler                             │
│                                                                   │
│  DDL methods (called by executor, NOT dispatcher):                │
│  ddl_create_table():                                              │
│      INSERT pg_class, pg_attribute, pg_depend, pg_constraint     │
│      append events to ring_buffer_                                │
│      return { result, events, new_version }                      │
│      NOTE: does NOT write WAL — executor writes WAL separately   │
│                                                                   │
│  ddl_create_index():                                              │
│      INSERT pg_class (relkind='i'), pg_index (indisvalid=false)  │
│      append events to ring_buffer_                                │
│      return { result, events, new_version }                      │
│                                                                   │
│  ddl_index_set_valid(index_oid):                                  │
│      UPDATE pg_index SET indisvalid=true WHERE indexrelid=oid    │
│      append event to ring_buffer_                                 │
│      return { result, events, new_version }                      │
│                                                                   │
│  storages_:                                                       │
│      --- ALWAYS LOADED (eager, at startup via _sync()) ---        │
│      "pg_catalog.pg_namespace"   = data_table_t                  │
│      "pg_catalog.pg_class"       = data_table_t                  │
│      "pg_catalog.pg_attribute"   = data_table_t                  │
│      "pg_catalog.pg_type"        = data_table_t                  │
│      "pg_catalog.pg_proc"        = data_table_t                  │
│      "pg_catalog.pg_depend"      = data_table_t                  │
│      "pg_catalog.pg_constraint"  = data_table_t                  │
│      "pg_catalog.pg_index"       = data_table_t                  │
│      "pg_catalog.pg_computed_column" = data_table_t              │
│      --- LOADED ON DEMAND (lazy, on first access) ---             │
│      "mydb.users"                = data_table_t  (or absent)     │
│      "mydb.orders"               = data_table_t  (or absent)     │
└─────────────────────────────────────────────────────────────────┘
```

### CREATE INDEX Flow (indisvalid, through executor)

```
dispatcher -> send(executor, execute_ddl_plan, create_index_plan)

  executor:
    Step 1: co_await send(disk, ddl_create_index)
            INSERT pg_class (relkind='i'), pg_index (indisvalid=false, indisready=true)
            INSERT pg_depend (index→table, deptype='a')

    Step 2: co_await send(wal, write_physical_insert, "pg_catalog.pg_class", ...)
            co_await send(wal, write_physical_insert, "pg_catalog.pg_index", ...)
            co_await send(wal, commit_txn, txn_id)

    Step 3: co_await send(index, build_btree, table_oid, keys)
            DML concurrent with build writes to index (indisready=true)

    Step 4: co_await send(disk, ddl_index_set_valid, index_oid)
            UPDATE pg_index SET indisvalid=true

    Step 5: co_await send(wal, write_physical_insert, "pg_catalog.pg_index", new_row, ...)
            co_await send(wal, commit_txn, txn_id)

dispatcher: process_events → cache invalidated
```

### Bootstrap (D4: Eager Catalog + Lazy Storage)

Inspired by both PostgreSQL (nailed relations + relcache on demand) and DuckDB
(eager catalog entries + lazy DataTable). System catalog tables are loaded eagerly
at startup (small, fixed set). User table storage is created on demand at first access.

#### Startup Sequence

```
base_spaces.cpp (constructor):

  PHASE 1: Load catalog from disk (sync, before actors)
      // Same as current code — read catalog.otbx, WAL, index defs

  PHASE 2: Spawn actors (sync)
      manager_disk_ = spawn<manager_disk_t>(...)
      manager_dispatcher_ = spawn<manager_dispatcher_t>(...)
      manager_index_ = spawn<manager_index_t>(...)

  PHASE 2.5: Bootstrap system catalog (sync, _sync() pattern)
      if (first_run):
          disk_ptr->bootstrap_system_tables_sync()
          //   Create 9 data_table_t from hardcoded schemas
          //   Insert bootstrap rows (namespaces, self-describing, types)
          //   storages_["pg_catalog.pg_class"] = data_table_t
          //   storages_["pg_catalog.pg_attribute"] = data_table_t
          //   ... (9 tables total)
          //   Cost: <10ms always (hardcoded schema, ~100 rows)
      else (restart):
          disk_ptr->load_system_tables_sync()
          //   load_from_disk() for 9 system tables from checkpoint
          //   Metadata + row group pointers only (data lazy via buffer_manager)
          //   Cost: <10ms (9 tables, metadata only)
      // NOTE: user table storages NOT created here

  PHASE 3: Sync addresses + WAL replay (sync, as current code)
      manager_dispatcher_->sync(disk_addr, index_addr, wal_addr)
      // WAL replay via direct_append_sync() etc.

  PHASE 4: Start schedulers (async begins)
      scheduler_->start()

  PHASE 5: Index recreation (async, via wrapper, as current code)
```

#### On-Demand User Table Loading (V4: Coroutine Lazy Resolve)

```
First SELECT name, age FROM users WHERE age > 25:

  dispatcher::execute_plan():
    co_await validate_types(cache, disk_address, plan):
      encounter "users":
        cache miss → co_await send(disk, resolve_table, "mydb.users")
          manager_disk_t::resolve_table():
            scan pg_class WHERE relname='users' → oid=16400, relkind='r'
            scan pg_attribute WHERE attrelid=16400 → columns
            scan pg_index WHERE indrelid=16400 AND indisvalid=true → indexes
            storages_.contains("mydb.users")? NO → load_from_disk (D4 lazy)
            return relation_info_t {oid, schema, indexes}
        cache.add("mydb.users", result)

    co_await validate_schema(cache, disk_address, plan):
      "users" → cache HIT (just added above)
      "name" → found in cached schema
      "age" → found in cached schema
      ">" operator → built-in, no catalog lookup
      → validation OK

Second SELECT * FROM users:
  validate_types: "users" → cache HIT → 0 roundtrips
  validate_schema: all from cache → 0 roundtrips
```

#### DDL on Unloaded Tables

All DDL through executor (Variant E), even for unloaded tables.

```
DROP TABLE users (never accessed, not in storages_):
  dispatcher -> send(executor, execute_ddl_plan, drop_plan)
    executor:
      co_await send(disk, ddl_drop_table, oid=16400)
        disk: DELETE from pg_class, pg_attribute, pg_depend, pg_constraint, pg_index
              storages_ does NOT contain "mydb.users" — nothing to remove
              return { result, events }
      co_await send(wal, write_physical_delete, ..., txn_id)
      co_await send(wal, commit_txn, txn_id)
      // No drop_storage needed — table was never loaded

ALTER TABLE users ADD COLUMN (never accessed):
  dispatcher -> send(executor, execute_ddl_plan, alter_plan)
    executor:
      co_await send(disk, ddl_add_column, oid=16400, column_def)
        disk: INSERT pg_attribute row
              return { result, events }
      co_await send(wal, write_physical_insert, ..., txn_id)
      co_await send(wal, commit_txn, txn_id)
      // storage not loaded — OK, schema change is in pg_attribute
    // When users is first accessed, load_from_disk will see updated schema
    return { result, invalidation_events }
```

#### Key Properties

- System tables: 9 data_table_t, **always in storages_** (loaded at startup via _sync())
- User tables: **created in storages_ on first access** (lazy)
- pg_class = single source of truth for ALL tables (system + user)
- SHOW TABLES = scan pg_class (does not depend on storages_ map)
- DDL on unloaded table = modify pg_class/pg_attribute only (no storage needed)
- disk actor is single-threaded: on-demand create is serialized (no race conditions)
- load_from_disk() reads metadata + row group pointers; data blocks are lazy via buffer_manager
- Startup time: <10ms regardless of user table count

### Async Roundtrip Budget

| Operation | co_await count | Notes |
|---|---|---|
| SELECT (cache hit, no DDL) | 0 | cache_versions_ lookup |
| SELECT (cache miss, table loaded) | 1 | V4 co_await resolve_table |
| SELECT (cache miss, table NOT loaded) | 1 | resolve_table + D4 on-demand load inside disk |
| CREATE TABLE | 5 | executor: ddl_create + wal_inserts + wal_commit + create_storage + register_index |
| DROP TABLE (loaded) | 5 | executor: ddl_drop + wal_deletes + wal_commit + drop_storage + unregister_index |
| DROP TABLE (not loaded) | 3 | executor: ddl_drop + wal_deletes + wal_commit |
| CREATE INDEX | 4 | executor: ddl_create_index + build_btree + ddl_set_valid + wal |
| ALTER TABLE ADD COLUMN | 3 | executor: ddl_add_column + wal_insert + wal_commit |
| SELECT * FROM pg_class (SQL) | 1 | Normal executor path: storage_scan |

All DDL goes through executor (same actor that handles DML).
Executor writes WAL for DDL — same write_physical_insert/delete + commit_txn pattern as DML.

### Current vs Target Comparison

| Aspect              | Current                          | Target (B3)                      |
|---------------------|----------------------------------|----------------------------------|
| Metadata storage    | Custom in-memory structures      | data_table_t system tables in manager_disk_t |
| Object identification | String paths                  | OID (uint32/uint64)              |
| DDL transactions    | Separate system (schema_diff)    | Same MVCC as data tables         |
| Dependencies        | None                             | pg_depend graph                  |
| Cache               | N/A (all in memory as catalog_)  | Versioned cache in dispatcher + ring buffer invalidation |
| Catalog access      | Sync (catalog_ field in dispatcher) | Async actor messages with plan_cache optimization |
| Self-describing     | No                               | Yes (pg_class describes itself)  |
| Type persistence    | Lost on restart                  | pg_type table                    |
| Function persistence| Lost on restart                  | pg_proc table                    |
| Constraint persistence | Lost on restart               | pg_constraint table              |
| Index metadata      | indexes_METADATA file            | pg_index + pg_class (relkind='i') with indisvalid |
| Multi-dispatcher    | N/A (single dispatcher)          | Pull-based ring buffer (eventual consistency) |
| Snapshot isolation   | None                            | Per-transaction versioned cache  |

---

## 4. Phase 0: OID System

### Overview

Introduce a global OID system as the foundation for cross-referencing between
system catalog tables.

### OID Type Definition

```cpp
// New file: components/catalog/catalog_oids.hpp

using oid_t = uint32_t;
constexpr oid_t INVALID_OID = 0;
constexpr oid_t FIRST_USER_OID = 16384;  // matches PostgreSQL

class oid_generator {
    std::atomic<oid_t> next_oid_{FIRST_USER_OID};
public:
    oid_t next() { return next_oid_.fetch_add(1); }
    void set_next(oid_t v) { next_oid_.store(v); }
};
```

### Well-Known OIDs

| OID    | Object                  |
|--------|-------------------------|
| 1      | pg_catalog namespace    |
| 2      | public namespace        |
| 3      | information_schema      |
| 10     | pg_namespace table      |
| 11     | pg_class table          |
| 12     | pg_attribute table      |
| 13     | pg_type table           |
| 14     | pg_proc table           |
| 15     | pg_depend table         |
| 16     | pg_constraint table     |
| 17     | pg_index table          |
| 20     | bool type               |
| 21     | int2 type               |
| 23     | int4 type               |
| 25     | text type               |
| 26     | int8 type               |
| 701    | float8 type             |
| 101-105| DEFAULT_FUNCTIONS (sum, min, max, count, avg) |

### Files to Change

| File | Change | Priority |
|------|--------|----------|
| **NEW** `catalog/catalog_oids.hpp` | OID type definitions, oid_generator, well-known constants | P0 |
| `catalog/table_id.hpp/cpp` | Add `oid_t oid_` field, getter/setter, update hash | P1 |
| `catalog/schema.hpp/cpp` | Add `vector<oid_t> column_oids_`, lookup by OID | P1 |
| `catalog/table_metadata.hpp/cpp` | Add `oid_t table_oid_`, `oid_t next_column_oid_` | P1 |
| `catalog/namespace_storage.hpp/cpp` | Add `unordered_map<oid_t, table_id>` index | P1 |
| `catalog/catalog.hpp/cpp` | Add `get_table_by_oid()`, OID generation in create_table | P1 |
| `table/column_definition.hpp/cpp` | Type existing `oid_`/`storage_oid_` via catalog_oids | P1 |
| `table/data_table.hpp/cpp` | Accept OIDs from schema, not generate internally | P1 |
| `catalog/transaction/schema_diff.hpp/cpp` | Generate OID for new columns, never reuse | P1 |
| `disk/catalog_storage.hpp/cpp` | Add OID to entry types, update serialization | P1 |
| `table/storage/metadata_writer.hpp` | Serialize column OIDs in checkpoint | P1 |
| `table/storage/metadata_reader.hpp` | Deserialize column OIDs on load | P1 |
| `integration/base_spaces.cpp` | Assign loaded OIDs to catalog objects | P1 |
| `types/types.hpp` | Add optional `oid_t type_oid_` to complex_logical_type | P2 |
| `compute/function.hpp` | Map function_uid to oid_t | P2 |
| `logical_plan/node_create_collection.hpp` | Track OID assignments | P2 |

### Design Rules

1. OIDs are **immutable** after assignment (set once, error on double-set)
2. OIDs are **never reused** after DROP (gaps are acceptable)
3. `field_id_t` remains for schema-relative column positions; `oid_t` is global
4. OID generation must be **atomic** (thread-safe counter)
5. On restart, `oid_generator.set_next(max_oid_from_all_tables + 1)`

### Tests

```
test_oid_generation_uniqueness       -- 1000 OIDs without collision
test_oid_immutability                -- double set_oid raises error
test_table_oid_assignment            -- create_table assigns OID
test_column_oid_assignment           -- columns get unique OIDs
test_oid_persistence                 -- checkpoint -> load -> same OID
test_oid_no_reuse_after_drop         -- dropped OID never reassigned
test_schema_evolution_oid_stability  -- rename column preserves OID
test_get_table_by_oid                -- lookup by OID finds correct table
test_oid_generator_restore           -- generator starts at max+1 after load
```

---

## 5. Phase 1: System Catalog Tables

### Overview

Create system catalog tables as `data_table_t` instances using the existing
columnar storage engine. These tables store all metadata and are self-describing.

### System Table Schemas

#### pg_namespace

| Column   | Type           | NOT NULL | Description       |
|----------|----------------|----------|-------------------|
| oid      | BIGINT         | yes      | Namespace OID     |
| nspname  | STRING_LITERAL | yes      | Namespace name    |
| nspowner | BIGINT         | yes      | Owner OID         |

#### pg_class

| Column       | Type           | NOT NULL | Description                        |
|--------------|----------------|----------|------------------------------------|
| oid          | BIGINT         | yes      | Relation OID                       |
| relname      | STRING_LITERAL | yes      | Relation name                      |
| relnamespace | BIGINT         | yes      | Namespace OID (FK pg_namespace)    |
| relkind      | STRING_LITERAL | yes      | r=table, c=computing, i=index, v=view, S=sequence |
| reltuples    | DOUBLE         | yes      | Estimated row count                |
| relpages     | BIGINT         | yes      | Estimated page count               |
| reltype      | BIGINT         | yes      | Associated composite type OID      |

#### pg_attribute

| Column        | Type           | NOT NULL | Description                    |
|---------------|----------------|----------|--------------------------------|
| attrelid      | BIGINT         | yes      | Owning relation OID            |
| attname       | STRING_LITERAL | yes      | Column name                    |
| atttypid      | BIGINT         | yes      | Type OID (FK pg_type)          |
| attnum        | BIGINT         | yes      | Column number (1-based, immutable) |
| attnotnull    | BOOLEAN        | yes      | NOT NULL constraint            |
| attstattarget | BIGINT         | yes      | Statistics target              |
| attdefval     | STRING_LITERAL | no       | Default value expression       |

#### pg_type

| Column       | Type           | NOT NULL | Description                     |
|--------------|----------------|----------|---------------------------------|
| oid          | BIGINT         | yes      | Type OID                        |
| typname      | STRING_LITERAL | yes      | Type name                       |
| typnamespace | BIGINT         | yes      | Namespace OID                   |
| typlen       | BIGINT         | yes      | Size in bytes (-1=variable)     |
| typbyval     | BOOLEAN        | yes      | Pass by value                   |
| typtype      | STRING_LITERAL | yes      | b=base, c=composite, e=enum     |

#### pg_proc

| Column       | Type           | NOT NULL | Description                     |
|--------------|----------------|----------|---------------------------------|
| oid          | BIGINT         | yes      | Function OID                    |
| proname      | STRING_LITERAL | yes      | Function name                   |
| pronamespace | BIGINT         | yes      | Namespace OID                   |
| proowner     | BIGINT         | yes      | Owner OID                       |
| prorettype   | BIGINT         | yes      | Return type OID                 |
| proargtypes  | STRING_LITERAL | no       | Comma-separated argument type OIDs |

#### pg_depend

| Column     | Type           | NOT NULL | Description                     |
|------------|----------------|----------|---------------------------------|
| classid    | BIGINT         | yes      | Catalog OID of dependent class  |
| objid      | BIGINT         | yes      | OID of dependent object         |
| objsubid   | BIGINT         | yes      | Sub-object (e.g. column attnum, 0 = whole object) |
| refclassid | BIGINT         | yes      | Catalog OID of referenced class |
| refobjid   | BIGINT         | yes      | OID of referenced object        |
| refobjsubid| BIGINT         | yes      | Sub-object of referenced object |
| deptype    | STRING_LITERAL | yes      | n=normal, a=auto, i=internal    |

This is a NEW table -- no legacy format to migrate from. Currently otterbrix
has no dependency tracking; all DROP operations are unconditional. pg_depend
must be populated on every CREATE that establishes a reference:

| Operation | Dependency Created |
|---|---|
| CREATE TABLE t (col custom_type) | (pg_class, t_oid, col_attnum) -> (pg_type, type_oid, 0, 'n') |
| CREATE INDEX idx ON t(col) | (pg_class, idx_oid, 0) -> (pg_class, t_oid, 0, 'a') |
| ALTER TABLE ADD CONSTRAINT fk | (pg_constraint, fk_oid, 0) -> (pg_class, ref_table_oid, 0, 'n') |
| CREATE TABLE in namespace ns | (pg_class, t_oid, 0) -> (pg_namespace, ns_oid, 0, 'n') |

#### pg_constraint

| Column   | Type           | NOT NULL | Description                     |
|----------|----------------|----------|---------------------------------|
| oid      | BIGINT         | yes      | Constraint OID                  |
| conname  | STRING_LITERAL | yes      | Constraint name                 |
| conrelid | BIGINT         | yes      | Table OID (FK pg_class)         |
| contype  | STRING_LITERAL | yes      | p=PK, u=unique, c=check, f=FK  |
| conindid | BIGINT         | no       | Supporting index OID            |
| conkey   | STRING_LITERAL | no       | Comma-separated attnum list     |
| conexpr  | STRING_LITERAL | no       | CHECK expression (raw SQL text) |

Replaces: `catalog_table_entry_t.primary_key_columns` and `table_constraint_t`.

**Migration from current format:**

| Current | New (pg_constraint) |
|---|---|
| `catalog_table_entry_t.primary_key_columns` (vector<string>) | Row with `contype='p'`, `conkey` = attnum list |
| `table_constraint_t.type` (PK/UNIQUE/CHECK) | `contype` column |
| `table_constraint_t.columns` (vector<string>) | `conkey` (attnum references) |
| `table_constraint_t.check_expression` (SQL text) | `conexpr` column |

#### pg_index

| Column       | Type           | NOT NULL | Description                     |
|--------------|----------------|----------|---------------------------------|
| indexrelid   | BIGINT         | yes      | Index OID (FK pg_class with relkind='i') |
| indrelid     | BIGINT         | yes      | Table OID (FK pg_class)         |
| indisprimary | BOOLEAN        | yes      | Is primary key                  |
| indisunique  | BOOLEAN        | yes      | Is unique index                 |
| indtype      | STRING_LITERAL | yes      | Index type: single, composite, multikey, hashed, wildcard |
| indkey       | STRING_LITERAL | yes      | Comma-separated attnum list of indexed columns |

Replaces: `indexes_METADATA` msgpack file and `node_create_index_t` serialization.

Each index also gets a `pg_class` row with `relkind='i'` for its name and OID.

**Migration from current format:**

| Current (`node_create_index_t`) | New (pg_index + pg_class) |
|---|---|
| `name_` (string) | `pg_class.relname` |
| `collection_` (db.table) | `pg_index.indrelid` (table OID from pg_class) |
| `index_type_` (enum: single/composite/multikey/hashed/wildcard) | `pg_index.indtype` |
| `keys_` (vector of key_t with hierarchical paths) | `pg_index.indkey` (attnum references) |
| File path `<db>/<collection>/<index>/` | `pg_class.relfilenode` (future, or derived from OID) |

#### pg_computed_column (otterbrix-specific, for computing tables)

Computing tables (relkind='c') use versioned, reference-counted fields instead
of fixed schema columns. This is an otterbrix-specific extension — PostgreSQL
has no equivalent. pg_attribute stores columns for regular tables (relkind='r'),
pg_computed_column stores versioned fields for computing tables (relkind='c').

| Column     | Type           | NOT NULL | Description                     |
|------------|----------------|----------|---------------------------------|
| attrelid   | BIGINT         | yes      | Computing table OID (FK pg_class, relkind='c') |
| attname    | STRING_LITERAL | yes      | Field name                      |
| atttypid   | BIGINT         | yes      | Type OID (FK pg_type)           |
| attversion | BIGINT         | yes      | Version number (monotonic per field name) |
| attrefcount| BIGINT         | yes      | Reference count (0 = dead version) |

Multiple rows can share the same (attrelid, attname) — each with different
atttypid and attversion. This models the versioned_trie from computed_schema.

Operations:
- `append(field, type)`: INSERT row (attversion = max existing + 1, attrefcount = 1)
  or UPDATE attrefcount + 1 if same type exists
- `drop(field)`: UPDATE attrefcount - 1. DELETE row if attrefcount reaches 0
- `latest_types_struct()`: SELECT attname, atttypid WHERE attrefcount > 0
  GROUP BY attname HAVING attversion = max(attversion)

Transition computing → regular:
  1. SELECT latest_types_struct() from pg_computed_column
  2. INSERT rows into pg_attribute (one per field, fixed schema)
  3. DELETE all rows from pg_computed_column WHERE attrelid = table_oid
  4. UPDATE pg_class SET relkind = 'r'

DDL methods for computing tables:
```cpp
unique_future<ddl_result_t> ddl_create_computing_table(
    session_id_t session,
    oid_t namespace_oid,
    std::pmr::string table_name);
// INSERT pg_class (relkind='c'), no pg_attribute rows, no pg_computed_column rows

unique_future<ddl_result_t> ddl_computed_append(
    session_id_t session,
    oid_t table_oid,
    std::pmr::string field_name,
    oid_t type_oid);
// INSERT or UPDATE pg_computed_column

unique_future<ddl_result_t> ddl_computed_drop(
    session_id_t session,
    oid_t table_oid,
    std::pmr::string field_name);
// UPDATE attrefcount - 1, DELETE if 0

unique_future<ddl_result_t> ddl_adopt_computing_schema(
    session_id_t session,
    oid_t table_oid);
// Transition: pg_computed_column → pg_attribute, relkind='c' → 'r'
```

### Bootstrap Sequence (D4: Eager Catalog + Lazy Storage)

System tables use `single_file_block_manager_t` (disk-backed, checkpointable).
Bootstrap rows use `transaction_data{0, 0}` (committed at time 0).
User table storages are NOT created at startup — loaded on demand at first access.

```
First run (no checkpoint files exist):
  Step 1: Create 9 data_table_t with hardcoded schemas (disk-backed)
  Step 2: INSERT pg_namespace: (1, "pg_catalog"), (2, "public"), (3, "information_schema")
  Step 3: INSERT pg_class: 9 rows (one per system table, self-describing)
  Step 4: INSERT pg_attribute: ~60 rows (columns of all 9 tables)
  Step 5: INSERT pg_type: built-in types (bool=20, int2=21, int4=23, int8=26, text=25, float8=701)
  Step 6: pg_proc: DEFAULT_FUNCTIONS (sum, min, max, count, avg)
  Step 7: pg_depend, pg_constraint, pg_index, pg_computed_column: empty initially
  Step 8: Checkpoint all system tables + fsync
  Cost: <10ms

Restart (checkpoint files exist):
  Step 1: load_from_disk() for 9 system tables (metadata + row group pointers)
  Step 2: Replay WAL entries for system tables (see WAL Integration)
  Cost: <10ms (metadata only, data blocks lazy via buffer_manager)

User tables:
  NOT loaded at startup.
  pg_class contains metadata for all user tables.
  Storage (data_table_t) created on first access via resolve_catalog.
  DDL on unloaded tables: modify pg_class/pg_attribute only (no storage needed).
```

Self-description: pg_class contains a row for pg_class itself. This is data, not
schema -- no circular dependency exists at the structural level.

### WAL Integration for System Tables (W4: WAL + Batch Commit)

DDL operations on system tables are WAL-logged, closing the current gap where
otterbrix does not log DDL in WAL. This follows the PostgreSQL and DuckDB approach.

#### Design

All INSERT/DELETE/UPDATE on system tables go through WAL before being considered
durable. DDL transactions can batch multiple operations with a single WAL commit.

```
Single DDL (auto-commit):
  CREATE TABLE users (id INT, name VARCHAR)
    -> INSERT pg_class row          -> WAL: PHYSICAL_INSERT("pg_class", row)
    -> INSERT pg_attribute row (id) -> WAL: PHYSICAL_INSERT("pg_attribute", row)
    -> INSERT pg_attribute row (name)-> WAL: PHYSICAL_INSERT("pg_attribute", row)
    -> INSERT pg_depend rows        -> WAL: PHYSICAL_INSERT("pg_depend", rows)
    -> WAL: COMMIT
    -> fsync WAL
    // System table data_table_t updated in memory
    // Checkpoint happens later (not per-DDL)

Batch DDL (explicit transaction):
  BEGIN
    ALTER TABLE users ADD COLUMN age INT
    ALTER TABLE users ADD COLUMN email VARCHAR
    CREATE INDEX idx_email ON users(email)
  COMMIT
    -> 5+ WAL entries (pg_attribute inserts, pg_class insert, pg_index insert)
    -> 1 WAL COMMIT
    -> 1 fsync
    // All-or-nothing: crash before COMMIT -> all rolled back at recovery
```

#### WAL Record Types

Existing WAL record types are reused for system tables — system tables are
regular data_table_t instances, so PHYSICAL_INSERT/DELETE/UPDATE apply directly.
System table records are distinguished by collection name prefix ("pg_catalog.").

```
record_t {
    id: wal_id,
    transaction_id: uint64,
    record_type: PHYSICAL_INSERT | PHYSICAL_DELETE | PHYSICAL_UPDATE | COMMIT,
    collection_name: "pg_catalog.pg_class" | "pg_catalog.pg_attribute" | "mydb.users",
    physical_data: data_chunk_t*,
    physical_row_ids: vector<int64_t>,
}
```

No new record types needed — existing PHYSICAL_INSERT/DELETE/UPDATE cover all
DDL operations (DDL = DML on system tables).

#### Recovery Sequence

```
Startup after crash:

  Phase 1: Load system tables from last checkpoint (sync, _sync() pattern)
      load_system_tables_sync()
      // 9 system tables restored to last checkpoint state

  Phase 2: Partition WAL records
      system_records = WAL records WHERE collection starts with "pg_catalog."
      user_records = WAL records WHERE collection does NOT start with "pg_catalog."

  Phase 3: Replay system table WAL (sync, before scheduler start)
      for each system_record:
          if PHYSICAL_INSERT: direct_append_sync(pg_class, row)
          if PHYSICAL_DELETE: direct_delete_sync(pg_class, row_ids)
          if PHYSICAL_UPDATE: direct_update_sync(pg_class, row_ids, data)
      // System tables now reflect all committed DDL

  Phase 4: Start schedulers

  Phase 5: User table WAL replay (async or sync, as current code)
      // User table storages created on demand (D4)
      // WAL replay for user data uses current mechanism

  Order guarantee: system DDL replayed BEFORE user DML
  Reason: user DML may reference tables/columns created by DDL
```

#### Checkpoint Interaction

System tables are checkpointed FIRST to prevent orphan data on crash mid-checkpoint.
If crash after system checkpoint but before user checkpoint — user data recovered
from WAL, and pg_class correctly describes all tables.

```
CHECKPOINT command:
  1. Flush all dirty index btrees (as current)
  2. Checkpoint system tables FIRST:
       for each system_table in storages_ WHERE name starts with "pg_catalog.":
           system_table.compact()
           system_table.checkpoint()
  3. Checkpoint user tables (DISK mode only, as current):
       for each user_table in storages_ WHERE DISK mode:
           user_table.compact()
           user_table.checkpoint()
  4. Record WAL position in checkpoint
  5. Truncate WAL before checkpoint position

Auto-checkpoint:
  Triggered when WAL size exceeds threshold (configurable, default ~16MB)
  Covers both system DDL and user DML WAL entries

Crash scenarios:
  Crash during step 2: last good system checkpoint used, WAL replays DDL
  Crash during step 3: system tables current, user data from WAL
  Crash during step 5: all data safe, WAL has extra entries (harmless)
```

#### Invalidation Ring Buffer

The ring buffer is an in-memory circular buffer in manager_disk_t that stores
per-object invalidation events. It is transient — not persisted to disk.
After restart, the buffer is empty and dispatchers perform a full resolve.

```cpp
struct invalidation_event_t {
    uint64_t version;        // monotonic, auto-incremented per event
    event_type_t type;       // TABLE_CREATED, TABLE_DROPPED, TYPE_CREATED, ...
    oid_t object_oid;        // OID of affected object
    oid_t parent_oid;        // table OID for columns, namespace OID for tables
};

class invalidation_ring_buffer_t {
    static constexpr size_t DEFAULT_CAPACITY = 4096;  // matches PostgreSQL sinval

    vector<invalidation_event_t> buffer_;   // fixed-size circular
    uint64_t write_pos_{0};
    uint64_t oldest_version_{0};
    uint64_t current_version_{0};

    // Append event, advance write_pos, update current_version
    void append(invalidation_event_t event);

    // Get events since given version
    // Returns: events vector OR overflow signal
    events_result_t events_since(uint64_t since_version);
};
```

Overflow policy (same as PostgreSQL SINVAL_QUEUE_OVERFLOW):
- If dispatcher requests events_since(V) and V < oldest_version (evicted):
  return OVERFLOW signal instead of events.
- Dispatcher receiving OVERFLOW performs full cache reset + fresh resolve.
- This is rare: only when dispatcher is idle for >4096 DDL operations.

Ring buffer lifecycle:
- Populated on every DDL commit inside manager_disk_t
- Read by dispatchers via resolve_catalog(last_seen_version) response
- NOT persisted — lost on restart (by design, not a bug)
- After restart: empty buffer, all dispatchers do full resolve on first query
- GC: automatic via circular overwrite (oldest entries evicted by new ones)

#### Consumer Customization

| Knob | Options | Default |
|---|---|---|
| WAL commit mode | per-DDL (auto-commit) / batch (explicit transaction) | auto-commit |
| WAL fsync policy | fsync / fdatasync / no-sync | fsync |
| Auto-checkpoint threshold | WAL size in bytes | 16MB |
| WAL implementation | Pluggable interface (local file / remote log / in-memory) | Local file |
| Ring buffer capacity | Number of invalidation events | 4096 |

### Catalog Resolve Methods in manager_disk_t

Two levels of resolve: per-item (primary, called from V4 coroutines) and
batch (additional, for consumers who know multiple items in advance).
All scans happen synchronously inside the actor message handler — no nested
actor messages. data_table_t::scan() called directly on system tables in storages_.

#### Primary: Per-Item Resolve (V4)

Called by dispatcher validate coroutines on cache miss via co_await.

```cpp
// Resolve single table: schema + indexes + constraints + D4 lazy load
unique_future<resolve_table_result_t> resolve_table(
    session_id_t session,
    collection_full_name_t table_name,
    uint64_t last_seen_version);          // for ring buffer events
// Scans (synchronous inside handler):
//   pg_namespace WHERE nspname=X          (EQUAL filter)
//   pg_class WHERE relname=Y AND relnamespace=ns_oid  (EQUAL)
//   pg_attribute WHERE attrelid=oid       (EQUAL) — for relkind='r'
//   pg_computed_column WHERE attrelid=oid (EQUAL) — for relkind='c'
//   pg_index WHERE indrelid=oid AND indisvalid=true   (EQUAL + AND)
//   pg_constraint WHERE conrelid=oid      (EQUAL)
//   D4: load_from_disk if not in storages_
// Returns: relation_info + events_since(last_seen_version) + catalog_version

// Resolve single function
unique_future<resolve_function_result_t> resolve_function(
    session_id_t session,
    std::pmr::string name,
    std::pmr::vector<oid_t> arg_type_oids,
    uint64_t last_seen_version);
// Scans: pg_proc WHERE proname=name (EQUAL), match signatures

// Resolve single type
unique_future<resolve_type_result_t> resolve_type(
    session_id_t session,
    std::pmr::string type_name,
    uint64_t last_seen_version);
// Scans: pg_type WHERE typname=name (EQUAL)

// Check namespace
unique_future<resolve_namespace_result_t> resolve_namespace(
    session_id_t session,
    std::pmr::string ns_name,
    uint64_t last_seen_version);
// Scans: pg_namespace WHERE nspname=name (EQUAL)
```

#### Inside resolve_table (synchronous within handler)

```
resolve_table("mydb.users"):
    scan pg_namespace WHERE nspname="mydb" → ns_oid
    scan pg_class WHERE relname="users" AND relnamespace=ns_oid
      → oid=16400, relkind='r'

    if relkind == 'r':
        scan pg_attribute WHERE attrelid=16400 → columns
    elif relkind == 'c':
        scan pg_computed_column WHERE attrelid=16400 → versioned fields

    scan pg_index WHERE indrelid=16400 AND indisvalid=true → indexes
    scan pg_constraint WHERE conrelid=16400 → constraints

    if not storages_.contains("mydb.users"):
        storages_["mydb.users"] = load_from_disk("mydb/users.otbx")  // D4

    events = ring_buffer_.events_since(last_seen_version)
    return { relation_info, events, catalog_version }
```

#### Additional: Batch Resolve (S5)

For consumers who know multiple items in advance. Uses IN filter for efficiency.
Not used by V4 validate coroutines — available as optimization API.

```cpp
// Batch resolve multiple tables in one call
unique_future<batch_resolve_result_t> resolve_tables_batch(
    session_id_t session,
    std::vector<collection_full_name_t> table_names,
    uint64_t last_seen_version);
// Uses set_membership_filter_t (IN filter) for pg_class, pg_attribute, etc.
// One scan per system table instead of N scans

// Batch resolve multiple functions
unique_future<batch_resolve_result_t> resolve_functions_batch(
    session_id_t session,
    std::vector<std::pmr::string> function_names,
    uint64_t last_seen_version);
```

#### batch_scan (used inside batch resolve)

```
batch_scan(system_table, column, values):
    if values.size() == 1:
        filter = constant_filter_t(EQUAL, values[0])
    else if in_filter_supported():
        filter = set_membership_filter_t(column, values)
    else:
        // Fallback: sequential EQUAL scans, merge results
        for each value: scan with EQUAL, merge
        return merged

    return scan(system_table, filter)
```

#### set_membership_filter_t (New Filter Type)

```cpp
class set_membership_filter_t : public table_filter_t {
    uint64_t column_index_;
    std::pmr::unordered_set<types::logical_value_t> values_;
public:
    bool compare(const types::logical_value_t& value) const {
        return values_.contains(value);  // O(1) hash lookup
    }
};
```

#### V4 + Cache Flow

```
dispatcher::validate_schema (coroutine):

    encounter "users":
        cached = versioned_cache_.get(txn.catalog_version, "users")
        if cached: use cached.schema                           -- 0 roundtrips
        else:
            info = co_await send(disk, resolve_table, "users") -- 1 roundtrip
            versioned_cache_.add("users", info)
            process_events(info.events)
            use info.schema

    encounter function "count":
        cached = versioned_cache_.get_function("count", args)
        if cached: use cached                                  -- 0 roundtrips
        else:
            info = co_await send(disk, resolve_function, "count", args)
            versioned_cache_.add_function("count", info)       -- 1 roundtrip
            use info

Cold (first query):   N roundtrips (1 per unique table/function)
Warm (repeated):      0 roundtrips
Typical OLTP:         0-2 roundtrips
```

#### Consumer Customization

| Knob | Description |
|---|---|
| Custom resolve_table | Replace with indexed scan, remote catalog, etc. |
| Custom resolve_function | Custom function resolution logic |
| Batch resolve methods | Use S5 batch when items known in advance |
| set_membership_filter | Extend or replace IN filter implementation |
| Additional resolve methods | Add resolve for custom objects (tenants, policies) |

### DDL Method Signatures (O4: Disk Allocates All OIDs)

All DDL methods live in manager_disk_t. Dispatcher sends logical structures
without OIDs. manager_disk_t owns the oid_generator, allocates all OIDs,
inserts rows into system tables, appends ring buffer events,
and returns the allocated OIDs in the response.
NOTE: Disk DDL methods do NOT write WAL. Executor writes WAL separately
(Variant E: unified DDL+DML pipeline through executor).

#### Common Return Type

```cpp
struct ddl_result_t {
    cursor_t_ptr result;                              // success or error
    std::pmr::vector<invalidation_event_t> events;    // for ring buffer + cache
    uint64_t new_catalog_version;                      // for version tracking
    oid_t created_oid;                                 // primary OID (0 for drop/alter)
    std::pmr::unordered_map<std::string, oid_t> all_oids;  // full OID map
};
```

#### Input Row Types (no OIDs — disk assigns them)

```cpp
struct column_def_t {
    std::pmr::string attname;
    oid_t atttypid;               // type OID (already known)
    bool attnotnull;
    std::pmr::string attdefval;   // empty = no default
};

struct constraint_def_t {
    std::pmr::string conname;
    std::pmr::string contype;     // "p", "u", "c", "f"
    std::pmr::vector<std::pmr::string> columns;  // column names
    std::pmr::string conexpr;     // CHECK expression SQL
};

struct index_def_t {
    std::pmr::string index_name;
    std::pmr::string indtype;     // single, composite, multikey, hashed, wildcard
    std::pmr::vector<std::pmr::string> columns;  // column names
    bool indisunique;
    bool indisprimary;
};
```

#### Namespace Methods

```cpp
unique_future<ddl_result_t> ddl_create_namespace(
    session_id_t session,
    std::pmr::string name,
    oid_t owner_oid);
// Disk: allocate oid → INSERT pg_namespace (no WAL — executor writes WAL separately)
// Events: [{NAMESPACE_CREATED, oid, 0}]
// Returns: created_oid = namespace OID

unique_future<ddl_result_t> ddl_drop_namespace(
    session_id_t session,
    std::pmr::string name,
    bool cascade);
// Disk: find oid from pg_namespace
//   cascade: ddl_drop_table for each table, ddl_drop_type for each type
//   DELETE pg_namespace (no WAL — executor writes WAL separately)
// Events: [{NAMESPACE_DROPPED, oid, 0}, cascade events...]
```

#### Table Methods

```cpp
unique_future<ddl_result_t> ddl_create_table(
    session_id_t session,
    oid_t namespace_oid,
    std::pmr::string table_name,
    char relkind,                          // 'r' or 'c'
    std::vector<column_def_t> columns,
    std::vector<constraint_def_t> constraints);
// Disk: allocate table_oid, constraint_oids
//   INSERT pg_class (table_oid, table_name, namespace_oid, relkind)
//   INSERT pg_attribute (N rows, attnum assigned sequentially 1..N)
//   INSERT pg_constraint (PK etc., auto-generated conname if empty)
//   INSERT pg_depend (table→namespace, columns→types)
//   WAL: PHYSICAL_INSERT for each system table
// Events: [{TABLE_CREATED, table_oid, namespace_oid}]
// Returns: created_oid = table_oid
//          all_oids = {"table": table_oid, "col.id": col1_attnum, ...}
// NOTE: does NOT create storage — D4 lazy, or dispatcher calls create_storage

unique_future<ddl_result_t> ddl_drop_table(
    session_id_t session,
    oid_t table_oid,
    bool cascade);
// Disk: cascade → scan pg_depend → drop dependents (indexes, constraints)
//   DELETE pg_constraint WHERE conrelid=table_oid
//   DELETE pg_index WHERE indrelid=table_oid
//   DELETE pg_attribute WHERE attrelid=table_oid
//   DELETE pg_depend WHERE objid=table_oid OR refobjid=table_oid
//   DELETE pg_class WHERE oid=table_oid
//   Remove from storages_ if loaded
//   WAL: PHYSICAL_DELETE for each
// Events: [{TABLE_DROPPED, table_oid, ns_oid}, cascade events...]
```

#### Alter Table Methods

```cpp
unique_future<ddl_result_t> ddl_add_column(
    session_id_t session,
    oid_t table_oid,
    column_def_t column);
// Disk: determine next attnum from max(pg_attribute WHERE attrelid=table_oid) + 1
//   INSERT pg_attribute (1 row)
//   INSERT pg_depend (column→type)
//   WAL: PHYSICAL_INSERT
// Events: [{TABLE_ALTERED, table_oid, ns_oid}]

unique_future<ddl_result_t> ddl_drop_column(
    session_id_t session,
    oid_t table_oid,
    std::pmr::string column_name);
// Disk: find attnum from pg_attribute
//   DELETE pg_attribute WHERE attrelid=table_oid AND attname=column_name
//   DELETE related pg_depend
//   WAL: PHYSICAL_DELETE
// Events: [{TABLE_ALTERED, table_oid, ns_oid}]

unique_future<ddl_result_t> ddl_rename_column(
    session_id_t session,
    oid_t table_oid,
    std::pmr::string old_name,
    std::pmr::string new_name);
// Disk: find attnum, DELETE old pg_attribute row, INSERT new with same attnum
//   WAL: PHYSICAL_DELETE + PHYSICAL_INSERT
// Events: [{TABLE_ALTERED, table_oid, ns_oid}]
```

#### Index Methods

```cpp
unique_future<ddl_result_t> ddl_create_index(
    session_id_t session,
    oid_t table_oid,
    index_def_t index);
// Disk: allocate index_oid
//   INSERT pg_class (index_oid, index_name, relkind='i')
//   INSERT pg_index (indexrelid=index_oid, indrelid=table_oid,
//                    indisvalid=false, indisready=true)
//   INSERT pg_depend (index→table, deptype='a')
//   WAL: PHYSICAL_INSERT × 3
// Events: [{INDEX_CREATED, index_oid, table_oid}]
// NOTE: btree NOT built — dispatcher sends build_btree to manager_index_t

unique_future<ddl_result_t> ddl_index_set_valid(
    session_id_t session,
    oid_t index_oid);
// Disk: DELETE old pg_index row, INSERT new with indisvalid=true
//   WAL: PHYSICAL_DELETE + PHYSICAL_INSERT
// Events: [{INDEX_VALIDATED, index_oid, table_oid}]

unique_future<ddl_result_t> ddl_drop_index(
    session_id_t session,
    oid_t index_oid);
// Disk: DELETE pg_index, pg_depend, pg_class
//   WAL: PHYSICAL_DELETE × 3
// Events: [{INDEX_DROPPED, index_oid, table_oid}]
// NOTE: btree NOT destroyed — dispatcher sends to manager_index_t
```

#### Type Methods

```cpp
unique_future<ddl_result_t> ddl_create_type(
    session_id_t session,
    oid_t namespace_oid,
    std::pmr::string typname,
    int64_t typlen,
    bool typbyval,
    std::pmr::string typtype);
// Disk: allocate type_oid → INSERT pg_type (no WAL — executor writes WAL separately)
// Events: [{TYPE_CREATED, type_oid, namespace_oid}]

unique_future<ddl_result_t> ddl_drop_type(
    session_id_t session,
    oid_t type_oid,
    bool cascade);
// Disk: cascade → scan pg_depend → drop columns using type
//   DELETE pg_type → DELETE pg_depend (no WAL — executor writes WAL separately)
// Events: [{TYPE_DROPPED, type_oid, ns_oid}, cascade events...]
```

#### Function Methods

```cpp
unique_future<ddl_result_t> ddl_create_function(
    session_id_t session,
    oid_t namespace_oid,
    std::pmr::string proname,
    oid_t prorettype,
    std::pmr::string proargtypes,
    uint64_t function_uid);
// Disk: allocate func_oid → INSERT pg_proc (no WAL — executor writes WAL separately)
// Events: [{FUNCTION_CREATED, func_oid, namespace_oid}]

unique_future<ddl_result_t> ddl_drop_function(
    session_id_t session,
    oid_t func_oid);
// Disk: DELETE pg_proc → DELETE pg_depend (no WAL — executor writes WAL separately)
// Events: [{FUNCTION_DROPPED, func_oid, ns_oid}]
```

#### OID Allocation Inside manager_disk_t

```cpp
class manager_disk_t {
    oid_generator oid_gen_;  // sole owner, atomic counter

    // Used inside DDL handlers:
    oid_t allocate_oid() { return oid_gen_.next(); }

    // On restart: restore from max OID in all system tables
    void restore_oid_generator() {
        oid_t max_oid = 0;
        max_oid = max(max_oid, max_oid_in(pg_namespace));
        max_oid = max(max_oid, max_oid_in(pg_class));
        max_oid = max(max_oid, max_oid_in(pg_type));
        max_oid = max(max_oid, max_oid_in(pg_proc));
        max_oid = max(max_oid, max_oid_in(pg_constraint));
        oid_gen_.set_next(max_oid + 1);
    }
};
```

### Tests

```
Bootstrap:
test_bootstrap_creates_all_tables       -- 9 tables exist after bootstrap
test_pg_class_self_describing           -- pg_class contains row for pg_class
test_pg_attribute_describes_pg_class    -- pg_attribute has pg_class columns
test_pg_type_has_builtins               -- bool, int4, int8, text, float8 present
test_pg_namespace_has_pg_catalog        -- pg_catalog namespace exists
test_bootstrap_row_count                -- correct number of rows per table

Scan:
test_scan_pg_attribute_by_relid         -- filter by attrelid returns correct columns
test_append_user_table_to_pg_class      -- INSERT user table metadata into pg_class

Lazy loading (D4):
test_user_table_not_in_storages_at_start -- storages_ has only 9 system tables
test_first_select_creates_storage        -- SELECT triggers on-demand load
test_second_select_uses_existing         -- no reload on second access
test_drop_unloaded_table                 -- DROP modifies pg_class only
test_alter_unloaded_table                -- ALTER modifies pg_attribute only
test_show_tables_from_pg_class           -- lists all tables including unloaded

Checkpoint + WAL:
test_system_table_checkpoint_load        -- checkpoint -> load -> data preserved
test_ddl_wal_logged                      -- CREATE TABLE generates WAL entries
test_ddl_crash_recovery                  -- crash after COMMIT -> DDL recovered from WAL
test_ddl_crash_before_commit             -- crash before COMMIT -> DDL rolled back
test_batch_ddl_single_commit             -- BEGIN + N DDL + COMMIT -> 1 WAL commit
test_batch_ddl_rollback                  -- BEGIN + N DDL + ROLLBACK -> no WAL entries
test_wal_replay_order                    -- system DDL replayed before user DML
test_checkpoint_truncates_wal            -- CHECKPOINT -> old WAL entries removed
test_auto_checkpoint_on_wal_size         -- WAL exceeds threshold -> auto checkpoint

Checkpoint ordering:
test_checkpoint_system_tables_first      -- system tables checkpointed before user tables
test_checkpoint_crash_mid_system         -- crash during system checkpoint -> last good checkpoint
test_checkpoint_crash_after_system       -- system OK, user tables recovered from WAL

Ring buffer:
test_ring_buffer_events_since            -- returns events after given version
test_ring_buffer_overflow                -- old version -> OVERFLOW signal -> full reset
test_ring_buffer_empty_after_restart     -- restart -> buffer empty -> first resolve is full
test_ring_buffer_capacity                -- 4096 events before overflow
test_ring_buffer_monotonic_version       -- versions always increasing

Recovery integration:
test_recovery_system_wal_before_user     -- system DDL WAL replayed first
test_recovery_ring_buffer_empty          -- after recovery, dispatchers full resolve
test_recovery_ddl_then_dml              -- CREATE TABLE in WAL + INSERT data in WAL -> both recovered

DDL methods:
test_ddl_create_table_assigns_oid       -- table_oid allocated by disk, returned in response
test_ddl_create_table_columns_attnum    -- attnum assigned sequentially 1..N
test_ddl_create_table_pg_depend         -- dependencies auto-generated (table->ns, col->type)
test_ddl_drop_table_cascade             -- drops indexes, constraints, depends
test_ddl_drop_table_restrict            -- fails if dependents exist
test_ddl_add_column_next_attnum         -- new column gets max(attnum)+1, never reuses
test_ddl_drop_column_preserves_attnum   -- other columns attnum unchanged
test_ddl_rename_column_preserves_attnum -- same attnum, new name
test_ddl_create_index_indisvalid_false  -- created with indisvalid=false
test_ddl_index_set_valid                -- updates to indisvalid=true
test_ddl_drop_index_removes_pg_class    -- removes both pg_index and pg_class rows
test_ddl_create_type_assigns_oid        -- type_oid allocated by disk
test_ddl_drop_type_cascade              -- drops columns using type
test_ddl_create_function_assigns_oid    -- func_oid allocated by disk
test_ddl_drop_namespace_cascade         -- drops all tables, types, functions inside
test_ddl_events_in_response             -- invalidation events returned for each DDL
test_ddl_oid_generator_restart          -- after restart, oid_gen starts at max+1

Resolve + scan:
test_resolve_batch_scan_3_tables        -- 6 scans for 3 tables (batch IN filter)
test_resolve_sequential_fallback        -- force sequential, 18 scans for 3 tables
test_resolve_single_table_uses_equal    -- 1 table uses EQUAL filter, not IN
test_resolve_includes_indexed_keys      -- pg_index data in resolve response
test_resolve_skips_invalid_indexes      -- indisvalid=false excluded from response
test_resolve_lazy_load_creates_storage  -- first resolve triggers load_from_disk

Computing tables:
test_ddl_create_computing_table         -- pg_class relkind='c', no pg_attribute rows
test_ddl_computed_append_field          -- INSERT pg_computed_column with version=1
test_ddl_computed_append_same_field     -- second type for same field -> version=2
test_ddl_computed_drop_field            -- attrefcount decremented, deleted if 0
test_ddl_computed_latest_types          -- latest version per field returned
test_ddl_adopt_computing_schema         -- pg_computed_column -> pg_attribute, relkind 'c'->'r'
test_computing_table_persists_restart   -- computing table survives crash (WAL + checkpoint)
test_resolve_computing_table            -- resolve returns versioned fields for relkind='c'
test_computing_table_pg_attribute_empty -- relkind='c' has no pg_attribute rows
```

---

## 6. Phase 2: Versioned Plan Cache

### Overview

The dispatcher's plan cache replaces the old catalog_ in-memory object. It stores
multiple versions of resolved catalog data (schemas, functions, types), keyed by
catalog_version. Each transaction is pinned to a version at begin_transaction.

This provides per-transaction snapshot isolation (Decision 4: C1) without
depending on Phase 3 MVCC DDL.

### Cache Architecture

```cpp
class versioned_plan_cache_t {
    // Data deduplication (alias): versions without DDL share same data
    struct resolved_data_t {
        catalog_resolve_response_t data;   // schemas, functions, types, indexes
        size_t ref_count{0};               // number of version entries referencing
        size_t memory_bytes{0};            // estimated memory footprint
    };

    std::map<uint64_t, uint64_t> version_to_data_;           // version → data_version
    std::unordered_map<uint64_t, resolved_data_t> data_store_; // data_version → data
    std::unordered_map<session_id_t, uint64_t> active_txns_;  // txn → pinned version

    // Limits
    size_t max_data_versions_;    // default: 100
    size_t max_memory_bytes_;     // default: 256MB
    size_t current_memory_{0};
    float gc_soft_limit_ratio_;   // default: 0.75

    // GC strategy: lazy (trigger at soft limit) + every-commit fallback
    enum class gc_strategy_t { LAZY, EVERY_COMMIT, PERIODIC };
    gc_strategy_t gc_strategy_;   // default: LAZY
};
```

### Data Deduplication (Alias)

When no DDL occurs between versions, multiple version entries point to the
same resolved_data_t. This prevents memory waste for concurrent transactions
that all see the same schema.

```
Example: 100 concurrent auto-commit queries, 0 DDL
  version_to_data_ = {v=40→d=40, v=41→d=40, v=42→d=40, ..., v=139→d=40}
  data_store_ = {d=40: {data, ref_count=100}}
  Memory: 1 copy of data, not 100

Example: 3 DDL operations during 5 concurrent transactions
  version_to_data_ = {v=40→d=40, v=41→d=41, v=42→d=41, v=43→d=43, v=44→d=43}
  data_store_ = {d=40: {old_data, ref=1}, d=41: {mid_data, ref=2}, d=43: {new_data, ref=2}}
  Memory: 3 copies of data
```

### GC Algorithm

**Strategy: Lazy with every-commit fallback.**

- Default: GC triggers when current_memory_ exceeds soft limit (75% of max)
- Fallback: if gc_strategy = EVERY_COMMIT, runs at every commit/abort
- Consumer configurable

```
gc():
    lowest_active = min(active_txns_.values())
    if active_txns_.empty(): lowest_active = UINT64_MAX

    // Remove version entries below lowest active
    for v in version_to_data_ where v < lowest_active:
        data_ver = version_to_data_[v]
        version_to_data_.erase(v)
        data_store_[data_ver].ref_count--
        if data_store_[data_ver].ref_count == 0:
            current_memory_ -= data_store_[data_ver].memory_bytes
            data_store_.erase(data_ver)

    // Hard limit enforcement: evict oldest data versions if over limit
    while data_store_.size() > max_data_versions_
          or current_memory_ > max_memory_bytes_:
        evict oldest data_version not pinned by any active_txn
        // Affected transactions will force re-resolve on next access
```

### Pin / Unpin Lifecycle

```
begin_transaction(session):
    txn.catalog_version = last_known_version
    active_txns_[session] = txn.catalog_version
    // 0 roundtrips — just record the version

execute_plan(session):
    version = active_txns_[session]
    data = cache.get(version, deps)
    if data: use data                    // 0 roundtrips (cache hit)
    else: co_await resolve_catalog(deps) // 1 roundtrip (cache miss)
          populate(version, response)

commit/abort_transaction(session):
    active_txns_.erase(session)
    if gc_strategy_ == LAZY:
        if current_memory_ > max_memory_bytes_ * gc_soft_limit_ratio_:
            gc()
    else:  // EVERY_COMMIT fallback
        gc()
```

### Restart Behavior

On dispatcher restart (crash or graceful):
- cache_versions_ lost completely (in-memory only)
- active_txns_ lost — all transactions aborted
- First query: cache miss → co_await resolve_catalog → populate fresh
- Ring buffer in manager_disk_t also empty → full resolve (not incremental)
- This is correct: restart = cold start

### Memory Estimation

| Scenario | Concurrent txns | DDL frequency | Unique data versions | Memory |
|---|---|---|---|---|
| Typical OLTP | 10 | 1/hour | 1-2 | ~50KB-1MB |
| Batch analytics | 100 | 0 | 1 | ~5KB-50KB |
| Migration heavy | 10 | 100/minute | 10 | ~5-50MB |
| Worst case | 1000 | every txn | 1000 | Hard limited to 256MB |

### Consumer Customization

| Knob | Description | Default |
|---|---|---|
| max_data_versions | Hard limit on unique data snapshots | 100 |
| max_memory_bytes | Memory limit for cache | 256MB |
| gc_strategy | lazy / every_commit / periodic(interval) | lazy |
| gc_soft_limit_ratio | Trigger lazy GC at X% of max_memory | 0.75 |
| eviction_policy | evict_oldest / error / force_re_resolve | evict_oldest |

### Tests

```
GC:
test_cache_gc_removes_old_versions           -- commit removes versions below lowest active
test_cache_gc_keeps_active_version           -- active txn version not removed
test_cache_gc_lazy_triggers_at_soft_limit    -- GC runs when memory hits 75%
test_cache_gc_every_commit_fallback          -- GC runs at every commit when configured

Alias:
test_cache_alias_no_ddl_same_data            -- no DDL → versions share data pointer
test_cache_alias_ddl_creates_new_data        -- DDL → new data version
test_cache_alias_ref_count_correct           -- ref count matches version count

Limits:
test_cache_memory_limit_evicts               -- exceeding max_memory triggers eviction
test_cache_max_versions_evicts               -- exceeding max_data_versions triggers eviction
test_cache_eviction_does_not_evict_active    -- active txn version never evicted

Lifecycle:
test_cache_restart_empty                     -- after restart, cache empty
test_cache_first_query_populates             -- first query fills cache from resolve
test_cache_long_txn_holds_version            -- long txn prevents GC of its version
test_cache_concurrent_txns_snapshots         -- each txn sees its own schema version
```

---

## 7. Phase 3: Transactional DDL via MVCC

### Overview

DDL operations become regular INSERT/DELETE operations on system tables,
using the same MVCC machinery as data tables. The separate catalog transaction
system (`transaction_scope`, `schema_diff`, `metadata_diff`) is removed.

### DDL to DML Mapping

| DDL Statement            | System Table Operations                           |
|--------------------------|---------------------------------------------------|
| CREATE NAMESPACE ns      | INSERT pg_namespace                                |
| DROP NAMESPACE ns CASCADE| DELETE pg_namespace + CASCADE (pg_class, pg_attribute, pg_type) |
| CREATE TABLE t (cols...) | INSERT pg_class + INSERT pg_attribute (N rows)     |
| DROP TABLE t             | DELETE pg_class + DELETE pg_attribute (N rows)      |
| ALTER TABLE ADD COLUMN   | INSERT pg_attribute (1 row)                        |
| ALTER TABLE DROP COLUMN  | DELETE pg_attribute (1 row)                        |
| ALTER TABLE RENAME COLUMN| DELETE old pg_attribute + INSERT new (same attnum)  |
| CREATE TYPE t            | INSERT pg_type                                     |
| DROP TYPE t              | DELETE pg_type                                     |
| CREATE FUNCTION f        | INSERT pg_proc                                     |
| DROP FUNCTION f          | DELETE pg_proc                                     |

### Code Removed

```
components/catalog/transaction/
    transaction_scope.hpp/cpp       -- REMOVED
    metadata_transaction.hpp/cpp    -- REMOVED
    schema_diff.hpp/cpp             -- REMOVED
    metadata_diff.hpp/cpp           -- REMOVED
    transaction_list.hpp/cpp        -- REMOVED
```

### MVCC Visibility for System Tables

DDL changes are visible only after commit, using the same row version manager:

```
T1 (txn_id=100, start_time=50): ALTER TABLE users ADD COLUMN age INT
  -> Appends pg_attribute row with insert_id=100

T2 (start_time=60): SELECT * FROM users
  -> Scans pg_attribute
  -> Row insert_id=100 >= TRANSACTION_ID_START (~2^62, value 4611686018427388000): uncommitted
  -> 100 != T2.txn_id: not own transaction
  -> Row INVISIBLE to T2 (correct)

T1 commits at commit_id=70:
  -> insert_id converted: 100 -> 70

T3 (start_time=80): SELECT * FROM users
  -> Row insert_id=70 < start_time=80: VISIBLE (correct)
```

### Schema Snapshot Isolation

Queries must see the schema as it existed at their start_time:

```cpp
// Current (NOT snapshot-aware):
const schema& catalog::get_table_schema(const table_id& id) const;

// Phase 3 (snapshot-aware):
schema catalog::get_table_schema_at(const table_id& id, uint64_t start_time) const {
    // Scan pg_attribute WHERE attrelid=oid
    // MVCC filter: visible_at(start_time)
    // Reconstruct schema from visible rows
}
```

Optimization: if `cache_.last_mutation_time() < start_time`, the cached schema
is valid and no scan is needed (covers 99% of cases).

### Known Bug: Visibility Gap (E2.1A)

Current MVCC logic has a gap for uncommitted deletes:

```
T1 (txn_id >= 2^62): DELETE row (uncommitted)
T2 (start_time << 2^62): reads row

use_inserted_version(start_time, T2.txn_id, txn_id):
  txn_id < start_time? NO (txn_id is huge)
  txn_id == T2.txn_id? NO
  -> FALSE

use_deleted_version = !FALSE = TRUE -> T2 sees uncommitted delete (BUG)
```

Fix: System table reads MUST use `committed_version_operator` (already exists in
codebase) which checks `id < TRANSACTION_ID_START`.

### Design Rules

1. attnum is **immutable** -- never reuse after DROP COLUMN
2. Savepoints remain **in-memory** (accumulate changes, apply on commit)
3. DDL operations are **serialized** per table (single lock on pg_class row)
4. Multi-table DDL (CREATE TABLE = pg_class + N pg_attribute) is **atomic** via single transaction

### Edge Cases

| ID    | Scenario                                   | Severity | Mitigation                          |
|-------|--------------------------------------------|----------|-------------------------------------|
| E2.1A | Uncommitted delete visible to other txns   | CRITICAL | Use committed_version_operator      |
| E3.1  | attnum preservation after DROP              | HIGH     | Never reuse attnum                  |
| E5.1  | Physical column data survives DROP COLUMN   | HIGH     | Schema snapshot -> skip column      |
| E7.1  | Query sees dropped column via old schema    | HIGH     | get_schema_at(start_time)           |
| E8.3  | Write skew in multi-table DDL              | MEDIUM   | Schema versioning                   |
| E9.1  | Schema snapshot isolation                  | HIGH     | Snapshot per query start_time       |

### Tests

```
test_ddl_mvcc_visibility            -- CREATE TABLE invisible until commit
test_ddl_rollback_cleans_up         -- ROLLBACK -> system rows invisible
test_concurrent_ddl_dml             -- INSERT INTO users during ALTER TABLE
test_schema_snapshot_isolation      -- query sees schema at start_time
test_attnum_never_reused            -- DROP + ADD -> new attnum
test_multi_table_ddl_atomicity      -- CREATE TABLE = pg_class + pg_attribute atomic
test_uncommitted_delete_invisible   -- fix for bug E2.1A
test_self_read_own_writes           -- DDL sees own changes within txn
test_drop_cascade_mvcc              -- CASCADE delete via MVCC
test_alter_rename_column_mvcc       -- DELETE old + INSERT new, same attnum
```

---

## 8. Phase 4: Dependency Tracking (pg_depend)

### Overview

Implement dependency tracking to support CASCADE/RESTRICT semantics on DROP
operations. Currently all DROP operations succeed silently without checking
dependents.

### Current State

- Parser supports CASCADE/RESTRICT (gram.y -> `DropBehavior` enum)
- **No implementation** in dispatcher -- all drops are unconditional
- No dependency tracking anywhere in the system

### Dependency Graph

```
Table      -> Namespace        (normal)
Table      -> Column Types     (normal)
Index      -> Table            (auto -- deleted on DROP TABLE)
View       -> Tables           (normal -- CASCADE for deletion)
Constraint -> Table            (internal)
Constraint -> Referenced Table (normal, for FK)
Function   -> Argument Types   (normal)
Function   -> Return Type      (normal)
Column     -> Type             (normal)
```

### Dependency Types

| deptype | Meaning   | On DROP of referenced object               |
|---------|-----------|---------------------------------------------|
| n       | normal    | CASCADE required, RESTRICT blocks            |
| a       | auto      | Automatically deleted                        |
| i       | internal  | Treated as part of the referenced object     |

### pg_depend Examples

```
CREATE INDEX idx ON users(name):
  (pg_class_oid, idx_oid, 0) -> (pg_class_oid, users_oid, 0, 'a')

CREATE VIEW v AS SELECT * FROM users:
  (pg_class_oid, v_oid, 0) -> (pg_class_oid, users_oid, 0, 'n')

Column users.name references type VARCHAR:
  (pg_class_oid, users_oid, 2) -> (pg_type_oid, varchar_oid, 0, 'n')
```

### DROP CASCADE Algorithm

```
function drop_cascade(classid, objid):
    dependents = scan pg_depend WHERE refclassid=classid AND refobjid=objid
    for each dependent:
        if deptype == 'a' or deptype == 'i':
            drop_cascade(dependent.classid, dependent.objid)  // recursive
        elif deptype == 'n':
            drop_cascade(dependent.classid, dependent.objid)  // recursive
    delete from pg_depend WHERE refclassid=classid AND refobjid=objid
    delete object itself from its catalog table
```

### DROP RESTRICT Algorithm

```
function drop_restrict(classid, objid):
    dependents = scan pg_depend WHERE refclassid=classid AND refobjid=objid
                                  AND deptype IN ('n', 'a')
    if dependents not empty:
        raise error "cannot drop: other objects depend on it"
    delete from pg_depend WHERE refclassid=classid AND refobjid=objid
    delete object itself
```

### Tests

```
test_drop_restrict_table_with_index     -- DROP TABLE RESTRICT fails if indexes exist
test_drop_cascade_table_with_index      -- DROP TABLE CASCADE drops indexes too
test_drop_restrict_type_with_columns    -- DROP TYPE RESTRICT fails if used in column
test_drop_cascade_namespace             -- CASCADE deletes everything inside
test_pg_depend_created_on_create_table  -- dependencies recorded on CREATE
test_pg_depend_cleaned_on_drop          -- dependencies removed on DROP
test_drop_restrict_function_in_check    -- function in CHECK constraint
test_circular_dependency_detection      -- no cycles in dependency graph
```

---

## 9. Phase 5: Full Migration

### Overview

Migrate all remaining metadata objects to system tables and remove the old
catalog_storage_t persistence.

### Critical Gaps Fixed

| Object      | Current State              | After Migration                 |
|-------------|----------------------------|---------------------------------|
| Types       | In-memory only (LOST)      | pg_type table (persisted)       |
| Functions   | In-memory only (LOST)      | pg_proc table (persisted)       |
| Constraints | Not saved to disk          | pg_constraint table (persisted) |
| Sequences   | Loaded, not registered     | pg_class relkind='S' + pg_sequence |
| Views       | Loaded, not registered     | pg_class relkind='v' + pg_rewrite  |
| Macros      | Loaded, not registered     | pg_class relkind='m' + pg_rewrite  |

### Additional System Tables

#### pg_sequence

| Column       | Type   | Description                     |
|--------------|--------|---------------------------------|
| seqrelid     | BIGINT | Sequence OID (FK pg_class)      |
| seqstart     | BIGINT | Start value                     |
| seqincrement | BIGINT | Increment                       |
| seqmin       | BIGINT | Minimum value                   |
| seqmax       | BIGINT | Maximum value                   |
| seqcycle     | BOOLEAN| Whether it cycles               |

#### pg_rewrite (for views and macros)

| Column    | Type           | Description                     |
|-----------|----------------|---------------------------------|
| oid       | BIGINT         | Rule OID                        |
| rulename  | STRING_LITERAL | View/macro name                 |
| ev_class  | BIGINT         | Related table OID               |
| ev_type   | BIGINT         | 1=SELECT, 2=UPDATE              |
| ev_action | STRING_LITERAL | View SQL or macro body          |

### Serialization Notes

- `complex_logical_type` extensions: reuse existing `write_complex_type()` /
  `read_complex_type()` from `catalog_storage.cpp` for pg_type.typextension
- `kernel_signature_t`: NOT serializable (contains function pointers). Store only
  name + arg OIDs in pg_proc; resolve via function_registry at load time
- CHECK expressions: store as SQL string in pg_constraint
- View/Macro SQL: store as string in pg_rewrite.ev_action

### Migration Strategy (M0: No Migration, Clean Break)

No backward compatibility with old catalog format. Existing databases using
catalog.otbx must be recreated from scratch. This is acceptable because
otterbrix is a framework — current users can recreate their data.

```
base_spaces.cpp startup:
    if system_tables_exist_on_disk():
        load_system_tables_sync()       // restart from checkpoint
        replay_system_wal_sync()        // WAL replay for system tables
    else:
        bootstrap_system_tables_sync()  // first run, always from scratch

    // catalog.otbx is NEVER read
    // indexes_METADATA is NEVER read
    // Old format code is deleted from codebase
```

### Code Removed

| File / Format                    | Status                              |
|----------------------------------|-------------------------------------|
| `catalog_storage_t`             | DELETED entirely |
| `catalog.otbx` binary file      | NOT READ — format abandoned |
| `catalog_column_entry_t`        | DELETED |
| `catalog_table_entry_t`         | DELETED |
| `catalog_database_entry_t`      | DELETED |
| `catalog_sequence_entry_t`      | DELETED (replaced by pg_class relkind='S') |
| `catalog_view_entry_t`          | DELETED (replaced by pg_class relkind='v') |
| `catalog_macro_entry_t`         | DELETED (replaced by pg_class relkind='m') |
| `catalog_schema_update_t`       | DELETED |
| `update_catalog_schemas()`      | DELETED |
| `serialize_catalog()` / `deserialize_catalog()` | DELETED |
| `write_complex_type()` / `read_complex_type()` | MOVED to pg_type serialization |
| `indexes_METADATA` msgpack file  | NOT READ — format abandoned |
| `write_index_to_metafile()`     | DELETED |
| `read_indexes_from_metafile()`  | DELETED |
| `read_index_definitions()`      | DELETED |
| `node_create_index_t` serialization | DELETED (index metadata in pg_index) |
| `table_constraint_t` in-memory  | DELETED (replaced by pg_constraint) |
| `catalog/transaction/` directory | DELETED (5 files: transaction_scope, metadata_transaction, schema_diff, metadata_diff, transaction_list) |
| `namespace_storage` class        | DELETED (replaced by system tables) |
| `catalog` class (current)        | DELETED (replaced by resolve_catalog + DDL methods) |

### New Load Sequence

```
Startup (D4 + W4):
  1. load_system_tables_sync() — 9 system tables from checkpoint (or bootstrap)
  2. replay_system_wal_sync() — WAL entries for pg_catalog.* tables
  3. restore_oid_generator() — max(all OIDs in system tables) + 1
  4. Start schedulers
  5. User tables loaded on demand (D4 lazy):
       First access → resolve_catalog → scan pg_class → load_from_disk
  6. User DML WAL replayed on first access or batch at startup
```

### Tests

```
test_type_persistence_across_restart    -- CREATE TYPE -> restart -> type exists
test_function_persistence               -- CREATE FUNCTION -> restart -> callable
test_constraint_persistence             -- PRIMARY KEY -> restart -> enforced
test_sequence_persistence               -- CREATE SEQUENCE -> restart -> NEXTVAL works
test_view_persistence                   -- CREATE VIEW -> restart -> SELECT works
test_macro_persistence                  -- CREATE MACRO -> restart -> callable
test_index_metadata_in_system_table     -- indexes in pg_index
test_pg_class_lists_all_objects         -- SELECT * FROM pg_class -> all objects
test_load_sequence_correctness          -- full load cycle -> verify
test_catalog_otbx_not_needed            -- old format no longer required
```

---

## 10. Query Validation and Resolution Path

### Current Path (9 catalog calls for simple SELECT)

```
SELECT name, age FROM users WHERE age > 25

execute_plan():
  1. catalog.namespace_exists(["mydb"])              -- (1) namespace check
  2. catalog.table_exists(table_id("mydb","users"))  -- (2) table check
  3. catalog.table_computes(table_id("mydb","users"))-- (3) computing check
  4. catalog.get_table_schema(id).columns()          -- (4) schema fetch

  validate_schema():
  5. catalog.table_exists(id)                        -- (5) DUPLICATE
  6. catalog.get_table_schema(id).columns()          -- (6) DUPLICATE
  7. catalog.function_name_exists(">")               -- (7) operator lookup
  8. catalog.function_exists(">", [BIGINT, BIGINT])  -- (8) signature check
  9. catalog.get_function(">", [BIGINT, BIGINT])     -- (9) function resolve

Total: ~9 calls, 3-4 duplicated
```

### New Path (2-3 cache lookups)

```
SELECT name, age FROM users WHERE age > 25

execute_plan():
  query_context = { start_time: txn_manager.now() }

  validate_types():
    rel = syscache.get_relation("users", ns_oid)    -- (1) relcache lookup
      [MISS]: scan pg_class + pg_attribute + pg_type
              build relcache_entry_t
              populate cache
      [HIT]:  O(1) hash lookup
    -> rel contains: exists, computes, schema, constraints

  validate_schema():
    rel = syscache.get_relation("users", ns_oid)    -- [CACHE HIT from step 1]
    schema = rel->cached_schema
      OR: catalog.get_schema_at(id, start_time)     -- if DDL happened after start_time
    resolve "name" -> STRING from schema
    resolve "age"  -> BIGINT from schema

    func = syscache.get_function(">", [BIGINT, INT]) -- (2) function lookup
      [MISS]: scan pg_proc, match signatures
      [HIT]:  O(1)
    -> {function_uid, kernel_signature_t}

Total: 2 lookups (1 relcache + 1 function), 0 system table scans on warm cache
```

### Lookup Layers

```
Calling code (dispatcher, validate_logical_plan)
    |
    v
catalog API (facade -- signatures DO NOT CHANGE)
    |
    v
syscache (in-memory hash maps, O(1) hit)
    |  hit: return cached value
    |  miss: fall through
    v
system table scan (data_table_t with MVCC filter)
    |
    |  pg_class.scan(filter: relname=X AND relnamespace=Y)
    |  pg_attribute.scan(filter: attrelid=Z)
    |  pg_type.scan(filter: oid=W)
    |
    v
populate cache, return result
```

### MVCC Snapshot During Resolution

```
Timeline:
  T=100: Query Q1 starts (start_time=100)
  T=110: ALTER TABLE users ADD COLUMN email (commits at 110)
  T=120: Q1 continues validate_schema()

Q1 accesses syscache at T=120:
  - Cache version != catalog version (DDL happened)
  - Invalidate rel_cache for "users"
  - Scan pg_attribute WHERE attrelid=users_oid:
      row "name"  (insert_id=50)  -> 50 < 100 -> VISIBLE
      row "age"   (insert_id=50)  -> 50 < 100 -> VISIBLE
      row "email" (insert_id=110) -> 110 < 100? NO -> INVISIBLE
  - Q1 sees schema: [name, age] WITHOUT email (correct)

Query Q2 starts at T=130:
  - Scan pg_attribute: all three rows visible
  - Q2 sees schema: [name, age, email] (correct)
```

### Snapshot vs Cached Lookup Decision

```
DDL validation (CREATE/DROP/ALTER):
  -> syscache.get_relation()
  -> Cached, current version (sufficient for existence checks)

DML validation (SELECT/INSERT/UPDATE/DELETE):
  -> If cache_.last_mutation_time() < query.start_time:
       return cache_.get_relation(id)->cached_schema    -- fast path (99%)
  -> Else:
       return scan_schema_at(id, query.start_time)      -- slow path (1%)
```

---

## 11. Error Handling

Follows existing otterbrix patterns. No new error mechanisms introduced.

### Current Patterns (preserved in new catalog)

| Pattern | Where Used | Mechanism |
|---|---|---|
| cursor_t_ptr with error_code_t | dispatcher, executor | Primary error path — 16 error codes |
| void _sync() with trace logging | manager_disk_t startup methods | No error return, silent fail, log only |
| nullptr / {0,0} on failure | storage_scan, storage_append | Empty result = implicit error |
| No exceptions in services | disk, dispatcher, executor | Only expression validation throws |
| Log warning, continue | base_spaces.cpp startup | Graceful degradation |

### How New Catalog Methods Use These Patterns

#### resolve_table / resolve_function / resolve_type (V4)

Returns cursor_t_ptr — same as current validate_types / validate_schema.

```
dispatcher validate coroutine:
    auto result = co_await send(disk, resolve_table, "users", last_seen);
    if (result->is_error()) {
        co_return std::move(result);   // propagate to client
    }
    // use result data
```

Not found → cursor with error_code_t::collection_not_exists.
Namespace not found → cursor with error_code_t::database_not_exists.
Function not found → cursor with error_code_t::unrecognized_function.

#### DDL methods (ddl_create_table, ddl_drop_table, etc.)

Returns ddl_result_t containing cursor_t_ptr — same as execute_plan return.

```
ddl_create_table_impl():
    auto ns = scan pg_namespace WHERE oid=namespace_oid
    if (ns empty):
        return {make_cursor(resource, error_code_t::database_not_exists, "..."), {}, 0, 0}

    auto existing = scan pg_class WHERE relname=name AND relnamespace=ns_oid
    if (existing not empty):
        return {make_cursor(resource, error_code_t::collection_already_exists, "..."), {}, 0, 0}

    // ... proceed with INSERT, WAL write ...
    return {make_cursor(resource, operation_status_t::success), events, version, oid}
```

Error before WAL COMMIT → no WAL entry, no system table changes.
Error code in cursor → dispatcher returns to client.

#### bootstrap_system_tables_sync / load_system_tables_sync

void with trace logging — follows current create_storage_sync pattern.

```
void load_system_tables_sync():
    for (auto& name : system_table_names):
        trace(log_, "loading system table: {}", name)
        // load_from_disk — if fails, data_table_t not in storages_
        // subsequent resolve_table will return error for missing metadata
```

Known limitation: startup sync methods do not report errors explicitly.
If system table load fails, system operates with partial catalog — queries
to missing metadata return error cursors at query time.

#### WAL replay errors

void with trace logging — follows current direct_append_sync pattern.

```
replay_system_wal_sync():
    for each record:
        trace(log_, "replaying WAL record for {}", record.collection)
        direct_append_sync(name, data)  // void, silent fail
```

Known limitation: WAL replay failures are silent. If replay fails for a
system table entry, that metadata is lost. System continues with stale
checkpoint data.

### Known Limitations (inherited from current codebase)

| Limitation | Impact | Future Fix |
|---|---|---|
| void _sync() methods | Startup failures silent | Add return code to _sync() methods |
| WAL replay void | Lost entries on replay failure | Add error accumulator for replay |
| No disk full detection | WAL/checkpoint may fail silently | Add I/O error propagation |
| No retry on transient errors | Single attempt for all operations | Add retry policy for I/O |

These limitations exist in the current codebase and are preserved for
consistency. Fixing them is orthogonal to the catalog migration.

---

## 12. End-to-End Flow

Complete lifecycle showing all decisions working together:
B3 (all-async), D4 (eager catalog + lazy storage), W4 (WAL for DDL),
V4 (coroutine lazy resolve), S5 (batch resolve additional), O4 (disk allocates OIDs),
C1 (pg_computed_column), M0 (no migration).
Invalidation: per-object events + pull ring buffer.
Indexes: indisvalid column in pg_index.
Snapshot: per-transaction versioned cache with alias dedup + lazy GC.

### Phase 1: First Startup (Bootstrap)

```
base_spaces_t constructor:
    // No checkpoint files exist — first run

    PHASE 2.5: disk_ptr->bootstrap_system_tables_sync()
        Create 9 data_table_t with hardcoded schemas (disk-backed):
            pg_namespace, pg_class, pg_attribute, pg_type, pg_proc,
            pg_depend, pg_constraint, pg_index, pg_computed_column
        INSERT bootstrap rows:
            pg_namespace: (oid=1, "pg_catalog"), (oid=2, "public")
            pg_class: 9 rows (self-describing)
            pg_attribute: ~60 rows (columns of all 9 tables)
            pg_type: bool(20), int2(21), int4(23), int8(26), text(25), float8(701)
            pg_proc: sum(101), min(102), max(103), count(104), avg(105)
        Checkpoint all 9 tables + fsync
        restore_oid_generator() → next_oid = 16384

    PHASE 3: sync addresses
        manager_dispatcher_->sync(disk_addr, index_addr, wal_addr)

    PHASE 4: scheduler_->start()
        // Actor system now async
        // storages_ has 9 system tables, 0 user tables
        // versioned_plan_cache_ empty
        // ring_buffer_ empty
```

### Phase 2: CREATE DATABASE + CREATE TABLE

```
User: CREATE DATABASE mydb

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            encounter namespace "mydb":
                cache miss → co_await send(disk, resolve_namespace, "mydb")
                → not found → OK for CREATE (namespace should NOT exist)

        // DDL through executor (unified pipeline)
        co_await send(executor, execute_ddl_plan, plan)

            executor:
                txn = txn_manager.begin_transaction(session)

                ddl_result = co_await send(disk, ddl_create_namespace, "mydb", owner=10)
                    disk: oid = oid_gen_.next() → 16384
                    disk: INSERT pg_namespace (16384, "mydb", 10)
                    disk: ring_buffer_.append({NAMESPACE_CREATED, 16384, 0})
                    disk: catalog_version_ = 1
                    disk: return {success, events, version=1, oid=16384}

                co_await send(wal, write_physical_insert, "pg_catalog.pg_namespace", row, txn.id)
                co_await send(wal, commit_txn, txn.id)

                return {ddl_result, events}

        dispatcher: process_events → cache invalidated for namespaces
        co_return success

User: CREATE TABLE mydb.users (id BIGINT NOT NULL, name TEXT, age BIGINT)

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            encounter "mydb.users":
                cache miss → co_await send(disk, resolve_table, "mydb.users")
                → namespace "mydb" found (oid=16384), table "users" NOT found
                → return cursor with collection_not_exists
                → OK for CREATE (table should NOT exist)

            column types: BIGINT(26), TEXT(25), BIGINT(26) — built-in, no resolve needed

        // DDL through executor (DuckDB-style unified pipeline)
        co_await send(executor, execute_ddl_plan, plan)

            executor (same actor that handles INSERT/SELECT):
                txn = txn_manager.begin_transaction(session)

                // Step 1: DDL metadata → disk
                ddl_result = co_await send(disk, ddl_create_table,
                    namespace_oid=16384, name="users", relkind='r',
                    columns=[{id, 26, true}, {name, 25, false}, {age, 26, false}],
                    constraints=[{contype="p", columns=["id"]}])

                    disk: table_oid = oid_gen_.next() → 16385
                    disk: constraint_oid = oid_gen_.next() → 16386
                    disk: INSERT pg_class (16385, "users", 16384, "r", 0.0, 0, 0)
                    disk: INSERT pg_attribute (16385, "id", 26, 1, true, 0, "")
                    disk: INSERT pg_attribute (16385, "name", 25, 2, false, 0, "")
                    disk: INSERT pg_attribute (16385, "age", 26, 3, false, 0, "")
                    disk: INSERT pg_constraint (16386, "pk_users", 16385, "p", "1")
                    disk: INSERT pg_depend (pg_class, 16385, 0, pg_namespace, 16384, 0, "n")
                    disk: ring_buffer_.append({TABLE_CREATED, 16385, 16384})
                    disk: catalog_version_ = 2
                    disk: return {success, events, version=2, oid=16385}

                // Step 2: WAL (same path as DML)
                co_await send(wal, write_physical_insert, "pg_catalog.pg_class", row, txn.id)
                co_await send(wal, write_physical_insert, "pg_catalog.pg_attribute", rows, txn.id)
                co_await send(wal, write_physical_insert, "pg_catalog.pg_constraint", row, txn.id)
                co_await send(wal, write_physical_insert, "pg_catalog.pg_depend", rows, txn.id)
                co_await send(wal, commit_txn, txn.id)

                // Step 3: Create storage + register
                co_await send(disk, create_storage_with_columns, "mydb.users", columns)
                co_await send(index, register_collection, "mydb.users")

                return {ddl_result, events}

        dispatcher: process_events → cache invalidated
        co_return success
```

### Phase 3: INSERT + SELECT

```
User: INSERT INTO mydb.users VALUES (1, 'Alice', 30), (2, 'Bob', 25)

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            encounter "mydb.users":
                cache miss → co_await send(disk, resolve_table, "mydb.users")
                → found: oid=16385, relkind='r', schema=[id,name,age], indexes=[]
                → cache.add("mydb.users", version=2, result)

        co_await validate_schema(cache, disk_addr, plan):
            "mydb.users" → cache HIT (just added)
            columns match → OK

        co_await execute_on_executor(physical_plan)
            executor: operator_insert → send(disk, storage_append, "mydb.users", data)
            executor: send(index, insert_rows, "mydb.users", data, start_row=0, count=2)
            executor: WAL write_physical_insert
        co_return success (2 rows inserted)

User: SELECT name, age FROM mydb.users WHERE age > 25

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            "mydb.users" → cache HIT (version=2) → 0 roundtrips

        co_await validate_schema(cache, disk_addr, plan):
            "mydb.users" → cache HIT
            "name" → found in cached schema
            "age" → found in cached schema
            ">" → built-in operator, no catalog lookup
            → 0 roundtrips

        co_await execute_on_executor(physical_plan)
            executor: full_scan → send(disk, storage_scan, "mydb.users", filter: age>25)
            → returns: [(1, 'Alice', 30)]
        co_return cursor with 1 row
```

### Phase 4: ALTER TABLE

```
User: ALTER TABLE mydb.users ADD COLUMN email TEXT

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            "mydb.users" → cache HIT → exists, OK for ALTER

        // DDL through executor (unified pipeline)
        co_await send(executor, execute_ddl_plan, plan)

            executor:
                txn = txn_manager.begin_transaction(session)

                ddl_result = co_await send(disk, ddl_add_column,
                    table_oid=16385, column={email, 25, false})
                    disk: attnum = max(pg_attribute WHERE attrelid=16385).attnum + 1 = 4
                    disk: INSERT pg_attribute (16385, "email", 25, 4, false, 0, "")
                    disk: INSERT pg_depend (column→type)
                    disk: ring_buffer_.append({TABLE_ALTERED, 16385, 16384})
                    disk: catalog_version_ = 3
                    disk: return {success, events, version=3}

                co_await send(wal, write_physical_insert, "pg_catalog.pg_attribute", row, txn.id)
                co_await send(wal, commit_txn, txn.id)

                return {ddl_result, events}

        dispatcher: process_events → invalidate "mydb.users" in cache
        co_return success

User: SELECT * FROM mydb.users

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            "mydb.users" → cache MISS (invalidated by ALTER events)
            → co_await send(disk, resolve_table, "mydb.users")
            → schema now: [id, name, age, email] (4 columns)
            → cache.add version=3

        co_await validate_schema → cache HIT → 0 roundtrips
        co_await execute_on_executor → full_scan
        → returns: [(1,'Alice',30,NULL), (2,'Bob',25,NULL)]
        co_return cursor with 2 rows
```

### Phase 5: CREATE INDEX

```
User: CREATE INDEX idx_age ON mydb.users(age)

    dispatcher::execute_plan():
        // DDL through executor (unified pipeline, same as current code)
        co_await send(executor, execute_ddl_plan, plan)

            executor:
                txn = txn_manager.begin_transaction(session)

                // Step 1: Register in catalog (indisvalid=false)
                ddl_result = co_await send(disk, ddl_create_index,
                    table_oid=16385, index={name="idx_age", type="single",
                                            columns=["age"], unique=false, primary=false})
                    disk: index_oid = oid_gen_.next() → 16387
                    disk: INSERT pg_class (16387, "idx_age", 16384, "i", 0.0, 0, 0)
                    disk: INSERT pg_index (16387, 16385, false, false, "single", "3",
                                           indisvalid=false, indisready=true)
                    disk: INSERT pg_depend (index→table, deptype='a')
                    disk: ring_buffer_ + version=4
                    disk: return {success, events, version=4, oid=16387}

                // Step 2: WAL for catalog changes
                co_await send(wal, write_physical_insert, "pg_catalog.pg_class", row, txn.id)
                co_await send(wal, write_physical_insert, "pg_catalog.pg_index", row, txn.id)
                co_await send(wal, commit_txn, txn.id)

                // Step 3: Build btree (DML writes via indisready=true)
                co_await send(index, build_btree, "mydb.users", "idx_age", keys=[age])
                    index: scan all rows → build btree → persist

                // Step 4: Mark valid
                co_await send(disk, ddl_index_set_valid, index_oid=16387)
                    disk: DELETE old pg_index row, INSERT new with indisvalid=true
                    disk: ring_buffer_ + version=5

                co_await send(wal, write_physical_insert, "pg_catalog.pg_index", new_row, txn.id)
                co_await send(wal, commit_txn, txn.id)

                return {ddl_result, events}

        dispatcher: process_events → cache invalidated
        co_return success
```

### Phase 6: DROP TABLE

```
User: DROP TABLE mydb.users CASCADE

    dispatcher::execute_plan():
        co_await validate_types(cache, disk_addr, plan):
            "mydb.users" → cache miss → co_await resolve_table
            → found, oid=16385

        // DDL through executor (unified pipeline)
        co_await send(executor, execute_ddl_plan, plan)

            executor:
                txn = txn_manager.begin_transaction(session)

                ddl_result = co_await send(disk, ddl_drop_table, table_oid=16385, cascade=true)
                    disk: scan pg_depend WHERE refobjid=16385
                        → found: idx_age (16387, deptype='a')
                    disk: CASCADE: drop index 16387
                        DELETE pg_index WHERE indexrelid=16387
                        DELETE pg_depend WHERE objid=16387
                        DELETE pg_class WHERE oid=16387
                    disk: DROP table 16385
                        DELETE pg_constraint WHERE conrelid=16385
                        DELETE pg_attribute WHERE attrelid=16385
                        DELETE pg_depend WHERE objid=16385 OR refobjid=16385
                        DELETE pg_class WHERE oid=16385
                    disk: if storages_.contains("mydb.users"): remove
                    disk: ring_buffer_.append([
                        {INDEX_DROPPED, 16387, 16385},
                        {TABLE_DROPPED, 16385, 16384}])
                    disk: catalog_version_ = 6
                    disk: return {success, events, version=6}

                // WAL for all catalog deletes
                co_await send(wal, write_physical_delete, "pg_catalog.pg_index", row_ids, txn.id)
                co_await send(wal, write_physical_delete, "pg_catalog.pg_depend", row_ids, txn.id)
                co_await send(wal, write_physical_delete, "pg_catalog.pg_class", row_ids, txn.id)
                co_await send(wal, write_physical_delete, "pg_catalog.pg_constraint", row_ids, txn.id)
                co_await send(wal, write_physical_delete, "pg_catalog.pg_attribute", row_ids, txn.id)
                co_await send(wal, commit_txn, txn.id)

                co_await send(disk, drop_storage, "mydb.users")
                co_await send(index, unregister_collection, "mydb.users")

                return {ddl_result, events}

        dispatcher: process_events → cache invalidated
        co_return success
```

### Phase 7: CHECKPOINT

```
User: CHECKPOINT

    dispatcher::execute_plan():
        co_await send(index, flush_all_indexes)
        wal_max_id = co_await send(wal, current_wal_id)

        co_await send(disk, checkpoint_all, wal_max_id)
            disk: checkpoint system tables FIRST:
                pg_namespace.checkpoint()    // 2 rows
                pg_class.checkpoint()        // 9 system + 0 user (table dropped)
                pg_attribute.checkpoint()    // ~60 rows (system only)
                pg_type.checkpoint()
                pg_proc.checkpoint()
                pg_depend.checkpoint()       // system deps only
                pg_constraint.checkpoint()
                pg_index.checkpoint()
                pg_computed_column.checkpoint()

            disk: checkpoint user tables (DISK mode):
                // "mydb.users" was dropped — not in storages_

            disk: record WAL position
            disk: return checkpoint_wal_id

        co_await send(wal, truncate_before, checkpoint_wal_id)
        co_return success
```

### Phase 8: Restart

```
Process restart (clean shutdown or crash):

    base_spaces_t constructor:
        PHASE 2.5: disk_ptr->load_system_tables_sync()
            load_from_disk() for 9 system tables from checkpoint
            // pg_class: 9 system rows (users dropped, not in catalog)
            // pg_attribute: system columns only
            // pg_namespace: pg_catalog + public + mydb
            restore_oid_generator() → max_oid=16387 → next=16388

        PHASE 2.5b: disk_ptr->replay_system_wal_sync()
            // If crash: replay WAL entries since last checkpoint
            // If clean shutdown: WAL truncated, nothing to replay

        PHASE 3: sync addresses
        PHASE 4: scheduler_->start()
            // storages_: 9 system tables only
            // versioned_plan_cache_: empty (cold start)
            // ring_buffer_: empty

        PHASE 5: index recreation (if needed)

    First query after restart:
        SELECT 1 FROM mydb.users → cache miss → resolve_table
            → pg_class scan → "users" NOT FOUND
            → error: "collection does not exist"
            // Correct: table was dropped before checkpoint
```

---

## 13. Risk Registry

| ID    | Risk                                      | Severity | Phase | Mitigation                                |
|-------|-------------------------------------------|----------|-------|-------------------------------------------|
| R1    | OID collision after restart               | LOW      | 0     | Restore generator from max(all OIDs) + 1  |
| R2    | Performance regression from table scans   | HIGH     | 1     | Syscache (Phase 2) mitigates completely    |
| R3    | Uncommitted delete visibility bug (E2.1A) | CRITICAL | 3     | Use committed_version_operator for reads (CONFIRMED in validation) |
| R4    | Schema snapshot not implemented            | HIGH     | 3     | get_schema_at() with MVCC filter           |
| R5    | attnum reuse after DROP COLUMN            | HIGH     | 3     | Never decrement next_column_id             |
| R6    | Physical column data survives DROP        | HIGH     | 3     | Schema snapshot -> skip dropped columns    |
| R7    | Lost DDL updates (concurrent ALTER)       | MEDIUM   | 3     | Serialize DDL per table (pg_class lock)    |
| R8    | Bootstrap circular dependency             | MEDIUM   | 1     | Self-description is data, not structure    |
| R9    | Types/functions lost on restart           | HIGH     | 5     | Persist in pg_type/pg_proc                 |
| R10   | Constraints not enforced after restart    | HIGH     | 5     | Persist in pg_constraint                   |
| R11   | CASCADE/RESTRICT not implemented          | MEDIUM   | 4     | pg_depend with graph traversal             |
| R12   | Cache thrashing under DDL-heavy workload  | LOW      | 2     | Fine-grained invalidation by entity type   |

---

## 14. Complete Test Plan

All tests collected from document sections. Total: 139 tests.

### OID System (9 tests)

```
test_oid_generation_uniqueness       -- 1000 OIDs without collision
test_oid_immutability                -- double set_oid raises error
test_table_oid_assignment            -- create_table assigns OID
test_column_oid_assignment           -- columns get unique OIDs
test_oid_persistence                 -- checkpoint -> load -> same OID
test_oid_no_reuse_after_drop         -- dropped OID never reassigned
test_schema_evolution_oid_stability  -- rename column preserves OID
test_get_table_by_oid                -- lookup by OID finds correct table
test_oid_generator_restore           -- generator starts at max+1 after load
```

### Bootstrap (6 tests)

```
test_bootstrap_creates_all_tables       -- 9 tables exist after bootstrap
test_pg_class_self_describing           -- pg_class contains row for pg_class
test_pg_attribute_describes_pg_class    -- pg_attribute has pg_class columns
test_pg_type_has_builtins               -- bool, int4, int8, text, float8 present
test_pg_namespace_has_pg_catalog        -- pg_catalog namespace exists
test_bootstrap_row_count                -- correct number of rows per table
```

### D4 Lazy Loading (8 tests)

```
test_scan_pg_attribute_by_relid         -- filter by attrelid returns correct columns
test_append_user_table_to_pg_class      -- INSERT user table metadata into pg_class
test_user_table_not_in_storages_at_start -- storages_ has only 9 system tables
test_first_select_creates_storage        -- SELECT triggers on-demand load
test_second_select_uses_existing         -- no reload on second access
test_drop_unloaded_table                 -- DROP modifies pg_class only
test_alter_unloaded_table                -- ALTER modifies pg_attribute only
test_show_tables_from_pg_class           -- lists all tables including unloaded
```

### WAL + Checkpoint (12 tests)

```
test_system_table_checkpoint_load        -- checkpoint -> load -> data preserved
test_ddl_wal_logged                      -- CREATE TABLE generates WAL entries
test_ddl_crash_recovery                  -- crash after COMMIT -> DDL recovered from WAL
test_ddl_crash_before_commit             -- crash before COMMIT -> DDL rolled back
test_batch_ddl_single_commit             -- BEGIN + N DDL + COMMIT -> 1 WAL commit
test_batch_ddl_rollback                  -- BEGIN + N DDL + ROLLBACK -> no WAL entries
test_wal_replay_order                    -- system DDL replayed before user DML
test_checkpoint_truncates_wal            -- CHECKPOINT -> old WAL entries removed
test_auto_checkpoint_on_wal_size         -- WAL exceeds threshold -> auto checkpoint
test_checkpoint_system_tables_first      -- system tables checkpointed before user tables
test_checkpoint_crash_mid_system         -- crash during system checkpoint -> last good
test_checkpoint_crash_after_system       -- system OK, user tables recovered from WAL
```

### Ring Buffer (5 tests)

```
test_ring_buffer_events_since            -- returns events after given version
test_ring_buffer_overflow                -- old version -> OVERFLOW signal -> full reset
test_ring_buffer_empty_after_restart     -- restart -> buffer empty -> full resolve
test_ring_buffer_capacity                -- 4096 events before overflow
test_ring_buffer_monotonic_version       -- versions always increasing
```

### Recovery (3 tests)

```
test_recovery_system_wal_before_user     -- system DDL WAL replayed first
test_recovery_ring_buffer_empty          -- after recovery, dispatchers full resolve
test_recovery_ddl_then_dml              -- CREATE TABLE + INSERT in WAL -> both recovered
```

### DDL Methods (17 tests)

```
test_ddl_create_table_assigns_oid       -- table_oid allocated by disk
test_ddl_create_table_columns_attnum    -- attnum assigned sequentially 1..N
test_ddl_create_table_pg_depend         -- dependencies auto-generated
test_ddl_drop_table_cascade             -- drops indexes, constraints, depends
test_ddl_drop_table_restrict            -- fails if dependents exist
test_ddl_add_column_next_attnum         -- new column gets max(attnum)+1
test_ddl_drop_column_preserves_attnum   -- other columns attnum unchanged
test_ddl_rename_column_preserves_attnum -- same attnum, new name
test_ddl_create_index_indisvalid_false  -- created with indisvalid=false
test_ddl_index_set_valid                -- updates to indisvalid=true
test_ddl_drop_index_removes_pg_class    -- removes both pg_index and pg_class
test_ddl_create_type_assigns_oid        -- type_oid allocated by disk
test_ddl_drop_type_cascade              -- drops columns using type
test_ddl_create_function_assigns_oid    -- func_oid allocated by disk
test_ddl_drop_namespace_cascade         -- drops all objects inside
test_ddl_events_in_response             -- invalidation events returned
test_ddl_oid_generator_restart          -- after restart, oid_gen starts at max+1
```

### Resolve + Scan (6 tests)

```
test_resolve_batch_scan_3_tables        -- S5: 6 scans for 3 tables (batch IN filter)
test_resolve_sequential_fallback        -- S5: force sequential, 18 scans for 3 tables
test_resolve_single_table_uses_equal    -- 1 table uses EQUAL filter
test_resolve_includes_indexed_keys      -- pg_index data in resolve response
test_resolve_skips_invalid_indexes      -- indisvalid=false excluded
test_resolve_lazy_load_creates_storage  -- first resolve triggers load_from_disk
```

### Computing Tables (9 tests)

```
test_ddl_create_computing_table         -- pg_class relkind='c', no pg_attribute rows
test_ddl_computed_append_field          -- INSERT pg_computed_column with version=1
test_ddl_computed_append_same_field     -- second type for same field -> version=2
test_ddl_computed_drop_field            -- attrefcount decremented, deleted if 0
test_ddl_computed_latest_types          -- latest version per field returned
test_ddl_adopt_computing_schema         -- pg_computed_column -> pg_attribute, 'c'->'r'
test_computing_table_persists_restart   -- computing table survives crash
test_resolve_computing_table            -- resolve returns versioned fields for relkind='c'
test_computing_table_pg_attribute_empty -- relkind='c' has no pg_attribute rows
```

### Versioned Cache GC (14 tests)

```
test_cache_gc_removes_old_versions           -- commit removes versions below lowest
test_cache_gc_keeps_active_version           -- active txn version not removed
test_cache_gc_lazy_triggers_at_soft_limit    -- GC runs when memory hits 75%
test_cache_gc_every_commit_fallback          -- GC at every commit when configured
test_cache_alias_no_ddl_same_data            -- no DDL -> versions share data pointer
test_cache_alias_ddl_creates_new_data        -- DDL -> new data version
test_cache_alias_ref_count_correct           -- ref count matches version count
test_cache_memory_limit_evicts               -- exceeding max_memory triggers eviction
test_cache_max_versions_evicts               -- exceeding max_data_versions triggers eviction
test_cache_eviction_does_not_evict_active    -- active txn version never evicted
test_cache_restart_empty                     -- after restart, cache empty
test_cache_first_query_populates             -- first query fills cache from resolve
test_cache_long_txn_holds_version            -- long txn prevents GC of its version
test_cache_concurrent_txns_snapshots         -- each txn sees its own schema version
```

### V4 Coroutine Resolve (6 tests)

```
test_v4_cache_hit_zero_roundtrips        -- warm cache -> 0 co_await
test_v4_cache_miss_one_roundtrip         -- cold -> co_await resolve_table
test_v4_incremental_warming              -- second query fewer roundtrips than first
test_v4_function_resolve_on_demand       -- function resolved when encountered
test_v4_validate_is_coroutine            -- validate_types/schema use co_await
test_v4_mixed_hit_miss                   -- some tables cached, some not
```

### MVCC DDL (10 tests)

```
test_ddl_mvcc_visibility            -- CREATE TABLE invisible until commit
test_ddl_rollback_cleans_up         -- ROLLBACK -> system rows invisible
test_concurrent_ddl_dml             -- INSERT during ALTER TABLE
test_schema_snapshot_isolation      -- query sees schema at start_time
test_attnum_never_reused            -- DROP + ADD -> new attnum
test_multi_table_ddl_atomicity      -- CREATE TABLE = pg_class + pg_attribute
test_uncommitted_delete_invisible   -- fix for bug E2.1A
test_self_read_own_writes           -- DDL sees own changes within txn
test_drop_cascade_mvcc              -- CASCADE delete via MVCC
test_alter_rename_column_mvcc       -- DELETE old + INSERT new, same attnum
```

### pg_depend CASCADE/RESTRICT (8 tests)

```
test_drop_restrict_table_with_index     -- RESTRICT fails if indexes exist
test_drop_cascade_table_with_index      -- CASCADE drops indexes
test_drop_restrict_type_with_columns    -- RESTRICT fails if type used in column
test_drop_cascade_namespace             -- CASCADE deletes everything inside
test_pg_depend_created_on_create_table  -- dependencies recorded
test_pg_depend_cleaned_on_drop          -- dependencies removed
test_drop_restrict_function_in_check    -- function in CHECK constraint
test_circular_dependency_detection      -- no cycles in dependency graph
```

### Persistence (10 tests)

```
test_type_persistence_across_restart    -- CREATE TYPE -> restart -> exists
test_function_persistence               -- CREATE FUNCTION -> restart -> callable
test_constraint_persistence             -- PRIMARY KEY -> restart -> enforced
test_sequence_persistence               -- SEQUENCE -> restart -> NEXTVAL works
test_view_persistence                   -- VIEW -> restart -> SELECT works
test_macro_persistence                  -- MACRO -> restart -> callable
test_index_metadata_in_system_table     -- indexes in pg_index
test_pg_class_lists_all_objects         -- all objects in pg_class
test_load_sequence_correctness          -- full load cycle -> verify
test_catalog_otbx_not_needed            -- old format no longer required
```

### Error Handling (16 tests)

```
test_error_resolve_table_not_found         -- returns error cursor, not crash
test_error_resolve_namespace_not_found     -- returns error cursor
test_error_resolve_function_no_match       -- no matching signature -> error
test_error_resolve_type_not_found          -- returns error cursor
test_error_resolve_corrupted_storage       -- load_from_disk fails -> error
test_error_ring_buffer_overflow_resets     -- full cache reset, re-resolve works
test_error_ddl_table_already_exists        -- returns error, no WAL entry
test_error_ddl_namespace_missing           -- returns error, no changes
test_error_ddl_drop_restrict_blocked       -- dependents exist -> error
test_error_ddl_partial_no_wal              -- error before COMMIT -> no WAL
test_error_ddl_wal_write_failure           -- disk full -> error, no changes
test_error_startup_corrupted_checkpoint    -- fatal, abort
test_error_startup_truncated_wal           -- truncate at last valid, continue
test_error_startup_wal_replay_failure      -- fatal, abort
test_error_cache_evicted_re_resolves       -- transparent re-resolve
test_error_cache_version_mismatch_reset    -- full reset on inconsistency
```

### Summary

| Category | Count |
|---|---|
| OID System | 9 |
| Bootstrap | 6 |
| D4 Lazy Loading | 8 |
| WAL + Checkpoint | 12 |
| Ring Buffer | 5 |
| Recovery | 3 |
| DDL Methods | 17 |
| Resolve + Scan | 6 |
| Computing Tables | 9 |
| Versioned Cache GC | 14 |
| V4 Coroutine Resolve | 6 |
| MVCC DDL | 10 |
| pg_depend CASCADE | 8 |
| Persistence | 10 |
| Error Handling | 16 |
| **Total** | **139** |

---

## Appendix A: Breaking Changes from Current Code

Verified against codebase (commit 4443c98). Each item shows current behavior
and planned replacement. All changes are intentional — the document describes
the TARGET architecture, not current code.

### 1. CREATE TABLE: dispatcher direct → executor pipeline

| | Current (dispatcher.cpp:495-561) | Target |
|---|---|---|
| Path | Dispatcher → disk directly (append_collection) | Dispatcher → executor → disk (ddl_create_table) + wal |
| Catalog write | `append_collection` → catalog.otbx entry | `ddl_create_table` → INSERT pg_class + pg_attribute |
| WAL | Not written for DDL | Executor writes WAL (same as DML) |
| Validation | Inline `check_collection_exists` + UNKNOWN type resolution | V4 coroutine `co_await validate_types` in dispatcher |
| In-memory update | `update_catalog(plan)` → sync catalog_ field | None — system tables ARE the catalog |

### 2. ALL DDL through executor (DuckDB-style unified pipeline)

| | Current | Target |
|---|---|---|
| CREATE TABLE | Dispatcher → disk directly (no executor) | Dispatcher → executor → disk + wal |
| CREATE INDEX | Executor → index (dispatcher does nothing) | Executor → disk (pg_index) + index (btree) + wal |
| DROP TABLE | Dispatcher → disk directly | Dispatcher → executor → disk + wal |
| WAL for DDL | Not written (DDL bypasses WAL) | Executor writes WAL (same path as DML) |
| Reason | Unified pipeline: one WAL writer, one txn_manager, one abort/commit path |

Executor already owns CREATE INDEX (executor.cpp:96-144). Now all DDL goes
through executor. Dispatcher creates logical plan, sends to executor pool.
Executor sends to disk (ddl_*), wal (write_physical_*), index (build_btree).

### 3. CHECKPOINT: unordered → system tables first

| | Current (manager_disk.cpp:508-529) | Target |
|---|---|---|
| Order | `storages_` map iteration (unordered) | System tables (pg_catalog.*) first, then user tables |
| Reason | Crash mid-checkpoint: system tables must be consistent for recovery |

### 4. WAL RECOVERY: parallel unordered → system first then parallel

| | Current (base_spaces.cpp:399-449) | Target |
|---|---|---|
| Grouping | By collection name, parallel threads | Phase 1: system tables (sequential). Phase 2: user tables (parallel) |
| Ordering | No system-before-user guarantee | System DDL replayed before user DML |
| Reason | User DML may reference tables created by DDL in same WAL |

Parallel replay for user tables is preserved (efficient).

### 5. STORAGE LOADING: nullptr → D4 lazy load

| | Current (manager_disk.cpp:903-910) | Target |
|---|---|---|
| On miss | `get_storage()` returns nullptr, error logged | `resolve_table()` calls `load_from_disk()`, adds to storages_ |
| Startup | All tables loaded via `create_storage_sync` / `load_storage_disk_sync` | Only 9 system tables loaded; user tables on demand |
| Reason | D4: startup <10ms regardless of table count |

### 6. VALIDATION: inline per-type → V4 generic coroutines

| | Current (dispatcher.cpp:245-260) | Target |
|---|---|---|
| CREATE TABLE | Inline `check_collection_exists` + UNKNOWN type resolution loop | `co_await validate_types(cache, disk_addr, plan)` |
| SELECT/INSERT | `validate_types(catalog_, plan)` + `validate_schema(catalog_, plan)` — sync | `co_await validate_types(...)` + `co_await validate_schema(...)` — coroutines |
| Catalog access | `catalog_` field (sync, in-memory) | `co_await resolve_table/function/type` on cache miss |
| Reason | V4: uniform path for DDL and DML, no special cases |

---

## Appendix B: Implementation Plan

This is NOT an incremental refactor. It is a full replacement: delete old code
first, write all new code, build only when everything is ready.

### Step 1: DELETE Legacy Code

Remove all old catalog-related code before writing anything new.

```
DELETE files:
  components/catalog/catalog.hpp                    -- old catalog class
  components/catalog/catalog.cpp
  components/catalog/namespace_storage.hpp           -- old namespace trie
  components/catalog/namespace_storage.cpp
  components/catalog/transaction/transaction_scope.hpp
  components/catalog/transaction/transaction_scope.cpp
  components/catalog/transaction/metadata_transaction.hpp
  components/catalog/transaction/metadata_transaction.cpp
  components/catalog/transaction/schema_diff.hpp
  components/catalog/transaction/schema_diff.cpp
  components/catalog/transaction/metadata_diff.hpp
  components/catalog/transaction/metadata_diff.cpp
  components/catalog/transaction/transaction_list.hpp
  components/catalog/transaction/transaction_list.cpp
  components/catalog/catalog_error.hpp               -- replaced by cursor errors
  components/catalog/catalog_error.cpp
  services/disk/catalog_storage.hpp                  -- old binary format
  services/disk/catalog_storage.cpp

DELETE from files (partial removal):
  services/disk/manager_disk.hpp                     -- remove catalog_*_entry_t references
  services/disk/agent_disk.hpp                       -- remove update_catalog_schemas
  services/disk/agent_disk.cpp
  services/dispatcher/dispatcher.hpp                 -- remove catalog_ field
  services/dispatcher/dispatcher.cpp                 -- remove sync catalog calls
  services/dispatcher/validate_logical_plan.hpp      -- remove catalog& parameter
  services/dispatcher/validate_logical_plan.cpp
  integration/cpp/base_spaces.cpp                    -- remove old load sequence
  services/index/manager_index.cpp                   -- remove write/read_indexes_from_metafile

KEEP (will be modified):
  components/catalog/table_id.hpp/cpp                -- add OID field
  components/catalog/table_metadata.hpp/cpp          -- add OID field
  components/catalog/schema.hpp/cpp                  -- add column OIDs
  components/catalog/catalog_types.hpp/cpp           -- keep field_id_t, add types
  components/catalog/computed_schema.hpp/cpp          -- adapt to new system
  components/table/column_definition.hpp/cpp         -- type oid fields
```

### Step 2: WRITE Tests (139 tests, do NOT compile yet)

Write all test files first. Tests define expected behavior — they serve as
specification before implementation exists.

```
2.T  Write all 139 tests from Section 15 (Complete Test Plan)

     File locations:
       components/catalog/tests/test_oids.cpp            -- OID System (9 tests)
       components/catalog/tests/test_bootstrap.cpp        -- Bootstrap (6 tests)
       components/catalog/tests/test_computing.cpp        -- Computing Tables (9 tests)
       services/disk/tests/test_system_tables.cpp         -- D4 Lazy Loading (8 tests)
       services/disk/tests/test_ddl_methods.cpp           -- DDL Methods (17 tests)
       services/disk/tests/test_resolve.cpp               -- Resolve + Scan (6 tests)
       services/disk/tests/test_wal_catalog.cpp           -- WAL + Checkpoint (12 tests)
       services/disk/tests/test_ring_buffer.cpp           -- Ring Buffer (5 tests)
       services/disk/tests/test_recovery.cpp              -- Recovery (3 tests)
       services/dispatcher/tests/test_plan_cache.cpp      -- Versioned Cache GC (14 tests)
       services/dispatcher/tests/test_v4_resolve.cpp      -- V4 Coroutine Resolve (6 tests)
       services/dispatcher/tests/test_mvcc_ddl.cpp        -- MVCC DDL (10 tests)
       services/dispatcher/tests/test_pg_depend.cpp       -- CASCADE/RESTRICT (8 tests)
       services/dispatcher/tests/test_persistence.cpp     -- Persistence (10 tests)
       services/dispatcher/tests/test_error_handling.cpp  -- Error Handling (16 tests)

     Tests reference types and methods that don't exist yet — expected.
     Tests do NOT compile at this step — that's intentional.
     Tests define the contract that implementation must satisfy.
```

### Step 3: WRITE Implementation Code (order of file creation)

```
3.0  components/catalog/catalog_oids.hpp
       oid_t type, oid_generator, well-known OID constants (pg_class=11, etc.)
       FIRST — everything else depends on this

3.1  components/catalog/system_table_schemas.hpp
       Hardcoded column_definition_t vectors for 9 system tables
       pg_class_schema, pg_attribute_schema, pg_type_schema,
       pg_computed_column_schema, ...

3.2  services/disk/manager_disk_t — bootstrap methods
       bootstrap_system_tables_sync()
       load_system_tables_sync()
       restore_oid_generator()
       DEPENDS ON: 3.0, 3.1

3.3  components/table/column_state.hpp — new filter type
       set_membership_filter_t (IN filter for batch scan)
       DEPENDS ON: nothing new (extends existing filter hierarchy)

3.4  services/disk/manager_disk_t — per-item resolve (V4) + batch resolve (S5)
       resolve_table(), resolve_function(), resolve_type(), resolve_namespace()
       resolve_tables_batch(), resolve_functions_batch()
       batch_scan() helper
       D4 lazy load: create_storage on demand
       DEPENDS ON: 3.2 (system tables in storages_), 3.3 (IN filter)

3.5  services/disk/invalidation_ring_buffer.hpp
       invalidation_event_t, invalidation_ring_buffer_t
       events_since(), append(), overflow detection
       DEPENDS ON: 3.0 (oid_t)

3.6  services/disk/manager_disk_t — DDL methods
       ddl_create_namespace, ddl_drop_namespace
       ddl_create_table, ddl_drop_table
       ddl_add_column, ddl_drop_column, ddl_rename_column
       ddl_create_index, ddl_index_set_valid, ddl_drop_index
       ddl_create_type, ddl_drop_type
       ddl_create_function, ddl_drop_function
       ddl_create_computing_table, ddl_computed_append, ddl_computed_drop
       ddl_adopt_computing_schema
       All use O4 (disk allocates OIDs), append ring buffer
       NOTE: disk does NOT write WAL — executor writes WAL separately (Variant E)
       DEPENDS ON: 3.2, 3.4, 3.5

3.7  services/disk/manager_disk_t — WAL integration
       WAL write for system table mutations (W4)
       replay_system_wal_sync() for recovery
       Checkpoint ordering: system tables FIRST
       DEPENDS ON: 3.6 (DDL methods generate WAL entries)

3.8  services/disk/manager_disk_t — pg_depend logic
       CASCADE/RESTRICT traversal in ddl_drop_* methods
       populate pg_depend on every CREATE
       DEPENDS ON: 3.6

3.9  services/dispatcher/versioned_plan_cache.hpp
       versioned_plan_cache_t with alias dedup, hard limits, lazy GC
       pin_version, unpin_version, get, populate, gc
       DEPENDS ON: 3.0 (oid_t), 3.4 (resolve response types)

3.10 services/dispatcher/dispatcher.hpp/cpp — refactor
       Remove catalog_ field
       Add versioned_plan_cache_
       execute_plan: co_await validate_types → co_await validate_schema (V4)
       DDL: co_await ddl_* methods → process invalidation events
       begin/commit/abort: pin/unpin cache versions
       DEPENDS ON: 3.4, 3.6, 3.9

3.11 services/dispatcher/validate_logical_plan.hpp/cpp — refactor
       Convert to coroutines (V4: lazy resolve on demand)
       validate_types(cache, disk_address, plan) → coroutine, co_await on cache miss
       validate_schema(cache, disk_address, plan) → coroutine, co_await on cache miss
       Each table/function/type encounter: cache hit → sync, miss → co_await resolve
       No collect_dependencies — resolve as AST is walked
       DEPENDS ON: 3.4 (resolve response types)

3.12 integration/cpp/base_spaces.cpp — new startup sequence
       Phase 0: load catalog from disk (sync) or bootstrap
       Phase 1: spawn actors + sync addresses
       Phase 2: start schedulers
       Phase 3: index recreation (async via wrapper)
       DEPENDS ON: 3.2, 3.7, 3.10
```

### Step 4: BUILD

```
cmake --build .
Fix compilation errors in implementation code AND test code
Iterate until clean build (tests compile but not yet executed)
```

### Step 5: TEST + FIX

```
Run all 139 new tests
Run existing integration tests (expect failures from removed API — fix)
Fix bugs found by tests
Iterate until all tests pass
```

### Dependency Graph

```
Step 1: DELETE legacy

Step 2: WRITE tests (2.T) — references types/methods from Step 3

Step 3: WRITE implementation:

3.0 (OIDs) ──────────────────────────────────────────────────┐
    │                                                          │
3.1 (schemas) ────────┐                                       │
    │                  │                                       │
3.2 (bootstrap) ◄─────┘                                       │
    │                                                          │
    ├──── 3.3 (IN filter)                                      │
    │         │                                                │
    ├──── 3.4 (resolve V4+S5) ◄── 3.3                          │
    │         │                                                │
    │    3.5 (ring buffer) ◄───────────────────────────────────┘
    │         │
    ├──── 3.6 (DDL methods) ◄── 3.4, 3.5
    │         │
    │    3.7 (WAL) ◄── 3.6
    │         │
    │    3.8 (pg_depend) ◄── 3.6
    │
    │    3.9 (plan cache) ◄── 3.0, 3.4
    │         │
    └──── 3.10 (dispatcher) ◄── 3.4, 3.6, 3.9
              │
         3.11 (validate V4) ◄── 3.4
              │
         3.12 (base_spaces) ◄── 3.2, 3.7, 3.10

Step 4: BUILD (compile implementation + tests)

Step 5: TEST + FIX (run 139 tests + integration tests)
```

## Appendix C: File Impact Summary

| Directory                       | Files Deleted | Files Added (impl) | Files Added (tests) | Files Modified |
|---------------------------------|---------------|--------------------|--------------------|----------------|
| components/catalog/             | 14            | 2                  | 3                  | 5              |
| components/catalog/transaction/ | 10            | 0                  | 0                  | 0              |
| components/table/               | 0             | 0                  | 0                  | 2              |
| components/types/               | 0             | 0                  | 0                  | 1              |
| components/compute/             | 0             | 0                  | 0                  | 1              |
| services/dispatcher/            | 0             | 1                  | 5                  | 3              |
| services/disk/                  | 2             | 1                  | 5                  | 3              |
| services/index/                 | 0             | 0                  | 0                  | 1              |
| integration/cpp/                | 0             | 0                  | 2                  | 2              |
| **Total**                       | **26**        | **4**              | **15**             | **18**         |

---

## Appendix D: Document Validation Results

Validation performed 2026-03-29 against current codebase (commit 4443c98).

### Claims Verified: 41 total

| Group             | Checked | Confirmed | Issues |
|-------------------|---------|-----------|--------|
| Phase 0: OID      | 10      | 10        | 0      |
| Phase 1: Tables   | 11      | 11        | 0      |
| Phase 3: MVCC     | 10      | 8         | 2      |
| Phase 5: Gaps     | 10      | 9         | 1      |

### Corrections Applied

**1. TRANSACTION_ID_START value**
- Document originally stated "2^62"
- Actual code value: `4611686018427388000` (off by 96 from true 2^62 = 4611686018427387904)
- Corrected to: "~2^62 (value 4611686018427388000)"

**2. NOT_DELETED_ID value**
- Code uses `std::numeric_limits<uint64_t>::max() - 1` = 2^64 - 2
- Not 2^64 - 1 as originally implied by code comment

**3. Constraint persistence nuance**
- PRIMARY KEY columns ARE persisted in `catalog_table_entry_t.primary_key_columns`
- CHECK, UNIQUE, FK constraints and DEFAULT values are NOT persisted
- DEFAULT has a TODO comment in `catalog_column_entry_t`
- Corrected from "Constraints are NOT persisted" to "NOT fully persisted"

### Confirmed Critical Findings

**Bug E2.1A: CONFIRMED**
`transaction_version_operator::use_deleted_version` returns TRUE for uncommitted
deletes from other transactions. The `committed_version_operator` (already in
codebase) correctly handles this case by checking `id < TRANSACTION_ID_START`.

**Types/Functions lost on restart: CONFIRMED**
No serialization code exists for `registered_types_` or `registered_functions_`
in `catalog_storage_t`.

**Sequences/Views/Macros not registered: CONFIRMED**
`base_spaces.cpp` loads these from disk but only logs them; no registration in
catalog or dispatcher occurs.

**CASCADE/RESTRICT not implemented: CONFIRMED**
Parser supports `DropBehavior` enum; dispatcher has zero references to CASCADE
or RESTRICT logic.
