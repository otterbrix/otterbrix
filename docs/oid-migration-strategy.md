# OID Migration Strategy — Reference

**Status**: COMPLETE.
- **Phase 8** (storage/WAL/index/dispatch all keyed by `pg_class.oid`) — shipped 2026-05-10.
- **Phase 9** (logical-plan node base carries `table_oid_`, virtual cfn accessors removed) — shipped 2026-05-10.
- **Phase 10** (hard rename `collection_full_name_t` → `qualified_name_t`, drop `init_from_state` dead path) — shipped 2026-05-14 (commit `7569e6b`).

This file is reference material for future work: post-mortem of the bug that
triggered the migration, the deferred-risk register (R7-R10), and the
design decisions that explain why current code looks the way it does.
Implementation detail is in code + git log; do not duplicate here.

---

## 1. Triggering bug post-mortem

`relkind='g'` INSERT wrapped into `sequence_t(insert, computed_field_register)`:
executor's commit-side at `services/collection/executor.cpp` read
`logical_plan->collection_full_name()` which was **empty** (the sequence_t was
constructed with `{}`). MVCC tags never flipped → SELECT in another session
returned 0 rows.

Root cause: `collection_full_name_t` had **3 different shapes** in the codebase:

| Source | Shape |
|---|---|
| `wrapper_dispatcher::create_collection` (C++ API, 2-arg ctor) | `{database="X", schema="", collection="Y"}` |
| SQL parser via `rangevar_to_collection` | `{database="", schema="X", collection="Y"}` |
| `operator_insert::name_` | varied with upstream plan shape |

`storage_append` normalized at lookup time. `WAL commit_txn` did NOT — read
`coll_name.database` directly → empty string for SQL-form → phantom WAL
worker → cascading state corruption.

OID has one shape (`uint32_t`), one hash, no schema/database ambiguity.
Eliminates this class plus: case-sensitivity mismatches, rename staleness,
multi-database name collisions, `unordered_map<cfn,X>` hash collisions.

---

## 2. End-state invariants (post Phase 8+9+10)

These are the rules current code follows; future changes must preserve them.

- `qualified_name_t` (formerly `collection_full_name_t`) lives ONLY at:
  - SQL parser output (transformer)
  - error-reporting (`table_id` → cursor error text)
  - dispatcher's `collections_` membership cache (DDL existence checks)
- Storage routing keys: `services/disk/manager_disk_t::storages_` is
  `unordered_map<oid_t, …>`. Same for `manager_index_t::engines_`.
- WAL records carry `oid_t table_oid` (4 bytes); workers keyed by
  `database_oid` (resolved once at startup from `pg_namespace`).
- `components/context/execution_context_t = {session, txn, table_oid}` —
  no name field.
- `node_t::table_oid_` is the routing identity for every DML/DDL operator.
  Resolved-stage code never re-derives cfn from pg_class.

---

## 3. Deferred risks (precondition register)

These are NOT scheduled. Document here so future feature work knows the
precondition. None block current functionality.

### R7 — Multi-stage / rolling upgrade (mixed binaries)

- *Failure*: rolling upgrade across binaries with different WAL formats —
  v1 reader chokes on v2-format records, split brain across replicas.
- *Mitigation when replication ships*: `wal_format_min_supported` byte in
  WAL header; v1 binaries fail-closed on v2 logs.
- *Residual today*: low — otterbrix is single-process.

### R8 — OID overflow (32-bit space exhaustion)

- *Failure*: `oid_t = uint32_t`, monotonic, never reused even on DROP.
  At 1k DDL/sec → ~50 days. At 1 DDL/sec → ~136 years.
- *Mitigation*: monitor `oid_gen_.next()`; alarm at 80% of `UINT32_MAX`.
  Long-term: widen to `uint64_t` (mechanical: WAL format bump,
  `pg_class.oid` widens, storage maps re-key). Defer until 50% of
  UINT32_MAX hit, or replication ships (whichever sooner).
- *Residual*: very low for foreseeable workloads.

### R9 — Cross-instance OID collision (replication / federation)

- *Failure*: two instances each allocate oid 16 for their first user
  table; replication routes data to the wrong table.
- *Mitigation when replication ships*: scope `(instance_id, oid)` or
  global Snowflake-style allocation (high bits per instance, low bits
  local). The 64-bit migration (R8) opens room for the prefix.
- *Residual today*: N/A (no replication).

### R10 — Catalog import/export (pg_dump-style)

- *Failure*: dump from instance A carries oid 47 = `users`; import to
  instance B where oid 47 = `orders` → silent misrouting.
- *Mitigation when dump tool ships*: dump format encodes
  `(oid → relname, relnamespace)` mapping; import translates source-oids
  → fresh-allocated target-oids, rewriting FK/index/pg_depend references.
- *Residual today*: N/A (no dump tool).

---

## 4. Design decisions (rationale, not actionable)

These explain WHY current code does what it does. Re-litigation deferred
until a triggering feature lands.

- **Schema field**: kept as SQL/display concept; internal routing uses
  `namespace_oid`. PostgreSQL parity (SQL portability) > internal purity.
- **Multi-database WAL**: per-database, NOT global. Preserves per-DB
  recovery isolation and DROP DATABASE atomicity. Revisit if sharding /
  replication wants a global log.
- **Cross-namespace JOIN**: enrich resolves all referenced cfns → oids
  BEFORE plan generation. Operators see only oids. Namespace boundaries
  are a parser/catalog concern, not a storage concern.
- **System tables**: NOT migrated at runtime. `well_known_oid::*`
  constants are hard-coded; bootstrap creates the 12 system tables at
  those oids deterministically. Changing well-known oids is a
  binary-incompatible change.

---

## 5. References

- `components/base/collection_full_name.hpp` — `qualified_name_t` definition.
- `components/catalog/oid_batch.hpp` — `oid_gen_` allocator.
- `components/catalog/system_table_schemas.cpp` — well-known oid mapping.
- `services/disk/manager_disk.hpp` — `storages_` shape.
- `services/wal/record.hpp` — WAL record format (carries `table_oid`).