# PR #496 follow-ups (post-G13)

> **Status:** tracking document. Captured 2026-05-21 as part of PR #496 (G13 SELECT-time
> view expansion + relkind normalization). Each item is a sized future-PR candidate.

PR #496 ships:
- **G13 Phase A** — regular view expansion through pipeline (CREATE VIEW + SELECT * FROM v).
- **M0** — relkind normalization (`macro` 'm' → 'F'; 'm' freed for matview per PostgreSQL).
- Earlier in PR: G1 (comma-join), G8/G9 (string kernels), G11 (LEFT OUTER JOIN), N-ary
  column_pruning, G15 (IF NOT EXISTS), operator_join.cpp:125 root-cause fix.

The list below captures work intentionally NOT in PR #496. Each entry should become its
own PR when picked up.

---

## 1. Wildcard `SELECT *` over view (composition on top of view)

**Status:** scoped out of Phase A. First iteration handles `SELECT * FROM v` via full
plan replacement; outer queries with extra projections, filters, or joins on top of v
fall back to the unexpanded plan and currently error.

**Blocker:** requires schema introspection during `rewrite_views_sync` — the sub-plan
needs to be resolved enough that the outer aggregate's column references can be remapped
through the view body. Current Phase A `expand_view_body` swaps the entire plan with
the sub-plan, dropping the outer's projection/filter envelope.

**Sketch:** keep the outer aggregate, splice sub-plan as its source child, then re-run
`column_pruning` after Phase 1.6 Pass 1 stamps the underlying table's columns. ~80 LOC
in `services/dispatcher/dispatcher.cpp` plus a backpropagation step from the sub-plan's
schema to the outer expressions.

**When needed:** real SSB-style view-based catalog abstractions, anything beyond
`SELECT * FROM v`.

---

## 2. Materialized views (CREATE MATERIALIZED VIEW / REFRESH)

**Status:** scoped out of PR #496 during execution after architecture deep-dive. Parser
+ grammar already accept `CREATE MATERIALIZED VIEW …` and `REFRESH MATERIALIZED VIEW
…` (`gram.y:5855`, `gram.y:5898`). M0 normalization frees `relkind='m'` for matviews.

**Blocker:** `derive_output_schema` for the body SELECT requires column-type resolution
through the catalog at transform-time. The transformer has no actor address / disk
access — that machinery lives behind `pipeline::context_t` in operators. Either we add
a pre-pass that runs the body's `catalog_resolve_table` resolves before
`transform_create_matview` returns (turning the transformer into an async coroutine
chain), or we defer schema derivation until after Pass 1 in dispatcher (which means
matview lowering moves into dispatcher orchestration too).

**Sketch** (PG-canonical, ~800-1000 LOC end-to-end):
- `components/logical_plan/node_create_matview.{hpp,cpp}` + `node_refresh_matview.{hpp,cpp}`
  (~120 LOC; B1/B2 from the original plan).
- `components/sql/transformer/impl/transform_matview.cpp` + switch cases in
  `transformer.cpp` (~140 LOC).
- `components/sql/transformer/impl/derive_output_schema.cpp` — **hardest piece**, walks
  `SelectStmt.targetList` and resolves column types via deferred catalog access; ~200 LOC.
- `components/catalog/ddl_metadata_builder.cpp::build_matview_rewrite_writes` — only
  pg_rewrite + pg_depend; pg_class + pg_attribute reuse the existing
  `build_create_table_writes` with `relkind = relkind::materialized_view` (already a
  parameterised builder, verified Round 2). ~60 LOC.
- `components/planner/planner.cpp::rewrite_create_matview` +
  `::rewrite_refresh_matview` (~100 LOC). Reuses `operator_create_collection_t` for
  heap creation (relkind-agnostic — confirmed Round 2) plus an `insert_t` child for
  initial INSERT-SELECT.
- 2 integration tests in `test_sql_features.cpp` (~90 LOC).

**Verified pipeline flows** (left from the original plan for future reference):
- CREATE MATERIALIZED VIEW → sequence_t(create_collection [relkind='m'], pg_rewrite +
  pg_depend writes, insert_t with body_plan as child).
- REFRESH MATERIALIZED VIEW → sequence_t(delete_all, insert_t with re-transformed body
  as child). Body SQL fetched from pg_rewrite.ev_action via Phase A.A2 which already
  stamps `view_sql` for both 'v' and 'm' relkinds.
- SELECT * FROM mv → operator_resolve_table's else-branch already handles 'm'
  identically to 'r' (`manager_disk_resolve.cpp:73`).

**When needed:** analytic workloads that pre-compute join/aggregate results.

---

## 3. REFRESH MATERIALIZED VIEW CONCURRENTLY

**Blocker:** requires MVCC snapshot reads + a unique index on the matview for diff-based
update. Otterbrix MVCC is limited; matview unique indexes don't exist.

**Future scope:** weeks of work; depends on a fuller MVCC story. Out of scope until
matview baseline (Item #2) ships.

---

## 4. CREATE TABLE AS SELECT (CTAS without MATERIALIZED)

**Blocker:** non-matview CTAS requires its own planner lowering — no pg_rewrite, no
REFRESH path; just `CREATE TABLE + INSERT-SELECT`.

**Future scope:** ~150 LOC. Mirrors matview lowering minus pg_rewrite + relkind='r'.
Depends on `derive_output_schema` from Item #2 — same blocker, so this and Item #2 likely
ship together.

---

## 5. GRANT / REVOKE / privileges on views and matviews

**Blocker:** no permissions system in otterbrix.

**Future scope:** separate initiative (pg_authid, pg_class.relacl, etc.).

---

## 6. Recursive CTE / `WITH RECURSIVE`

**Blocker:** requires an iteration engine in the physical plan; orthogonal to view
expansion.

**Future scope:** separate initiative.

---

## 7. Writable views (VIEW INSERT / UPDATE)

**Blocker:** PostgreSQL does this through INSTEAD OF triggers or auto-updateable view
detection. Either requires a pg_rewrite trigger engine.

**Future scope:** significant. Out of scope until baseline view + matview support
stabilises.

---

## 8. Multi-table matview body

**Blocker:** first iteration of `derive_output_schema` (Item #2) handles single-table
FROM only. JOIN + subquery bodies require deeper type resolution in the targetList walk.

**Future scope:** ~150-200 LOC extension to `derive_output_schema`. Depends on Item #2
landing first.

---

## 9. Persistence migration for legacy macros with relkind='m'

**Blocker:** M0 (`macro` 'm' → 'F') means WAL replay of pre-PR #496 macro data would
mis-classify rows as matviews.

**Mitigation in PR #496:** no production-deployed macro data exists; the project
is in active development. We rely on a clean WAL.

**Future scope:** ~50 LOC bootstrap heuristic — if pg_class shows `relkind='m'` WITHOUT
pg_attribute and WITH pg_rewrite `ev_type='m'` → upgrade-rewrite to `'F'`. Needed
before the first production deployment that has macros pre-PR #496.

---

## 10. EXPLAIN plan expansion through view

**Future scope:** ~30 LOC. Annotation in `node_t::to_string_impl` with `[view: v]`
markup so EXPLAIN output shows the view origin of expanded sub-plans.

---

## 11. View dependency tracking on DROP TABLE

**Blocker:** Phase A writes pg_depend rows for views referring to their underlying
tables, but `DROP TABLE` currently doesn't check pg_depend → leaves dangling views.

**Future scope:** ~100 LOC. Dispatcher pre-DROP check on pg_depend; CASCADE drop
dependent views/matviews or RESTRICT with an error.

**When needed:** immediately after Phase A lands — protects against broken catalog
state. Prime candidate for the next PR.

---

## 12. SET DYNAMIC runtime switch

Tracked separately in earlier work (see memory: `project_phase7_deferred.md`).
