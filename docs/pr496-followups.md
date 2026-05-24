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

## 2. Materialized view INITIAL POPULATION + REFRESH

**Status of baseline matview:** SHIPPED in PR #496 second commit (CREATE MATERIALIZED
VIEW creates a real `relkind='m'` table with pg_class + pg_attribute + pg_rewrite +
pg_depend rows via the pipeline-canonical `operator_create_matview_t` composite
physical operator). The matview behaves like an empty table after CREATE — equivalent
to PostgreSQL's `WITH NO DATA` default. `SELECT * FROM mv` returns 0 rows via the
standard scan pipeline (relkind='m' falls through to the regular scan path via
`operator_resolve_table.cpp:306` else-branch). Body SQL is stored in pg_rewrite for
REFRESH and inspection.

**What's deferred to this follow-up:**

### 2a. Initial population from body SELECT inside CREATE MATERIALIZED VIEW

**Blocker discovered during PR #496 implementation:** the composite physical operator
`operator_create_matview_t` performs heap + catalog rows + (planned) body scan +
storage_append in a single `await_async_and_resume` coroutine. Driving the body sub-
operator chain (`body_op_->find_waiting_operator + co_await`) from inside this outer
coroutine triggers a nested actor_zeta await scenario which SIGSEGVs in
`operator_full_scan::await_async_and_resume` (specifically inside the
`actor_zeta::send` for `storage_types` on the source table). The executor's main
loop in `services/collection/executor::execute_sub_plan_` uses the exact same nested
pattern successfully, so the issue is subtle — likely a context_t lifetime / sender
identity mismatch when an operator's await drives another operator's await without
going through the executor's pool dispatch.

**Investigation directions (~200 LOC):**
1. Forward source table's `resolved_table_metadata_t` (from outer dispatcher_idx) to
   the matview op so body's full_scan doesn't need to re-resolve via Pass 1.
2. Try the SAME nested-await pattern via the executor's pool dispatch: have
   `operator_create_matview_t` request execution of `body_op_` as a `pass1_root`-style
   sub-plan via send to the executor (not direct co_await), so the body chain gets a
   fresh pipeline context.
3. Alternative: extend `operator_sequence_t.on_execute_impl` to async-drive `steps_`
   via `find_waiting_operator` + `co_await` (currently steps_ runs sync). Then matview
   lowering can use sequence_t([create_collection, insert_t(body)]) and the
   sequence operator handles async wiring generically. This is a broader fix but
   benefits any future multi-step DDL.

### 2b. REFRESH MATERIALIZED VIEW

Re-runs the stored body SQL (from `pg_rewrite.ev_action`) against the matview's heap:
DELETE all rows + INSERT-SELECT. Requires:
- `transform_refresh_matview` (currently scaffolded; planner returns the node
  unchanged with a TODO).
- A planner pass that fetches body_sql from sibling catalog_resolve_table's stamped
  metadata, re-parses + re-transforms (needs `sql_compiler_t` service — see Item #13
  below), and emits `sequence_t(delete_all, insert_t(re-body))`.
- Pipeline-canonical: zero raw_parser calls in dispatcher.

**When needed:** analytics use of matviews. Without 2a/2b, matviews are catalog
artifacts that need manual INSERT to populate.

---

## 13. `sql_compiler_t` service — extract parser/transformer out of dispatcher (Phase A correction)

**Status:** Phase A view expansion (shipped in PR #496) calls `raw_parser` +
`transformer::transform` directly from `services/dispatcher/dispatcher.cpp`'s Phase
1.5 view-expansion block. This is a layer violation: dispatcher should not know about
parser/transformer internals.

**Future PR scope (~210 LOC):**
- `services/sql_compiler/sql_compiler.{hpp,cpp}` — service wrapping raw_parser +
  transformer behind a `compile(sql) → (plan, params)` API.
- `components/planner/planner_t::create_plan` accepts `sql_compiler_t*` via DI.
- View expansion (`rewrite_views_sync` helper) moves from dispatcher to planner pass
  that uses sql_compiler_t.
- Dispatcher removes raw_parser + transformer includes.
- REFRESH MATERIALIZED VIEW (Item #2b) reuses the same sql_compiler_t.

**When needed:** before adding any more SQL re-compilation paths (REFRESH, view
recompilation on source schema change, prepared-statement caches).

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
