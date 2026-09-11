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

## 1. Wildcard `SELECT *` over view (composition on top of view) — DONE

**Status:** DONE. `components/planner/view_expansion.{hpp,cpp}`; the executor drives it
(`services/collection/executor.cpp`), tests in
`integration/cpp/test/test_view_expansion.cpp` and
`components/planner/test/test_view_expansion.cpp`.

**Correction to what this entry used to claim:** it said composition on top of a view
"falls back to the unexpanded plan and currently errors". It did not error. The whole
logical plan was replaced by the view body (`plan.sub_queries.back() = std::move(...)`),
so an outer WHERE / narrowed projection / aggregate / join was silently DROPPED and the
unfiltered body came back as a successful answer — a wrong result, not a missing
feature. Do not use this entry's old wording as evidence of past behaviour.

**What was done:** the body is SPLICED as `children()[0]` of the reference node (the
shape an inlined CTE reference already has), the reference's name/oid/metadata are
cleared so it stops being a source, `bind_catalog_data` no longer stamps a relkind='v'
entry onto query nodes, and the body's bound parameters are re-registered under fresh
ids (they used to collide with the outer query's, silently replacing the view's own
constants). Nested views loop with one resolve round per level. No change was needed in
the validator, the plan generator or the optimizer — the spliced shape is the CTE shape
they already handle.

---

## 2. Materialized view INITIAL POPULATION + REFRESH

**Status of baseline matview:** SHIPPED in PR #496 second commit (CREATE MATERIALIZED
VIEW creates a real `relkind='m'` table with pg_class + pg_attribute + pg_rewrite +
pg_depend rows via the pipeline-canonical `operator_create_matview_t` composite
physical operator). The matview is an empty table after CREATE. `SELECT * FROM mv`
returns 0 rows via the standard scan pipeline (relkind='m' falls through to the regular
scan path via `operator_resolve_table.cpp:306` else-branch). Body SQL is stored in
pg_rewrite for REFRESH and inspection.

**Correction:** "equivalent to PostgreSQL's `WITH NO DATA` default" was wrong twice
over — PostgreSQL's default is `WITH DATA`, and the statement accepted the implicit form
while silently producing an empty matview. Since nothing populates a matview at CREATE
time and 2b below is not implemented either, the implicit form is now REFUSED at the
transformer (`components/sql/transformer/impl/transform_matview.cpp`, reading
`IntoClause::skipData` which the grammar already sets from `opt_with_data`). `WITH NO
DATA` — written out — still creates the empty matview it names. The dead `body_op`
parameter of `operator_create_matview_t`, silenced with `(void) body_op_;`, is gone with
it. Implementing 2a/2b is what re-opens the implicit form.

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

**Re-checked 2026-09-01, and the estimate below is too low.** The third direction —
lowering CREATE to `sequence_t(create, insert_t(body))` — also needs an insert node whose
`column_bindings` / `fill_list` are stamped by validate + enrich. Both run BEFORE the
planner, which is where the matview's oid is minted, and against a relation that does not
exist yet, so there is nothing to bind by name. That is a new pipeline capability
("insert into a relation created in the same statement"), not a flag. Note also that 2b
is NOT implemented in any form: `planner.cpp`'s `case node_type::refresh_matview_t`
returns the node unchanged with a TODO, so there is no `sequence_t(delete, insert)`
lowering to reuse.

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
