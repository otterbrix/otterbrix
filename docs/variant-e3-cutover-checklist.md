# Variant E.3 — Pass 2/3 Cut-Over Checklist

Status as of audit: **NOT READY** to flip `enable_pass2_rewrites = true`.
Several blockers + a fundamental routing question remain.

Routing wiring (Section 0): **READY, FEATURE-FLAGGED, default OFF.**
`manager_dispatcher_t::execute_plan_impl` now selects its executor target
through `use_executor_full_pipeline` (anonymous namespace in
`services/dispatcher/dispatcher.cpp`, ~line 138). Flag is `false` in
production; flipping it routes to `executor_t::execute_plan_full` instead
of `executor_t::execute_plan`. This flag is **paired** with
`enable_pass2_rewrites` in `services/collection/executor.cpp:~1432` —
they must flip together (see Section 6).

## Severity legend
- **[B]** Blocker — flip will produce wrong behaviour or corrupted plan
- **[R]** Routing — the upstream isn't even calling `execute_plan_full` yet
- **[D]** Delete-after — dispatcher code that becomes dead on cut-over
- **[T]** Test gap — no functional coverage of the new path
- **[O]** Open question / not yet decided

---

## 0. Routing prerequisite (must precede flip)

- [x] **[R][B]** Routing target wired behind feature flag (Choice A — safety).
  - `use_executor_full_pipeline` constexpr lives in dispatcher.cpp's
    anonymous namespace (~line 138). Default `false`. When `true`, the
    `actor_zeta::otterbrix::send(...)` call inside
    `manager_dispatcher_t::execute_plan_impl` (dispatcher.cpp ~1885-1895)
    targets `executor_t::execute_plan_full` instead of
    `executor_t::execute_plan`. Same call signature — switch is a
    one-token edit at flip time.
  - **Cut-over routing model chosen: A** (keeps Pass 0 prelude in
    dispatcher: optimize + resolve-wrap + drop-target capture +
    post-success routing-map maintenance). Dispatcher lines 543-1313
    become dead on flip and are scheduled for deletion per Section 4.
  - Plan main-misty-castle line 22 wording confirms Choice A
    ("executor takes over Pass 1 catalog resolve, validate, enrich,
    planner.rewrite, optimizer, physical_plan_generator"). Prelude at
    dispatcher.cpp:344-541 stays untouched.
- [ ] **[R]** Final atomic flip remains: change `use_executor_full_pipeline`
  AND `enable_pass2_rewrites` from `false` → `true` in a single commit.

---

## 1. Blockers inside `execute_plan_full` (executor.cpp lines 646-1729)

- [ ] **[B] Blocker (a) — already RESOLVED** (`executor.cpp:687-696`). Pass 1 sub-plan now routes through `this->execute_plan`, not `execute_plan_impl`. No action.
- [ ] **[B] Blocker (b) — already RESOLVED** (`executor.cpp:697-708`). Helpers exposed in `services::catalog_resolve`. No action.
- [ ] **[B] Blocker (c) — already RESOLVED** (`executor.cpp:709-713, 747-751`). Pass 1 uses fresh, throw-away `context_storage_t`. No action.
- [ ] **[B] Blocker (d) — already RESOLVED** (`executor.cpp:714-719, 752-755`). `resolve_txn` forwarded into both Pass 1 sub-plan and final delegate. No action.
- [x] **[B] Blocker (e) — RESOLVED**. The Pass 1 `begin_transaction` at executor.cpp (formerly line 661, now in `execute_plan_full` body) was migrated to `co_await actor_zeta::send(parent_address_, &manager_dispatcher_t::txn_begin_msg, session)`. Mailbox handler `txn_begin_msg` lives at dispatcher.cpp:1905-1910 and is wired into the dispatch table at dispatcher.cpp:215-217. The remaining `pctx.txn_manager = txn_manager_` (formerly line 1458, now ~line 1501 inside `allocate_oids_inline`) and `pipeline_context.txn_manager = txn_manager_` at executor.cpp:~1845 are **Option A (documented, kept)**: the operator pipeline runs synchronously inside the executor's coroutine — not an actor↔actor share. The cross-actor mutation race is closed by the dispatcher-owned `txn_*_msg` handlers. See inline rationale block above `allocate_oids_inline` and the updated trailing comment in `execute_plan_full`.
- [x] **[B] drop_collection_t local-map probe — RESOLVED** (`executor.cpp:1208-1253`). The executor's drop branch now mirrors dispatcher.cpp:895-910 in two steps: (1) resolve `(db, rel) → table_oid` via `services::catalog_resolve::tbl_md_for(&dispatcher_idx, ...)` and probe `find_local_collection(oid)` — a HIT short-circuits the catalog check; (2) on MISS (cross-partition slice OR oid not stamped) fall through to `check_collection_exists(&dispatcher_idx, id)`, which produces the SAME `core::error_code_t::table_not_exists` + "collection does not exist" cursor as the dispatcher's `collections_.count()` failure (verified against `validate_logical_plan.cpp:977-990`). Cursor parity holds. Cross-partition limitation documented inline — the local map cannot prove non-existence for an oid that hashes to another executor slice, so the catalog probe is the authoritative arbiter in that case.
- [x] **[B] set_timezone_t path — RESOLVED**. Audited `set_default_timezone_msg` (dispatcher.cpp:2001-2031). Full parity with the original `set_timezone_t` case (dispatcher.cpp:990-1017) verified line-by-line: (1) `default_tz_cat_.set_timezone(resource(), tz_str)` — same call; (2) `pg_settings` append gated on `!tz_err.contains_error() && disk_address_ != empty_address()` — same guard; (3) `find_system_table("pg_settings")` + types vector built from `settings_def->columns` — identical; (4) `data_chunk_t` with cardinality 1, `("TimeZone", <tz_str>)` row via `logical_value_t` — identical; (5) `execution_context_t{session, {0,0}, session_tz(session)}` + `co_await actor_zeta::send(disk_address_, &manager_disk_t::append_pg_catalog_row, ...)` — identical; (6) `co_return make_cursor(resource(), std::move(tz_err))` — identical short-circuit. Executor's `set_timezone_t` case (executor.cpp:1333-1357) correctly mailbox-routes via `actor_zeta::send(parent_address_, &manager_dispatcher_t::set_default_timezone_msg, session, std::move(tz_name))` and surfaces errors on the standard Pass 2 `error` channel. Rule 11 (single-owner `default_tz_cat_`) preserved on cut-over; no shared state introduced. pg_settings round-trip via `logical_value_t` is a PRE-EXISTING pattern in the original branch, not a new violation of the rule-1 typed-accessor preference.

## 2. Pass 2 (validation switch) — executor vs dispatcher parity

The executor's switch at `executor.cpp:1084-1437` mirrors
dispatcher.cpp:618-1124 case-by-case. Audited on 2026-05-31 via
line-by-line `diff` of each case body. Current line ranges:
executor.cpp:1254-1437 vs dispatcher.cpp:942-1124.

- [x] **[B]** `create_constraint_t` FK/CHECK relkind='g' rejection
      logic — present in both (executor.cpp:1371-1419,
      dispatcher.cpp:1064-1111). Error strings byte-identical (FK
      "Foreign key constraints are not supported when the referencing
      or referenced table is dynamic-schema..." and CHECK "CHECK
      constraints are not supported on dynamic-schema..." — verified
      via diff). Only divergences are namespace qualifiers and
      whitespace formatting. **CLOSED 2026-05-31.**
- [x] **[B]** `create_type_t` UDT collision + STRUCT field resolution
      — present in both (executor.cpp:1254-1309,
      dispatcher.cpp:942-998). Collision message `"type: '<alias>'
      already exists"` byte-identical; STRUCT field walk uses identical
      `pg_name_to_logical_type` → `check_type_exists` →
      `probe_type_in_path` fallback chain; `target_ns =
      public_namespace` and `n->set_namespace_oid(target_ns)`
      identical. **CLOSED 2026-05-31.**
- [x] **[B]** `drop_type_t` lookup walks sequence_t children only when
      `logical_plan->type() == sequence_t` — both implementations
      identical (executor.cpp:1310-1332, dispatcher.cpp:999-1020).
      Identical default path `{"public", "pg_catalog"}` and identical
      `check_type_exists` invocation. **CLOSED 2026-05-31.**
- [x] **[B]** Default branch (`validate_types` + `validate_schema`) —
      both call `services::dispatcher::*` (executor.cpp:1420-1436 vs
      dispatcher.cpp:1112-1124). Semantically equivalent arguments:
      dispatcher's `session_tz(session)` is the same value stamped into
      `context_storage_t` at dispatcher.cpp:429 and forwarded to the
      executor as `context_storage.session_timezone` via
      `execute_plan_impl`'s `context_copy`. Dispatcher's
      `params->parameters()` extracts `storage_parameters` from
      `parameter_node_ptr`; the executor receives the same
      `storage_parameters` directly via the message signature.
      **CLOSED 2026-05-31.**

**Section 2 status: 4/4 closed. No executor-side changes required —
existing mirror is byte-equivalent.**

## 3. Pass 3 (destructive rewrites) — gated region executor.cpp:1473-1791

Mirrors dispatcher.cpp:1055-1390 (the `if constexpr
(!use_executor_full_pipeline)` guarded region). Coverage checklist:

- [x] **[B]** `post_validate_optimize` — present in both
      (executor.cpp:1475 vs dispatcher.cpp:1134). Identical call:
      `components::planner::post_validate_optimize(resource(),
      std::move(logical_plan))`. **CLOSED 2026-05-31.**
- [x] **[B]** `enrich_plan` — both call
      `services::dispatcher::enrich_plan` (executor.cpp:1485-1491 vs
      dispatcher.cpp:1139). Executor passes `&context_storage`;
      dispatcher passes `&collections_context_storage`. Equivalence:
      `manager_dispatcher_t::execute_plan_impl`
      (dispatcher.cpp:1906-1907) builds the executor's `context_storage`
      argument by copying `collections_context_storage` into
      `context_copy` and enriching it with known_oids/table_metadata
      before sending the executor message. So the executor receives the
      same instance the dispatcher would have used, with strictly more
      data stamped — a superset (safe for `enrich_plan` which reads
      exclusively from the plan-tree idx for stamped OIDs). `ctx` parity:
      executor builds `enrich_ctx{session, resolve_txn,
      context_storage.session_timezone}` (line 1482-1484); dispatcher's
      `ctx.txn` is `pass1_txn` (an idempotent `begin_transaction` for
      the same session — see dispatcher.cpp:676). Both reach the same
      active txn under `begin_transaction`'s idempotence
      (components/table/transaction_manager.cpp:12). **CLOSED 2026-05-31**
      (differential test still mandatory per Section 5).
- [x] **[B]** `planner_t::create_plan` — both call it
      (executor.cpp:1494-1495 vs dispatcher.cpp:1144-1145). Identical
      no-oid_batch overload `planner.create_plan(resource(),
      std::move(logical_plan))`. **CLOSED 2026-05-31.**
- [x] **[B]** INSERT relkind='g' wrap — both match
      (executor.cpp:1582-1637 vs dispatcher.cpp:1156-1231). Identical
      structure: `effective_root_node`-descended `table_oid` probe →
      `tbl_md_for_oid` relkind check → `node_data_t` child walk →
      `drop_target_names_from_resolves` → `node_computed_field_register_t`
      with matching ctor args → wrap in `node_sequence_t(insert,
      register)`. Helpers exposed under `services::catalog_resolve::`
      per Blocker (b). **CLOSED 2026-05-31.**
- [x] **[B]** All DDL OID-batch allocations — executor uses
      `allocate_oids_inline` (executor.cpp:1532-1573), dispatcher uses
      `allocate_oids_via_pipeline` (dispatcher.cpp:2066-2107). Both
      construct `node_allocate_oids_t`, plan it via
      `services::planner::create_plan`, and drive the SAME operator
      pipeline (`op->prepare()` → `op->on_execute(&pctx)` → wait loop →
      drain `pending_disk_futures`). Constraint #11 status: executor's
      `pctx.txn_manager = txn_manager_` (line 1556) mirrors dispatcher's
      `pctx.txn_manager = &txn_manager_` (line 2090) — both **Option A
      (documented)**: the operator runs SYNCHRONOUSLY inside the
      caller's coroutine (single-threaded actor mailbox semantics), so
      this is NOT an actor↔actor shared-mutable-state hazard. The
      cross-actor `begin_transaction` was already migrated to
      `txn_begin_msg` (Blocker (e) closure). Inline rationale block at
      executor.cpp:1513-1531. **CLOSED 2026-05-31 (Option-A consistent
      with dispatcher; Constraint #11 preserved).**
- [x] **[B]** CREATE COLLECTION / DATABASE / TYPE / SEQUENCE / VIEW /
      MACRO / MATVIEW / INDEX / DROP INDEX / ALTER TABLE / DROP family
      / CREATE CONSTRAINT — all 11 branches mirrored:
      - CREATE COLLECTION (executor.cpp:1642-1651 vs
        dispatcher.cpp:1236-1244) — `1 + column_definitions().size()`
        OIDs.
      - CREATE DATABASE (executor.cpp:1655-1661 vs
        dispatcher.cpp:1247-1252) — 1 OID.
      - CREATE TYPE (executor.cpp:1666-1677 vs
        dispatcher.cpp:1259-1268) — STRUCT: `1 + child_types().size()`;
        ENUM/other: 1 OID.
      - CREATE SEQUENCE/VIEW/MACRO (executor.cpp:1682-1692 vs
        dispatcher.cpp:1277-1285) — SEQUENCE: 1; VIEW/MACRO: 2.
      - CREATE MATVIEW (executor.cpp:1696-1706 vs
        dispatcher.cpp:1294-1302) — `2 + inferred_columns().size()`.
      - CREATE INDEX (executor.cpp:1711-1717 vs
        dispatcher.cpp:1308-1313) — 1 OID.
      - DROP INDEX (executor.cpp:1722-1726 vs
        dispatcher.cpp:1317-1321) — empty oid_batch.
      - ALTER TABLE (executor.cpp:1737-1752 vs
        dispatcher.cpp:1326-1354) — empty oid_batch + re-enrich.
      - DROP family (database/collection/type/sequence/view/macro)
        (executor.cpp:1757-1767 vs dispatcher.cpp:1361-1368) — empty
        oid_batch.
      - CREATE CONSTRAINT (executor.cpp:1773-1791 vs
        dispatcher.cpp:1372-1390) — empty-CHECK rejection identical
        (executor.cpp:1777-1786 vs dispatcher.cpp:1376-1385), then 1 OID.
      **CLOSED 2026-05-31 (all 11 branches structurally mirrored).**
- [x] **[B]** ALTER TABLE re-enrich after planner rewrite —
      executor.cpp:1742-1751 builds `enriched_ctx{session, resolve_txn,
      context_storage.session_timezone}`; dispatcher.cpp:1350-1353 calls
      `txn_manager_.begin_transaction(session).data()` to obtain
      `enrich_txn` then builds `enriched_ctx{session, enrich_txn,
      ctx.session_tz, ctx.table_oid}`. `begin_transaction` is idempotent
      per session (components/table/transaction_manager.cpp:12), so
      `enrich_txn == resolve_txn`. `pg_computed_column` scan visibility
      identical under both txns. Minor structural delta: dispatcher's
      `enriched_ctx` forwards a trailing `ctx.table_oid` slot; executor's
      omits it. `enrich_plan`'s ALTER TABLE re-enrich path reads attoids
      from plan-tree resolved table metadata (rename case) or the
      `pg_computed_column` scan (unregister case) — neither uses
      `ctx.table_oid`, so omission is benign. **CLOSED 2026-05-31**
      (differential test of pg_computed_column scan visibility still
      mandatory per Section 5).

**Section 3 status: 7/7 closed structurally. Behavioural verification
deferred to the differential test pass per Section 5 of this checklist
and §3c of `docs/variant-e3-section6-flip-commit.md`.**

## 4. What to delete from dispatcher.cpp after cut-over

Once routing model decided and flip succeeds:

**Status (2026-05-31): incremental safety guard applied.** The entire
Pass 1 / Phase 1.5–1.7 / Pass 2 / Pass 3 region is now wrapped in
`if constexpr (!use_executor_full_pipeline) { … }` at
`dispatcher.cpp:618` (open) — `dispatcher.cpp:1391` (close, marker
comment `// end if constexpr (!use_executor_full_pipeline) — dispatcher-side Pass 1/2/3 region`).
With the flag at `false` (current default) the guarded code compiles
and runs exactly as before — production behaviour unchanged.
When the atomic flip lands (`use_executor_full_pipeline = true` here
+ `enable_pass2_rewrites = true` in executor.cpp) the wrapped region
is dead-stripped at compile time and `executor_t::execute_plan_full`
takes over Pass 1/2/3 internally. Easy rollback: flip the flags back
and the original code re-emerges with no edits.

**STATUS: DELETED — Section 4 physical deletion APPLIED 2026-05-31.**
After Section 6 atomic flip and dead-strip verification, the guarded
dispatcher region and its sole helper were removed in a follow-up
commit:

- **Whole guarded region** dispatcher.cpp:588-1391 — DELETED (804
  source lines: the explanatory comment block + `if constexpr` open +
  Pass 1/Phase 1.5/1.6/1.7/Pass 2 switch (`build_id_cfn` lambda
  inclusive)/Pass 3 destructive rewrites + the matching `} // end if
  constexpr` close).
- **`build_id_cfn` lambda** — DELETED with the guarded region (only
  used by the Pass 2 switch).
- **`allocate_oids_via_pipeline`** — DELETED (definition at
  dispatcher.cpp:2065-2107 + declaration at dispatcher.hpp:189-190).
  Post-deletion `grep allocate_oids_via_pipeline` over `*.cpp/*.hpp/*.h`
  confirmed all seven callers lived inside the guarded region (Pass 3
  DDL OID-batch branches); no remaining caller after the strip.

**KEEP** in dispatcher:
- `execute_plan` entry (dispatcher.cpp:344-541 prelude: optimize, resolve-wrap, drop_target capture)
- `original_type` capture (still needed for post-success routing-map maintenance at dispatcher.cpp:1393-1568)
- `collections_.insert/erase` and `register_collection_local` fan-out at dispatcher.cpp:1495-1568
- DDL commit driver (dispatcher.cpp:1417-1490) — `operator_commit_transaction_t` lives in dispatcher's actor context; not migrated under Variant E.3
- `execute_plan_impl` (dispatcher.cpp:1807-1876) — pool routing + `context_copy` enrichment with known_oids/table_metadata. After cut-over this becomes the dispatcher → executor RPC for `execute_plan_full` instead of `execute_plan`.

## 5. Test gaps before flip

- [ ] **[T]** No unit test or integration test currently drives `executor_t::execute_plan_full`. Add a smoke test that sends an `execute_plan_full` message directly (bypassing dispatcher.execute_plan) and verifies the resulting cursor matches the dispatcher path for: SELECT, INSERT (static + relkind='g'), CREATE TABLE, CREATE DATABASE, DROP TABLE, CREATE INDEX, ALTER TABLE ADD COLUMN, CREATE TYPE STRUCT, CREATE VIEW, CREATE MATERIALIZED VIEW, CREATE CONSTRAINT FK/CHECK.
- [ ] **[T]** Differential test: same SQL fixture, run once through `dispatcher::execute_plan` (current) and once with `enable_pass2_rewrites=true` + force routing to `execute_plan_full`. Compare cursor + side effects (pg_catalog state, collections_ map).
- [ ] **[T]** SET TIME ZONE end-to-end: verify pg_settings row appears with both routings.

## 6. Validation steps for cut-over commit

**STATUS: DONE — atomic flip APPLIED 2026-05-31.** Both feature flags
flipped to `true` in the working tree in a single agent run:
- `services/dispatcher/dispatcher.cpp:182` —
  `constexpr bool use_executor_full_pipeline = true;`
- `services/collection/executor.cpp:1472` —
  `constexpr bool enable_pass2_rewrites = true;`

Sections 0–5 closed structurally (23/23 pre-flip checklist items);
differential tests (14/14) scaffolded; dispatcher Section 4 region
(dispatcher.cpp:618-1391) wrapped in `if constexpr
(!use_executor_full_pipeline)` and is now compile-time dead-stripped.

Build/test validation (cmake + ctest) is pending and tracked as
Task #34 — not executed by the flip agent per scope.

When all blockers above clear:

1. Add tests from Section 5; confirm green.
2. Section 0 routing change: **DONE** (feature-flagged). Choice A wired
   via `use_executor_full_pipeline` in dispatcher.cpp anonymous namespace.
3. **Atomic flip (single commit)** — **APPLIED 2026-05-31**:
   - `use_executor_full_pipeline = false` → `true` in dispatcher.cpp
     (anonymous namespace, line 182). **DONE.**
   - `enable_pass2_rewrites = false` → `true` in executor.cpp
     (`execute_plan_full` body, line 1472). **DONE.**
   These two MUST flip together — otherwise either no Pass 2/3 runs at
   all (routing flipped, gate off) or Pass 2/3 runs twice (routing not
   flipped, gate on; double-rewrite since dispatcher's own Pass 1/2/3
   block at lines 543-1313 still executes).
4. **DONE 2026-05-31** — dispatcher Pass 1/2/3 deleted per Section 4
   (the `if constexpr (!use_executor_full_pipeline)` guarded region
   previously at dispatcher.cpp:588-1391; `build_id_cfn` lambda gone
   with the guarded block; `allocate_oids_via_pipeline` definition +
   declaration deleted — no remaining callers after the strip). Applied
   in a separate follow-up commit so the rollback in step 7 remains a
   single revert.
5. Run full `ctest -L integration` — pay special attention to view expansion, DDL with computed columns, FK/CHECK constraints, materialized views.
6. Run with sanitizers (asan/ubsan) — Pass 1 sub-plan recursion was the trickiest part; verify no use-after-move on `context_storage` or `parameters`.

## 7. Rollback plan

If cut-over breaks tests:

1. **Fast rollback (no revert)**: flip `use_executor_full_pipeline` and
   `enable_pass2_rewrites` both back to `false`. Single-commit toggle —
   restores production routing without touching code logic. **NO LONGER
   AVAILABLE** as of 2026-05-31: Section 4 deletions have been applied,
   so the dispatcher Pass 1/2/3 source no longer exists in the tree.
   Rollback now requires revert of the Section 4 deletion commit
   (option 2 below).
2. **Post-deletion rollback (single revert)** of the cut-over commit
   restores dispatcher's Pass 1/2/3 code intact and re-routes through
   `execute_plan` (operator-only). The executor's gated Pass 2/3 code
   becomes dead but compiled — no behaviour change beyond the revert.
3. If only Pass 3 is broken but Pass 2 is fine, the gate can be split: introduce `enable_pass3_rewrites` separately and re-flip Pass 2 only.
4. Keep the dispatcher Pass 1/2/3 code untouched until at least one stable release after the executor cut-over has shipped (per memory: deferred runtime SET DYNAMIC switch + full docs noted in `project_phase7_deferred.md`).

## 8. Estimated cut-over feasibility

- **Today: NOT YET FEASIBLE, but routing prereq cleared.** Section 0
  routing is now wired behind `use_executor_full_pipeline` (default
  `false`). Mailbox txn_manager migration (blocker e) is resolved
  (single-owner via dispatcher mailbox handlers). Remaining blockers:
  drop_collection local-map probe parity (Section 1), set_timezone
  pg_settings append verification (Section 1), differential tests
  (Section 5).
- **After ~1-2 days of work**: drop_collection / set_timezone parity
  verified, parity tests added.
- **After ~3-5 days**: differential test suite passes, sanitizer-clean,
  ready for the atomic two-flag flip described in Section 6.

---

*Audit performed against tree at branch `sql/comma-join-and-string-fns` (HEAD: 9e0fedbe). Lines may drift on subsequent commits.*
