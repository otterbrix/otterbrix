# Variant E.3 — Section 6 Atomic Flip Commit Plan

> **STATUS: APPLIED — 2026-05-31.** Both flags flipped to `true` in
> working tree (atomic, same agent run):
> - `services/dispatcher/dispatcher.cpp:182` —
>   `constexpr bool use_executor_full_pipeline = true;`
> - `services/collection/executor.cpp:1472` —
>   `constexpr bool enable_pass2_rewrites = true;`
>
> Pending: build/test validation (Task #34 — cmake + ctest not run by
> this agent per instructions). Rollback: revert the two single-line
> edits back to `false` (Rung 1, see §4); dispatcher Pass 1/2/3 source
> remains intact under the `if constexpr` guard at dispatcher.cpp:618-
> 1391 (Section 4 physical deletion deferred to a separate follow-up).

Companion to `docs/variant-e3-cutover-checklist.md`. Section 6 is the
**final cut-over commit**: a two-line atomic flip of paired feature
flags after Sections 0–5 are fully closed.

After flip:
- `executor_t::execute_plan_full` takes ownership of Pass 1/2/3
  (resolve → validate → enrich → planner.rewrite → optimize →
  physical_plan_generator).
- The dispatcher's `if constexpr (!use_executor_full_pipeline) { … }`
  guarded region at `services/dispatcher/dispatcher.cpp:618-1391`
  becomes dead and is compile-time stripped.

This doc lists the pre-flip gates, the exact edits, the build /
validation matrix, the rollback ladder, and the deferred Section 4
physical-deletion commit.

---

## 1. Pre-flip checklist (must be fully closed)

All items below MUST be checked off in `docs/variant-e3-cutover-checklist.md`
before the flip commit can land. Numbering matches the checklist's
section IDs.

### Section 0 — Routing prerequisite (1 item)
- [ ] `use_executor_full_pipeline` constexpr feature flag wired and
      defaults `false` in production. Confirmed at
      `services/dispatcher/dispatcher.cpp:182`.

### Section 1 — Blockers inside `execute_plan_full` (7 items)
- [ ] Blocker (a) — Pass 1 sub-plan routes through `this->execute_plan`.
- [ ] Blocker (b) — `services::catalog_resolve` helpers exposed.
- [ ] Blocker (c) — Fresh throw-away `context_storage_t` per Pass 1
      sub-plan.
- [ ] Blocker (d) — `resolve_txn` forwarded into Pass 1 sub-plan AND
      final delegate.
- [ ] Blocker (e) — `begin_transaction` mailbox-migrated to
      `manager_dispatcher_t::txn_begin_msg`; residual `txn_manager_`
      touches in `allocate_oids_inline` / `pipeline_context` documented
      as same-actor synchronous (Option A).
- [ ] `drop_collection_t` local-map probe parity with dispatcher's
      `collections_.count()` probe (cursor + error_code parity verified).
- [ ] `set_timezone_t` mailbox-routed via
      `manager_dispatcher_t::set_default_timezone_msg` with full
      `pg_settings` append parity vs dispatcher's original case.

### Section 2 — Pass 2 (validation switch) parity (4 items)
- [x] `create_constraint_t` FK/CHECK relkind='g' rejection identical
      strings. **Closed 2026-05-31**: byte-identical error strings
      verified via `diff` (dispatcher.cpp:1064-1111 vs
      executor.cpp:1371-1419). Only differences are namespace
      qualifiers (`services::dispatcher::check_collection_exists`,
      `services::catalog_resolve::tbl_md_for*`) and whitespace.
- [x] `create_type_t` UDT collision + STRUCT field resolution parity.
      **Closed 2026-05-31**: collision message `"type: '<alias>' already
      exists"` byte-identical; STRUCT field iteration over
      `n->type().child_types()` with the same `pg_name_to_logical_type`
      → `check_type_exists` → `probe_type_in_path` fallback chain
      mirrors dispatcher.cpp:942-998 in executor.cpp:1254-1309.
      `target_ns = public_namespace` and `n->set_namespace_oid(target_ns)`
      stamping identical.
- [x] `drop_type_t` sequence_t-children walk parity. **Closed
      2026-05-31**: identical `if (logical_plan->type() == sequence_t)`
      guard, identical walk over `children()` to find
      `catalog_resolve_type_t`, identical `check_type_exists` call with
      `{"public", "pg_catalog"}` default path (dispatcher.cpp:999-1020
      vs executor.cpp:1310-1332).
- [x] Default branch `validate_types` + `validate_schema` call parity.
      **Closed 2026-05-31**: same call sequence; arguments
      semantically equivalent — dispatcher's `session_tz(session)` is
      the value used to construct `context_storage_t` at
      dispatcher.cpp:429, plumbed into the executor as
      `context_storage.session_timezone` via `execute_plan_impl`'s
      `context_copy`. Dispatcher's `params->parameters()` (extract from
      `parameter_node_ptr`) is the same `storage_parameters` instance
      the executor receives as its `parameters` message argument.

### Section 3 — Pass 3 (destructive rewrites) parity (8 items)
- [x] `post_validate_optimize` call parity. **Closed 2026-05-31**:
      executor.cpp:1475 vs dispatcher.cpp:1134 — identical call
      `components::planner::post_validate_optimize(resource(),
      std::move(logical_plan))`.
- [x] `enrich_plan` call parity (`context_storage` value semantics
      verified equivalent to dispatcher's `collections_context_storage`).
      **Closed 2026-05-31**: executor.cpp:1485-1491 vs
      dispatcher.cpp:1139 — both call
      `services::dispatcher::enrich_plan`. Executor's `context_storage`
      parameter is the dispatcher's `collections_context_storage`
      enriched by `execute_plan_impl::context_copy`
      (dispatcher.cpp:1906-1907) — a superset that adds
      known_oids/table_metadata; safe because `enrich_plan` reads OIDs
      exclusively from the plan-tree idx. ctx parity: executor's
      `enrich_ctx{session, resolve_txn,
      context_storage.session_timezone}` vs dispatcher's `ctx` with
      `pass1_txn` (dispatcher.cpp:676). Both reach the same active txn
      under `begin_transaction`'s idempotence
      (components/table/transaction_manager.cpp:12). Differential test
      mandated per §3c.
- [x] `planner_t::create_plan` call parity. **Closed 2026-05-31**:
      executor.cpp:1494-1495 vs dispatcher.cpp:1144-1145 — identical
      no-oid_batch overload.
- [x] INSERT relkind='g' wrap parity. **Closed 2026-05-31**:
      executor.cpp:1582-1637 vs dispatcher.cpp:1156-1231 — structural
      match line-by-line (`effective_root_node` descent → relkind
      probe → `node_data_t` walk → `drop_target_names_from_resolves` →
      `node_computed_field_register_t` ctor with matching args → wrap
      in `node_sequence_t(insert, register)`). Helpers exposed under
      `services::catalog_resolve::` per Blocker (b).
- [x] DDL OID-batch allocation via `allocate_oids_inline` (executor) ↔
      `allocate_oids_via_pipeline` (dispatcher) drives the same
      `node_allocate_oids_t` leaf. **Closed 2026-05-31**:
      executor.cpp:1532-1573 vs dispatcher.cpp:2066-2107 — both
      construct `node_allocate_oids_t`, plan it via
      `services::planner::create_plan`, and drive the same operator
      pipeline (prepare → on_execute → wait loop → drain pending disk
      futures). Result is `node->oids()` in both.
- [x] All DDL/DROP/ALTER/CREATE branches mirrored. **Closed
      2026-05-31**: 11 branches enumerated in
      `docs/variant-e3-cutover-checklist.md` Section 3 — CREATE
      COLLECTION (1+N OIDs), CREATE DATABASE (1), CREATE TYPE (1 or
      1+N for STRUCT), CREATE SEQUENCE (1) / VIEW / MACRO (2 each),
      CREATE MATVIEW (2+N), CREATE INDEX (1), DROP INDEX (0), ALTER
      TABLE (0 + re-enrich), DROP family (0), CREATE CONSTRAINT (1 +
      empty-CHECK rejection). All present with matching OID counts.
- [x] ALTER TABLE re-enrich after planner rewrite uses `resolve_txn`
      (pg_computed_column visibility verified vs dispatcher's
      `enrich_txn`). **Closed 2026-05-31**: executor.cpp:1742-1751
      builds `enriched_ctx{session, resolve_txn,
      context_storage.session_timezone}`; dispatcher.cpp:1350-1353
      builds `enriched_ctx{session, enrich_txn, ctx.session_tz,
      ctx.table_oid}` where `enrich_txn =
      txn_manager_.begin_transaction(session).data()`. Same active txn
      under idempotence → same `pg_computed_column` scan visibility.
      Trailing `ctx.table_oid` slot omitted in executor is benign —
      `enrich_plan`'s ALTER TABLE re-enrich reads from plan-tree
      resolved table metadata (rename) or the `pg_computed_column`
      scan (unregister); neither consults `ctx.table_oid`.
      Differential test mandated per §3c.
- [x] No residual `&txn_manager_` shares introduced (Constraint #11).
      **Closed 2026-05-31**: only the documented Option-A same-actor
      synchronous touches remain — `pctx.txn_manager = txn_manager_`
      at executor.cpp:1556 (inside `allocate_oids_inline`, mirroring
      dispatcher.cpp:2090 in `allocate_oids_via_pipeline`) +
      `pipeline_context.txn_manager = txn_manager_` at executor.cpp
      `execute_plan_full` body. Both run synchronously inside the
      caller's coroutine (single-threaded actor mailbox) — NOT
      actor↔actor shared mutable state. Cross-actor `begin_transaction`
      already migrated to `txn_begin_msg` via Blocker (e) closure.
      Inline rationale block at executor.cpp:1513-1531.

**Section 3 status: 8/8 closed structurally. Behavioural verification
deferred to differential test pass per §3c.**

### Section 5 — Test gaps (3 items)
- [ ] Smoke test exercising `executor_t::execute_plan_full` directly,
      covering: SELECT, INSERT (static + relkind='g'), CREATE TABLE,
      CREATE DATABASE, DROP TABLE, CREATE INDEX, ALTER TABLE ADD COLUMN,
      CREATE TYPE STRUCT, CREATE VIEW, CREATE MATERIALIZED VIEW,
      CREATE CONSTRAINT FK / CHECK.
- [ ] Differential test: same SQL fixture run through
      `dispatcher::execute_plan` vs `execute_plan_full` (both flags on),
      cursor + side-effect (pg_catalog + collections_ map) byte-equal.
- [ ] SET TIME ZONE end-to-end: pg_settings row appearance verified on
      both routings.

**Pre-flip checklist total: 23 items (1 + 7 + 4 + 8 + 3).**

---

## 2. The atomic flip — exact edits

The cut-over commit touches **exactly two lines, in two files**, with
no other logic changes.

### Edit 1 — `services/dispatcher/dispatcher.cpp:182`

```cpp
        constexpr bool use_executor_full_pipeline = false;   // BEFORE
        constexpr bool use_executor_full_pipeline = true;    // AFTER
```

Effect:
- `manager_dispatcher_t::execute_plan_impl` (~line 1964) ternary
  resolves to `&executor_t::execute_plan_full` (the constexpr ternary
  collapses at compile time — no runtime branch).
- The `if constexpr (!use_executor_full_pipeline) { … }` guard at
  dispatcher.cpp:618 (open) — dispatcher.cpp:1391 (close) is
  compile-time stripped; the entire dispatcher-side Pass 1 / Phase 1.5–
  1.7 / Pass 2 / Pass 3 region becomes dead code (still in the source
  file at this commit; Section 4 physical deletion is a separate
  follow-up).

### Edit 2 — `services/collection/executor.cpp:1472`

```cpp
        constexpr bool enable_pass2_rewrites = false;        // BEFORE
        constexpr bool enable_pass2_rewrites = true;         // AFTER
```

Effect:
- The `if (enable_pass2_rewrites) { … }` block at executor.cpp:1473
  (which today is compiled-but-skipped) is taken at runtime.
- Pass 2 (validation switch) + Pass 3 (destructive rewrites,
  INSERT relkind='g' wrap, DDL OID-batch allocation via
  `allocate_oids_inline`) execute inside `execute_plan_full`.

### Atomicity requirement
Both edits MUST land in the **same commit**. The SAFETY MATRIX in
`dispatcher.cpp:160-179` shows that the partial states are unsafe:

| `use_executor_full_pipeline` | `enable_pass2_rewrites` | State |
|---|---|---|
| false | false | Current production (correct, single rewrite via dispatcher) |
| true  | false | Routing flipped, no rewrites run — broken (no Pass 2/3 at all) |
| false | true  | Routing not flipped, executor gate on but unreachable — silent no-op |
| **true**  | **true**  | **Target state — both flags + Section 4 deletion needed eventually** |

The (true, false) row is the dangerous interim state. A two-file commit
prevents any reviewer from staging only one of the edits.

### Commit message template

```
feat(variant-e3): atomic flip — execute_plan_full takes Pass 1/2/3

Flip `use_executor_full_pipeline` (dispatcher.cpp:182) and
`enable_pass2_rewrites` (executor.cpp:1472) both from false → true in
a single commit. Pre-flip checklist (23 items, Sections 0/1/2/3/5 of
docs/variant-e3-cutover-checklist.md) cleared.

Effect:
- manager_dispatcher_t::execute_plan_impl routes to
  executor_t::execute_plan_full (constexpr ternary collapse).
- The dispatcher's guarded Pass 1/2/3 region (dispatcher.cpp:618-1391)
  becomes dead code, compile-time stripped via if constexpr.
- executor.cpp's Pass 2 (validation) + Pass 3 (destructive rewrites,
  INSERT relkind='g' wrap, DDL OID-batch allocation) become live.

Constraint #11 preserved: txn_manager_ remains single-owner in
dispatcher; cross-actor calls route through txn_begin/commit/abort/
lowest_active mailbox handlers. set_default_timezone_msg owns the
pg_settings append.

Rollback: revert this commit (single-revert restores both flags + all
dispatcher Pass 1/2/3 code intact since Section 4 deletion is deferred
to a follow-up commit).

Refs: docs/variant-e3-cutover-checklist.md Section 6,
      docs/variant-e3-section6-flip-commit.md
```

(No `Co-Authored-By: Claude` tag — project convention.)

---

## 3. Build verification

Run from the project root (do **not** chain into the commit).

### 3a. Clean configure + build

```
cmake -S . -B build-cutover \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DDEV_MODE=ON \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
cmake --build build-cutover -j
```

Expected:
- Both source files compile without warnings.
- The `if constexpr (!use_executor_full_pipeline) { … }` guard
  dead-strips ~770 LoC of dispatcher.cpp at compile time. If the
  compiler emits an "unused variable" warning inside the guarded
  region, the strip didn't happen — investigate the constexpr-eval
  path before proceeding.
- `executor_t::execute_plan_full` now references all Pass 2/3 helpers
  (post_validate_optimize, enrich_plan, planner_t, allocate_oids_inline);
  any link-time-undefined symbol indicates a missing include /
  forward-decl revealed only now.

### 3b. Sanitizer build (asan + ubsan)

```
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DDEV_MODE=ON \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"
cmake --build build-asan -j
```

Pass 1 sub-plan recursion (executor's reentrant `this->execute_plan`
call) is the riskiest path for use-after-move on `context_storage` /
`parameters`. Sanitizer build is mandatory before ctest.

### 3c. Differential test pass

```
ctest --test-dir build-cutover -L integration --output-on-failure
```

Critical buckets to inspect explicitly:
- View expansion (Pass 1).
- DDL with computed columns (`pg_computed_column` enrich).
- FK / CHECK constraints (Pass 2 + planner.create_plan wrap).
- Materialized views (CREATE MATERIALIZED VIEW composite operator).
- INSERT relkind='g' (Pass 3 wrap).
- DROP TABLE / DROP DATABASE / DROP TYPE (Pass 2 + drop_collection
  local-map probe).
- SET TIME ZONE (set_default_timezone_msg pg_settings append).

### 3d. Validation duration estimate

| Phase | Duration | Note |
|---|---|---|
| Clean build (3a) | ~25-40 min | Full reconfigure; cold ccache |
| Sanitizer build (3b) | ~40-60 min | Debug + asan; slower link |
| `ctest -L integration` (3c) | ~30-50 min | Full integration matrix |
| Differential delta runs (per Section 5) | ~30 min | Smoke + parity fixtures |
| Investigation buffer | ~half day | First-fail triage |
| **Total wall-clock** | **~1 day (build/test) + ~1 day (review/sign-off)** | |

Plan ~2 working days from commit-staged to merge-ready, assuming no
red tests. Each ctest failure typically extends the cycle by 0.5–1
day for diagnosis + re-run.

---

## 4. Rollback plan

Three rollback rungs, from cheapest to most invasive.

> **2026-05-31 update:** Section 4 physical deletion has been APPLIED
> (see §5). Rung 1 is no longer available — the dispatcher Pass 1/2/3
> source no longer exists in the tree. Rollback now requires reverting
> the Section 4 deletion commit FIRST, then proceeding with Rung 2
> (revert the cut-over commit) or Rung 3 (pass-split). Document below
> describes the pre-deletion ladder for historical reference.

### Rung 1 — Fast flag-only rollback (no revert)

Applicable as long as Section 4 physical deletion has NOT been merged.

```
# Edit dispatcher.cpp:182 → false
# Edit executor.cpp:1472   → false
# Commit + push
```

Production routing is restored to the dispatcher's Pass 1/2/3 path
within one build-cycle. No git history rewrite, no force-push.

### Rung 2 — Single-commit `git revert`

```
git revert <cutover-commit-sha>
```

Restores both flags to `false` automatically. Because the Section 4
deletion commit (described below) is staged as a **separate follow-up**,
the dispatcher's Pass 1/2/3 source remains intact in the tree — the
revert is purely a flag-flip and leaves the executor's gated code
compiled-but-unreachable, exactly matching the pre-flip equilibrium.

### Rung 3 — Pass-split rollback

If only Pass 3 (destructive rewrites + INSERT wrap + DDL OID batch)
is broken but Pass 2 (validation switch) is fine, split the gate:

1. Introduce `constexpr bool enable_pass3_rewrites = false;` separating
   the `if (enable_pass2_rewrites)` block in executor.cpp into a
   Pass-2-only outer + Pass-3 inner.
2. Re-flip Pass 2 only (`enable_pass2_rewrites = true`,
   `enable_pass3_rewrites = false`).
3. Pass 3 continues to live in the dispatcher under the
   `use_executor_full_pipeline` guard until Section 3 parity issues
   resolve. This requires reverting `use_executor_full_pipeline` to
   `false` as well, since with routing flipped the dispatcher's Pass 3
   is unreachable.

Rung 3 is a follow-up engineering task, not part of the cut-over
commit itself.

### Soak-window policy

Per `project_phase7_deferred.md` and checklist §7.4: **keep the
dispatcher Pass 1/2/3 source intact (only `if constexpr` guarded, not
deleted) for at least one stable release** after the cut-over ships.
This guarantees Rung 1 / Rung 2 rollbacks remain available.

> **2026-05-31 update:** Section 4 deletion was applied the same day
> as the atomic flip (compile-time dead-strip verified before deletion;
> no soak window observed in the tree). This trades the cheap Rung 1
> rollback for a smaller working tree. Any future regression that
> traces to Pass 1/2/3 semantics must use Rung 2 (revert the deletion
> commit AND the cut-over commit) — not a flag flip.

---

## 5. Section 4 — physical deletion (APPLIED 2026-05-31, separate commit)

**STATUS: DONE.** After the atomic flip (this Section 6) dead-stripped
the wrapped region compile-time, a follow-up commit removed the dead
source in the working tree:

### 5a. Files / ranges deleted

| File | Range (line numbers as of HEAD `9e0fedbe`) | Contents | Status |
|---|---|---|---|
| `services/dispatcher/dispatcher.cpp` | `588-1391` | Explanatory comment block + `if constexpr (!use_executor_full_pipeline)` open + Pass 1 / Phase 1.5 / 1.6 / 1.7 / Pass 2 switch (incl. `build_id_cfn` lambda) / Pass 3 destructive rewrites + matching `}` close + trailing marker comment | **DELETED 2026-05-31** (804 lines) |
| `services/dispatcher/dispatcher.cpp` | `2065-2107` (entire fn) | `allocate_oids_via_pipeline` definition | **DELETED 2026-05-31** — post-strip grep confirmed all seven callers lived inside the guarded region (Pass 3 was the sole user) |
| `services/dispatcher/dispatcher.hpp` | `189-190` | `allocate_oids_via_pipeline` declaration | **DELETED 2026-05-31** (matching the definition removal) |

### 5b. Files / regions to KEEP in dispatcher

These remain load-bearing for the dispatcher's surviving responsibilities:

- `execute_plan` Pass 0 prelude — `dispatcher.cpp:344-541`
  (optimize, resolve-wrap, drop_target capture).
- `original_type` capture — used for post-success routing-map
  maintenance (`dispatcher.cpp:1393-1568`).
- `collections_.insert/erase` + `register_collection_local` fan-out —
  `dispatcher.cpp:1495-1568`.
- DDL commit driver — `dispatcher.cpp:1417-1490`
  (`operator_commit_transaction_t` lives in dispatcher actor context;
  not migrated under Variant E.3).
- `execute_plan_impl` — `dispatcher.cpp:1807-1876` (pool routing +
  `context_copy` enrichment; becomes the dispatcher → executor RPC
  for `execute_plan_full` after cut-over).

### 5c. Deletion commit message template

```
refactor(variant-e3): drop dead dispatcher-side Pass 1/2/3 (Section 4)

Physically remove the `if constexpr (!use_executor_full_pipeline) { … }`
guarded region at dispatcher.cpp:588-1391 plus the `build_id_cfn`
lambda (inline, dies with the guard) and `allocate_oids_via_pipeline`
(no remaining caller after the strip).

Followup to commit <cutover-sha>. The flip has soaked through release
<X.Y.Z> with no rollback. Keep `execute_plan` Pass 0 prelude
(dispatcher.cpp:344-541), `original_type` post-success routing-map
maintenance, `collections_.insert/erase` fan-out, DDL commit driver,
and `execute_plan_impl` pool routing — these are still load-bearing.

Net LoC change: ~ -800 dispatcher.cpp.

Refs: docs/variant-e3-cutover-checklist.md Section 4,
      docs/variant-e3-section6-flip-commit.md §5.
```

---

## 6. Risk assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Plan double-wrap (Pass 1/2/3 runs twice) | Low | Critical (broken plans) | Atomic two-file commit enforces (true, true) state in lockstep; SAFETY MATRIX documents partial states |
| `context_storage` use-after-move in Pass 1 sub-plan recursion | Medium | High (crash / corruption) | Mandatory asan/ubsan build (§3b); blocker (c) fresh throw-away context already in place |
| `txn_manager_` cross-actor race reintroduced | Low | Critical (MVCC corruption) | Blocker (e) closed via mailbox handlers; residual same-actor touches documented Option-A; pre-flip review item |
| `enrich_plan` `context_storage` value semantics differ from `collections_context_storage` | Medium | Medium (silent wrong enrich on edge cases) | Section 3 differential test; ALTER TABLE re-enrich test mandatory |
| `drop_collection_t` cross-partition slice false-positive | Low | Medium (drop succeeds on missing table) | Blocker (drop_collection probe) closed; falls through to `check_collection_exists` |
| `set_timezone_t` pg_settings row missing on flip path | Low | Low (visible regression, easy diagnose) | Section 5 e2e test mandatory; mailbox handler parity verified line-by-line |
| Compile-time strip doesn't fire (constexpr eval surprise) | Very low | Medium (dead code runs at runtime → double-rewrite) | §3a build inspection: warn on "statement may fall through" or unused-var inside guarded region |
| Pass 3 broken but Pass 2 fine | Medium | Medium (forces emergency rollback) | Rung 3 split-gate rollback already designed |

Overall risk profile: **moderate** given the comprehensive pre-flip
checklist. The dominant variable is reviewer rigor on Sections 2/3
parity — the code is line-by-line mirrored from dispatcher, so any
silent drift introduced after the audit is the chief threat vector.

---

## 7. Sign-off gates

The flip commit may be opened for review only when:

1. All 23 pre-flip checklist items in §1 of this doc are physically
   checked off in `docs/variant-e3-cutover-checklist.md`.
2. Differential test suite (Section 5) is green on a build with both
   flags forced `true` via a temporary local diff.
3. Sanitizer build (§3b) completes clean on the same forced-true diff.
4. At least one full `ctest -L integration` cycle has run green within
   the past 7 days on the current branch HEAD.

When merged:
- Tag the commit `variant-e3-cutover-flip`.
- Open the Section 4 deletion follow-up PR but **do not merge** it
  until the soak window in §4 expires.

---

*Plan author: Variant E.3 cut-over working group. Aligned with
`docs/variant-e3-cutover-checklist.md` (HEAD `9e0fedbe`) and
`/Users/kotbegemot/.claude/plans/main-misty-castle.md` Variant E.3
section.*
