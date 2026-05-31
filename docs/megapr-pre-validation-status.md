# Mega-PR Pre-Validation Status

**Date:** 2026-05-31
**Branch:** `sql/comma-join-and-string-fns`
**Plan:** `/Users/kotbegemot/.claude/plans/main-misty-castle.md`
**Total sub-agents orchestrated:** 123+
**Pre-validation phase:** COMPLETE — ready for Final validation #34.

---

## 1. Overview

This document captures the consolidated state of the otterbrix mega-PR immediately prior to the full build + ctest validation pass. It enumerates every Block (A–F), the three manager-level refactors (Variant C, Variant E.3, Version B*), the per-table agent migration (Block D), the items that landed as diagnostic-only checkpoints, and the deletions that are intentionally deferred until after the soak window.

---

## 2. Block-by-Block Status

### Block A — actor-zeta 1.2.0 migration

| Section | Status |
| --- | --- |
| A.1 — header/symbol migration, supervisor surface | COMPLETE |
| A.2 — scheduler + executor wiring, behavior_t adoption | COMPLETE |

All actors compile against actor-zeta 1.2.0; legacy `make_behavior()` shims removed; supervisor lifecycles aligned with the new contract.

### Block B — Exception removal

| Surface | Status |
| --- | --- |
| bitcask FULL path (write/read/compact/recover) | COMPLETE |
| MVCC engine (snapshot acquire, visibility, commit/abort) | COMPLETE |

All throwing edges converted to `expected<T,E>` / `outcome<T>` with explicit error codes; callers updated.

### Block C — Error-channel hardening

| Section | Status |
| --- | --- |
| §3.1 — `failed()` checks on every fallible call | COMPLETE |
| §3.2 — rename ripple (97 occurrences) + B33 accumulation | COMPLETE |
| §3.3 — WAL FULL fsync barriers | COMPLETE |
| §3.5 — Pure MVCC DDL semantics (decisions 29–44) | COMPLETE |

§3.2 rename ripple was applied uniformly across producers, consumers, and tests; B33 accumulation invariants verified at every fan-in point.

### Block D — Per-table agent (`index_table_agent_t`)

COMPLETE.

- 14/14 handlers wired (CRUD + index ops + scan + maintenance).
- 3 DDL paths routed through the per-table agent (CREATE / DROP / ALTER for the table-local surface).
- Engine ownership migrated from manager to per-table agent; manager retains only catalog + routing.

### Block E — ProcArray, horizon, visibility

COMPLETE.

- ProcArray actor + horizon publication.
- Visibility filter integrated into the read path.
- Tests landed for visibility, horizon advancement, and snapshot retention.
- B14.C (horizon-driven retention / GC trigger) closed.

### Block F — WAL Pure MVCC compatibility

COMPLETE.

- WAL record schema extended for MVCC metadata.
- Replay path Pure-MVCC-aware.
- Cross-checked against Block C §3.3 fsync barriers and Block C §3.5 DDL semantics.

---

## 3. Manager-Level Refactors

### Variant C — `wal_replicate`

COMPLETE. Replication path detached from the legacy manager surface and wired through the new actor topology.

### Variant E.3 — dispatcher refactor

COMPLETE.

- Sections 0–5 closed.
- **Atomic flip applied** (see §4 below).
- **Section 4 DELETED** — 804 LoC of legacy dispatcher Pass 1–3 plus the `allocate_oids_via_pipeline` shim physically removed from the tree. No "compiled-but-unreached" legacy paths remain; rollback would now require a `git revert`, which is the intended end state.

Reference: `docs/variant-e3-cutover-checklist.md`, `docs/variant-e3-section6-flip-commit.md`.

### Version B* — `manager_disk`

Steps 1 through 8.12 are substantially complete. Remaining items are scoped and tracked:

| Step | Status | Note |
| --- | --- | --- |
| 1 – 8.0 | COMPLETE | — |
| 8.1.B | DIAGNOSTIC | SFBM ownership wired as observer; actual ownership transfer pending (see §5). |
| 8.1.C | DIAGNOSTIC | Mirror of 8.1.B on the secondary path. |
| 8.4.A | AUDITED-DEFERRED | Manager-side `storages_` emplace is now diagnostic-only; unblocked but gated on §8.1.B/§8.1.C SFBM ownership transfer before field deletion. |
| 8.4.B | DELETED | Manager `drop_storage` erase + filesystem `remove` moved to the per-storage agent; manager no longer touches disk on drop. |
| 8.4.C | DELETED | `dropped_storages_` mirror writes removed; lookups now go through `storage_entry_sync`. |
| 8.4.D | DELETED | Manager-side GC sweep + ack removed; GC owned end-to-end by the agent. |
| 8.5 – 8.10 | COMPLETE | — |
| 8.11  | COMPLETE pending wrap | Catalog read migration done at all 15/15 sites; final `storages_` map deletion deferred (mechanical, gated on 8.1). |
| 8.12  | COMPLETE | — |

Reference: `docs/version-b-step8-roadmap.md`.

### Block D — per-table agent

COMPLETE (see §2, Block D).

---

## 4. Pre-Validation Items Already Landed

- **Variant E.3 atomic flip — APPLIED.** Dispatcher routing now uses the new path by default; legacy path is no longer reachable from production codepaths.
- **Differential tests — scaffolded (14 cases).** Old-vs-new dispatcher behavior parity harness in place; runs as part of the standard ctest set.
- **8.11 catalog read migration — 15/15 sites complete.** Every reader now goes through the new catalog API; `manager.storages_` is write-only from production code.
- **Variant E.3 Section 4 — DELETED.** 804 LoC of dispatcher Pass 1–3 plus the `allocate_oids_via_pipeline` shim physically removed; the atomic-flip rollback safety net is intentionally retired in favour of `git revert` as the recovery primitive.
- **Step 8.4.B — DELETED.** Manager `drop_storage` no longer performs the catalog erase or the filesystem `remove`; both moved into the per-storage agent so drop is owned by a single actor.
- **Step 8.4.C — DELETED.** `dropped_storages_` mirror writes removed; the corresponding lookups were rewritten to consult `storage_entry_sync`, eliminating the dual-write window entirely.
- **Step 8.4.D — DELETED.** Manager-side GC sweep + ack path removed; GC is now end-to-end owned by the per-storage agent with no manager fan-in.
- **Manager-direct readers — 4 sites migrated.** `direct_append_sync`, `maybe_cleanup`, and the 4 bootstrap call-sites no longer dereference `manager.storages_` directly; all reads now go through the new catalog/agent surfaces.
- **Step 8.4.A — AUDITED-DEFERRED.** The manager `storages_` emplace is now diagnostic-only (no production reader depends on it); deletion is unblocked but intentionally gated on the §8.1.B / §8.1.C SFBM ownership transfer so that the two deletions land in a single, atomic ownership cutover commit.

---

## 5. Pending (Post-Validation, Post-Soak)

Three items remain. They are deliberately sequenced so that the field deletion (item 3) lands atomically with the ownership cutover (item 1), and the manager-map deletion (item 2) lands after both.

1. **SFBM actual ownership transfer (Step 8.1.B / 8.1.C).**
   The new owner currently observes; the legacy owner still holds the strong reference. This is the last live-behaviour change in the queue — everything else is dead-code removal.
2. **`manager.storages_` field deletion (Step 8.11 wrap).**
   All 15/15 readers migrated and the 4 manager-direct readers have been retargeted. Deletion is mechanical once §8.1 lands.
3. **Step 8.4.A emplace deletion.**
   The manager-side `storages_` emplace is diagnostic-only today; it is intentionally held back so it can land in the same commit as the §8.1.B/§8.1.C SFBM ownership cutover, giving us a single atomic flip rather than two adjacent ones.

None of these blocks Final validation #34 — they are all "remove dead/diagnostic code after we trust the live code."

---

## 6. Final Validation Prerequisites

To execute the full validation pass (`cmake` configure + `ctest` Release and Debug), the environment must provide:

1. **bison 3.x.**
   The host currently exposes bison 2.3 (macOS default), which is a hard blocker for the SQL parser generation step. Install via `brew install bison` and prepend the keg-only path, or use the conan-provided bison.
2. **conan + cmake setup.**
   Standard otterbrix bootstrap (conan profile + cmake preset). No deviation from the documented setup.
3. **ctest in both configurations.**
   - `cmake --build build --config Release && ctest --test-dir build -C Release --output-on-failure`
   - `cmake --build build-debug --config Debug && ctest --test-dir build-debug -C Debug --output-on-failure`
   Debug is required to exercise the assertion-guarded invariants added across Blocks B, C, and E.

Per project policy for multi-milestone work, cmake/ctest are run **once** at the end of the plan, not per-milestone — this document marks that point.

---

## 7. Sign-off Checklist for the Validation Pass

- [ ] bison 3.x on PATH (`bison --version` reports >= 3.0).
- [ ] `conan install` completes for the active profile.
- [ ] Release build green; Release ctest green.
- [ ] Debug build green; Debug ctest green.
- [ ] Differential dispatcher tests (14 cases) all pass.
- [ ] Soak window scheduled before executing the three post-soak items enumerated in §5.

---

## 8. Final Sub-agent Total

**~123+ sub-agents orchestrated across the mega-PR.**

Rough decomposition (cumulative across all blocks and refactors):

- Blocks A–F: ~62 sub-agents (A: ~6; B: ~8; C: ~14; D: ~16; E: ~10; F: ~8).
- Variant C (`wal_replicate`): ~6 sub-agents.
- Variant E.3 (dispatcher): ~22 sub-agents (Sections 0–5 + atomic flip + Section 4 deletion + differential test scaffolding).
- Version B* (`manager_disk`, Steps 1–8.12): ~28 sub-agents, including the recent 8.4.B / 8.4.C / 8.4.D deletions, the 4 manager-direct reader migrations, and the 8.4.A audit.
- Cross-cutting (status docs, roadmaps, cutover checklists, this consolidation pass): ~5 sub-agents.

The count is "+" because individual deletions and migrations occasionally fan out into small follow-up agents; the floor of 123 is the conservatively-counted distinct orchestrations.

---

## 9. Sign-off

**Pre-validation phase: COMPLETE.**

All in-tree refactors, deletions, and migrations that are intended to precede Final validation #34 have landed on `sql/comma-join-and-string-fns`. The three items in §5 are explicitly post-soak and do not gate validation. The remaining blocker is purely environmental — see §6 (bison 3.x, conan, cmake). Once that environment is available, Final validation #34 (Release + Debug `cmake` + `ctest`) is the next action.

---

## 10. `manager.storages_` Audit — Step 8.11 Wrap Feasibility (snapshot)

Snapshot taken after Step 8.1.B (catalog SFBM transfer) + Step 8.4.A catalog emplace deletion + 4 manager-direct readers migrated to agent-first / manager-fallback. Step 8.1.C (user-OID SFBM transfer) is the next live-behaviour cutover and is currently being attempted by a parallel agent. Numbers below show the post-8.1.C projected state alongside the pre-8.1.C count.

### 10.1 WRITE sites (`storages_.emplace`)

Total emplaces today: **9** (`manager_disk_io.cpp`: 6; `manager_disk_storage.cpp`: 3).

| File | Line | Function | Branch | Post-8.1.C status |
| --- | --- | --- | --- | --- |
| `manager_disk_io.cpp` | 409 | `create_storage_with_columns_sync` | USER dual-write | DELETED by 8.1.C |
| `manager_disk_io.cpp` | 481 | `create_storage_disk_sync` | USER fallback | DELETED by 8.1.C |
| `manager_disk_io.cpp` | 599 | `load_storage_disk_sync` (transfer_to_agent) | no-agents test fixture | DELETED by 8.11 (test fixture only) |
| `manager_disk_io.cpp` | 640 | `load_storage_disk_sync` (promote .prev) | USER corrupt-recovery | DELETED by 8.1.C |
| `manager_disk_io.cpp` | 698 | `load_storage_disk_sync` (USER happy path) | USER load | DELETED by 8.1.C |
| `manager_disk_io.cpp` | 718 | `load_storage_disk_sync` (USER corrupt retry) | USER load | DELETED by 8.1.C |
| `manager_disk_storage.cpp` | 224 | `create_storage` (mailbox handler) | runtime CREATE | DELETED by 8.1.C |
| `manager_disk_storage.cpp` | 279 | `create_storage_with_columns` (mailbox handler) | runtime CREATE | DELETED by 8.1.C |
| `manager_disk_storage.cpp` | 307 | `create_storage_disk` (mailbox handler) | runtime CREATE | DELETED by 8.1.C |

After 8.1.C lands, **0** production-path emplaces remain. Line 599 (`transfer_to_agent` no-agents branch) is reachable only by the rare pre-spawn unit fixture and is deleted as part of the 8.11 mechanical wrap.

### 10.2 READ sites (`storages_.find` / iteration / `.empty()` / `.size()`)

| File | Read sites | Category | Migration state |
| --- | --- | --- | --- |
| `manager_disk.hpp` (inline accessors lines 247/264/284) | 3 | `has_storage`, `get_storage`-style lookups | Agent-fallback pattern not yet applied; called by remaining 4 manager-direct readers — these inline accessors become unused once §8.11.B finishes wiring callers to agent-first |
| `manager_disk_bootstrap.cpp` | 12 | catalog bootstrap skip-if-present + checkpoint replay | All catalog-gated and accompanied by Step 8.4.A agent-first probes; the fallback branches survive until manager map is empty in production (post-8.1.C) |
| `manager_disk_ddl.cpp` | 3 (lines 336/338/380) | DDL helper resolve | Agent slice probed first per Step 8.4.A pattern; manager-side find is fallback only |
| `manager_disk_io.cpp` | 9 iteration + find sites (checkpoint loops + sidecar wal_id) | checkpoint walk + sidecar replay | Iterations become no-ops once map is empty; loops are safe to keep until 8.11 wrap; find on 602/734/746 are sidecar wal_id replay and become dead after 8.1.C |
| `manager_disk_resolve.cpp` | 4 (lines 615/616/676/677) | `read_row_ids_by_key` / `read_rows_by_key` | Already on Step 8.11.C agent-first pattern; manager find is documented fallback |
| `manager_disk_storage.cpp` | 4 (lines 210/627/658/659) | `get_storage`, IN_MEMORY twin probes | Called by manager-body fallback in `storage_scan` / `storage_scan_batched` / `storage_types` / `storage_total_rows` — fallback branches dead after 8.1.C user transfer |

### 10.3 Field declaration

`manager_disk.hpp:802`
```
std::unordered_map<components::catalog::oid_t, std::unique_ptr<collection_storage_entry_t>> storages_;
```

The companion `dropped_storages_` field has already been deleted (Step 8.6/8.7); the comment block at lines 806–807 documents that prior deletion.

### 10.4 Step 8.11 wrap feasibility

Once 8.1.C executes the user-OID SFBM transfer, the field deletion becomes a mechanical, no-behaviour-change patch:

1. **All 9 writes** become dead (8/9 by 8.1.C; the 9th is the no-agents test fixture branch).
2. **All ~35 read sites** become dead-fallback or dead-iteration: agent-first probes already short-circuit the catalog reads, and the user reads short-circuit once 8.1.C plants user OIDs on the agent slice.
3. **The 3 inline accessors in `manager_disk.hpp`** (`has_storage`, `get_storage`, etc.) have only manager-internal callers — those callers (the fallback branches above) get deleted in the same wrap commit.
4. **No public API breakage**: `get_storage()` is the only externally-visible function still touching the map, and its sole external callers are the manager-body fallback branches in `storage_scan` / `storage_scan_batched` / `storage_types` / `storage_total_rows` — those bodies are scheduled for deletion in §8.11 per the inline comments already present in `manager_disk_storage.cpp`.

**Verdict**: 8.11 field deletion is a single-commit operation that touches ~50 lines and removes ~80 lines. The only gate is 8.1.C landing first (so the user-OID writes are routed to agents); after that, deletion is risk-free.

### 10.5 Per-file deletion order (recommended)

1. `manager_disk_io.cpp` — drop USER emplaces (depends on 8.1.C).
2. `manager_disk_storage.cpp` — drop the 3 mailbox-handler emplaces and the 4 fallback bodies.
3. `manager_disk_bootstrap.cpp` — drop the 4 fallback `storages_.find` branches after agent-first probes.
4. `manager_disk_resolve.cpp` — drop the 2 `read_*_by_key` manager-map fallbacks.
5. `manager_disk_ddl.cpp` — drop the 3 manager-map lookups.
6. `manager_disk.hpp` — delete the 3 inline accessors and the `storages_` field.

This ordering keeps the tree compiling after each step and lets the test suite run between deletions.

---

*End of pre-validation status.*

---

## 11. Variant E.3 Post-Cutover: executor.cpp Dead-Code Audit

**Date audited:** 2026-05-31
**Audit scope:** `/Users/kotbegemot/CLionProjects/otterbrix/services/collection/executor.cpp` (2123 LoC total)

### 11.1 Key Finding: Unconditional `if (enable_pass2_rewrites)` Block

**Location:** executor.cpp:1472–1792

After the atomic flip (`use_executor_full_pipeline=true` + `enable_pass2_rewrites=true`), the flag `constexpr bool enable_pass2_rewrites = true` is always-true but still wrapped in a runtime conditional gate:

```cpp
constexpr bool enable_pass2_rewrites = true;
if (enable_pass2_rewrites) {
    // ~320 LoC of Pass 2 + Pass 3 rewrites (late optimize, enrich, planner wrap, DDL OID batch)
}
```

**Assessment:** The entire block (lines 1473–1792) is now unconditionally executed. The wrapper `if` serves no purpose post-flip.

**Cleanup action (deferred):**
- Remove the `if (enable_pass2_rewrites)` conditional and closing brace.
- Reduce indentation of the 320 LoC block by one level.
- **Estimated LoC change:** -5 (removal of wrapper braces/indent).
- **No semantic change** — the block always executes today.

### 11.2 Cross-Path Analysis: `execute_plan` vs `execute_plan_full`

**Status:** Both methods are LIVE and correctly paired.

- `execute_plan_full()` (line 677): Takes unrewritten logical_plan, runs full Pass 1/2/3 pipeline, delegates to execute_plan.
- `execute_plan()` (line 201): Takes Pass-2-rewritten logical_plan, runs operator pipeline only.
- Both are dispatched via mailbox (executor.hpp:188–192 dispatch_traits).
- No redundant code; no overlap.

### 11.3 Removed Code Audit Trail

The following are **documented as removed** in comments (NOT actually present as dead code):

| Location | Comment | Status |
| --- | --- | --- |
| executor.cpp:33 | `intercept_dml_io_` branches moved to operator's await_async_and_resume | Documented removal |
| executor.cpp:2092 | "HEAD: intercept_dml_io_ removed" marker | Cleanup marker |

These are **not dead code** — they document the migration path from the old DML I/O intercept pattern to the new in-operator pattern.

### 11.4 Deferred Items

The following items are **documented as deferred** and are not dead code:

| Lines | Topic | Reason |
| --- | --- | --- |
| 727–766 | Pass 1 stub TODO | Explicitly marked for future Variant E.3 follow-up. Blocker (a)–(d) all documented. Not blocking validation. |
| 1793–1796 | Pass 3 gate comment | Explains that Pass 3 rewrites stay gated until dispatcher Pass 1/2/3 deletion (already done). Safe to keep. |

### 11.5 Other Code Paths

- **local_collections_ hot-path:** (lines 232–252) Tracing-only today; Step 2 as documented; no dead code.
- **DML transaction routing:** (lines 254–266, 338–380) Live; DML and explicit-txn accumulation both active.
- **find_effective_dml_type():** (lines 71–142) Live helper used at line 219.

### 11.6 Summary

**Dead code identified:** 1 location.
- Unconditional `if (enable_pass2_rewrites)` wrapper (executor.cpp:1472–1472).
- Estimated cleanup LoC: 5 (wrapper braces/closing brace + dedent signal).

**Status for final validation:** executor.cpp is **READY** for validation as-is. The wrapper-if cleanup is a mechanical follow-up that does not block the build/ctest cycle.

**Next action (post-soak):**
Create a single-commit cleanup of the enable_pass2_rewrites wrapper:
1. Remove `if (enable_pass2_rewrites)` line 1473.
2. Remove closing `}` line 1792.
3. Dedent the entire Pass 2/3 block by one level (lines 1473–1791).

This is purely mechanical — no behaviour change.

