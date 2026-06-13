# Post-PR #508 roadmap: durability, index coherence, transaction semantics, performance

Status: IMPLEMENTED on this branch. M1 (601a672b, 7b1ce270), M2 (607c04d1),
M3.1 (a577855f, superseded by M3.2 which lifts the ban), M3.3/M3.4/M3.5 +
M4 P1/P4/P5/P6/P8 + the M1.3/M2.4 analyses (02032d7a), M3.2 transactional DDL
(final commit). M4-P3 closed by measurement: the per-autocommit commit-node
planner pass is negligible (single-operator plan; the measured INSERT-SELECT
delta is commit execution, i.e. actor round-trips). M4-P7 (Q6 watchdog
removal) SKIPPED — blocked on an upstream actor-zeta fix. M2 phase 2b kept
gated (same measurement). Scope was: everything surfaced by the multi-agent
review of PR #508 (34 confirmed/plausible findings) plus previously deferred
items. The five "pending follow-up track" findings below were fixed in
d656fe87.

Ordering decision: **M1 Durability → M2 Index coherence → M3 Semantics → M4 Perf**.

## Cross-cutting constraints (apply to every milestone)

1. `logical_value_t` — minimize or remove; no new round-trips, typed accessors
   (`.value<T>()`) inline.
2. No exceptions — internal error system (`core::error_t`, result wrappers,
   cursor errors).
3. Everything executes through: logical plan → planner → optimizer → physical
   plan generator → executor → disk/index. (Directly constrains M4-P3: no
   operator construction bypassing the planner.)
4. Build at the last step, after all steps.
5. Task list + parallel execution with agents.
6. No fallbacks, no backward compatibility (e.g. M1.1 has no "replay-all when
   no set was provided" mode — the gate is mandatory).
7. Use agents wherever possible.
8. `std::pmr::*` wherever possible (cross-mailbox value structs stay plain std
   per the `transaction_data` pattern, with an inline comment).
9. Exceptions on hot paths and between actors are forbidden.
10. No shared objects/state between actors, actor↔actor_mixin,
    actor_mixin↔actor_mixin — mailbox-only. (M1.1 passes `committed_txns` by
    VALUE during the single-threaded pre-scheduler bootstrap phase — legal.)
11. Direct sync method calls via parent pointer are forbidden; only in
    base_spaces (the M1.1 bootstrap phase is exactly that case).
12. Pure MVCC (no locks).
13. All callbacks + parent-pointer sync calls between actors must go; only
    base_spaces may use them.
14. Forbidden: `std::shared_ptr`, `std::any`, `std::variant`,
    `std::tuple`/`<tuple>`, `std::function`,
    `std::pmr::get_default_resource()`, defaulted
    `std::pmr::null_memory_resource()`.
15. No goto/labels.
16. Always key by oid; where impossible, ask (txn handlers are keyed by
    `session_id_t` — sessions own transactions, agreed earlier;
    `committed_txns` is txn-id space by design of M1.1).

## Pending PR #508 follow-up track (NOT this roadmap; listed for completeness)

| Review finding | Where | Fix shape |
|---|---|---|
| Use-after-free in batch `commit_inserts`/`commit_deletes` (raw engine pointers cached across co_await; `unregister_collection`/`on_horizon_advanced` can erase the engine) | `services/index/manager_index.cpp` | Re-lookup `engines_.find(oid)` after the awaits; vanished engine = dropped table, skip |
| `tables_without_indexes` treats an EMPTY engine as "has index" (bootstrap creates an engine per table) → compact gate blocks everything, MVCC compact dead again | `services/index/manager_index.cpp` | Gate on "engine holds ≥1 real index" (`engine->size()`), not entry presence |
| SET TIMEZONE runs under a txn that is never committed nor aborted → pg_settings row lost on replay + GC horizon pinned forever | `services/collection/executor.cpp` | Route SET TIMEZONE through the unified commit tail |
| VACUUM's pg_computed_column deletes run under a txn that the read-only release tail aborts → tombstones invisible, GC effect nil | `services/collection/executor.cpp` | Classify VACUUM as commit-bearing (same mechanism as SET TIMEZONE fix) |
| SQL ROLLBACK does not send `revert_insert` → PENDING in-memory index entries of the aborted txn linger | `components/physical_plan/operators/operator_abort_transaction.cpp` | Mirror the failed-DML revert: `revert_insert` per drained table oid |

## Finding → milestone map

All 34 verified findings (CONFIRMED/PLAUSIBLE survivors of adversarial
verification; 24 further candidates were refuted). Effort: S < 1 day,
M = days, L = weeks.

| # | Finding (file:line) | Milestone | Effort |
|---|---|---|---|
| F1 | Bitcask txn-log durable before WAL marker; replay ungated → phantom index entries (`operator_commit_transaction.cpp:257`) | M1.1 | M |
| F2 | Dead auto-checkpoint config (`manager_wal_replicate.hpp:119`) | M1.2 | S |
| F3 | Storage publish flips MVCC before WAL marker (`operator_commit_transaction.cpp:169`) | M1.3 (analysis) | S |
| F4 | DDL WAL prefix written with commit_id=0 before allocation (`operator_commit_transaction.cpp:40`) | M1.3 (analysis) | S |
| F5 | `lowest_active_snapshot_horizon` vs in-flight (allocated, unpublished) commits (`transaction_manager.cpp:116`) | M2.4 (analysis) | S |
| F6 | checkpoint/vacuum compact indexed tables mid-session (pre-existing) | M2.1 | L |
| F7 | VACUUM index rebuild produces invisible PENDING entries (pre-existing, found by plan validation) | M2.1 | (same) |
| F8 | On-disk hash index returns wrong-row results after row_id reuse (`disk_hash_single_field_index.cpp:62-68`) | M2.2 | M |
| F9 | `on_horizon_advanced` index drop chain edge (`manager_index.cpp:1172`) | M2.4 (analysis) | S |
| F10 | GC broadcast timing from `txn_publish_msg` (`dispatcher.cpp:346`) | M2.4 (analysis) | S |
| F11 | DDL inside BEGIN commits the whole txn; ROLLBACK silently loses prior DML (`executor.cpp:1468`) | M3.1 + M3.2 | S + L |
| F12 | Bare COMMIT allocates a spurious commit_id and advances the horizon (`dispatcher.cpp:587`) | M3.3 | S |
| F13 | Failed DDL leaves its txn orphaned (`executor.cpp:1534`) | M3.4 | M |
| F14 | No CREATE INDEX failure abort path (`executor.cpp:1528`) | M3.4 | M |
| F15 | Failed-DML revert skips pending index DELETE entries (`executor.cpp:1429`) | M3.4 | S |
| F16 | Index commit error after storage publish = partial commit, no undo (`operator_commit_transaction.cpp:230-231`, two findings) | M3.5 | M |
| F17 | Bitcask is assert-terminal; error channel latent | M3.5 | M |
| F18 | DROP-GC remap fires only in ddl-commit mode (`operator_commit_transaction.cpp:110`, two findings) | M3.2 (with drop-tracking flag) | S |
| F19 | Cascade drop fan-out cross-mailbox ordering note (`operator_dynamic_cascade_delete.cpp:287`) | M3.4 (analysis) | S |
| F20 | Two extra round-trips on every append-only commit (`operator_commit_transaction.cpp:312`) | M4-P1 | S |
| F21 | Remap fan-out on every DDL commit without drops (`operator_commit_transaction.cpp:92`) | M4-P2 | S |
| F22 | Full planner pass per autocommit commit node (`executor.cpp:1040`) | M4-P3 | M |
| F23 | Non-pmr containers + by-value lambda in resolve wrap (`executor.cpp:336`) | M4-P4 | S |
| F24 | Publish gate over-specified (start-time value where bool suffices) (`dispatcher.cpp` txn_publish_msg) | M4-P5 | S |
| F25 | maybe_cleanup gate semantics note (`agent_disk.cpp:1150`) | M4-P5 (document) | S |
| F26 | Triple materialization of oid containers in commit operator (`operator_commit_transaction.cpp:297`) | M4-P6 | S |
| F27 | `swap_backfills` copied element-wise from drain instead of moved (`operator_commit_transaction.cpp:78`) | M4-P6 | S |
| F28 | `scan_by_keys` empty-result shape inconsistency (`manager_disk_resolve.cpp:710`) | M4-P6 | S |
| F29 | `scan_by_keys_inner` per-key move asymmetry (`agent_disk.cpp:79`) | M4-P6 | S |
| F30 | Batch engine flip notes (`manager_index.cpp:391/:455`) | superseded by the follow-up UAF fix (re-lookup) | — |
| F31 | Q6 lost-wakeup watchdog (pre-existing; `docs/actor-zeta-lost-wakeup.md`) | M4-P7 | M |
| F32 | Pre-existing `std::make_tuple` (manager sync packs), `std::function` (`validate_logical_plan.cpp`) | M4-P8 | S |

(In-PR follow-up track findings — UAF, empty-engine gate, SET TIMEZONE, VACUUM
aborted deletes, ROLLBACK revert — complete the 34; see the table above.)

---

## M1 — Durability

### M1.1 Bitcask txn-log commit gate (F1)

**Problem.** Index txn-log frames are fsync'd durable
(`bitcask_index_disk.cpp:602`) BEFORE the WAL commit marker is written
(`operator_commit_transaction.cpp:216→264`). `recover_txn_log_unlocked`
(`bitcask_index_disk.cpp:605-669`) replays every synced frame unconditionally.
A crash inside the window leaves durable index entries for a transaction whose
table records WAL replay rejects (no COMMIT marker) → phantom index entries.

**Design (chain verified).**
- `wal.cpp` already computes `committed_txns` during load (`wal.cpp:281-286`) —
  a plain local `std::set<uint64_t>` discarded today. Return it alongside the
  records (no WAL types leak).
- base_spaces spawns the index disk agents ITSELF
  (`base_spaces.cpp:444`, `actor_zeta::spawn<index_agent_disk_t>`), not via
  manager_index. Thread the set: spawn → new `index_agent_disk_t` ctor param →
  `make_index_disk` (`index_agent_disk.cpp:9-24`) → `bitcask_index_disk_t`
  ctor → recover gate. `manager_index::bootstrap_index_sync` is untouched (it
  receives an already-built agent). Value copy during the single-threaded
  pre-scheduler window (`base_spaces.cpp:363`, schedulers start at
  `:366-368`) — no cross-actor sharing.
- Recover gate: apply a frame only if `txn_id ∈ committed`. There is NO
  `txn_id==0` frame class — both writers are guarded by `txn_id != 0`
  (`index_agent_disk.cpp:81/:111`); no special case.
- A skipped frame still advances `write_applied_log_offset(frame_end)` —
  mechanics verified safe (tellg-based frame end, atomic tmp+rename offset
  write, `bitcask_index_disk.cpp:539-560/:666-667`).
- btree / disk_hash have no txn logs — out of scope.

**Test.** Unit test in `services/index/tests`: write frames under an
uncommitted txn → recover with the gate → index empty, applied offset advanced.

### M1.2 Auto-checkpoint revival (F2)

**Problem.** `auto_checkpoint_threshold_bytes` config is silently dead: the
byte counter is alive (`manager_wal_replicate.cpp:362`, updated after every
commit_txn) and `needs_auto_checkpoint()`/`reset_auto_checkpoint_bytes()`
(`manager_wal_replicate.hpp:119-126`) exist, but the deleted handler was a
never-wired stub — nothing triggers checkpoint+truncate on WAL growth.

**Design.** Today's orchestration is the CHECKPOINT statement operator
(`operator_checkpoint.cpp:15-54`: flush_all_indexes → current_wal_id →
checkpoint_all → truncate_before, gated on checkpoint_wal_id>0). The WAL
manager already holds the disk address (`manager_disk_`,
`manager_wal_replicate.hpp:155`, unpacked in sync()). Auto variant:
self-orchestrate in manager_wal after commit_txn when the threshold trips —
checkpoint_all via its own `manager_disk_`, then its own truncate_before; a
"checkpoint in flight" dedup flag prevents stacking.

**Test.** Lower the threshold in config → a series of commits → WAL segments
truncated.

### M1.3 Crash-window analyses (F3, F4) — document, then decide

- **F3**: `storage_publish_commits/deletes` flip MVCC commit ids before the WAL
  marker. The flip is in-memory state whose durability comes from
  WAL+checkpoint, so a crash discards it together with the unmarked txn —
  expected harmless, but write the invariant down next to the publish block and
  add a crash-replay test around it.
- **F4**: the DDL prefix writes its WAL record with commit_id=0 before the real
  commit_id exists. Replay gates by transaction_id, not cid, so this is benign
  today; the analysis (and the constraint it puts on future replay changes)
  belongs in `docs/` and a comment at `operator_commit_transaction.cpp:40`.

## M2 — Index coherence: rebuild-on-compact

⚠️ Plan validation REFUTED the "vacuum already does this correctly" model and
found a NEW pre-existing bug (F7): the vacuum rebuild
(`operator_vacuum.cpp:143-151`) sends `insert_rows` under `ctx->txn`, so the
rebuilt entries land in the PENDING bucket (`single_field_index.cpp:73-82`)
under a txn that never index-commits (VACUUM is neither DML nor DDL) — the
rebuilt index is INVISIBLE to all subsequent readers
(`index_entry_visible`, `components/index/index.hpp:29-36`). The correct model
is `bootstrap_repopulate_sync` (`manager_index.cpp:755-784`): repopulate with
**txn_id=0**, which the visibility rule treats as committed-for-everyone.

### M2.1 Runtime repopulate + checkpoint/vacuum coverage (F6, F7)

- New mailbox handler `manager_index_t::repopulate_table` — the runtime
  analogue of `bootstrap_repopulate_sync`: clear the engine, re-insert with
  txn_id=0. Used by BOTH the vacuum fix and the checkpoint path.
- New enumeration API `manager_index_t::all_indexed_oids` (engines_ keys
  holding ≥1 real index). Verified: no existing handler enumerates indexed
  oids.
- Checkpoint path: after checkpoint_all, the CHECKPOINT operator runs
  `all_indexed_oids → per-oid storage_scan_segment → repopulate_table`.
  Vacuum fix: replace insert_rows-under-txn with `repopulate_table`.
  System pg_catalog tables compact too — verify engines_ coverage for system
  oids.

### M2.2 On-disk index staleness is NOT harmless (F8)

`disk_hash find_impl` returns stored row_ids with NO validation
(`disk_hash_single_field_index.cpp:62-68`). After compact reuses small row_ids,
an in-bounds stale id maps to a DIFFERENT live row → wrong-row query results
(worse than the out-of-bounds-skip case). In-session rebuild must therefore
either refresh the on-disk index too, or exclude it from search until rebuilt.
(The bootstrap path repairs only the in-memory engine — documented gap,
`base_spaces.cpp:470-484`.)

### M2.3 Commit-path compact gate (interim)

The `tables_without_indexes` gate stays during M2 (verified compatible with
the checkpoint rebuild). Optional phase 2b: lift the gate by returning
compacted-oids from `maybe_cleanup_many` and repopulating — decide after
measuring scan cost on the commit path; otherwise the gate remains and
checkpoint/vacuum reclaim indexed tables.

### M2.4 GC-horizon edge analyses (F5, F9, F10)

- F5: `lowest_active_snapshot_horizon()` returns `published_horizon_` when no
  txn is active — but a commit that has ALLOCATED a commit_id and not yet
  published sits in `in_flight_commits_`. Analyze whether the broadcast can
  overtake an in-flight commit's tombstones (remap runs pre-publish, so the
  suspect window is narrow); add an invariant test.
- F9: the `on_horizon_advanced` index reclaim sends the terminal
  `index_agent_disk_t::drop` via `disk_agents_per_oid_` lookup — verify the
  map entry lifecycle vs late drops.
- F10: the broadcast fires from `txn_publish_msg` while the remap was sent
  earlier from the operator — both ride the same mailboxes; write down the
  ordering proof next to `try_trigger_cleanup_if_horizon_advanced`.

## M3 — Transaction semantics

### M3.1 Forbid DDL inside an explicit transaction (F11, short-term)

Pre-check `is_explicit && needs_ddl_txn` → error cursor
"DDL is not allowed inside a transaction block".
Placement (verified): BEFORE the rewrite block at `executor.cpp:998` (after
needs_ddl classification `:466` / validate `:985`) — inside the rewrites,
allocate_oids already bumps the persistent OID counter
(`:1165/:1229/:1311`), and the pre-check must precede that side effect.
Test: `BEGIN; INSERT; CREATE INDEX` → error; ROLLBACK undoes the INSERT.

### M3.2 Full transactional DDL (PG-style, long-term)

- Already works: catalog rows are MVCC-visible to their own txn
  (`row_version_manager.cpp:23-24`, self-write rule).
- Needs undo channels for non-MVCC side effects: `create_storage` (disk),
  index-engine creation, oid allocation (decide: oid leak on rollback
  acceptable?), and `drop_storage` must MOVE from the DROP plan to commit time
  (pairs with the remap mechanism).
- Subsumes F18: generalize the DROP-GC remap gate from "ddl-commit mode" to a
  "txn had drops" flag carried by the drain (the transaction tracks drops, the
  operator keys off the drain, not the node mode).

### M3.3 Empty COMMIT must not allocate a commit_id (F12)

In `txn_commit_drain_msg` (`dispatcher.cpp:615-642`): when nothing was
accumulated → abort instead of commit, return commit_id=0. Verified: the
operator skips all publishes/WAL/barrier on `commit_id_==0`, and a read-only
explicit COMMIT does not depend on a real commit_id. Requires a new
`transaction_t::has_accumulated()` — the pending base vectors are private with
no emptiness probe (mirror `txn_accumulate_payload_t::empty()`,
`txn_messages.hpp:90`). Test: COMMIT with no open txn → horizon unchanged.

### M3.4 DDL failure paths (F13, F14, F15, F19)

- Add a DDL-failure branch mirroring the DML error path
  (`executor.cpp:1395-1460`): revert `exec_result.pg_catalog_appends`
  (+ CREATE INDEX backfill dml_appends) via `storage_revert_appends`, then
  `txn_abort_msg`. Verified boundary: the FAILING fragment's own appends never
  reach exec_result (the operator failed before the lift) — only
  earlier-completed fragments are reverted; document that.
- F15: extend the failed-DML revert to pending index DELETE entries (today
  only dml_appends drive `revert_insert`; aborted delete markers linger in the
  pending bucket).
- CREATE INDEX failure (F14): capture the pg_index oid at rewrite time
  (verified derivable next to `create_index_table_oid`), revert the pg_index
  row, drop the index disk agent (new message — design here).
- F19: write down the cross-mailbox ordering argument for the cascade-drop
  two-phase fan-out (index vs disk mailboxes are independent FIFOs; the only
  ordering that matters is per-mailbox).

### M3.5 Real index error channel (F16, F17)

Bitcask is assert-terminal today, so the operator's `set_error` channel after
index commits is unreachable — AND the error would arrive after storage
publishes already ran (partial commit, no undo). Prerequisite for M3.4:
convert bitcask write failures to `core::error_t`, then design the
compensation (un-publish is impossible under pure MVCC — the realistic shape
is: index commit moves BEFORE storage publish, or a WAL-replayable repair
record).

## M4 — Performance / cleanup

- **P1 (F20)**: send `tables_without_indexes`/`maybe_cleanup_many` ONLY when
  `base_delete_table_oids` is non-empty. Verified safe: append-only commits can
  never trip the 30% dead-rows threshold (deleted = total − committed = 0), and
  aborted appends are physically reverted, not left as garbage. S.
- **P2 (F21)**: gate the DROP-GC remap sends on a "txn had drops" drain flag
  (shared mechanism with M3.2/F18). S.
- **P3 (F22)**: per-autocommit planner pass for the commit node. Verified: the
  create_plan impl is trivial, so the cost is planner dispatch +
  traverse_plan_ + pipeline_context setup. Measure first; options are a
  fast-path inside create_plan or caching the logical node — NOT direct
  operator construction (rule 3). M.
- **P4 (F23)**: pmr-ify the resolve-wrap block (`executor.cpp:318-431`, 8
  containers, by-value string lambda → string_view). Verified pure locals. S.
- **P5 (F24, F25)**: shrink the publish gate to 0/1 semantics and document the
  two value spaces (compact gate = start-time, GC broadcast = commit-id) next
  to both call sites. S.
- **P6 (F26-F29)**: micro-cleanups in the commit operator and scan_by_keys
  (single materialization of oid vectors; move `swap_backfills` out of the
  drain — align the drain struct field with the pmr operator local; unify
  scan_by_keys empty-result shape — always one row per key; symmetric per-key
  move in `scan_by_keys_inner`). S.
- **P7 (F31)**: upstream the actor-zeta lost-wakeup fix — bug is the
  blocked-check before the busy-check (`cooperative_actor.hpp:288-294`; Q6
  block `:296-308`; conan package is read-only → upstream patch + version
  bump). Then delete: dispatcher watchdog loop (`dispatcher.cpp:189-209`),
  `stale_ticks++` (`:154-155`), `in_flight_entry_t::stale_ticks`, executor
  `poke_msg` (decl/traits/case/def). M.
- **P8 (F32)**: pre-existing `std::make_tuple` in other managers' sync packs,
  `std::function` in `validate_logical_plan.cpp` — convert to named structs /
  function_ref-free shapes per rule 14. S.

## Dependencies

- M2.1 removes the semantic load from the commit-path compact gate; M2.2 may
  block lifting it (wrong-row hazard).
- M3.2 depends on M3.4 mechanics (undo channels) and on moving
  `drop_storage` to commit time; F18/P2 share the drop-tracking flag.
- M3.5 is a prerequisite for honest M3.4 error handling on the index side.
- P7 depends on an upstream actor-zeta release.

## Test additions (cross-milestone)

- Crash-window replay tests around the commit pipeline (M1.1, M1.3).
- CHECKPOINT → index-scan in the same session; VACUUM on an indexed table →
  index-scan still sees rows (catches F7); DELETE>30% → compact → correct scans
  (M2).
- `BEGIN; INSERT; CREATE INDEX` → error; ROLLBACK restores (M3.1).
- COMMIT with no open txn → horizon unchanged (M3.3).
- Failed DDL → txn aborted, catalog rows reverted (M3.4).
