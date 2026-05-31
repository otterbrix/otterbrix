# Version B* Step 8 — Roadmap: Delete `manager_disk_t::storages_` / `dropped_storages_`

Status: **PLANNING ONLY** — no code change in this document. Risk is high
because Step 8 deletes the canonical owner of every collection_storage_entry_t
and moves the live `single_file_block_manager_t` across the actor boundary.

## Scope (recap from `manager_disk.hpp` lines 666-743)

1. Delete `manager_disk_t::storages_` and `manager_disk_t::dropped_storages_`.
2. Move `single_file_block_manager_t` ownership (DISK-mode entries) to agents.
3. Update sync accessors `has_storage`, `total_rows_sync`,
   `checkpoint_wal_id_sync` to delegate to `agents_[pool_idx_for_oid(oid)]`.
4. Migrate runtime user-table create paths
   (`create_storage` / `create_storage_with_columns` / `create_storage_disk`
   mailbox handlers) so user OIDs land in the routed agent slice instead of
   the manager map.
5. Drop the `FIRST_USER_OID` routing gate from the storage_* mailbox handlers
   (catalog-only fanout becomes "every OID" fanout).

## Inventory of accesses that must be redirected

### Manager-side direct `storages_.find/emplace/erase`

| File | Lines | Role |
|------|-------|------|
| `services/disk/manager_disk.hpp` | 230, 235-239, 242-247, 754 | sync accessors + the map itself |
| `services/disk/manager_disk_storage.cpp` | 178-184, 192, 205, 221-222, 235-249, 543-545, 574-606 | get_storage + create_storage_*/drop_storage mailbox bodies + storage_append schema-growth |
| `services/disk/manager_disk_io.cpp` | 67-80, 248-281, 288-432, 436-453, 457-460 | checkpoint_all ordered walk + maybe_cleanup + create_storage_*_sync + load_storage_disk_sync + peek_checkpoint_wal_id_from_disk + load_storage_for_wal_replay_sync |
| `services/disk/manager_disk_bootstrap.cpp` | 152, 212-213, 279-280, 300, 316, 328-329, 404, 427-428, 473-474, 530-531 | bootstrap/load system tables, restore_oid_generator, load_user_table_storages, alive_user_oids, scan_dropped_oids, read_setting |
| `services/disk/manager_disk_resolve.cpp` | 13, 42, 74-75, 166-167, 245-246, 271-272, 301-302, 384-385, 424-425, 463-464, 507-508 | resolve_namespace/table/type/function/_by_name scans |
| `services/disk/manager_disk_ddl.cpp` | 54-55, 120-121, 246-247 | DDL: pg_catalog row scans/appends |
| `services/disk/manager_disk.cpp` | 488-515, 575, 605, 645 | on_horizon_advanced manager-side GC body + mark_storage_dropped reader + register_dropped_storage_sync push_back |

### Sync accessor public callers (header lines 230, 235, 242)

* `integration/cpp/base_spaces.cpp` lines 207-223 — WAL-replay `has_storage`
  loop + the lazy `create_storage_with_columns_sync` fallback.
* `services/disk/tests/test_d4_lazy_load.cpp` lines 83-92, 100, 116, 142, 159, 165, 186 — uses `manager->has_storage(oid)` to assert lazy-load semantics.
* `services/disk/tests/test_ddl_methods.cpp` line 618 — references storages_ for a never-allocated oid.

### `FIRST_USER_OID` gate in storage_* mailbox handlers

`services/disk/manager_disk_storage.cpp` lines 264-266, 299-301, 358-360,
413-415, 460-462, 500-502, 781-782, 819-820, 857-858, 900-901, 961-963,
1014-1016, 1072-1074. Plus `manager_disk_io.cpp` 69-80 (checkpoint_all
catalog-first ordering). 13 routing-gate sites + 1 ordering site.

## Sub-step decomposition (12 sub-steps; serial ordering enforced by data dependencies)

### Sub-step 8.1 — Move DISK-mode entry ownership (single_file_block_manager_t)

Promote Step 3 DISK record-only markers (null `unique_ptr`) to real
entries on the agent side. Replace
`agent_disk_t::bootstrap_record_oid_sync(oid)` with a new
`bootstrap_disk_inner_sync(oid, otbx_path, sidecar_wal_id)` that constructs
the `collection_storage_entry_t` directly on the agent (no twinning;
exclusive `file_handle_t` lives on the agent thread).

**Pre-requisite**: must run AFTER the agent's mailbox lifetime starts the
`single_file_block_manager_t` background flushes safely — verify
`agent_disk_t` does not yet hold an `std::pmr::synchronized_pool_resource`
that would cross threads. (Status today: agent already owns its
`resource()`; OK.)

**Touch**:
- `services/disk/agent_disk.hpp` — add `bootstrap_disk_inner_sync`,
  retire `bootstrap_record_oid_sync` (keep until 8.2 finishes; remove in 8.12).
- `services/disk/agent_disk.cpp` — body (open file, construct entry).
- `services/disk/manager_disk_io.cpp` lines 322-432
  (`create_storage_disk_sync` + `load_storage_disk_sync`) — flip to the new
  call. Manager-side `storages_.emplace` becomes the secondary path (still
  kept until 8.4).

**LOC**: ~120 added / ~30 changed.
**Days**: 1.0.
**Tests**: `test_d4_lazy_load.cpp` (system-table half), checkpoint round-trip
in `test_recovery_*`.

#### Sub-sub-step decomposition (added by Step 8.1.A landing)

The original 8.1 spec is incompatible with the live reader contract:
`manager_disk_resolve.cpp`, `manager_disk_ddl.cpp`,
`manager_disk_bootstrap.cpp::read_setting_sync`, and the
`bootstrap_system_tables_sync` post-creation checkpoint loop all walk
`storages_.find(pg_*_oid)` synchronously and dereference
`it->second->table_storage.table()`. Moving SFBM ownership to the agent
without first migrating those readers either crashes (null deref on a
marker entry) or double-opens the `.otbx` (file_handle_t WRITE_LOCK is
posix-advisory per-process — closing either fd releases the lock for
both, see `core/file/local_file_system.cpp:380` `F_SETLK`). The
roadmap's "Manager-side storages_.emplace becomes the secondary path"
line therefore cannot be satisfied as-written without a co-landing of
8.8 (bootstrap) + 8.9 (resolve+ddl) reader migration, which is ~2000 LOC
out of scope for a "smallest-move" step.

Splitting 8.1 into 3 sub-sub-steps preserves the ownership-handoff
spirit while keeping each commit revertable in isolation:

* **8.1.A — API surface only (this commit).**
  Land `agent_disk_t::bootstrap_disk_inner_sync(oid, otbx_path,
  sidecar_wal_id)` + `bootstrap_create_disk_inner_sync(oid, columns,
  otbx_path)` as bootstrap-only pre-scheduler primitives. No call site
  routes through them yet — every catalog AND user DISK entry remains
  owned by `manager_disk_t::storages_`. `bootstrap_record_oid_sync`
  (Step 3 marker) is left untouched. Touch: ~70 LOC added (header +
  bodies + roadmap doc), 0 LOC behaviour change.

* **8.1.B — Catalog SFBM cutover (depends on 8.8 + 8.9).**
  Once `manager_disk_bootstrap.cpp` and `manager_disk_resolve.cpp` read
  through the catalog agent (8.8 Path A `storage_entry_sync` accessor),
  flip the two manager-side helpers
  (`create_storage_disk_sync` lines 322-332,
  `load_storage_disk_sync` lines 334-432) to route catalog OIDs (oid <
  `FIRST_USER_OID`) through `agents_[0]->bootstrap_(create_)disk_inner_sync`
  instead of `storages_.emplace`. Delete the catalog branch of the
  manager-side emplace; user OIDs still take the legacy path until 8.2.
  Touch: ~80 LOC changed in `manager_disk_io.cpp` + agent's
  `checkpoint_inner` extended to actually checkpoint DISK entries it now
  owns (~40 LOC), ~10 LOC deleted in record-only marker path.

  > **Status (2026-05-31) — CATALOG-APPLIED.** The catalog SFBM
  > ownership transfer is now EXECUTED for `oid < FIRST_USER_OID` in
  > both helpers:
  >
  > - `create_storage_disk_sync` — catalog branch routes through
  >   `agents_[0]->bootstrap_create_disk_inner_sync(oid, columns,
  >   otbx_path)`; manager-side `storages_.emplace` is SKIPPED entirely
  >   for catalog OIDs (`return` after the agent call).
  > - `load_storage_disk_sync` — catalog branch routes through
  >   `agents_[0]->bootstrap_disk_inner_sync(oid, otbx_path,
  >   sidecar_wal_id)` after a probe-and-release pre-construction on the
  >   manager thread (catches corrupt-file `std::exception` for the
  >   `.prev` recovery branch; immediately resets the probe to release
  >   the file_handle_t WRITE_LOCK before the agent reopens — posix
  >   advisory lock is per-process so the close-then-reopen window is
  >   safe pre-scheduler-start). Sidecar `.wal_id` is read on the
  >   manager thread and passed through so the agent SFBM picks up the
  >   checkpoint floor atomically.
  > - The `[step-8.1.B-sfbm-construct]` trace tag is preserved on both
  >   helpers (kind={CATALOG,USER} discriminator) so log scans can
  >   confirm the new invariant: per `.otbx` per process boot, exactly
  >   one entry construction (agent-side for catalog, manager-side for
  >   user until §8.1.C).
  > - `manager_disk_t::storages_[catalog_oid]` is empty post-load;
  >   readers route through `agents_[0]->storage_entry_sync(oid)` per
  >   §8.11.B catalog-reader migration (already applied 2026-05-31).
  > - USER OID branch (`oid >= FIRST_USER_OID`) preserves the legacy
  >   manager-side emplace + agent record-only marker pattern until
  >   §8.1.C lands. The `[step-8.1.C-user-sfbm]` discriminator trace
  >   remains additive on user-OID paths.
  >
  > **Co-landed bootstrap-reader migration**: the catalog SFBM
  > transfer leaves `manager.storages_[catalog_oid]` empty post-load,
  > which breaks bootstrap-time readers that scan pg_class / pg_settings
  > via `storages_.find(...)`. Four helpers in
  > `manager_disk_bootstrap.cpp` were migrated to the agent-first /
  > manager-fallback pattern (mirrors §8.11.B catalog reader migration
  > in `manager_disk_resolve.cpp`):
  > - `read_setting_sync` — probes
  >   `agents_[0]->storage_entry_sync(pg_settings_oid)` first; manager
  >   fallback retained for the no-agents test fixture only.
  >   Without this migration, `stored_catalog_.timezone_offset` would
  >   default to UTC on every restart because the seeded pg_settings
  >   row would be invisible.
  > - `alive_user_oids_sync` (const) — same pattern against
  >   `pg_class_oid` with `const_cast` carve-out for the non-const
  >   `table_storage.table()` accessor (matches the resolve.cpp
  >   pattern). Without it, base_spaces dec-18-V1 GC re-registration
  >   would see an empty alive-OID set and re-tombstone every user
  >   table on restart.
  > - `scan_dropped_oids_sync` — same pattern against `pg_class_oid`;
  >   without it the dec-37-V1 dropped-OID rebuild on restart silently
  >   skips every tombstoned user table.
  > - `restore_oid_generator_sync` — same pattern looped over
  >   `well_known_oid_for_system_table(...)`. The early-out
  >   `storages_.empty()` is widened to also check
  >   `agents_[0]` so the no-agents test fixture stays correct.
  >   Without it, `oid_gen_` would seed at `FIRST_USER_OID - 1` on
  >   every restart and re-allocate user OIDs that are still on disk.

* **8.1.C — User SFBM cutover (depends on 8.2).** APPLIED 2026-05-31.
  After 8.2 routed runtime user-table CREATE through the agent slice,
  the residual user-OID branch of `load_storage_disk_sync` /
  `create_storage_disk_sync` (lazy WAL replay load via
  `load_storage_for_wal_replay_sync`) now flips to the agent path. The
  manager-side `storages_.emplace` for user OIDs is DELETED; the routed
  agent's `bootstrap_disk_inner_sync` /
  `bootstrap_create_disk_inner_sync` is the sole writer for `oid >=
  FIRST_USER_OID`. The `bootstrap_record_oid_sync` call site in
  `load_storage_disk_sync` is DELETED — agent now owns the full entry,
  no null markers needed. The function definition + declaration are
  RETAINED for one release as a safe-revert harness (per roadmap
  §8.12).

  > **Status (2026-05-31) — APPLIED.**
  > Touches:
  > - `services/disk/manager_disk_io.cpp`:
  >   - `load_storage_disk_sync` — user/catalog branches unified into a
  >     single `transfer_to_agent(...)` path. Old user dual-write
  >     (`storages_.emplace` + `record_on_agent()`) and the
  >     `record_on_agent` lambda DELETED. Diagnostic trace now emits
  >     `[step-8.1.C-applied]` for user OIDs (replaces the
  >     diagnostic-only `[step-8.1.C-user-sfbm]`).
  >   - `create_storage_disk_sync` — symmetric collapse: all OIDs route
  >     through `agent->bootstrap_create_disk_inner_sync`; manager
  >     emplace retained only for the no-agents test fixture.
  >   - `peek_checkpoint_wal_id_from_disk` — agent-first probe added
  >     (user entries no longer live in `manager.storages_`).
  > - `services/disk/manager_disk_bootstrap.cpp`:
  >   - `load_user_table_storages_sync` skip-if-present check switched
  >     from `storages_.find` to `has_storage` (which is agent-first
  >     post-§8.5).
  > - `services/disk/manager_disk_resolve.cpp`:
  >   - `scan_by_key` / `read_rows_by_key` §8.11.C manager-fallback
  >     blocks RETAINED for one release as a safe-revert harness;
  >     comments updated noting they are now dead code in production
  >     and tracked for deletion in §8.11 remaining / §8.12.
  > Net behaviour: per `.otbx` per process boot, exactly one of
  > `[step-8.1.B-sfbm-construct]` (no-agents test fixture only) OR
  > `[step-8.1.B-agent-sfbm-construct]` fires, for BOTH catalog and
  > user OIDs.

**Dependency graph addendum**:

```
8.1.A ────────────────────────────────────────────────┐
                                                       ▼
8.8 ──► 8.1.B ─┐                                  (no behaviour change)
               ├─► 8.10 (full checkpoint on agent)
8.2 ──► 8.1.C ─┘
```

**Tests** (cumulative):
- 8.1.A: build-only; no semantic test change. Existing checkpoint
  round-trip in `test_recovery_*` must remain green.
- 8.1.B: `test_d4_lazy_load.cpp` system-table half + catalog
  checkpoint sidecar in `test_recovery_*` after 8.8+8.9 land.
- 8.1.C: full WAL-replay + recovery suite (post-8.2 user table
  ownership).

### Sub-step 8.2 — Route runtime user-table create paths through agents

Update the three mailbox handlers
(`create_storage`, `create_storage_with_columns`, `create_storage_disk` in
`manager_disk_storage.cpp` lines 187-251) to write into
`agents_[pool_idx_for_oid].storages_` via a new mailbox-safe
`create_storage_inner` / `create_storage_with_columns_inner` /
`create_storage_disk_inner` family on `agent_disk_t`.

Keep the manager-side `storages_.emplace` as a dual-write for one
intermediate commit to make the bootstrap/runtime split observable in
isolation.

**Pre-requisite**: 8.1 must land first (otherwise DISK runtime creates
collide on file_handle_t between manager and agent).

**Touch**:
- `services/disk/agent_disk.hpp` — 3 new mailbox handlers (~30 LOC).
- `services/disk/agent_disk.cpp` — 3 bodies (~60 LOC) + 3 `dispatch_traits`
  entries + 3 behavior cases.
- `services/disk/manager_disk_storage.cpp` lines 186-251 (~70 LOC changed).
- `services/disk/disk_contract.hpp` — no change (manager's contract API is
  unchanged; agent_disk has its own dispatch_traits).

**LOC**: ~200.
**Days**: 1.5.
**Tests**: `test_pg_depend.cpp` (uses user OIDs), `test_mvcc_ddl.cpp` line 291
(asserts user OID range), runtime CREATE TABLE integration tests.

### Sub-step 8.3 — Migrate `drop_storage` mailbox handler

`manager_disk_t::drop_storage` (manager_disk_storage.cpp 226-251) currently
reads `storages_.find(table_oid)` to extract `otbx_path` and then performs
`std::filesystem::remove` itself. Move the body into
`agent_disk_t::drop_storage_inner` and turn the manager handler into a
pure forward.

**Pre-requisite**: 8.2 — manager must no longer be the canonical owner of
user IN_MEMORY entries; otherwise the manager-side erase would orphan the
agent slice.

**Touch**:
- `services/disk/agent_disk.hpp` + `.cpp` — new `drop_storage_inner` mailbox handler (~40 LOC).
- `services/disk/manager_disk_storage.cpp` lines 226-251 — replace body with mailbox forward (~20 LOC).

**LOC**: ~60.
**Days**: 0.5.
**Tests**: `test_recovery_drop.cpp`, dynamic_cascade_delete operator suite.

### Sub-step 8.4 — Stop manager-side dual writes in `create_storage_*`, `drop_storage`, `register_dropped_*` and `on_horizon_advanced`

> **Status (2026-05-31)** — sub-substeps **8.4.B / 8.4.C / 8.4.D** are
> all DELETED; only **8.4.A** remains diagnostic-only checkpointed (see
> its own §8.4.A status block for the surviving manager-direct reader
> blockers).
>
> **8.4.B is DELETED** (deletion landed 2026-05-31): manager-side
> `storages_.find` + `erase` + `std::filesystem::remove` sequence
> removed from `drop_storage` in `manager_disk_storage.cpp`; the agent's
> `drop_storage_inner` now owns the filesystem removal (.otbx + .wal_id
> sidecar + .prev sidecar + per-oid parent directory) alongside the
> slice erase. Manager body collapsed to a pure mailbox forward.
> Residual orphan rows in `manager_disk_t::storages_` (if any survive
> from pre-8.4.A emplace paths) are retired by §8.11/§8.12 when the
> manager map is removed entirely. `mark_storage_dropped` continues to
> read `manager.storages_[oid].otbx_path` (operator_dynamic_cascade_
> delete sends mark first, drop second) — that path is migrated by
> §8.4.C above which has already landed.
>
> **8.4.C is DELETED** (deletion landed 2026-05-31): both
> `dropped_storages_.push_back` sites in `register_dropped_storage_sync`
> (bootstrap) and `mark_storage_dropped` (runtime) removed; the runtime
> `storages_.find` lookup is replaced with a `storage_entry_sync`
> raw-pointer accessor against the routed agent slice (per Step 8.11.A
> contract — mailbox-serialized, race-free). The per-agent slice is sole
> writer of GC state. `dropped_storages_` member deletion landed under
> §8.11 (see below).
>
> **8.4.D is DELETED** (final deletion landed 2026-05-31): manager-side
> GC sweep + ack-on-empty removed; per-agent slice is sole emitter.
> Fanout preserved. `dropped_storages_` member deletion landed under
> §8.11 (see below).
>
> **§8.11 partial — `manager_disk_t::dropped_storages_` field DELETED
> (2026-05-31)**: with §8.4.C (writers) and §8.4.D (reader/GC body)
> already retired, the member declaration in `manager_disk.hpp`
> (formerly line 821) has been removed. The agent-side
> `dropped_storages_` slice (one per `agent_disk_t`) is now the SOLE
> owner of GC state. Stale doc-comments in `manager_disk.hpp`/`.cpp`,
> `agent_disk.hpp`/`.cpp`, and `operator_dynamic_cascade_delete.cpp`
> updated to reflect the new ownership. The remaining `storages_` map
> deletion is a separate (bigger) §8.11 wrap commit.

After 8.1-8.3 the agent owns every IN_MEMORY twin AND every DISK entry, but
the manager still mirror-writes into `storages_` / `dropped_storages_` on
every create / drop / dropped-register / horizon-advance path. Step 8.4
deletes those mirror writes so the per-agent slice becomes the *canonical*
source of truth. After 8.4 the manager's two maps are populated **only** by
the bootstrap helpers (`manager_disk_bootstrap.cpp`) and the read-only catalog
walkers (`manager_disk_resolve.cpp`, `manager_disk_ddl.cpp`) per Path A from
§8.8 / §8.9; those callsites are removed in §8.11 (Path B body move).

The work decomposes into 4 sub-substeps that touch disjoint mailbox handlers
and can be reviewed / reverted independently:

#### 8.4.A — Drop manager `storages_.emplace` in runtime + bootstrap CREATE paths

**Status: CATALOG-APPLIED (2026-05-31).** The manager-side emplace in
`create_storage_with_columns_sync` (`manager_disk_io.cpp:~345`) is now
DELETED for `oid < FIRST_USER_OID` (catalog) — the agent slice
(`agents_[0]` by routing) is the canonical owner for catalog OIDs.
The user-OID branch (`oid >= FIRST_USER_OID`) retains the manager
emplace + agent dual-write until §8.1.C lands (user SFBM ownership
transfer depends on §8.2 user-create routing). The remaining
create_*/load_* helpers in the §8.4.A "Sites" list below are still
pending. The four manager-direct readers flagged in the original
audit have been migrated to the agent-first / manager-fallback
pattern (mirrors §8.11.B):

- ✅ `manager_disk_storage.cpp::direct_append_sync` — probes
  `agents_[pool_idx]->storage_entry_sync(table_oid)` first, falls back to
  `get_storage`. The old `agents_[pool_idx]->direct_append_sync` sync
  fanout at the top of the body is collapsed into the unified write path
  (single append, agent-twin canonical when present, no double-write).
- ✅ `manager_disk_io.cpp::maybe_cleanup` — probes
  `storage_entry_sync(ctx.table_oid)` first, falls back to the manager
  map. const_cast on the entry covers the non-const `table()` /
  `compact()` calls (Constraint #11 carve-out: agent mailbox idle vs.
  this sync probe).
- ✅ `manager_disk_bootstrap.cpp::bootstrap_one` /
  `load_system_tables_sync` (skip-if-present probes :190 / :345) —
  `has_storage_sync` agent-first probe (returns true for record-only
  markers AND IN_MEMORY twins; `storage_entry_sync` would not, hence the
  choice of `has_storage_sync` here specifically for skip-if-present
  semantics).
- ✅ `manager_disk_bootstrap.cpp` post-seed checkpoint loops (:250
  pg_settings early branch + :317 general loop) — agent-first
  `storage_entry_sync` probe with const_cast on the resolved entry's
  `table_storage.checkpoint()` call. `table_storage_t::checkpoint` is a
  no-op when `mode_ != DISK`, so the IN_MEMORY-twin branch is harmless
  even before 8.1.B promotes the catalog SFBM onto the agent.

In `create_storage_with_columns_sync` the catalog branch now emits a
`step-8.4.A-applied-catalog` trace (post-deletion confirmation); the
user branch still emits `step-8.4.A-dual-write-user` so soak logs can
keep tracking the surviving user dual-write surface. The other
`step-8.4.A-dual-write` tags in `create_storage_disk_sync` /
`load_storage_disk_sync` are unchanged. Re-run this sub-step AFTER:
1. ~~§8.11 bootstrap-reader migration~~ — done in this commit
   (`bootstrap_system_tables_sync` :250,317 +
   `bootstrap_one`/`load_system_tables_sync` :190,345).
2. ~~`direct_append_sync` agent-first refactor~~ — done in this commit;
   `start_row` now originates from the agent twin when present
   (`agents_[pool_idx]->storage_entry_sync(oid)->storage->append`),
   falling back to the manager SFBM for catalog DISK entries that
   §8.1.B has not yet transferred.
3. ~~`maybe_cleanup` agent-first compact threshold check~~ — done in
   this commit.
4. §8.1.B + §8.1.C upgraded from diagnostic-only to executed transfers
   (REMAINING blocker — until catalog/user SFBM ownership physically
   moves onto the agent slice, the manager-side fallback path stays
   live for DISK record-only markers and is exercised by the bring-up
   test matrix).

Remove the manager-side `storages_.emplace` in the 6 create / load
helpers; the agent slice owns these entries after 8.1/8.2.

**Sites** (deletions only — agent fanout already in place):
- `manager_disk_storage.cpp:192` `create_storage` (Step 8.2 dual-write).
- `manager_disk_storage.cpp:247` `create_storage_with_columns` (Step 8.2
  dual-write).
- `manager_disk_storage.cpp:275` `create_storage_disk` (already
  fanned via 8.1.C; delete manager emplace).
- `manager_disk_io.cpp:335` `create_storage_with_columns_sync` (Step 3
  bootstrap dual-write) — CATALOG branch DELETED (2026-05-31); user
  branch retained until §8.1.C.
- `manager_disk_io.cpp:358` `create_storage_disk_sync` (Step 8.1.B
  bootstrap dual-write).
- `manager_disk_io.cpp:410, 416, 436` three `load_storage_disk_sync`
  branches (otbx-present, otbx-then-prev, corrupt-recovery promote).
  Sidecar-read `storages_.find(table_oid)` at lines 452-454 must move
  to `agents_[pool_idx]->bootstrap_set_checkpoint_wal_id_sync(oid, v)`
  (new ~15 LOC helper on agent).

**Pre-requisite**: 8.1.A + 8.1.B + 8.1.C + 8.2 all landed. 8.8 Path A
documentation must still apply — bootstrap reader sites in
`manager_disk_bootstrap.cpp` will start seeing an empty
`manager.storages_`; verify each reader was already migrated by §8.11
Path B preview OR retains a `co_await agent->storage_entry_sync(oid)`
fallback (TBD which lands first; if 8.4 ships before 8.11, the
bootstrap readers MUST be re-pointed at the agent inside 8.4.A as a
sub-deliverable — see "Dependencies" below).

**Risk**: HIGH. The 7 readers under §8.8 (`alive_user_oids_sync`,
`scan_dropped_oids_sync`, `read_setting_sync`,
`restore_oid_generator_sync`, `load_system_tables_sync`,
`load_user_table_storages_sync`, `bootstrap_system_tables_sync`) walk
the manager map. If 8.4.A lands before 8.11 Path B, all 7 must be
re-pointed in this commit. That is a ~200 LOC ripple that the original
8.4 estimate did NOT account for.

**Touch**: ~−40 LOC manager + ~15 LOC agent helper + ripple in
bootstrap readers (~200 LOC if 8.11 has not landed).

**LOC**: ~−25 net (or ~+175 if bootstrap ripple is bundled).
**Days**: 0.5 (or 2.0 with bootstrap ripple).
**Tests**: `test_d4_lazy_load.cpp`, `test_recovery_*` (catalog half),
`test_pg_depend.cpp` (user-table CREATE surfaces in agent only).

#### 8.4.B — Drop manager `storages_.erase` in `drop_storage`

**Status: DELETED (2026-05-31).** Manager-side `storages_.find` +
`erase` + `std::filesystem::remove` block in
`manager_disk_storage.cpp::drop_storage` removed; the `step-8.4.B-mirror-
erase` diagnostic trace was removed alongside it. The agent's
`drop_storage_inner` (in `services/disk/agent_disk.cpp`) now reads
`it->second->otbx_path` from the agent's slice BEFORE the erase and
performs the physical `.otbx` + `.wal_id` + `.prev` + `parent_path()`
removal using the `std::error_code` overload (Constraint #1 — no
exceptions). Manager body collapsed to a pure mailbox forward to
`agents_[pool_idx_for_oid(table_oid)]->drop_storage_inner`.

**Reader audit (validated):**
- `manager_disk_io.cpp` `load_storage_disk_sync` sidecar read — runs
  at bootstrap (pre-scheduler-start), never during the mailbox
  drop_storage path, so the manager-side erase removal does not affect
  it.
- `manager_disk.cpp` `mark_storage_dropped` `otbx_path` derivation
  — was migrated to `storage_entry_sync` against the routed agent in
  §8.4.C above (landed first per pre-req); the manager-side
  `storages_.find` was already gone before §8.4.B landed.

Residual orphan rows that may still live in `manager_disk_t::storages_`
from pre-8.4.A emplace paths persist until §8.11/§8.12 retires the
manager map entirely. Behaviour-equivalent: subsequent DROP TABLE on
the same OID still no-ops at the manager (idempotent erase removed),
and the agent slice + filesystem cleanup is canonical.

**Touched (final delivered)**:
- `services/disk/manager_disk_storage.cpp` `drop_storage` — −34 LOC
  (find/erase/remove block + diagnostic trace + Step 8.4.B note removed;
  comment block summarising the rationale retained).
- `services/disk/agent_disk.cpp` `drop_storage_inner` — +43 LOC
  (otbx_path capture before erase + std::error_code filesystem removal
  sequence + extended block comment documenting the canonical-owner
  contract).

**LOC**: +9 net.
**Tests**: `test_recovery_drop.cpp`, dynamic_cascade_delete operator
suite, on-disk-cleanup-after-drop integration suite — all still cover
the migrated behaviour.

#### 8.4.C — Drop manager `dropped_storages_.push_back` in `register_dropped_storage_sync` + `mark_storage_dropped`

**Status: DELETED (2026-05-31).** Both push_back sites
(`manager_disk.cpp` `register_dropped_storage_sync` bootstrap-path tail
and `mark_storage_dropped` runtime-path tail) removed, and the
intermediate `storages_.find(table_oid)` lookup in `mark_storage_dropped`
replaced with the Step 8.11.A `storage_entry_sync` raw-pointer accessor
against the routed agent slice (`agents_[pool_idx_for_oid(table_oid,
agents_.size())]->storage_entry_sync(table_oid)`). The agent
`register_dropped_storage_inner` mailbox fanout (runtime) and
`register_dropped_storage_inner_sync` direct call (bootstrap) remain the
sole writers of GC state. The `step-8.4.C-mirror-push-bootstrap /
mirror-find / mirror-push-runtime` diagnostic traces are removed
alongside the mirror code. `manager_disk_t::dropped_storages_` field
deletion deferred to §8.11 wrap (no remaining readers after §8.4.D
removed the manager GC body).

Two mirror writes into `manager_disk_t::dropped_storages_` from Step 7.
Per the §8.6 doc above, the agent slice is already the canonical owner;
the manager push_backs (lines 596-599 and 674-677 in `manager_disk.cpp`)
exist only to keep the manager-side GC observable.

**Sites**:
- `manager_disk.cpp:596-599` `register_dropped_storage_sync` —
  bootstrap-only WAL-replay carve-out (`base_spaces.cpp:305-349`).
  Delete the push_back. The function becomes a pure router into
  `agents_[pool_idx_for_oid]->register_dropped_storage_inner_sync`.
- `manager_disk.cpp:630-639, 674-677` `mark_storage_dropped` —
  runtime DROP TABLE path from `operator_dynamic_cascade_delete`.
  Replace the `storages_.find(table_oid)` lookup (lines 630-639,
  deriving `otbx_path` + sidecars) with a co_await of a new agent
  accessor `agent_disk_t::storage_paths_inner_sync(oid)` returning
  `{otbx_path, sidecars}`. Delete the trailing `dropped_storages_.push_back`.

**Pre-requisite**: §7 agent slice owns `dropped_storages_`, and §8.4.A
has already emptied `manager.storages_` so the manager-side find at
line 630 would otherwise return `.end()` for every oid. 8.4.C MUST
land at the same time as or after 8.4.A.

**Risk**: MEDIUM. The new `storage_paths_inner_sync` accessor is
*sync-across-actors*. Per Constraint #11, sync calls cross the actor
boundary only pre-scheduler-start; runtime `mark_storage_dropped`
runs inside a mailbox handler. We must instead use the mailbox path:
`co_await agent->mark_storage_dropped_inner(...)` where the agent
reads its own slice. That replaces the entire manager body with a
single forward — cleaner and matches §8.6 Path B (which §8.6 already
budgets).

**Touch**:
- `manager_disk.cpp:557-678` — replace 2 bodies with mailbox forwards
  (~−100 LOC).
- `agent_disk.cpp` — extend `register_dropped_storage_inner` to take
  the runtime `mark_storage_dropped` path (read entry's `otbx_path`
  inline) (~+30 LOC). Likely already done by §8.6 — confirm.

**LOC**: ~−70 net.
**Days**: 0.75.
**Tests**: `test_recovery_drop.cpp`, dispatcher GC ack
(`test_dispatcher_horizon_advance.cpp`), `base_spaces` PHASE 2c
catalog-rebuild integration test.

#### 8.4.D — Delete manager-side GC sweep in `on_horizon_advanced`

**Status: DELETED (2026-05-31).** Manager-side GC sweep body
(`manager_disk.cpp:485-538`) and ack-on-empty branch (`:539-554`)
removed. Per-agent `on_horizon_advanced_inner` handlers (fanned out at
`:470-483`) own the canonical sweep over per-agent
`dropped_storages_` slices and emit their own
`on_subscriber_empty(DISK_KIND)` acks (one per agent — dispatcher
idempotently collapses N-fold acks). Fanout preserved verbatim. The
manager `dropped_storages_` member itself survives until §8.11
(separate commit).

`manager_disk.cpp:485-523` runs the manager-side GC body that walks
`dropped_storages_`, physically removes `.otbx` + sidecars, and emits
the `on_subscriber_empty(DISK_KIND)` ack when the local mirror
drains. Per §8.6, the per-agent `on_horizon_advanced_inner` handlers
already do this canonically; the manager body is "legacy mirror".

**Sites**:
- `manager_disk.cpp:485-523` — delete the entire body block (the
  agent fanout above remains at lines 463-484). After 8.4.C the
  manager's `dropped_storages_` is empty, so the loop is a no-op,
  but the dead member still survives until §8.11. The ack-on-empty
  branch (lines 524-532) must move to the agent's
  `on_horizon_advanced_inner` ack ladder — verify §8.6 already wired
  it, otherwise add ~15 LOC inside the agent body.
- `manager_disk.cpp:524-532` — delete the manager-side ack (the
  per-agent ack from §8.6 is now the sole emitter).

**Pre-requisite**: 8.4.C (manager mirror must already be unwritten,
otherwise the agent fanout double-acks when the per-agent slices
drain but the manager mirror still has stale entries). Also requires
§8.6 wiring of per-agent `on_subscriber_empty` — verify before this
commit.

**Risk**: LOW (post-8.4.C the body is a no-op anyway). The only
runtime visible change is the ack source: previously the
*manager-side* mirror determined when the dispatcher's
`disk_has_dropped_` flag flips; after 8.4.D every agent's per-slice
drain triggers an ack and the dispatcher's `on_subscriber_empty`
handler must idempotently accept N acks (one per agent) instead of 1.
Verify dispatcher implementation handles this.

**Touch**: `manager_disk.cpp:485-532` ~−50 LOC delete.
**LOC**: ~−50 net.
**Days**: 0.5.
**Tests**: `test_dispatcher_horizon_advance.cpp` or analogue —
crucial test that N-fold ack from N agents collapses to one
dispatcher state transition. Add a new test if absent.

#### 8.4 — Dependency graph (sub-substeps)

```
        8.1.A/B/C ──┐
                    │
8.2  ───────────────┼──► 8.4.A ──┐                 (manager.storages_ unwritten)
                    │            │
8.3  ───────────────┘            ▼
                          8.4.B (drop_storage)
                                 │
                                 ▼
8.6 partial (agent ack) ──► 8.4.C (register_dropped + mark_storage_dropped)
                                 │
                                 ▼
                          8.4.D (on_horizon_advanced)
                                 │
                                 ▼
                          → 8.5 / 8.6 / 8.7 / 8.10 (parallel after 8.4)
```

8.4.A is the only sub-substep with a HIGH-risk ripple (bootstrap
readers). If §8.11 Path B is sequenced AHEAD of 8.4, 8.4.A reduces
to a 25-LOC delete; if 8.4 is sequenced ahead of 8.11, 8.4.A
absorbs the ~200 LOC bootstrap reader migration. Recommendation:
land §8.11 Path B (resolve + ddl + bootstrap readers via new
`agent->storage_entry_sync`) first, then 8.4 becomes a clean 4-commit
mirror-write removal.

#### 8.4 — Aggregate estimate

| Sub-substep | LOC delta | Days | Risk | Status |
|-------------|-----------|------|------|--------|
| 8.4.A — drop create_*/load_* emplaces | −25 (or +175 w/ bootstrap ripple) | 0.5 (or 2.0) | HIGH | CATALOG+USER APPLIED (2026-05-31): `create_storage_with_columns_sync`, `create_storage_disk_sync`, `load_storage_disk_sync` all flipped; remaining create/load helpers (`create_storage_disk` runtime mailbox) pending §8.2 follow-up. |
| 8.4.B — drop drop_storage erase | +9 (actual) | — | — | DELETED (2026-05-31) |
| 8.4.C — drop dropped_storages push_backs | −70 | — | — | DELETED (2026-05-31) |
| 8.4.D — delete on_horizon_advanced GC body | −50 | — | — | DELETED (2026-05-31) |
| 8.11 partial — delete `manager_disk_t::dropped_storages_` member | ~−25 (member + transient-mirror doc block) | — | LOW | DELETED (2026-05-31) |
| 8.11 Path B (catalog) — delete manager-map fallback in 10 catalog read sites of `manager_disk_resolve.cpp` | −90 (fallback blocks + Path A header doc) | 0.25 | LOW (catalog SFBM already on agents_[0] per §8.1.B) | APPLIED (2026-05-31) |
| 8.11 Path B (ddl partial) — delete manager-map fallback in 2 catalog DDL sites of `manager_disk_ddl.cpp` (`delete_pg_catalog_rows`, `update_pg_attribute_commit_id_field`) | ~−10 (fallback blocks; header doc rewritten) | 0.1 | LOW (same rationale — catalog SFBM on agents_[0]) | APPLIED (2026-05-31) |
| **8.4 total landed (B/C/D + 8.11 dropped_storages_ + 8.11 Path B catalog + ddl partial)** | **~−235** | — | — | — |
| **8.4.A remaining (clean path, §8.11 first)** | **−25** | **0.5** | HIGH | pending |
| **8.11 wrap — user-OID fallback in `scan_by_key` / `read_rows_by_key` + delete `storages_` map** | ~−250 (field decl + 3 inline accessors + emplace sites + fallback blocks across 6 files) | 0.5 | LOW | **COMPLETE (2026-05-31)** — manager_disk_t::storages_ field DELETED; has_storage / total_rows_sync / checkpoint_wal_id_sync now pure agent-delegation; get_storage() reduced to `return nullptr;` stub (manager-side bodies inert, removal tracked under §8.12); checkpoint_all / vacuum_all collapsed to per-agent fanout aggregation; resolve / ddl / bootstrap catalog readers now agent-only |

**Tests (cumulative, must all stay green between sub-substeps)**:
WAL-replay full integration suite; checkpoint sidecar round-trip
(`test_recovery_*`); `test_recovery_drop.cpp`;
`test_d4_lazy_load.cpp`; `test_pg_depend.cpp`;
`test_dispatcher_horizon_advance.cpp`; dynamic_cascade_delete
operator suite; SQL integration (smoke on every sub-substep).

**Rollback granularity**: each sub-substep is one commit. 8.4.D
revert is safe (post-8.4.C the manager mirror is empty so the GC
body is a no-op even when re-enabled). 8.4.A revert is the most
expensive — must also revert the bootstrap reader ripple if
bundled.

### Sub-step 8.5 — Delegate `has_storage`, `total_rows_sync`, `checkpoint_wal_id_sync`

Replace the three inline accessors in `manager_disk.hpp` lines 230-247 with
delegations of the form

```
bool has_storage(oid_t oid) const noexcept {
    if (agents_.empty()) return false;
    return agents_[pool_idx_for_oid(oid, agents_.size())]->has_storage_sync(oid);
}
```

Add matching `total_rows_sync` and `checkpoint_wal_id_sync` on
`agent_disk_t` (already have `has_storage_sync`; we need to add the other
two). They must walk the agent's local slice, looking up
`table_storage.table().calculate_size()` and
`table_storage.checkpoint_wal_id()` respectively.

**Pre-requisite**: 8.4 — manager map MUST already be empty for these
accessors to be safe to delegate. (Until 8.4 the manager map is still the
authoritative source.)

**Caveat**: `has_storage_sync` is `noexcept const` but reads a
`std::pmr::unordered_map`; the agent thread may be mutating its slice
concurrently. Either move the call onto the agent mailbox (loses noexcept
+ sync semantics required by WAL replay), or guarantee these accessors are
ONLY called pre-scheduler-start (the WAL-replay branch in base_spaces and
the bootstrap helpers). Verify all 3 call sites today:
- `base_spaces.cpp:207,212` — pre-scheduler, OK.
- `tests/test_d4_lazy_load.cpp` — uses test fixture; either pre-scheduler
  or single-threaded by construction; mark callsites with a comment.

**Touch**:
- `services/disk/manager_disk.hpp` lines 230-247 (~30 LOC changed).
- `services/disk/agent_disk.hpp` — add `total_rows_sync` + `checkpoint_wal_id_sync` (~10 LOC).
- `services/disk/agent_disk.cpp` — 2 bodies (~30 LOC).

**LOC**: ~70.
**Days**: 0.5.
**Tests**: `test_d4_lazy_load.cpp` (full suite — these accessors are its
core REQUIRE targets).

### Sub-step 8.6 — Migrate `mark_storage_dropped` and remove manager `dropped_storages_`

`manager_disk_t::mark_storage_dropped` (manager_disk.cpp 581-645) still
reads `storages_.find(table_oid)` to derive `otbx_path`. After 8.4 the
manager map is empty; the body must instead `co_await` the owning agent
for the path. Easier path: move the entire body into the agent (the agent
already owns the entry's `otbx_path`).

Simultaneously:
- delete `manager_disk_t::dropped_storages_` (manager_disk.hpp 769).
- delete `register_dropped_storage_sync` (manager_disk.hpp 472-475) —
  base_spaces dec 18 V1 carve-out calls
  `agent->register_dropped_storage_inner_sync` directly via a helper that
  picks the agent by `pool_idx_for_oid`. Manager exposes a thin static
  helper `route_dropped_to_agent_sync` that base_spaces uses (or
  base_spaces inlines the routing — preferred, fewer abstraction layers).
- delete the manager-side GC body in `on_horizon_advanced` (manager_disk.cpp
  485-514).
- the `on_subscriber_empty(DISK_KIND)` ack already fires from
  `agent_disk_t::on_horizon_advanced_inner` (per Step 8 partial); remove
  the manager-side ack (manager_disk.cpp 515-523).

**Pre-requisite**: 8.4 — manager map must be empty; `set_manager_dispatcher_sync`
already fans the address to every agent (landed in current commit).

**Touch**:
- `services/disk/manager_disk.cpp` lines 464-525, 548-645 — delete manager
  GC body, simplify mark_storage_dropped to a forward (or delete entirely
  and route from the caller — `operator_dynamic_cascade_delete`).
- `services/disk/manager_disk.hpp` — drop `dropped_storages_` member, drop
  `register_dropped_storage_sync`.
- `services/disk/agent_disk.cpp` — `mark_storage_dropped_inner` extension
  that reads `entry->otbx_path` and registers itself (~40 LOC added).
- `integration/cpp/base_spaces.cpp` lines 305-349 — replace
  `disk_ptr->register_dropped_storage_sync` call with a per-agent routing
  loop using `pool_idx_for_oid` (~30 LOC changed).
- `components/physical_plan/operators/operator_dynamic_cascade_delete.cpp`
  line 316 — `&manager_disk_t::drop_storage` still sends via the
  manager-routed mailbox (manager handler from 8.3 forwards to agent). NO
  change at the operator.

**LOC**: ~−180 net (delete dominates).
**Days**: 1.5.
**Tests**: `test_recovery_drop.cpp`, dispatcher GC ack tests
(`test_dispatcher_horizon_advance.cpp` or analogue).

### Sub-step 8.7 — Migrate `maybe_cleanup`

`manager_disk_t::maybe_cleanup` (manager_disk_io.cpp 240-284) currently reads
`storages_.find(ctx.table_oid)` to access the row_group + compact path.
Move the body to `agent_disk_t::maybe_cleanup_inner`; manager handler
becomes a forward to `agents_[pool_idx_for_oid]`.

**Pre-requisite**: 8.4.

**Touch**:
- `services/disk/agent_disk.hpp` + `.cpp` — new mailbox handler (~50 LOC).
- `services/disk/manager_disk_io.cpp` 240-284 — replace body with forward (~20 LOC).

**LOC**: ~70.
**Days**: 0.5.
**Tests**: vacuum-on-threshold integration tests.

### Sub-step 8.8 — Migrate bootstrap helpers (`manager_disk_bootstrap.cpp`) — DONE (Path A, documentation-only)

The big one. `bootstrap_system_tables_sync`,
`load_system_tables_sync`, `restore_oid_generator_sync`,
`load_user_table_storages_sync`, `alive_user_oids_sync`,
`scan_dropped_oids_sync`, `read_setting_sync` — every one of these scans
`storages_` or emplaces into it. All are pre-scheduler-start.

Two paths:

**Path A (preferred, minimal churn) — ADOPTED**: keep these methods on
`manager_disk_t` and rely on the dual-write fanout already wired by
Step 8.2 (creation) and Step 4 (mutation) to populate the agent slice
implicitly. `bootstrap_system_tables_sync` calls
`create_storage_{with_columns,disk}_sync` + `direct_append_sync`, both
of which fan to `agents_[pool_idx_for_oid]` on the way through.
`load_{system,user}_table_storages_sync` calls `load_storage_disk_sync`
which records the OID on the routed agent via
`bootstrap_record_oid_sync`. Reads (`alive_user_oids_sync`,
`scan_dropped_oids_sync`, `read_setting_sync`,
`restore_oid_generator_sync`) stay against `manager.storages_` until
Step 8.9 migrates the catalog reader pattern wholesale.

Concretely, **no behaviour change** was needed at 8.8: the dual-write
fanout had already been wired. Step 8.8 reduces to inline documentation
of the Path A contract per helper plus this roadmap mark.

**Path B (cleaner, more churn) — DEFERRED**: move the bodies onto the
agent. The catalog agent (agent 0) owns every pg_* OID, so most
bootstrap walks would live on agent 0 anyway. Tracked under Step 8.1.C
/ Step 8.11; not attempted here to keep Step 8 incremental.

**Decision**: Path A. Step 8 is risky enough; encapsulation deferred to a
future cleanup.

**Touch (actual)**:
- `services/disk/manager_disk_bootstrap.cpp` — inline comments for the
  7 helpers documenting the Path A dual-write contract (~80 LOC of
  comments, 0 LOC of behaviour change).

**Pending for Path B (Step 8.11 / 8.12 follow-up)**:
- Add `agent_disk_t::storage_entry_sync(oid)` const accessor (~20 LOC).
- Repoint the 4 read-only helpers
  (`restore_oid_generator_sync`, `alive_user_oids_sync`,
  `scan_dropped_oids_sync`, `read_setting_sync`) to read through that
  accessor, deleting their dependence on `manager.storages_`.
- Delete `manager.storages_` (Step 8.11).

**LOC (actual)**: ~80 added (pure documentation).
**Days (actual)**: 0.25.
**Tests**: unchanged — Path A is contract documentation only, no
runtime path was modified. Full WAL-replay + recovery suite remains
authoritative for Step 8.1/8.2 dual-write correctness.

### Sub-step 8.9 — Migrate `manager_disk_resolve.cpp` and `manager_disk_ddl.cpp` — AUDITED (Path A, documentation-only)

Following the same Path A vs Path B split adopted by §8.8, the 8.9 audit
inventories every `storages_.find(...)` read site in resolve + ddl and
defers the actual body move to the Step 8.11 cutover. Per-site TODO
markers identify exactly which agent index each future call must hit.

**Resolve sites (12, all read-only scans)** — `services/disk/manager_disk_resolve.cpp`:

| # | Line | Oid              | Method                       | Notes |
|---|------|------------------|------------------------------|-------|
| 1 | 13   | pg_namespace     | `resolve_namespace`          | catalog → agent 0 |
| 2 | 42   | pg_class         | `resolve_table` (lookup)     | catalog → agent 0 |
| 3 | 74   | pg_computed_column | `resolve_table` (relkind=g)| catalog → agent 0 |
| 4 | 166  | pg_attribute     | `resolve_table` (regular)    | catalog → agent 0 |
| 5 | 245  | pg_type          | `resolve_type_sync`          | catalog → agent 0 |
| 6 | 271  | pg_class         | `resolve_type_sync` (composite) | catalog → agent 0 |
| 7 | 301  | pg_attribute     | `resolve_type_sync` (composite fields) | catalog → agent 0 |
| 8 | 384  | pg_proc          | `resolve_function`           | catalog → agent 0 |
| 9 | 424  | pg_proc          | `resolve_function_by_name`   | catalog → agent 0 |
| 10| 463  | pg_namespace     | `list_namespaces`            | catalog → agent 0 |
| 11| 507  | **generic table_oid** | `scan_by_key`           | user-table-aware → `pool_idx_for_oid` |
| 12| 552  | **generic table_oid** | `read_rows_by_key`      | user-table-aware → `pool_idx_for_oid` |

**DDL sites (3, all read-then-mutate)** — `services/disk/manager_disk_ddl.cpp`:

| # | Line | Oid              | Method                                | Notes |
|---|------|------------------|---------------------------------------|-------|
| 1 | 89   | catalog table_oid | `delete_pg_catalog_rows`             | Path B APPLIED (2026-05-31): catalog OID, manager fallback deleted; mutation half = `direct_delete_sync` (already fanned) |
| 2 | 171  | pg_attribute     | `update_pg_attribute_commit_id_field` | Path B APPLIED (2026-05-31): catalog OID, manager fallback deleted; mutation half = `direct_update_sync` (already fanned) |
| 3 | 315  | **user table_oid** | `compact_relkind_g_storage`          | KEPT conservatively (relkind 'g' = USER OID, awaiting §8.1.C SFBM move); mutation half = non-const `drop_column` on `storages_` |

**Path A contract (this commit)**: every site continues to read through
`manager_disk_t::storages_`. The dual-write fanout wired by Step 8.2
(creation) and Step 4 (`direct_append`/`delete`/`update`) keeps the
agent slice populated in lockstep, so the manager map is still a valid
catalog source. Mutation halves in DDL sites already route to the agent
via the existing `direct_*_sync` helpers — no change today.

**Path B (Step 8.11 cutover)**: replace each `storages_.find(<oid>)`
with `agents_[pool_idx_for_oid(<oid>, agents_.size())]->storage_entry_sync(<oid>)`
where `storage_entry_sync` is a new pre-scheduler const accessor on
`agent_disk_t` (NOT yet implemented — tracked here). Catalog sites
(10/12 resolve, 1/3 ddl) always land on agent 0; the four generic-
`table_oid` sites must hash via `pool_idx_for_oid`.

**Decision**: documentation-only this commit. Moving the resolve bodies
onto a new `agent_disk_resolve.cpp` (~1100 LOC) was the original §8.9
plan; it adds a per-resolve mailbox round-trip to every catalog probe
and is risky to land while Step 8.11 still hasn't deleted the manager
map. Audit + TODO markers preserve the migration plan without changing
behaviour.

**Touch (actual)**:
- `services/disk/manager_disk_resolve.cpp` — file-level Path A header
  comment + 12 inline `TODO(step-8.11):` markers (~40 LOC of comments).
- `services/disk/manager_disk_ddl.cpp` — file-level Path A header
  comment + 3 inline `TODO(step-8.11):` markers (~35 LOC of comments).

**Pending for Step 8.11 (Path B execution)**:
- Add `agent_disk_t::storage_entry_sync(oid) -> const collection_storage_entry_t*`
  pre-scheduler const accessor (~20 LOC). DONE — already in tree.
- Repoint all 15 sites to the new accessor (the TODO markers identify
  per-site agent indices); delete the manager map.
  - Catalog half (10 sites in `manager_disk_resolve.cpp`): APPLIED
    (2026-05-31). Agent probe is now the sole reader; manager-map
    fallback removed. See §8.4 status table for the −90 LOC entry.
  - User-OID half (2 sites: `scan_by_key`, `read_rows_by_key`):
    pending §8.1.C (user-table SFBM ownership move).
  - DDL half — catalog sites (2 of 3): APPLIED (2026-05-31).
    `delete_pg_catalog_rows` + `update_pg_attribute_commit_id_field`
    now treat a null `storage_entry_sync` as terminal; the prior
    manager-map fallback was dead code post-§8.1.B (catalog SFBM owned
    by `agents_[0]`). See §8.4 status table for the LOC entry.
  - DDL half — user OID site (1 of 3): pending §8.1.C.
    `compact_relkind_g_storage` operates on a USER OID and still needs
    the manager-map fallback for both the const read and the non-const
    `drop_column` mutation against `storages_`.
- Either turn `compact_relkind_g_storage` into a forward to a new
  `agent_disk_t::compact_relkind_g_storage_inner_sync` OR expose
  `drop_column` on the agent slice (currently only the manager-side
  entry exposes it).

**LOC (actual)**: ~80 added (pure documentation).
**Days (actual)**: 0.25.
**Tests**: unchanged — Path A is contract documentation only, no runtime
path was modified. SQL integration / planner-resolve / DDL roundtrip
suites remain authoritative for Step 8.11 once Path B lands.

**LOC (original Path B estimate, for Step 8.11 budgeting)**: ~1100
moved, ~150 net new.
**Days (original Path B estimate)**: 3.0.

### Sub-step 8.10 — Migrate `checkpoint_all` / `vacuum_all` bodies + drop `FIRST_USER_OID` gate

Today `manager_disk_t::checkpoint_all` (manager_disk_io.cpp 67-237) fanns
out to every agent AND iterates `storages_` for the manager-side
canonical checkpoint. After 8.4 the manager map is empty; the agent-side
`checkpoint_inner` becomes authoritative.

- Update `agent_disk_t::checkpoint_inner` to perform the full checkpoint
  sequence (compact + checkpoint(wal_id) + sidecar write) for DISK-mode
  entries (currently the agent skips DISK record-only markers). Status:
  after 8.1 the agent owns the live `single_file_block_manager_t`, so the
  full sequence runs cleanly inside the agent.
- Manager `checkpoint_all` becomes a pure fanout + `std::min` aggregator
  (~30 LOC).
- Drop the catalog-first ordering loop (manager_disk_io.cpp 67-80) —
  ordering between system and user tables happens within agent 0's slice
  (system tables are all there) followed by agents 1..N (user tables);
  agent 0 always finishes first since it gets the `checkpoint_inner` send
  first in the per-agent fanout loop.
- Same treatment for `vacuum_all` (~150 LOC reduced to fanout body).

Then drop the `FIRST_USER_OID` routing gate on every storage_* mailbox
handler (manager_disk_storage.cpp 13 sites). Routing becomes
unconditional — agents own every OID now.

**Pre-requisite**: 8.1, 8.4.

**Touch**:
- `services/disk/manager_disk_io.cpp` 30-237 (~210 LOC → ~50 LOC).
- `services/disk/manager_disk_storage.cpp` — remove 13 `FIRST_USER_OID`
  guards (~80 LOC removed).
- `services/disk/agent_disk.cpp` — extend `checkpoint_inner` body for DISK
  entries (~70 LOC added).

**LOC**: ~−170 net.
**Days**: 1.5.
**Tests**: checkpoint sidecar round-trip; vacuum integration; full SQL
suite (catches any per-OID fallback regression).

### Sub-step 8.11 — Delete `manager_disk_t::storages_` and `get_storage` — **COMPLETE (2026-05-31)**

Final wrap landed. The `manager_disk_t::storages_` map declaration has been
deleted from `manager_disk.hpp`. The three inline accessors
(`has_storage`, `total_rows_sync`, `checkpoint_wal_id_sync`) collapsed to
pure agent-delegation. `get_storage()` reduced to `return nullptr;` —
manager-side bodies fall through to their `if (!s) co_return ...;` early-
return guards and become inert; full body removal is tracked under §8.12.

Concretely:
- `services/disk/manager_disk.hpp` — field declaration + 6-step migration
  roadmap doc-block deleted; inline accessors simplified to single
  `agents_[pool_idx_for_oid(oid)]->...` delegation.
- `services/disk/manager_disk_storage.cpp` — 3 `storages_.emplace` sites
  deleted (create_storage / create_storage_with_columns /
  create_storage_disk); 4 fallback blocks deleted (storage_scan,
  storage_scan_batched, storage_types, storage_total_rows); schema-growth
  storages_.find probes inside storage_append's dead-code region deleted;
  `get_storage` body reduced to `return nullptr;`.
- `services/disk/manager_disk_io.cpp` — 6 emplace sites deleted (the
  no-agents test-fixture branches in create_storage_disk_sync /
  load_storage_disk_sync transfer_to_agent + the catalog/user dual-write
  in create_storage_with_columns_sync); the checkpoint_all body's iteration
  over storages_ is gone (per-agent checkpoint_inner is canonical); the
  vacuum_all body's iteration over storages_ is gone (per-agent
  vacuum_inner is canonical); maybe_cleanup / peek_checkpoint_wal_id_from_
  disk manager-map fallbacks deleted.
- `services/disk/manager_disk_bootstrap.cpp` — 12 reader fallback sites
  deleted across bootstrap_one, post-seed checkpoint loops,
  load_system_tables_sync skip-if-present probe,
  restore_oid_generator_sync, alive_user_oids_sync, scan_dropped_oids_sync,
  and read_setting_sync. Each is now a pure agents_[0] catalog-agent probe.
- `services/disk/manager_disk_resolve.cpp` — 2 generic-OID fallback blocks
  in scan_by_key + read_rows_by_key deleted.
- `services/disk/manager_disk_ddl.cpp` — compact_relkind_g_storage manager
  fallback deleted; the drop_column mutation routes purely through the
  agent (count derived from to_drop.size() under the contract that the
  manager mailbox handler is the sole writer for this OID).

Post-condition: every read/write path is agent-canonical;
`manager_disk_t::storages_` is no longer accessible from any compile unit;
docs/comments referencing the field are either removed or rewritten with
"deleted (2026-05-31)" markers.

**Net LOC**: ~−250 deletions, ~+150 insertions (post-cutover replacement
prose + simplified accessor bodies); commit body counts the actual
churn (heavily comment-weighted because the file had extensive doc-blocks
describing the migration phases that are now obsolete).
**Tests**: full build + entire test suite (safety net for 8.1-8.10);
no-agents test fixtures (test_table_storage.cpp et al) no longer reach
the deleted manager-side emplace paths because manager_disk_t's
constructor calls create_agent(config.agent) whenever config_.path is set
(default `agent=2`), so production and test bring-up always have
`agents_` populated.

### Sub-step 8.12 — Cleanup: drop `bootstrap_record_oid_sync`, retire dual-write comments ✅ COMPLETE (2026-05-31)

Final dead-code sweep:
- ~~Remove `agent_disk_t::bootstrap_record_oid_sync`~~ — **APPLIED
  (2026-05-31)**: zero call sites remained after §8.1.B (catalog SFBM
  transfer) and §8.1.C (user SFBM transfer); every DISK OID now reaches
  the agent slice through `bootstrap_disk_inner_sync` (load path,
  manager_disk_io.cpp 336-354) or `bootstrap_create_disk_inner_sync`
  (create path, manager_disk_io.cpp 256-273). Header now carries an
  audit comment in place of the declaration; the `.cpp` definition is
  deleted with a back-reference comment.
- Add `agent_disk_t::has_in_memory_inner_sync()` — **APPLIED
  (2026-05-31)**: restores the pre-cutover `checkpoint_all` semantic
  that suppresses the WAL ID seal when any IN_MEMORY entry exists. The
  Step 8.11 wrap replaced the manager's canonical-map walk with a per-
  agent `checkpoint_inner` fanout aggregated via `std::min` over each
  agent's `min(prev_checkpoint_wal_id_)`. That `numeric_limits::max()`
  sentinel cannot distinguish "no DISK entry AND no IN_MEMORY twin"
  (safe to seal) from "no DISK entry BUT IN_MEMORY twin present" (must
  NOT seal). The new const sync probe walks the slice for any non-null
  IN_MEMORY entry; `manager_disk_t::checkpoint_all` calls it after the
  await pass and gates the `fix_wal_id` seal on `all_disk_checkpointed
  && !any_in_memory`. Sync probe is legal under Constraint #11 because
  the manager body runs after every agent future resolves — agents are
  idle and the manager is the only writer to their slices.
- Remove all "// Step N: dual write until Step 8" comments across
  `agent_disk.*`, `manager_disk_*.cpp`. **APPLIED (2026-05-31)**: final
  comment polish sweep landed. Factually-stale claims ("manager-side
  body remains canonical", "fallback to manager", "dual-write
  contract", "deferred to manager-side until Step 8.X") rewritten to
  reflect the terminal slice-canonical state. Historical "Step N
  (landed)" walk-throughs in `agent_disk.hpp` collapsed into a single
  summary block. Audit-trail breadcrumbs that reference deleted code
  by section (`§8.1.B/C`, `§8.4.D`, `§8.11 wrap`, `§8.12 cleanup`) are
  retained as forensic anchors.
- ~~Delete `manager_disk_t::get_storage(oid)` + dead manager bodies~~ —
  **APPLIED (2026-05-31)**: `get_storage` returned `nullptr`
  unconditionally after §8.11 deleted `storages_`. Function declaration
  removed from manager_disk.hpp; definition removed from
  manager_disk_storage.cpp. Inert manager-side fallback bodies collapsed
  in: `direct_append_sync` (manager-side fallback ptr removed), `direct_
  delete_sync` / `direct_update_sync` (pure forward to agent now),
  `storage_fetch` / `storage_scan_segment` (manager-side fallback after
  agent miss removed), `storage_revert_append` (manager-side
  `s->revert_append` removed), `storage_publish_commits` /
  `storage_publish_deletes` / `storage_revert_appends` (dead manager-
  side `get_storage(...)` loops removed).
- Update the Version B* roadmap comment block in manager_disk.hpp 666-743
  to mark Step 8 ✅ and document the new ownership shape.
- Update the Step 8 partial comment in `set_manager_dispatcher_sync`
  (manager_disk.cpp 527-546) to remove the "dual path during Step 8" note.

**LOC**: ~−110 (one dead function + dead-body collapses, mostly in
manager_disk_storage.cpp).
**Days**: 0.5.
**Tests**: full suite as a smoke check; no semantic change expected.

## Dependency graph

```
8.1 ──┬─→ 8.2 ──→ 8.3 ──┐
      │                 │
      │                 ▼
      └────────────────→ 8.4 (= A→B→C→D, see §8.4 sub-graph) ──┬─→ 8.5
                               │
                               ├─→ 8.6
                               │
                               ├─→ 8.7
                               │
                               ├─→ 8.8 ──→ 8.9
                               │
                               └─→ 8.10 ──→ 8.11 ──→ 8.12
```

Sub-steps 8.5 / 8.6 / 8.7 may proceed in parallel after 8.4 (independent
code paths). 8.8 must precede 8.9 because the resolve path uses the
bootstrap entry-point accessor. 8.10 must precede 8.11 because the
`FIRST_USER_OID` gate removal is a precondition for the manager map
being unread.

## Test impacts

| Test file | Impact |
|-----------|--------|
| `services/disk/tests/test_d4_lazy_load.cpp` | 8.5 — `manager->has_storage(oid)` semantics must be preserved through the delegation. Add a fixture comment that the call is pre-scheduler-start (single-threaded). |
| `services/disk/tests/test_ddl_methods.cpp` (line 618) | 8.5 — same, but for a "never-allocated" oid (returns false). |
| `services/disk/tests/test_pg_depend.cpp` | 8.2 — runtime user-table CREATE must surface in the agent's slice. |
| `services/disk/tests/test_mvcc_ddl.cpp` (line 291) | 8.2 — asserts oid >= FIRST_USER_OID; pure assertion, no code change. |
| `services/disk/tests/test_wal_catalog.cpp` | 8.10 — `FIRST_USER_OID` gate removal must not break the catalog-only WAL filter (the test filters by oid; the production code stops checking after 8.10). |
| `services/disk/tests/test_recovery_*` | 8.1, 8.6 — checkpoint + drop recovery must run end-to-end against agent-owned entries. |
| All SQL integration tests under `integration/c/`, `integration/cpp/`, `test/sql/` | 8.9 — resolve path is now mailbox-routed; latency may shift slightly. |

No test today reads `manager.storages_` directly (private member). Six
tests use the public `has_storage` accessor; those are covered by 8.5.

## Rollback plan

Each sub-step is independently revertable as a single commit. Recommended
rollback granularity:

* If 8.1 destabilises checkpoint: revert 8.1, leaving the dual-write
  scheme in place. Manager remains the authoritative DISK owner.
* If 8.2 destabilises user-table CREATE: revert 8.2; manager keeps the
  user IN_MEMORY canonical owner (current Step 7 state).
* If 8.4 surfaces a latent reader that we missed: revert 8.4 and
  re-introduce the manager-side dual writes; investigate the reader.
  This is the most dangerous revert because 8.5/8.6/8.7 depend on 8.4.

For safety, **8.5-8.7 should not be merged until 8.4 has soaked in main
for at least 24h** (one full CI nightly + recovery cluster smoke test).
8.11 (the actual `storages_` deletion) should be its own commit with a
dedicated PR so revert is one-click.

## Total estimate

| Sub-step | LOC delta | Days |
|----------|-----------|------|
| 8.1 — DISK ownership move | +150 | 1.0 |
| 8.2 — User-table create routing | +200 | 1.5 |
| 8.3 — drop_storage migration | +60 | 0.5 |
| 8.4 — stop manager dual writes (4 sub-substeps A/B/C/D) | −140 (clean) / +60 (ripple) | 2.25 (clean) / 3.75 (ripple) |
| 8.5 — delegate sync accessors | +70 | 0.5 |
| 8.6 — dropped_storages_ migration | −180 | 1.5 |
| 8.7 — maybe_cleanup migration | +70 | 0.5 |
| 8.8 — bootstrap helpers Path A | +200 (changed) | 2.0 |
| 8.9 — resolve + ddl Path A (audit + TODOs) | +80 (doc-only) | 0.25 |
| 8.11.A — resolve + ddl Path B body move (deferred) | +150 net (~1100 moved) | 3.0 |
| 8.11 Path B (catalog) — delete manager-map fallback in catalog read sites | −90 (applied 2026-05-31) | 0.25 |
| 8.10 — checkpoint/vacuum + drop FIRST_USER_OID gate | −170 | 1.5 |
| 8.11 — delete `storages_` + `get_storage` | −15 | 0.5 |
| 8.12 — cleanup + comments | −80 | 0.5 | COMPLETE (2026-05-31) — `bootstrap_record_oid_sync` removed (declaration + definition + 1 caller-doc backref); `get_storage(oid)` removed (decl + def + manager-side dead bodies collapsed); `has_in_memory_inner_sync` added (35 LOC) and integrated into `checkpoint_all` (IN_MEMORY suppression restored). Final comment polish APPLIED across services/disk/agent_disk.{cpp,hpp} + manager_disk.{cpp,hpp}: stale "until Step 8" / "until Step 8.11/12" / "manager-side body remains canonical" / "fallback to manager" / "dual-write" claims rewritten to reflect terminal slice-canonical state; manager_disk.hpp field-level roadmap block reflows §8.11 wrap + §8.12 cleanup. Historical "Step N (landed)" breadcrumbs collapsed into a single summary block in agent_disk.hpp. |
| **Total** | **≈ +325 net (≈ −500 with file moves, ≈ +1600 gross churn)** | **14.0 days** |

Calendar estimate including review cycles and recovery soak: **3.5 weeks
of dedicated work**, assuming no scope expansion and a green test suite
between sub-steps. Compress to ~2.5 weeks if 8.5/8.6/8.7 run in parallel
between two engineers after 8.4.

## Risk register

1. **`single_file_block_manager_t` thread affinity (8.1)** — the file
   handle is mmap-backed; if any fsync/IO is in flight when ownership
   moves, behaviour is undefined. Mitigation: 8.1 lands while
   `agents_` is freshly spawned (file handle constructed on the agent
   thread, never on the manager thread). Verify by adding a TSan run to
   the 8.1 CI gate.
2. **`peek_checkpoint_wal_id_from_disk` (manager_disk_io.cpp 434-453)** —
   this is a sync read called by `base_spaces` WAL replay BEFORE the
   replay loop knows whether the storage is loaded. It currently checks
   `storages_.find` first, then falls back to a raw sidecar read. After
   8.4 it must call the agent's `checkpoint_wal_id_sync` (8.5) which
   only returns non-zero for loaded entries — the raw sidecar fallback
   stays. **Verify** the order in 8.5.
3. **`scan_dropped_oids_sync` (manager_disk_bootstrap.cpp 462)** — reads
   pg_class's row-version delete_ids, which means it must scan the
   catalog storage. After 8.8 Path A it reads through
   `agent->storage_entry_sync(pg_class_oid)`. Make sure the const
   accessor is genuinely safe pre-scheduler-start (no concurrent
   mutator).
4. **Comment debt** — Step 8 partial already references "Step 8 N+1
   commit will drop the manager ack" in 3 places. 8.6 retires those. If
   8.6 is split into 8.6a + 8.6b, the references must update in
   lockstep or the comments lie.
5. **`set_manager_dispatcher_sync` agent fanout (already landed)** —
   confirm in CI that the fanout fires before
   `register_dropped_storage_inner_sync` is called by base_spaces (the
   agent uses `manager_dispatcher_addr_` in `on_horizon_advanced_inner`).
   If base_spaces phase ordering reverses, the agent's ack never reaches
   the dispatcher. Verify with a unit test that spawns a manager + agents
   + a stub dispatcher and runs a horizon advance.

## Out-of-scope (deferred to a future "Step 9")

* Moving `oid_gen_` and `stored_catalog_` onto agent 0. These are still
  manager-owned; agent 0 only owns the pg_* storages. A clean ownership
  story aligns them with agent 0, but that's a separate refactor.
* Splitting `manager_disk_t` into a pure router class (no behaviour). The
  `behavior()` / `enqueue_impl` plumbing remains because operator-side
  mailbox sends still target the manager address.
* Removing the `FIRST_USER_OID` constant entirely (used in well-known OID
  validation across the codebase). Only the *routing gate* is dropped in
  8.10; the constant itself stays.
