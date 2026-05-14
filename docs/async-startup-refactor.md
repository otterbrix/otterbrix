# Async Startup Refactor — Design Notes

> **Status**: deferred / not started. Captured 2026-05-12.
> Architectural cleanup, not a bug fix.

## Problem

`integration/cpp/base_spaces.cpp` performs initialisation as a chain of
synchronous calls **before** any actor scheduler is started:

```
spawn(wal, disk, index, dispatcher)
disk->sync(wal_addr); dispatcher->sync(addrs); wal->sync(addrs)
disk_ptr->bootstrap_system_tables_sync()
disk_ptr->load_user_table_storages_sync()
// WAL replay loop: load_storage_for_wal_replay_sync,
//   create_storage_with_columns_sync, direct_append_sync, …
disk_ptr->restore_oid_generator_sync()
scheduler_dispatcher_->start()                 // mailboxes now process
```

The `_sync` suffix proliferates on `manager_disk_t`'s public surface — ~10
methods exist solely for pre-scheduler use, in parallel with the actor
contract. Recovery-decision logic (sidecar filter, system-vs-user routing,
in-memory storage synthesis) lives in `integration/cpp/` and cannot be
reused by a replication consumer or repair CLI without copy-paste.

Ordering invariant the chain enforces:

1. `bootstrap_system_tables_sync` → pg_catalog tables exist.
2. WAL replay → catalog reflects most recent durable state.
3. `restore_oid_generator_sync` → `oid_gen_` seeded to `max(oid)+1`.
4. Schedulers start → external traffic accepted.

The chain is correct. The cost is layering: durability logic lives in an
integration helper, not in the slice that owns the durability invariant.

## Goal

- WAL replay flows through the actor contract. No `_sync` replay methods
  on `manager_disk_t`'s public surface.
- Recovery logic lives in `services/disk/`.
- `base_spaces.cpp` becomes a thin assembly: spawn → wire → start →
  `co_await ready`.
- `manager_disk_t`'s public surface shrinks to just the actor contract.

## Approaches

### A. Start schedulers first; replay as a normal message

```
spawn actors → sync addresses → scheduler->start()
co_await disk->bootstrap_system_tables()
co_await disk->replay_wal_records(records)
co_await disk->restore_oid_generator()
manager_dispatcher_t enables external traffic    // gate flag
```

**Pros**: minimal architecture change; each sync method gets an actor
counterpart wrapped in `co_return`. Sync helpers can be deleted once
nothing pre-scheduler calls them.

**Cons**: `manager_dispatcher_t` needs an `enable_traffic()` gate so
SQL requests arriving during startup queue or reject until replay
completes. Without the gate, an early request could hit dispatcher
before pg_catalog is populated.

**Surface**: ~300 LOC across `base_spaces.cpp`, `manager_disk.{hpp,cpp}`,
`manager_disk_io.cpp`, `manager_dispatcher.{hpp,cpp}`.

### B. Self-bootstrapping disk actor [recommended]

`manager_disk_t` accepts `wal_records` (or `wal_reader` address) at
construction. Internally orchestrates bootstrap + replay + oid_gen
restore as chained handlers, then posts a `ready` signal. `base_spaces`
awaits the signal.

**Pros**: callers don't know about the steps. `manager_disk_t` public
surface shrinks further (no `bootstrap_*`, no `restore_oid_generator_*`
exposed). "Disk that's not yet ready" is a state of the disk, not of
its callers — correct layering.

**Cons**: WAL-reader ownership moves into disk-actor scope (disk grows a
dependency on wal config/paths it doesn't have today). The disk actor
grows a small state machine (`booting → bootstrapped → replayed →
oid_seeded → ready`).

**Surface**: ~400-500 LOC.

### C. Hybrid (rejected)

Replay moves into `services/disk/` as one method taking records as its
parameter; other init stays sync. This is relocation of sync code, not
removal — same observation that motivated the refactor. Not worth doing
on its own.

## Recommendation

**Approach B**. The encapsulation is the architectural win. If disk
swallowing WAL reader is a step too far, fall back to A — retires sync
helpers without requiring disk to grow WAL knowledge.

## Preserved invariants (DO NOT change in this refactor)

- The chain order itself (bootstrap → replay → oid_seed → traffic).
  Only the mechanism changes — direct sync calls → actor messages with
  `co_await`.
- Body of `bootstrap_system_tables_sync` stays synchronous inside the
  actor handler — what changes is the caller and the mechanism, not
  the work itself.
- W-TORN contract (`min(prev_checkpoint_wal_id_)`). Recovery semantics
  unchanged; only driver location changes.
- WAL replay must remain serialized (not re-parallelize across user
  tables — `storages_` unordered_map is not thread-safe; TSan-confirmed
  during Phase 10 cleanup, commit `7569e6b`).

## Risks

- **Ordering regressions.** Manifest as catalog inconsistency at
  startup. Hard to diagnose. Mitigation: keep the explicit step order
  as a comment in the new entry point; assert each step's postcondition.
- **Dispatcher gate (Approach A).** New concurrent state. Easy to test
  (synthetic early-message scenario), easy to miss in review.
- **WAL-reader ownership (Approach B).** Disk's dependency surface
  expands. Acceptable per Phase 8 ownership rules (disk owns durability).