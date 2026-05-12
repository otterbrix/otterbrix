# Async Startup Refactor — Design Notes

> **Status**: deferred / not started. Captured 2026-05-12 during #160 (P11.B) discussion.
> This is a follow-up architectural task, NOT a bug fix.

## Context

`base_spaces.cpp` currently performs initialisation as a sequence of synchronous
calls **before** any actor scheduler is started:

```
actor_zeta::spawn<manager_wal_replicate_t>(...)
actor_zeta::spawn<manager_disk_t>(...)
actor_zeta::spawn<manager_index_t>(...)
actor_zeta::spawn<manager_dispatcher_t>(...)
manager_disk_t::sync(wal_address)
manager_dispatcher_t::sync(addresses)
manager_wal_replicate_t::sync(addresses)
disk_ptr->bootstrap_system_tables_sync()       // <-- sync
manager_dispatcher_t::init_from_state(...)     // <-- sync
// WAL replay loop: peek_checkpoint_wal_id_from_disk, has_storage,
// load_storage_for_wal_replay_sync, create_storage_with_columns_sync,
// direct_append_sync / direct_delete_sync / direct_update_sync   <-- all sync
disk_ptr->restore_oid_generator_sync()         // <-- sync
scheduler_dispatcher_->start()                 // <-- mailboxes begin processing
```

The "sync" suffix proliferates across `manager_disk_t`'s public surface because
all of this work mutates shared state (catalog rows, oid_gen counter, storage
maps) BEFORE actors can serialise access via their mailboxes. Once schedulers
start, every mutator must go through actor messaging; up to that point, direct
sync calls are the only available path.

## Why this exists today

Strict ordering invariant — every step depends on the previous:

1. `bootstrap_system_tables_sync` loads / creates pg_catalog tables → catalog
   structure exists in memory.
2. WAL replay applies post-checkpoint catalog mutations → catalog reflects the
   most recent durable state.
3. `restore_oid_generator_sync` scans the now-complete catalog and seeds
   `oid_gen_` to `max(oid) + 1`.
4. `init_from_state` (currently mostly a no-op after the on-demand resolve
   migration) populates dispatcher cache.
5. Schedulers start → external traffic accepted.

If any earlier step's effect were not visible when a later step runs, the
system would mint colliding OIDs, miss catalog rows, or route INSERTs against
stale schemas. The sync chain is correct; what's awkward is that it lives in
the integration helper and pollutes `manager_disk_t`'s class API with
~10 `_sync` methods that exist solely for pre-scheduler use.

## Cost of the status quo

- `manager_disk_t` has a parallel sync API alongside its actor contract API.
  Every method that needs a pre-scheduler counterpart is duplicated. New
  features that touch startup grow this set.
- The replay loop lived in `integration/cpp/base_spaces.cpp`. Recovery-decision
  logic (sidecar filter, system vs user routing, lazy in-memory storage
  creation) is in an integration helper, not in `services/disk/`. Future
  recovery drivers (replication consumer, repair CLI, snapshot apply) cannot
  reuse it without copy-paste.
- #160 surfaced this: the bug — duplicate replay of system-table records after
  Phase 8.A made `.otbx` authoritative — lived in `base_spaces.cpp`'s logic,
  not in any disk-owned predicate.

The bug was fixed inline (uniform sidecar filter with per-table cache). The
architectural debt remains.

## Goal

After this refactor:

- WAL replay flows through the actor system. No `_sync` replay methods on
  `manager_disk_t`'s public surface.
- Recovery-decision logic lives in `services/disk/` (the slice that owns the
  durability invariant), not in `integration/cpp/`.
- `base_spaces.cpp` becomes a thin assembly file: spawn actors, wire
  addresses, start, await a single "ready" future.
- `manager_disk_t`'s public surface shrinks: only the actor contract (plus
  `bootstrap_system_tables_sync` and `restore_oid_generator_sync` if those
  remain in their current form — see §Approaches).

Bug class closed by this refactor: any future "pre-scheduler sync helper
needed because state-X must be ready before state-Y" instinct. Instead the
disk actor is self-bootstrapping; consumers wait for its ready signal.

## Approaches

### A. Start schedulers first, replay as a normal actor message

Init order changes to:

```
spawn actors
sync addresses
scheduler_dispatcher_->start()
co_await disk->bootstrap_system_tables()         // actor contract
co_await disk->replay_wal_records(wal_records)   // actor contract
co_await disk->restore_oid_generator()           // actor contract
co_await dispatcher->init_from_state(...)
manager_dispatcher_t enables external traffic    // gate flag
```

**Pros**: minimal change to actor architecture; each sync method gets an actor
contract counterpart with the same body wrapped in `co_return`. Existing sync
methods can be deleted once nothing pre-scheduler calls them.

**Cons**: `manager_dispatcher_t` needs an `enable_traffic()` gate so wrapper
dispatcher requests queued during startup are rejected (or queued) until
replay completes. This is new state. Without the gate, an early-arriving
SQL request could hit dispatcher before pg_catalog is populated.

**Surface estimate**: ~300 LOC across `base_spaces.cpp`, `manager_disk.hpp/cpp`,
`manager_disk_io.cpp`, `manager_dispatcher.hpp/cpp`. New actor contract
methods: `bootstrap_system_tables`, `replay_wal_records`, `restore_oid_generator`.

### B. Self-bootstrapping disk actor

`manager_disk_t` accepts `wal_records` (or wal_reader address) in its
constructor / first message. On startup it internally orchestrates
bootstrap + replay + oid_gen restore as a chain of internal handlers,
finishing by posting a `ready` signal. `base_spaces` awaits the ready signal.

**Pros**: callers don't know about the steps. Disk actor is fully
encapsulated; `manager_disk_t` public surface shrinks further (no
`bootstrap_*`, no `restore_oid_generator_*` exposed). Logically right —
"a disk that's not yet ready" is a state of the disk, not of its callers.

**Cons**: dependency direction needs care — WAL reader currently runs from
`base_spaces`; disk would need either to do its own WAL read (becomes
WAL-aware in a way it currently isn't) or to accept records as a ctor
argument. The latter is uglier but smaller.

**Surface estimate**: ~400-500 LOC. Larger because the disk actor grows a
small state machine (`booting → bootstrapped → replayed → oid_seeded →
ready`) and base_spaces' init is rewritten as a single await.

### C. Hybrid: encapsulate replay only, keep other sync init

A pragmatic middle ground:

- Replay moves into `services/disk/` as a method on `manager_disk_t` that
  internally calls existing sync helpers but **takes records as its single
  parameter**. Called from base_spaces as one line.
- Other init steps (`bootstrap_system_tables_sync`, `restore_oid_generator_sync`)
  remain sync because they don't pollute the codebase the way replay does
  (one caller, one site, well-understood).

**Pros**: smallest refactor that actually solves the "replay logic in
integration helper" layering problem. Doesn't restructure startup ordering.

**Cons**: keeps the `_sync` suffix pattern on the public class API. Doesn't
fully retire pre-scheduler sync calls. Honest critique: it's relocation of
sync, not removal — same observation that nudged us toward this doc.

**Surface estimate**: ~110 LOC (~95 new function impl + ~15 header) +
deletion of ~100 LOC from base_spaces.cpp.

## Recommendation

If undertaking this work: **Approach B**. The encapsulation is the actual
architectural win (a disk actor that owns its own readiness, not a class
exposing a parallel sync API). A and C are intermediate states that don't
retire the underlying pattern.

If the disk actor swallowing WAL reader is a step too far, fall back to A —
that retires the sync helpers without requiring disk to grow WAL knowledge.

C is not worth doing on its own (it's the "move sync code to a different
file" refactor that doesn't change anything substantive — observed during
the #160 discussion and rejected).

## Risks

- **Ordering bugs.** Subtle ordering invariants (the chain in §"Why this
  exists today") must be preserved when restructuring. A regression here
  manifests as catalog inconsistency at startup → hard to diagnose.
- **Dispatcher gate (Approach A).** A traffic gate is a new piece of
  concurrent state. Missing the gate = race where SQL hits dispatcher
  pre-replay. Easy to test (synthetic early-message scenario) but easy to
  miss in review.
- **WAL reader ownership (Approach B).** WAL reading currently runs from
  `base_spaces`. Moving it into disk-actor scope means the disk depends on
  the wal config / file paths in a way it currently doesn't. Acceptable
  per Phase 8 ownership rules (disk owns durability) but expands `manager_disk_t`'s
  dependency surface.
- **`init_from_state` semantics.** Currently passes an empty `collections`
  set (legacy artifact from pre-on-demand-resolve era). This refactor is a
  good moment to either delete `init_from_state` or repurpose it. Audit
  needed: any non-empty caller in tests?

## Dependencies / prerequisites

- Phase 11 stabilisation complete (#160 done, baseline restored, no
  outstanding sync-startup bugs).
- A clean understanding of `manager_wal_replicate_t::sync()` and
  `manager_disk_t::sync()` — both currently pass actor addresses, both
  could move into actor-contract handshake messages if we want zero sync
  on `base_spaces`.

## Out of scope (explicitly)

- Changing the catalog invariant chain itself. The ordering (bootstrap →
  replay → oid_seed → traffic) stays; only the mechanism changes from
  direct sync calls to actor messages with `co_await`.
- Async catalog reads inside `bootstrap_system_tables_sync`. The body of
  bootstrap can stay synchronous inside the actor handler — what changes is
  who calls it and through what mechanism.
- The W-TORN contract (`min(prev_checkpoint_wal_id_)`). Recovery semantics
  unchanged; only the driver location changes.

## Why this was deferred

`#160` is a bug fix; this is architectural cleanup. Bundling them would
mix risk profiles: the bug fix needs to land fast and be small enough to
review under regression pressure, this refactor needs careful design
review and a slow rollout. They are scheduled separately.