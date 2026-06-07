# TSAN: residual reports on actor-zeta message buffers (for the library author)

**Date:** 2026-06-04. **Context:** full TSAN run of 771 tests (Debug, `ENABLE_TSAN=ON`,
otterbrix on stock actor-zeta 1.2.0, managers = event-loop-in-thread). Run outcome:
44 warnings across 32 files; after subtracting the FP from `synchronized_pool_resource` in the WAL tests
(fixed: TSAN shim for the resource in the fixtures) and Catch2 races (fixed: REQUIRE removed from
worker threads), **one cluster of ~20 reports** remains in `test_otterbrix`, entirely
inside actor-zeta. Library fixes are out of scope for this session — this document records the
diagnosis and the decisive experiment.

## Cluster signature

All reports are on the bytes of ONE 120-byte `mailbox::message` (header) and its 400-byte
rtt argument buffer (`execute_plan`/`execute_plan_full`):

- WRITE: manager_dispatcher loop thread (`std::thread` from the ctor, dispatcher.cpp:203) —
  message construction: `make_message` → rtt copy-ctors of the arguments
  (`storage_parameters` pmr hashmap, `intrusive_ptr<node_t>`), `init_future_slot`
  (message.hpp:56, `result_slot_`).
- READ: `scheduler_dispatcher_` worker (cooperative_actor executor_t) — consumption:
  `dispatch()` → `get_result_promise` (message.hpp:177), `type_list_at` move-ctors from rtt.

That is, TSAN sees "the producer is still writing the message, the consumer is already reading it" — or
address reuse without a modeled edge.

## Two hypotheses (the analyst agents diverged)

### H1: a genuine visibility race (construction-vs-consumption)
The message becomes reachable to the consumer BEFORE the construction stores are
release-published. The send.hpp:51-57 contract — "fully construct, then enqueue" —
is correct only if ALL delivery paths go through a release/acquire edge
(`lifo_inbox::push_front` CAS seq_cst → `take_head` CAS). If there is a path where the
consumer reaches the message bypassing this chain (e.g., a resume via an already
cached pointer), the edge is lost.

### H2: memory reuse not modeled by TSAN
The push(release) → take_head(acquire) chain is complete; therefore the "race" is two DIFFERENT
messages on the same reused bytes: the worker read message-A, freed it
(message_guard; `transfer_ownership` in dispatch.hpp:127 only quenches cleanup_fn_),
the loop thread allocated message-B at the same address and wrote to it. The weak point of H2: in
test_otterbrix the allocations go through `tsan_resource_t` → `new_delete_resource` →
malloc/free, whose free→malloc edges TSAN models natively — a reuse FP should not survive.
This is the main argument AGAINST H2 and in favor of H1 or a third mechanism (e.g.,
message allocation through a different, non-intercepted path).

## Additional evidence (control run after twin-routing of the disk)

After moving the disk manager's DML/resolve paths onto mailbox routing (more sends per
request) the cluster grew proportionally to the traffic (44 → ~79 warnings), and meanwhile:
- not a single report has storage-internal frames (row_group/data_table/compact) —
  zero application-level races;
- the new reports appeared exactly on the NEW sends (e.g., the rtt buffer of
  `storage_publish_commits_inner`: WRITE = `rtt::push_back_no_realloc` in make_message on
  the manager's loop thread, READ = `rtt::get<>` move-out in dispatch on the agent's thread);
- in these pairs both sides are the same handler type, i.e., they are consistent with ONE
  message lifecycle. This shifts the balance in favor of H1 (construction-vs-consumption
  visibility): reuse (H2) would more often collide different message types at the same
  address. The decisive factor remains the message-generation experiment (below).

## Experiment with a malloc-backed resource — DONE, H2 practically closed

The standalone test_wal fixtures were moved to a malloc-backed resource (test_pool_resource_t →
new_delete_resource). Control-run result: the pool footprint disappeared COMPLETELY
(0 `__allocate_in_new_chunk` frames in all reports; message heap origin =
`operator new` via test_pool_resource_t::do_allocate), but 15 reports of
`message.hpp:177` in test_wal REMAINED. TSAN models the free→malloc happens-before
natively, so memory reuse (H2) cannot explain the surviving reports.
Together with the "same handler type in both stacks" pairing, the verdict shifts toward H1:
in the message handoff of actor-zeta 1.2.0 there is a path on which the construction stores
are not ordered with the consumer's read. The tests are green throughout (771/771) —
the window either practically never materializes on arm64, or is closed by adjacent
synchronization; nevertheless for the library this is a candidate for a real fix, not a
suppression. The final word comes from the message-generation experiment (below).

## Decisive experiment (when there's time for the library)

1. In a TSAN build, log the message address+generation (atomic counter in the ctor) and at
   report time check: one generation (→ H1) or different ones (→ H2).
2. Or: wrap `result_slot_` in `std::atomic_ref<void*>` release/acquire and re-run
   — if reports on the rtt fields remain, H1 is confirmed for the argument buffer
   (atomicity of a single slot does not heal the visibility of the whole buffer; only the correct
   edge on enqueue/dequeue does).
3. If H2: `__tsan_acquire/__tsan_release` annotations on message free/alloc are enough
   (or a suppression on message.hpp) — code correctness is unaffected.

## Related
- The cooperative_actor parking race (blocked-check before Q6) — separate document:
  `docs/actor-zeta-lost-wakeup.md`; worked around with a guard in the otterbrix dispatcher loop.
- The `synchronized_pool_resource` FP class is documented in `integration/cpp/base_spaces.hpp`
  (tsan_resource_t) and is now duplicated in the services/wal/tests fixtures.
