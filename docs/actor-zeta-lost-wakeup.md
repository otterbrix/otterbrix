# actor-zeta: lost-wakeup in cooperative_actor — diagnosis and fixes

**Date:** 2026-06-04
**Status:** root cause proven by inspecting the live process; fix proposed (not applied — the decision is up to the library author)
**Symptom in otterbrix:** hang in `test_otterbrix_multithread` (concurrent INSERTs, 4 threads) — a known flake; re-enqueue from `production_idle_tick` does not help even at 100k/sec (recorded back in b54f3256).

## Evidence base (live process, lldb)

The hang was reproduced; from the hung process the stacks of all threads and the actor states were captured:

| Observation | Value |
|---|---|
| executor[0] `current_behavior_.is_busy()` | **true** (coroutine suspended) |
| executor[0] `is_awaited_ready()` | **true** (future ready long ago!) |
| `awaited_continuation_` slot | **non-null** (0x5e403a810 — continuation in place) |
| `awaited_flags_` | 9 = value_set \| promise_released |
| executor[0] `state_` | **idle** (0) |
| executor[0] `mailbox().blocked()` | **true** ← the key |
| `resume_impl<executor_t>` | called continuously (breakpoint hits on 3 workers) |
| CPU | 72% (hot no-op loop), RSS — flat |
| Progress | zero |

**Control experiment:** a manual `try_unblock()` of the mailbox via lldb → the system moved instantly (`execute_plan: result received` appeared in the log after 8 hours of the hang), then the invariant assert `lifo_inbox::take_head:78` fired (as expected — an intervention outside the protocol). Root cause confirmed.

## Mechanism

```
1. The executor's coroutine suspends on a cross-actor co_await.
   In the suspension window awaited_flags_ are not yet published → is_busy() == false.
2. resume_impl in this window: "not busy, mailbox empty" → try_block() → the actor is PARKED
   (mailbox → reader_blocked).
3. The producer completes the future: release_promise sets ONLY the promise_released flag.
   Nobody unblocks the mailbox (readiness is not a message).
4. Every subsequent resume (even 100k/sec from re-enqueue):
       cooperative_actor.hpp:292   if (mailbox().blocked())
                                       return awaiting;        ← FIRES FIRST
       cooperative_actor.hpp:299   if (current_behavior_.is_busy()) { Q6 ... }  ← NOT REACHED
5. The ready continuation is never taken. An eternal no-op loop.
```

That is why the re-enqueue patch (the dispatcher's `production_idle_tick`) is powerless: the problem is not that "the actor was forgotten to be scheduled", but that resume **falls out on the blocked-check before Q6**.

## Proposed fix (cooperative_actor.hpp, entry resume_impl)

Lift the Q6 block above the blocked-check + re-check after resume (mirroring the in-loop guard):

```cpp
// Q6 FIRST — must execute even with a blocked mailbox (parked):
// readiness of the awaited-future is flag-based (release_promise), it does not unblock the inbox.
if (current_behavior_.is_busy()) {
    if (current_behavior_.is_awaited_ready()) {
        auto cont = current_behavior_.take_awaited_continuation();
        if (cont) {
            cont.resume();
        }
    }
    // Re-check (mirror of the in-loop guard): the await may not have been ready, or
    // the coroutine re-suspended after resume on the next co_await.
    // Falling through here into blocked-return / try_block re-strands it again.
    if (current_behavior_.is_busy()) {
        return finalize(scheduler::resume_result::resume, 0, true);
    }
}

if (mailbox().blocked()) {
    return finalize(scheduler::resume_result::awaiting, 0, false);
}
```

Properties:
- **Self-healing**: even if the race from step 1-2 parks the actor, the very first resume (from re-enqueue / a new message) drains the ready continuation. The awaited_flags_ publication race can be fixed separately — the system stops being fatally sensitive to it.
- It also closes the second asymmetry of the entry path: "busy+ready → resume → re-suspend → fall into try_block → drop" (the in-loop path has a guard at :348, the entry path did not).
- After this fix the re-enqueue patch in otterbrix (the dispatcher's `production_idle_tick`: loop over executors_ + 10µs sleep) becomes removable.

## Related fixes (candidates, based on the analysis)

1. **awaited_flags_ publication vs try_block** — the prime cause of parking a busy actor: close the window (the publication order of the chain before control returns from the suspension), or consider it acceptable given the self-healing above.
2. **`lifo_inbox::take_head:78` invariant** — the assert is correct; it does not anticipate external interventions. Do not touch.
3. **Document the contract**: "future readiness does not wake the actor — only a push into the mailbox or a forced resume does; resume must check busy/ready before any early-returns".

## Reproduction / verification of the fix

- Repro: `test_otterbrix` `integration::cpp::test_otterbrix_multithread` (4×INSERT of 25k values each) — hangs with ~50-100% probability per run before the fix.
- Fix criterion: 10 runs in a row without a hang + existing actor-zeta tests green.
- Minimal standalone repro for the library test-suite: actor A co_awaits actor B; between the suspension and the publication of the awaited-chain, insert a yield (or run under a load of 4+ sender threads); verify that A makes it through after B becomes ready while A's mailbox is blocked beforehand.
