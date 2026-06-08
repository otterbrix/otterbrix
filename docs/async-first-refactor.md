# Async-First Refactor: Embrace actor-zeta Non-Blocking Model

## Context

`actor-zeta` is **non-blocking by design**. The framework intentionally provides NO synchronous blocking API. Every attempt by user code to wrap async actor calls in synchronous semantics is architectural violence against the framework's purpose — and creates a chain of secondary bugs (pump driver races, lost wake-ups, test/prod divergence) that cannot be cleanly resolved while the sync wrapper exists.

This document proposes elimination of all sync wrappers and migration of `otterbrix` to fully async / coroutine-based call patterns at every layer above `actor-zeta`.

## Root cause analysis — one principle, all symptoms

The codebase has accumulated infrastructure to make non-blocking actor calls appear synchronous. Every piece is a symptom of one core architectural violation:

```
actor-zeta is non-blocking by design
  │
  ↓ (user code wants sync API)
wrapper_dispatcher_t::wait_future (polling-based sync wait)
  │
  ↓ (sync wait blocks the caller thread, blocking the only thread that could drive the actor)
caller-driven pump pattern (enqueue_impl override that loops until done)
  │
  ↓ (pump driver thread needs a way to yield)
run_fn_ Strategy (overloaded: prod yield + test scheduler drive + actor-zeta drop recovery)
  │
  ↓ (race between concurrent enqueues and pump state)
multithread test flakiness
  │
  ↓ (actor-zeta cooperative scheduler drops suspended actors that never get re-queued)
"actor-zeta drop bug"
```

**Conclusion**: the "actor-zeta drop bug" is NOT an actor-zeta bug. It is a symptom of our sync wrapper, which is incorrect by construction. Removing the sync wrapper makes the entire chain disappear.

## Symptoms to remove

| Component | Current behavior | Removal rationale |
|---|---|---|
| `wrapper_dispatcher_t::wait_future` | polling cv.wait_for over async future | Sync block over non-blocking framework — fundamentally wrong |
| `wrapper_dispatcher_t::execute_sql` returns `cursor_t_ptr` | Sync return, internally calls wait_future | Should return `unique_future<cursor_t_ptr>` |
| `manager_X_t::enqueue_impl` override (4 managers) | Caller-driven pump loop | actor-zeta `cooperative_actor::resume_impl` does this correctly |
| `in_flight_behaviors_`, `pumping_`, `pump_cv_`, `current_slot_` | Pump driver state | Not needed once pump is removed |
| `run_fn_` typedef + member + ctor param | Yield point abstraction | Not needed once pump is removed |
| `production_idle_tick()` (new, this session) | "Smart" yield strategy | Not needed once pump is removed |
| `set_run_fn` setter | Test override of yield strategy | Not needed once pump is removed |
| `non_thread_scheduler::scheduler_test_t` | Sync drive for tests | Tests should use real `shared_work` scheduler |
| 13+ test fixtures with `set_run_fn([this]{ scheduler->run(10000); })` | Manual drive of synchronous test scheduler | Tests rewritten with coroutine adapter |
| `pump_cv_.notify_one()` in enqueue branches | Wake pump driver | Not needed once pump is removed |
| Workaround for actor-zeta cooperative drop bug (re-enqueue executors_) | Compensation for lost wake-ups | Bug surface disappears with default cooperative_actor |
| `std::function<void()> run_fn_t` (rule 14 violation) | Type-erased yield callable | Disappears with run_fn_ |

## Target architecture

### Layer 1: actor-zeta (unchanged)

`actor-zeta` provides:
- `cooperative_actor` with default `resume_impl` (no override)
- `actor_zeta::send(addr, &Method, args...)` returns `unique_future<ReturnType>`
- `shared_work` work-sharing scheduler with N worker threads

We do not patch actor-zeta. We use it as-is.

### Layer 2: managers (4 actors)

Each manager (`manager_dispatcher_t`, `manager_disk_t`, `manager_index_t`, `manager_wal_replicate_t`) is a plain `actor_zeta::actor::actor_mixin<DerivedT>` (already true) or `cooperative_actor<DerivedT>` with default `enqueue_impl` (= no override).

Method bodies are coroutines:
```cpp
unique_future<cursor_t_ptr>
manager_dispatcher_t::execute_plan(session_id_t session, node_ptr plan, ...) {
    auto resolved = co_await planner_->resolve(plan);
    auto optimized = co_await optimizer_->optimize(resolved);
    auto physical = co_await physical_gen_->generate(optimized);
    auto result = co_await executor_->execute(physical, ...);
    co_return make_cursor(std::move(result));
}
```

NO pump state. NO mutex_ for pump. NO run_fn. Just coroutines that suspend on `co_await` and resume when the awaited result is ready — driven by the scheduler worker threads.

### Layer 3: wrapper_dispatcher (thin async forwarder)

```cpp
class wrapper_dispatcher_t {
public:
    // ALL methods return unique_future<T>. No sync wrapping.
    unique_future<cursor_t_ptr> execute_sql(session_id_t session, std::string query);
    unique_future<cursor_t_ptr> execute_plan(session_id_t session, node_ptr plan, params_ptr params);
    unique_future<bool> register_udf(session_id_t session, function_ptr fn);
    // ... etc
private:
    actor_zeta::address_t manager_dispatcher_;
};
```

Implementation is one-line forwarding:
```cpp
unique_future<cursor_t_ptr>
wrapper_dispatcher_t::execute_sql(session_id_t session, std::string query) {
    auto [_, future] = actor_zeta::send(manager_dispatcher_,
        &manager_dispatcher_t::execute_sql, session, std::move(query));
    return future;
}
```

NO wait_future. NO pump. NO cv. The caller gets a future and is responsible for awaiting it.

### Layer 4: tests (coroutine-based)

Integration tests use C++20 coroutines:

```cpp
// Catch2 v3 doesn't natively support coroutines. We provide a custom
// SECTION-equivalent that wraps a coroutine and drives it to completion
// using the test scheduler (real shared_work, no special test scheduler).

TEST_CASE("integration::cpp::test_otterbrix_multithread") {
    test_spaces space(config);
    co_await_test_section("initialization", [&]() -> task<void> {
        co_await space.dispatcher()->execute_sql(session, "CREATE DATABASE ...");
    });
    // ...
}
```

OR for simplicity in early phases — use `future.is_ready()` polling with bounded timeout AT THE TEST BOUNDARY only:

```cpp
auto future = dispatcher->execute_sql(session, "INSERT ...");
auto cursor = WAIT_FUTURE(future, 30s);  // test-only macro, polls is_ready, fails on timeout
```

The polling happens **outside** any actor code. Test thread sleeps cooperatively; scheduler workers drive actors in background. NO pump driver inside actor code.

### Layer 5: scheduler (shared_work in both prod and tests)

Remove `non_thread_scheduler::scheduler_test_t` entirely. Tests use `actor_zeta::scheduler::shared_work` with N=4 workers (matching production). Workers are real OS threads that drive `cooperative_actor::resume_impl` automatically.

Test fixtures construct managers exactly as production does — no `set_run_fn`, no `scheduler->run(10000)`.

## Phased migration

### Phase 1 — Audit (1 day)

Catalog all sync callsites:
- `wrapper_dispatcher_t` method usages
- `execute_sql` callers (~50 in tests + integration)
- `wait_future` direct usages
- `enqueue_impl` overrides (4 managers)
- `non_thread_scheduler::scheduler_test_t` usages (~15 test fixtures)

Output: `docs/async-migration-callsite-audit.md`.

### Phase 2 — Change wrapper_dispatcher_t return types (2 days)

Convert all `wrapper_dispatcher_t::*` methods from `T` to `unique_future<T>`. Remove `wait_future` private helper. Remove `event_loop_mutex_` / `event_loop_cv_` members.

This breaks all callers. They will be fixed in Phase 3.

### Phase 3 — Test infrastructure: coroutine adapter (3-5 days)

Either:
- (a) Build a minimal Catch2 coroutine adapter (custom SECTION + co_await driver)
- (b) Provide `WAIT_FUTURE(future, timeout)` macro for test-boundary polling

Migrate ~13 test fixtures + ~50 integration tests to new pattern.

### Phase 4 — Remove pump pattern from managers (2 days)

For each of `manager_dispatcher_t`, `manager_disk_t`, `manager_index_t`, `manager_wal_replicate_t`:
- Delete `enqueue_impl` override
- Delete `in_flight_behaviors_`, `pumping_`, `pump_cv_`, `current_slot_`, `slot_guard`
- Delete `run_fn_` typedef + member + ctor param + `production_idle_tick`
- Delete `record_session` (was used by slot_guard)
- Verify `behavior(msg)` is correct top-level coroutine dispatch

Default `cooperative_actor::resume_impl` handles everything.

### Phase 5 — Delete non_thread_scheduler (1 day)

- Delete `core/non_thread_scheduler/` directory entirely
- Migrate any remaining test that still references `scheduler_test_t` to use `shared_work`

### Phase 6 — Verify (ongoing)

- Full `ctest` should pass
- multithread test should pass reliably (no more pump race surface)
- CPU usage at idle should be 0% (real scheduler workers cv.wait)

## Risks & mitigations

| Risk | Mitigation |
|---|---|
| Catch2 coroutine support is non-trivial | Start with `WAIT_FUTURE(timeout)` macro; coroutine adapter is Phase-3.5 polish |
| Migration touches ~50+ test files | Use parallel sub-agents for mechanical migration |
| Some hidden sync assumption in non-pump code | Audit Phase 1 to surface |
| Performance regression | Async is the framework's optimization path — should be **faster** not slower |
| Bootstrap-time sync calls in `base_spaces` (rule 11 exception) | Stay sync — rule 11 already permits this carve-out before scheduler.start() |

## Non-goals

- Patching actor-zeta itself
- Replacing actor-zeta with a different framework (CAF, SObjectizer)
- Pre-emptive scheduler (work-stealing in actor-zeta is sufficient)
- Removing `unique_future<T>` typedef — this IS the canonical async return

## Rollback

If async migration proves too disruptive at any phase:
- Phase 1 (audit): pure read-only, no rollback needed
- Phase 2-3 (wrapper + tests): both can be reverted via git, no commit until phase completes
- Phase 4 (manager pump removal): atomic per-manager — can keep some managers async-only while others retain pump
- Phase 5 (non_thread_scheduler removal): atomic — undo if needed

## Project rules compliance check

| Rule | Compliance |
|---|---|
| 2: no exceptions, internal error system | ✓ async path uses `core::error_t` / `result_wrapper_t` |
| 3: pipeline canonical | ✓ unchanged |
| 4: build at end | ✓ each phase ends with build + ctest |
| 5: use TaskList for parallel agents | ✓ Phase 3 + Phase 4 parallelizable |
| 6: no fallback / backward compat | ✓ sync wrappers fully deleted, not kept alongside |
| 7: use agents | ✓ test migration via parallel sub-agents |
| 9: no exceptions on hot path / between actors | ✓ unchanged |
| 10: no shared state between actors | ✓ unchanged |
| 11: direct sync via parent pointer only in base_spaces | ✓ unchanged (bootstrap carve-out) |
| 12: Pure MVCC | ✓ removing pump mutex IMPROVES this (less locks) |
| 13: callbacks + parent ptr sync removal in base_spaces only | ✓ unchanged |
| 14: no std::shared_ptr / any / variant / function | ✓ `std::function<void()>` for `run_fn_t` REMOVED entirely |
| 15: no goto | ✓ unchanged |

This refactor advances rule 12 (Pure MVCC: fewer locks) and rule 14 (eliminates `std::function`) — currently the two rules most violated by pump infrastructure.

## Estimated total effort

7-12 working days for one engineer. Parallelizable through sub-agents:
- Phase 1: 1 day (sequential, single engineer)
- Phase 2: 2 days (sequential)
- Phase 3: 3-5 days (3 sub-agents for fixture migration → 1-2 days wallclock)
- Phase 4: 2 days (4 sub-agents per manager → 0.5-1 day wallclock)
- Phase 5: 1 day (sequential)
- Phase 6: ongoing validation

**Wallclock estimate with parallel sub-agents: 4-6 days.**

## Summary

The codebase has been fighting actor-zeta's non-blocking design for so long that we built workarounds-of-workarounds. The "multithread bug" is the visible tip — under it sits ~70% of pump infrastructure that exists only to fight the framework's intent.

Stop fighting. Embrace `unique_future<T>` returns everywhere above `actor-zeta`. Delete pump. Delete sync wrappers. Delete test scheduler. Result: simpler code, fewer bugs, no flakiness, full alignment with framework philosophy.
