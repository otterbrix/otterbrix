# Async-Migration Callsite Audit (Phase 1 of async-first-refactor.md)

**Date:** 2026-06-03
**Method:** 4 parallel audit agents (wrapper surface / pump infra / test scheduler / bootstrap _sync carve-out)
**Status:** audit complete; no code changed

---

## 1. wrapper_dispatcher_t sync surface

`integration/cpp/wrapper_dispatcher.{hpp,cpp}`

- **15 public methods**, ALL return sync values (`cursor_t_ptr` / `bool`), all rooted in
  `wait_future<T>` (hpp:126-134) / `wait_future_void` (cpp:46-54): 10ms-poll loop over
  `event_loop_mutex_` (hpp:113) + `event_loop_cv_` (hpp:114), final `take_ready()`.
- Only **3 direct wait_future callsites**: cpp:203 (`register_udf`), cpp:219 (`unregister_udf`),
  cpp:311 (`send_plan` — feeds the other 13 methods).
- **Phase 2 deletion list:** `wait_future`, `wait_future_void`, `event_loop_mutex_`, `event_loop_cv_`;
  all 15 signatures become `unique_future<T>`.

### Callsite inventory (~990 calls, 31 files)

| Category | Files | Calls |
|---|---|---|
| Integration tests (integration/cpp/test) | 19 | ~944 |
| Benchmarks (benchmark/) | 4 | 13 |
| Example (example/cpp/main.cpp) | 1 | 23 |
| Python bindings (integration/python/sql) | 2 | 2 |
| Main layer (base_spaces, otterbrix, connection) | 3 | 8 |

Top test files (migration hotspots): `test_sql_features.cpp` **321**, `test_arithmetic.cpp` **124**,
`test_persistence.cpp` **120**, `test_collection_sql.cpp` **82**, `test_production_scenarios.cpp` **75**,
`test_batch_execution.cpp` 56, `test_column_projection.cpp` 53, `test_correctness_bugs.cpp` 45.
→ Phase 3 `WAIT_FUTURE(future, timeout)` macro converts these near-mechanically (sed-able pattern:
`auto c = dispatcher->execute_sql(...)` → `auto c = WAIT_FUTURE(dispatcher->execute_sql(...))`).

---

## 2. Manager pump infrastructure (Phase 4 deletion targets)

| Aspect | dispatcher | disk | index | wal |
|---|---|---|---|---|
| `enqueue_impl` pump loop | dispatcher.cpp:215-325 | manager_disk.cpp:279-379 | manager_index.cpp:115-240 | manager_wal_replicate.cpp:94-198 |
| `in_flight_behaviors_` | hpp:257 | hpp:797 | hpp:350 | hpp:182 |
| `pumping_` / `pump_cv_` / `current_slot_` / `mutex_` | 259/239/260/231 | 798/747/799/745 | 351/349/360/347 | 183/187/184/185 |
| `pending_void_` + `poll_pending()` | YES (258, 347) | — | YES (333, 341) | — |
| `run_fn_` + ctor param | hpp:197/77 | hpp:744/261 | hpp:281/48 | hpp:180/51 |
| `production_idle_tick()` | cpp:327-345 | cpp:381-385 | cpp:242-246 | cpp:200-204 |
| behavior() msg cases (all `co_await dispatch`) | 13 | 38 | 21 | 10 |

Manager-specific deltas to keep in mind when deleting:

- **dispatcher only:** `production_idle_tick` re-enqueues `executors_` (actor-zeta lost-wake-up
  workaround) + 10µs sleep (~10% CPU per pump thread); `pending_void_` holds fire-and-forget
  `on_horizon_advanced` broadcasts (parked at dispatcher.cpp:466,473).
- **disk only:** second scheduler `scheduler_disk_` (hpp:743) used to enqueue agents at
  manager_disk.cpp:566 (`on_horizon_advanced`) and :677 (`mark_storage_dropped`) — these enqueues
  survive pump removal (they drive *agents*, not the manager pump).
- **index only:** `poll_pending()` also called at top of `behavior()` (manager_index.cpp:249).
- **wal:** cleanest — no fire-and-forget, no poll_pending (by design, comment at cpp:112-113).
- `set_run_fn` setters already removed (c0d5185f); run_fn now ctor-param only. ✓ verified.

Per-manager removal checklist: enqueue_impl override, 5 pump state members, mutex_, run_fn_t
typedef + member + ctor param, production_idle_tick, poll_pending/pending_void_ (dispatcher+index),
pump_cv_.notify_one sites, record_session + slot_guard usage.

---

## 3. Test scheduler infrastructure (Phase 3/5 scope)

- `core/non_thread_scheduler/`: scheduler_test.{hpp,cpp}, clock_test.{hpp,cpp}; API: `run(N)`,
  `run_once()`, `advance_time()`, `start/stop`.
- **14 test files** use `scheduler_test_t`, **132 TEST_CASEs** total:
  - services/disk/tests: 10 files (mvcc_ddl 15, persistence 13, ddl_methods 13, pg_depend 12,
    wal_catalog 12, d4_lazy_load 10, system_table_bootstrap 8, error_handling 8, resolve 6, recovery 4)
  - services/dispatcher/tests: 2 files (variant_e3_differential 14 — incl. is_ready() polling loop
    at :82-86, dispatcher_catalog 2)
  - integration/cpp/test: test_clean_break_startup.cpp (10 cases, 12 direct `run(10000)` calls,
    its own `fresh_disk` fixture — the only integration file on scheduler_test_t)
  - services/wal/tests: test_wal_torn_write.cpp (include only, no manager spawning)
- Fixture pattern is uniform: `new scheduler_test_t(1,1)` + spawn lambda `[this]{ scheduler->run(10000); }`
  + an `invoke()`-style helper doing `send → run(10000) → take_ready()`. Mechanical to migrate.
- Extra sync-wait debt outside fixtures: `sleep_for(100ms)` ×7 in
  services/index/tests/test_bitcask_index_disk.cpp; raw is_ready()/take_ready() loops in
  services/wal/tests (test_wal_worker.cpp, test_wal_manager.cpp — already future-based,
  only need WAIT_FUTURE normalization).
- **test_spaces / base_otterbrix_t already uses real `shared_work`** (base_spaces.cpp:25-32) —
  integration tests need no scheduler change, only return-type adaptation (WAIT_FUTURE).

---

## 4. Bootstrap `_sync` carve-out (rule 11) — stays, with one contract fix

**48 _sync methods total:** manager_disk_t 25, agent_disk_t 12, manager_index_t 6,
manager_dispatcher_t 3, manager_wal_replicate_t 2.

Verdict per async-first-refactor.md non-goals: the carve-out **stays synchronous** (runs strictly
before `scheduler.start()` at base_spaces.cpp:380-382). Confirmed: runtime mutations already have
mailbox-only paths (`storage_append_inner`, `storage_update_inner`, ...); `direct_*_sync` have
**no async twins and need none** — they are WAL-replay-only.

Bootstrap flow order (base_spaces.cpp): wiring (129-131) → `bootstrap_system_tables_sync` (143) →
`load_user_table_storages_sync` (148) → WAL replay `replay_one` (164-286: system then user,
sequential) → `restore_oid_generator_sync` (292) → drop-GC rebuild (304-346) →
`set_replay_horizon_sync` (364) → index bootstrap (455-527) → **scheduler start (380)**.

### pmr boundary contract (the actual bug surface)

| _sync path | Status | Evidence |
|---|---|---|
| `direct_append_sync` | **safe** — manager already rebuilds chunk on agent resource (manager_disk_storage.cpp:37 `rebuild_chunk`) | insert-replay tests green |
| `direct_delete_sync` | **safe** — ids_vec built on `resource()` (agent_disk.cpp:245-250) | delete-replay green |
| `direct_update_sync` | **BUG** — chunk from `&resource` zero-copy sliced into agent-resource storage | SIGABRT ×3 tests; see wal-recovery-pmr-mismatch.md |
| `bootstrap_repopulate_sync` (index) | **suspect** — `scan_storage_for_rebuild_sync(oid, &resource)` chunk (base_spaces.cpp:521) fed into index engines (527) | needs a Debug-assert exercise |

Contract to encode (applies equally in the future mailbox world): **any chunk crossing an actor
boundary is materialized on the receiver's resource at the receiving edge** — same rule
`direct_append_sync` already follows via `rebuild_chunk`.

---

## 5. Sizing & order of attack

| Phase | Scope confirmed by audit | Est. |
|---|---|---|
| 0 (pre) | Fix `direct_update_sync` pmr copy (3 red tests block everything) + check `bootstrap_repopulate_sync` | hours |
| 2 | 2 files (wrapper_dispatcher), 15 signatures, delete 2 wait helpers + 2 cv members | 1-2 d |
| 3 | WAIT_FUTURE macro + ~990 callsites in 31 files (19 test files mechanical, parallelizable per file) | 2-4 d wallclock with agents |
| 4 | 4 managers × ~14-item deletion checklist; keep disk's agent-enqueue sites; drop-recovery workaround disappears with pump | 1-2 d (4 agents parallel) |
| 5 | Delete core/non_thread_scheduler + migrate 14 fixture files / 132 cases | 1-2 d |

Risks sharpened by audit:
- Dispatcher's executor re-enqueue workaround is the only pump piece compensating an actor-zeta
  scheduling gap — Phase 4 must verify default `cooperative_actor::resume_impl` on 1.2.0 actually
  redelivers (multithread test is the canary).
- `pending_void_` (dispatcher/index) needs an async home: either co_await the broadcasts or a
  detached-continuation idiom — decide in Phase 4 design, do NOT silently drop futures.
- Python bindings (2 callsites) keep sync facade: bind `WAIT_FUTURE` at the binding edge.