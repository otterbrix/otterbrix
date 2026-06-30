// HIDDEN red repro for the REAL-path actor-zeta lost-wakeup that breaks per-batch
// fetch-next streaming (Task #13). Companion to test_lost_wakeup_streaming.cpp.
//
// WHY THE SIMPLIFIED TEST IS NOT ENOUGH
// -------------------------------------
// test_lost_wakeup_streaming.cpp models the consumer (executor) awaiting a producer
// that replies IMMEDIATELY from its own behavior() handler — a SINGLE cross-actor hop:
//
//      executor.execute_pipeline()  --send-->  source.fetch_batch()  (co_return idx)
//                       ^------------- reply (promise.set_value) -------------/
//
// The option-2 fix in cooperative_actor.hpp resume_impl ("Inter-await null-window
// guard": keep a live-but-!done() behavior scheduled instead of parking it) makes that
// one-hop shape GREEN: the consumer is kept scheduled across the inter-await window, so
// the producer's flag-only set_value() is eventually observed by the consumer's own
// resume_impl re-poll.
//
// But the REAL per-batch fetch-next reply is NOT one hop. From the production code
// (services/collection/executor.cpp execute_pipeline -> source_next ->
// services/disk/manager_disk_storage.cpp storage_fetch_next_batch ->
// services/disk/agent_disk.cpp storage_fetch_next_batch_inner) the reply chains TWO
// cross-actor hops across TWO schedulers:
//
//      executor (scheduler_)           manager_disk (scheduler_)         agent_disk (scheduler_disk_)
//   execute_pipeline()                                                                   ^
//     co_await source_next() --send--> storage_fetch_next_batch()                        |
//                                        co_await(send( ----------------> storage_fetch_next_batch_inner()
//                                          agent.inner ))                    co_return batch
//                                        co_return co_await fut   <-- reply (set_value, NO resume)
//     co_await result        <-- reply (set_value, NO resume) --/
//
// The INTERMEDIATE actor (manager_disk) is ITSELF suspended on a cross-actor co_await
// when the leaf (agent_disk) reply lands. The leaf's promise.set_value() (future.hpp
// promise<T>::set_value: "NO cont.resume() - consumer resumes in its own thread") only
// flips a flag; the intermediate must be re-polled by ITS OWN resume_impl to advance,
// and only THEN does the intermediate's reply flip the consumer's flag — which in turn
// only wakes the consumer if the consumer is still scheduled. There are now THREE actors
// (consumer, intermediate, leaf) on TWO schedulers that each must stay scheduled across
// their own inter-await null-window for a single batch to be delivered. The option-2
// guard keeps ONE actor scheduled across ONE local null-window; it does NOT cover the
// CHAINED case where the wakeup of actor A depends on actor B (also mid-await) being
// re-polled, whose wakeup in turn depends on leaf C — a flag-only completion three deep,
// across a scheduler boundary, looped once per batch.
//
// STRUCTURAL DIFFERENCE (the real invariant under test):
//   simplified : 1 producer, replies from its OWN behavior() handler, 1 scheduler.
//   real       : 2 producers chained (relay -> leaf); the RELAY replies only AFTER its
//                OWN cross-actor co_await to the leaf completes; relay+consumer share one
//                scheduler, leaf runs on a SECOND scheduler. The consumer's per-batch
//                wakeup is therefore a flag-only completion that must propagate up a chain
//                of suspended-on-co_await actors, none of which the leaf's set_value()
//                resumes.
//
// INVARIANT THE EVENTUAL actor-zeta AWAIT-CORE FIX MUST SATISFY
// ------------------------------------------------------------
//   For any chain of actors A_0 (consumer) -> A_1 -> ... -> A_k (leaf), where each A_j
//   (0 <= j < k) is suspended on a cross-actor co_await whose future is completed (via
//   promise::set_value, which does NOT resume the continuation) by the reply produced
//   when A_{j+1} finishes its handler, AND where the consumer performs N such chained
//   awaits sequentially from ONE inner coroutine frame under ONE behavior() handler:
//   NO completion may be lost. Concretely, completing A_{j+1}'s reply MUST guarantee
//   that A_j is (or stays) scheduled so its own resume_impl re-polls the now-ready
//   awaited future and advances — even when A_j is transiently in its inter-await
//   null-window (is_busy()==false, !done()), even when A_j and A_{j+1} live on
//   DIFFERENT schedulers, and even when A_{j+1}'s set_value() runs on a worker that
//   never touches A_j's mailbox or scheduler. Equivalently: a flag-only future
//   completion must wake (schedule) its waiting actor regardless of where the completer
//   runs; keeping an actor scheduled only across its OWN local null-window (the option-2
//   guard) is INSUFFICIENT when the waiter is parked at the instant a remote completer
//   flips the flag. The N sequential per-batch fetch-next awaits, each resolved by a
//   2-hop cross-scheduler reply, must ALL be delivered with zero reliance on an external
//   watchdog poke.
//
// THE DEEPER-NESTING MODEL (validated here) AND WHY IT STILL DOES NOT REPRODUCE STANDALONE
// ---------------------------------------------------------------------------------------
// To faithfully model the production nesting this harness adds a SEPARATE operator object
// (scan_operator_t, == full_scan / transfer_scan) whose source_next() is its OWN
// unique_future coroutine, co_awaited by the executor's execute_pipeline, and which itself
// performs TWO SEQUENTIAL cross-actor sub-awaits (co_await sf, then co_await
// emit_or_advance(...) which awaits AGAIN one frame deeper). The full nesting modeled is:
//
//   behavior() -> dispatch(run_query) -> execute_plan -> execute_pipeline
//                    -> co_await source->source_next(ctx)        [separate operator frame]
//                          -> co_await sf                        [sub-await #1]
//                          -> co_await emit_or_advance(...)       [sub-await #2, awaits again]
//
// each sub-await being a 2-hop (relay/manager_disk -> leaf/agent_disk) cross-SCHEDULER
// reply through a suspended intermediate. The hypothesis was that the DEEPEST frame's
// inter-sub-await null-window — where sub-await #1's clear_awaited_chain has walked the
// top behavior's awaited_flags_ to nullptr (is_busy()==false) through every intermediate
// unique_future frame's propagated_to_flags_ back-reference, but sub-await #2 has not yet
// re-published it via propagate_awaited_state — would lose the wakeup the simpler one-hop
// and direct-await models do not.
//
// EMPIRICAL RESULT (2026-06-21, standalone harness vs the linked actor-zeta carrying the
// option-2 resume_impl guard `current_behavior_ && !current_behavior_.done()` => keep
// scheduled): it does NOT reproduce. 3000 queries x 64 batches x 2 sub-awaits each, on BOTH
// 3-worker-per-scheduler and 1-worker-per-scheduler configurations, all complete with zero
// stalls. The option-2 `!done()` keep-scheduled check fires regardless of HOW MANY frames
// deep the await lives: a top behavior whose awaited_flags_ is transiently nullptr mid-walk
// is still `!done()`, so it stays scheduled and its own resume_impl re-polls once sub-await
// #2 re-publishes. The deep clear_awaited_chain/propagate walk does not, by itself, defeat
// the guard in a standalone harness.
//
// CONCLUSION — this is a BEST-EFFORT SPEC, not a RED reproducer. The definitive reproducer
// is the PRODUCTION per-batch fetch-next path (the fetch-next cursor infra in full_scan /
// transfer_scan, currently DORMANT — whole-scan buffering replaced it precisely to dodge
// this lost-wakeup; see full_scan::source_next), which lost-woke ~11000 watchdog pokes in
// the full SELECT suite only because the dispatcher watchdog masks it. The standalone
// harness lacks the real production load: concurrent unrelated actors contending the
// mailbox, real batch materialization timing (column reads + chunk build) widening the park
// window, and the true depth/branching of execute_plan -> execute_sub_plan_ ->
// run_resolve_subplan re-entrancy. RE-VALIDATE this test (and expect it to turn RED) when
// the fetch-next per-batch source is revived in the buffer-pool effort.
//
// HIDDEN (manual) — tag [.] so it never runs in CI. Run explicitly:
//   test_otterbrix "[lostwakeup-realpath]"
//
// CURRENT STATE: GREEN on the option-2-fixed actor-zeta (documents the invariant the fix
//   must keep satisfying). Should turn RED if the chained-wakeup invariant above is ever
//   violated — e.g. when the dormant per-batch fetch-next source is revived, or if the
//   option-2 guard regresses. The hard per-query deadline guarantees a violation surfaces
//   as a FAILED query (deadline exceeded) rather than an infinite hang, so CI never wedges.

#include <catch2/catch.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

using namespace actor_zeta;

namespace {

    // ------------------------------------------------------------------
    // LEAF producer == agent_disk_t::storage_fetch_next_batch_inner.
    // Runs on a SEPARATE scheduler (scheduler_disk_ in production). Each call is the
    // deepest cross-actor hop; it does a touch of real work then replies. Its reply is
    // delivered via promise::set_value (NO cont.resume) — the intermediate must re-poll.
    // ------------------------------------------------------------------
    class leaf_disk_actor final : public basic_actor<leaf_disk_actor> {
    public:
        explicit leaf_disk_actor(std::pmr::memory_resource* resource)
            : basic_actor<leaf_disk_actor>(resource) {}

        unique_future<std::int64_t> fetch_inner(std::int64_t idx) {
            // Non-trivial leaf work: widen the park race window the way a real batch
            // materialization (column reads + chunk build) would. The point is that the
            // intermediate is suspended on co_await for a real interval before this lands.
            std::this_thread::yield();
            std::atomic<std::int64_t> spin{0};
            for (int k = 0; k < 64; ++k) {
                spin += k;
            }
            (void) spin;
            co_return idx;
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&leaf_disk_actor::fetch_inner>;

        behavior_t behavior(mailbox::message* msg) {
            switch (msg->command()) {
                case msg_id<leaf_disk_actor, &leaf_disk_actor::fetch_inner>:
                    co_await dispatch(this, &leaf_disk_actor::fetch_inner, msg);
                    break;
                default:
                    break;
            }
        }
    };

    // ------------------------------------------------------------------
    // INTERMEDIATE == manager_disk_t::storage_fetch_next_batch (manager_disk_storage.cpp).
    // Runs on the SAME scheduler as the consumer (scheduler_ in production). Its handler
    // is a TRANSPARENT ROUTER: it does its OWN cross-actor co_await to the leaf (on the
    // OTHER scheduler) and only then replies to the consumer:
    //     auto [ns, fut] = send(leaf, &leaf_disk_actor::fetch_inner, idx);
    //     if (ns) leaf_sched->enqueue(leaf);
    //     co_return co_await std::move(fut);          // <-- intermediate is suspended here
    // This is the structural element the simplified test lacks: at the instant the leaf
    // flips the reply flag, the INTERMEDIATE is itself mid-co_await, and the CONSUMER is
    // mid-co_await one level up — a flag-only completion that must propagate up a chain.
    // ------------------------------------------------------------------
    class relay_disk_actor final : public basic_actor<relay_disk_actor> {
    public:
        relay_disk_actor(std::pmr::memory_resource* resource,
                         address_t leaf_addr,
                         scheduler::sharing_scheduler* leaf_sched,
                         leaf_disk_actor* leaf_raw)
            : basic_actor<relay_disk_actor>(resource)
            , leaf_addr_(leaf_addr)
            , leaf_sched_(leaf_sched)
            , leaf_raw_(leaf_raw) {}

        // == storage_fetch_next_batch: route to the leaf, await its reply, forward it.
        unique_future<std::int64_t> fetch(std::int64_t idx) {
            auto [needs_sched, f] = send(leaf_addr_, &leaf_disk_actor::fetch_inner, idx);
            if (needs_sched) {
                leaf_sched_->enqueue(leaf_raw_); // leaf lives on the OTHER scheduler
            }
            // co_return co_await std::move(fut): the relay is suspended on a cross-actor
            // co_await across a scheduler boundary; the leaf's set_value() does not wake it.
            auto v = co_await std::move(f);
            co_return v;
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&relay_disk_actor::fetch>;

        behavior_t behavior(mailbox::message* msg) {
            switch (msg->command()) {
                case msg_id<relay_disk_actor, &relay_disk_actor::fetch>:
                    co_await dispatch(this, &relay_disk_actor::fetch, msg);
                    break;
                default:
                    break;
            }
        }

    private:
        address_t leaf_addr_;
        scheduler::sharing_scheduler* leaf_sched_;
        leaf_disk_actor* leaf_raw_;
    };

    // ------------------------------------------------------------------
    // SCAN OPERATOR == full_scan / transfer_scan: a SEPARATE object (NOT an actor) whose
    // source_next() is its OWN unique_future coroutine, co_awaited by the executor's
    // execute_pipeline. This is the DEEPER-NESTING element the simplified models lack:
    //
    //   behavior() -> run_query -> execute_plan -> execute_pipeline
    //                                                  co_await source->source_next(ctx)   <-- separate frame
    //                                                       co_await sf                    <-- sub-await #1
    //                                                       co_await emit_or_advance(...)  <-- sub-await #2 (awaits again)
    //
    // source_next does TWO SEQUENTIAL cross-actor sub-awaits before returning, looped once
    // per batch. The inter-SUB-await null-window lives in this DEEPEST frame: when the first
    // sub-await completes (clear_awaited_chain walks awaited_flags_=nullptr up through
    // execute_pipeline -> execute_plan -> run_query -> behavior via propagated_to_flags_)
    // and the second sub-await has not yet republished (propagate_awaited_state), the
    // TOP-LEVEL behavior's awaited_flags_ is transiently nullptr => is_busy()==false even
    // though the coroutine is mid-flight several frames down. The intermediate frames'
    // propagated pointers are momentarily stale precisely during this walk. This is the
    // suspect the consumer-awaits-relay-directly models do not reproduce: there the await is
    // ONE frame under execute_pipeline, not a separate source_next coroutine doing two
    // chained sub-awaits.
    // ------------------------------------------------------------------
    class scan_operator_t {
    public:
        scan_operator_t(std::pmr::memory_resource* resource,
                        address_t relay_addr,
                        scheduler::sharing_scheduler* exec_sched,
                        relay_disk_actor* relay_raw)
            : resource_(resource)
            , relay_addr_(relay_addr)
            , exec_sched_(exec_sched)
            , relay_raw_(relay_raw) {}

        // actor-zeta requires a unique_future coroutine's `this` to expose resource()
        // (the promise extracts its memory_resource from it, exactly as the real operators
        // — full_scan / transfer_scan — do for their source_next coroutine).
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // == the SECOND sub-await: emit_or_advance — its own unique_future coroutine that
        // itself performs a cross-actor await. In the real path this is the cursor advance /
        // boundary-batch fetch that source_next awaits AFTER the primary scan reply. It
        // contributes nothing to the row payload (returns 0); its sole purpose is to BE a
        // second sequential cross-actor await one frame deeper, after sub-await #1 cleared
        // the chain. A lost wakeup here stalls source_next forever (caught by the deadline).
        unique_future<std::int64_t> emit_or_advance(std::int64_t idx) {
            auto [needs_sched, f] = send(relay_addr_, &relay_disk_actor::fetch, 0);
            if (needs_sched) {
                exec_sched_->enqueue(relay_raw_);
            }
            auto advanced = co_await std::move(f); // deeper cross-actor await, 2-hop to leaf
            (void) advanced;
            (void) idx;
            co_return 0; // cursor advance: no row payload
        }

        // == source_next: a SEPARATE unique_future coroutine of the OPERATOR object,
        // co_awaited by execute_pipeline. Performs TWO sequential cross-actor sub-awaits:
        //   co_await sf                     (primary scan reply)
        //   co_await emit_or_advance(...)    (which awaits AGAIN, one frame deeper)
        // The inter-sub-await null-window between these two — walked up through every
        // intermediate unique_future frame — is the structural suspect.
        unique_future<std::int64_t> source_next(std::int64_t idx) {
            // sub-await #1: co_await sf (the storage_scan_batched / fetch-next primary reply).
            auto [needs_sched, sf] = send(relay_addr_, &relay_disk_actor::fetch, idx);
            if (needs_sched) {
                exec_sched_->enqueue(relay_raw_);
            }
            auto first = co_await std::move(sf); // <-- sub-await #1 completes -> clear_awaited_chain

            // INTER-SUB-AWAIT WINDOW: between sub-await #1 completing (top behavior's
            // awaited_flags_ walked to nullptr) and sub-await #2 republishing it, do a
            // touch of operator work (cursor bookkeeping) so the deepest frame genuinely
            // dwells with a stale propagated chain.
            std::atomic<std::int64_t> spin{0};
            for (int k = 0; k < 32; ++k) {
                spin += k;
            }
            (void) spin;

            // sub-await #2: co_await emit_or_advance(...) — awaits AGAIN, deeper still.
            // Both awaits must arrive for source_next to return; a lost wakeup on either
            // stalls this frame forever (surfaced by the per-query deadline). The batch
            // payload is the primary reply (first); the advance contributes 0.
            auto second = co_await emit_or_advance(idx);
            co_return first + second;
        }

    private:
        std::pmr::memory_resource* resource_;
        address_t relay_addr_;
        scheduler::sharing_scheduler* exec_sched_;
        relay_disk_actor* relay_raw_;
    };

    // ------------------------------------------------------------------
    // CONSUMER == executor_t: behavior() -> dispatch(run_query) -> execute_plan ->
    // execute_pipeline, whose per-batch loop does N sequential cross-actor awaits — each
    // now via co_await source->source_next(), a SEPARATE operator coroutine that itself
    // does TWO sequential 2-hop (relay -> leaf) cross-scheduler sub-awaits. The consumer
    // also does inter-batch WORK (mirrors pump_one pushing the batch through operators) so
    // the suspend/re-publish timing matches the real path rather than a tight await loop.
    // ------------------------------------------------------------------
    class query_executor_actor final : public basic_actor<query_executor_actor> {
    public:
        query_executor_actor(std::pmr::memory_resource* resource,
                             address_t relay_addr,
                             scheduler::sharing_scheduler* exec_sched,
                             relay_disk_actor* relay_raw,
                             std::int64_t batches)
            : basic_actor<query_executor_actor>(resource)
            , batches_(batches)
            , source_(resource, relay_addr, exec_sched, relay_raw) {}

        // == execute_pipeline: the per-batch streaming loop. ONE co_await source_next per
        // batch (== co_await source->source_next), each of which is a SEPARATE operator
        // coroutine doing TWO sequential 2-hop cross-scheduler sub-awaits. This is the real
        // nesting: execute_pipeline -> source_next -> {co_await sf, co_await emit_or_advance}.
        unique_future<std::int64_t> execute_pipeline() {
            std::int64_t sum = 0;
            for (std::int64_t i = 0; i < batches_; ++i) {
                // co_await source->source_next(ctx): the separate operator frame whose two
                // sequential sub-awaits force the deep clear_awaited_chain / propagate walk.
                sum += co_await source_.source_next(i);

                // Inter-batch work (== pump_one): a short burst between awaits so the
                // consumer occupies the inter-await null-window the way the real operator
                // push does, instead of immediately re-publishing the next await.
                std::atomic<std::int64_t> spin{0};
                for (int k = 0; k < 128; ++k) {
                    spin += k * i;
                }
                (void) spin;
            }
            co_return sum;
        }

        // == execute_sub_plan_ / execute_plan
        unique_future<std::int64_t> execute_plan() {
            auto v = co_await execute_pipeline();
            co_return v;
        }

        // == execute_plan_full. Publishes to result_; caller polls result_ready_.
        unique_future<void> run_query() {
            auto v = co_await execute_plan();
            result_.store(v, std::memory_order_release);
            result_ready_.store(true, std::memory_order_release);
            co_return;
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&query_executor_actor::run_query>;

        behavior_t behavior(mailbox::message* msg) {
            switch (msg->command()) {
                case msg_id<query_executor_actor, &query_executor_actor::run_query>:
                    co_await dispatch(this, &query_executor_actor::run_query, msg);
                    break;
                default:
                    break;
            }
        }

        bool result_ready() const { return result_ready_.load(std::memory_order_acquire); }
        std::int64_t result() const { return result_.load(std::memory_order_acquire); }
        void reset() {
            result_.store(0, std::memory_order_release);
            result_ready_.store(false, std::memory_order_release);
        }

    private:
        std::int64_t batches_;
        scan_operator_t source_;
        std::atomic<std::int64_t> result_{0};
        std::atomic<bool> result_ready_{false};
    };

    constexpr std::int64_t expected_sum(std::int64_t n) { return (n * (n - 1)) / 2; }

} // namespace

// One query == ONE top-level behavior() dispatch performing N sequential per-batch
// awaits, each resolved by a 2-hop cross-scheduler reply (relay -> leaf). Run MANY such
// queries back to back, each waited on with a HARD per-query deadline so a lost-wakeup
// surfaces as a FAILED query (deadline exceeded) rather than an infinite hang.
TEST_CASE("real-path fetch-next: N per-batch awaits, each a 2-hop cross-scheduler reply through a "
          "suspended intermediate, must not lose a wakeup",
          "[.][lostwakeup-realpath]") {
    auto* resource = std::pmr::get_default_resource();

    // TWO schedulers, mirroring base_spaces.cpp: scheduler_ (executor + relay/manager_disk)
    // and scheduler_disk_ (leaf/agent_disk). 3 workers each, like production
    // (actor_zeta::shared_work(3, 1000)), so the producer-completes-while-consumer-parks
    // race across the scheduler boundary can actually occur.
    auto exec_scheduler = std::make_unique<scheduler::sharing_scheduler>(3, 100);
    auto disk_scheduler = std::make_unique<scheduler::sharing_scheduler>(3, 100);
    exec_scheduler->start();
    disk_scheduler->start();

    auto leaf = spawn<leaf_disk_actor>(resource);
    auto relay = spawn<relay_disk_actor>(resource, leaf->address(), disk_scheduler.get(), leaf.get());

    constexpr std::int64_t kBatchesPerQuery = 64; // 64 sequential 2-hop awaits per query
    constexpr int kQueries = 3000;                // hammer the chained-park race
    const auto kPerQueryTimeout = std::chrono::seconds(5);

    auto executor =
        spawn<query_executor_actor>(resource, relay->address(), exec_scheduler.get(), relay.get(), kBatchesPerQuery);

    int completed = 0;
    bool hung = false;
    int hung_query = -1;

    for (int q = 0; q < kQueries; ++q) {
        executor->reset();

        auto [needs_sched, fut] = send(executor.get(), &query_executor_actor::run_query);
        (void) fut; // run_query returns void; observed via result_ready_
        if (needs_sched) {
            exec_scheduler->enqueue(executor.get());
        }

        const auto deadline = std::chrono::steady_clock::now() + kPerQueryTimeout;
        while (!executor->result_ready()) {
            if (std::chrono::steady_clock::now() > deadline) {
                hung = true;
                hung_query = q;
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        if (hung) {
            WARN("LOST-WAKEUP (real path): query "
                 << q << " of " << kQueries
                 << " stalled after a 2-hop cross-scheduler reply through a suspended "
                    "intermediate (relay/manager_disk) — a chained flag-only completion "
                    "was lost; no watchdog poke here to mask it");
            break;
        }

        REQUIRE(executor->result() == expected_sum(kBatchesPerQuery));
        ++completed;
    }

    exec_scheduler->stop();
    disk_scheduler->stop();

    INFO("completed " << completed << " / " << kQueries << " queries, each with " << kBatchesPerQuery
                      << " sequential 2-hop cross-scheduler awaits"
                      << (hung ? (std::string(" — HUNG at query ") + std::to_string(hung_query)) : std::string{}));

    // Load-bearing assertion: NO query stalled across N sequential per-batch awaits, each a
    // separate source_next operator coroutine doing TWO sequential 2-hop cross-scheduler
    // sub-awaits. GREEN on the option-2-fixed actor-zeta (the chained-wakeup invariant
    // documented at the top of this file holds here); a violation of that invariant — e.g.
    // when the dormant per-batch fetch-next source is revived, or if the option-2 guard
    // regresses — surfaces as `hung` (a query that exceeded the hard per-query deadline)
    // rather than an infinite wait, so this assertion fails fast and CI never wedges.
    REQUIRE_FALSE(hung);
    REQUIRE(completed == kQueries);
}
