// Deterministic repro for the actor-zeta "lost-wakeup" question that gates the
// STEP-3 fetch-next streaming source (Task #1 / Task #6).
//
// QUESTION UNDER TEST
// -------------------
// The streaming-executor plan claims N *sequential* cross-actor co_awaits are SAFE
// when they live inside the executor's NESTED unique_future coroutines
//   behavior()  ->  dispatch()  ->  outer unique_future  ->  inner unique_future
//                                   (execute_plan*)         (execute_pipeline)
// and NOT inside a fresh top-level behavior() handler per await. The per-batch
// fetch-next source does exactly this: execute_pipeline loops
//      while (true) { auto b = co_await source->source_next(ctx); ... }
// so one scan turns into MANY sequential cross-actor awaits, all driven from the
// SAME inner coroutine frame, under ONE dispatched behavior() handler.
//
// This test reproduces that exact coroutine nesting against the REAL threaded
// sharing_scheduler (the same scheduler otterbrix runs executors on), because the
// lost-wakeup is a producer-completes-future-while-consumer-parks race that ONLY
// manifests under the threaded scheduler (the producer's promise.set_value does
// NOT poke the consumer — see actor-zeta future.hpp set_value "NO cont.resume()").
//
// HIDDEN (manual) — tag [.] so it never runs in CI; run explicitly with the tag.
//   test_otterbrix "[lostwakeup]"
//
// PASS  => per-batch fetch-next (N sequential cross-actor awaits in the nested
//          inner coroutine) is safe as-is on the linked actor-zeta; the earlier
//          flap was caused by something else (e.g. an await placed in the wrong
//          frame, or a per-batch re-entry of a top-level handler).
// HANG  => (caught by the watchdog timeout below, reported as a FAILED REQUIRE)
//          the construct genuinely flaps and needs the framework/structure fix.

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
    // "disk service" stand-in: the cross-actor producer. Each call to
    // fetch_batch() is ONE cross-actor round-trip, exactly like a per-batch
    // source_next() sending storage_scan_batched to manager_disk_t.
    // ------------------------------------------------------------------
    class batch_source_actor final : public basic_actor<batch_source_actor> {
    public:
        explicit batch_source_actor(std::pmr::memory_resource* resource)
            : basic_actor<batch_source_actor>(resource) {}

        // Returns the value for batch #idx (trivial work — the point is the
        // cross-actor await, not the payload).
        unique_future<std::int64_t> fetch_batch(std::int64_t idx) {
            // A tiny yield widens the suspend/park race window (mirrors the
            // upstream race tests). Not required for correctness.
            std::this_thread::yield();
            co_return idx;
        }

        using dispatch_traits = actor_zeta::dispatch_traits<&batch_source_actor::fetch_batch>;

        behavior_t behavior(mailbox::message* msg) {
            switch (msg->command()) {
                case msg_id<batch_source_actor, &batch_source_actor::fetch_batch>:
                    co_await dispatch(this, &batch_source_actor::fetch_batch, msg);
                    break;
                default:
                    break;
            }
        }
    };

    // ------------------------------------------------------------------
    // "executor" stand-in. Reproduces the EXACT nesting depth otterbrix uses:
    //
    //   behavior()                              <- ONE top-level dispatch / message
    //     -> dispatch(run_query)
    //       -> run_query()        (outer #1, ~ execute_plan_full)
    //         -> execute_plan()   (outer #2, ~ execute_plan / execute_sub_plan_)
    //           -> execute_pipeline()  (inner, the per-batch loop)
    //               for i in 0..N: co_await source.fetch_batch(i)   <- N sequential
    //                                                                   cross-actor awaits
    //
    // The N awaits all happen inside ONE inner unique_future frame, under ONE
    // behavior() handler — no per-batch top-level re-entry. This is the precise
    // shape the plan claims is safe and the shape the flap was attributed to.
    // ------------------------------------------------------------------
    class query_executor_actor final : public basic_actor<query_executor_actor> {
    public:
        query_executor_actor(std::pmr::memory_resource* resource,
                             address_t source_addr,
                             scheduler::sharing_scheduler* sched,
                             batch_source_actor* source_raw,
                             std::int64_t batches)
            : basic_actor<query_executor_actor>(resource)
            , source_addr_(source_addr)
            , sched_(sched)
            , source_raw_(source_raw)
            , batches_(batches) {}

        // Innermost coroutine: the per-batch streaming loop (== execute_pipeline).
        unique_future<std::int64_t> execute_pipeline() {
            std::int64_t sum = 0;
            for (std::int64_t i = 0; i < batches_; ++i) {
                // ONE cross-actor await per batch (== co_await source->source_next).
                auto [needs_sched, f] = send(source_addr_, &batch_source_actor::fetch_batch, i);
                // The producer must be scheduled to run on a worker thread; this is
                // the exact send()/enqueue handshake otterbrix and the upstream race
                // tests use. (needs_sched can be false if the producer was already
                // scheduled — then it is already on a worker queue.)
                if (needs_sched) {
                    sched_->enqueue(source_raw_);
                }
                sum += co_await std::move(f); // <-- the Nth sequential cross-actor await
            }
            co_return sum;
        }

        // outer #2 (== execute_plan / execute_sub_plan_)
        unique_future<std::int64_t> execute_plan() {
            auto v = co_await execute_pipeline();
            co_return v;
        }

        // outer #1 (== execute_plan_full). Result is published to result_
        // and the caller polls result_ready_ (no cross-actor reply needed).
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
        address_t source_addr_;
        scheduler::sharing_scheduler* sched_;
        batch_source_actor* source_raw_;
        std::int64_t batches_;
        std::atomic<std::int64_t> result_{0};
        std::atomic<bool> result_ready_{false};
    };

    // Expected sum of fetch_batch(0..N-1) == 0+1+...+(N-1).
    constexpr std::int64_t expected_sum(std::int64_t n) { return (n * (n - 1)) / 2; }

} // namespace

// One query = ONE top-level behavior() dispatch that internally performs N
// sequential cross-actor awaits in the nested inner coroutine. Run MANY such
// queries back to back, each waited on with a hard timeout: a lost-wakeup shows
// up as a query whose result never becomes ready (the executor parks busy &&
// ready and is never poked — exactly the watchdog symptom).
TEST_CASE("per-batch fetch-next: N sequential cross-actor awaits in nested executor coroutine do not lose a wakeup",
          "[.][lostwakeup]") {
    auto* resource = std::pmr::get_default_resource();

    // 4 worker threads — same as the lost-wakeup repro / upstream race tests, so
    // producer-completes-while-consumer-parks can actually race.
    auto scheduler = std::make_unique<scheduler::sharing_scheduler>(4, 100);
    scheduler->start();

    auto source = spawn<batch_source_actor>(resource);

    constexpr std::int64_t kBatchesPerQuery = 64; // 64 sequential cross-actor awaits per query
    constexpr int kQueries = 3000;                // hammer the park race many times
    const auto kPerQueryTimeout = std::chrono::seconds(5);

    auto executor =
        spawn<query_executor_actor>(resource, source->address(), scheduler.get(), source.get(), kBatchesPerQuery);

    int completed = 0;
    bool hung = false;

    for (int q = 0; q < kQueries; ++q) {
        executor->reset();

        auto [needs_sched, fut] = send(executor.get(), &query_executor_actor::run_query);
        (void) fut; // run_query returns void; result is observed via result_ready_
        if (needs_sched) {
            scheduler->enqueue(executor.get());
        }

        const auto deadline = std::chrono::steady_clock::now() + kPerQueryTimeout;
        while (!executor->result_ready()) {
            if (std::chrono::steady_clock::now() > deadline) {
                hung = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }

        if (hung) {
            WARN("LOST-WAKEUP: query " << q << " of " << kQueries << " hung after " << kBatchesPerQuery
                                       << " sequential cross-actor awaits in the nested executor coroutine "
                                          "(executor parked busy && ready, never poked)");
            break;
        }

        REQUIRE(executor->result() == expected_sum(kBatchesPerQuery));
        ++completed;
    }

    scheduler->stop();

    INFO("completed " << completed << " / " << kQueries << " queries, each with " << kBatchesPerQuery
                      << " sequential cross-actor awaits");

    // The load-bearing assertion: NO query hung. If this fails, per-batch
    // fetch-next is NOT safe as-is and a framework/structure fix is required
    // before the source can be converted (Task #6).
    REQUIRE_FALSE(hung);
    REQUIRE(completed == kQueries);
}
