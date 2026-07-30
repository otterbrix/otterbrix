// Delivery-status contract of manager_index_t's mailbox.
//
// manager_index_t does not use actor-zeta's default mailbox: it owns an event-loop thread and a
// lock-free inbox_, and enqueue_impl() is the seam every send() goes through. actor_zeta::send()
// builds {message, future} and hands the message to enqueue_impl; the message destructor is what
// cancels the future (cleanup_fn_ -> operation_canceled on the slot). So a message that is taken
// but never processed, and reported as `success`, leaves the sender awaiting a reply forever.
//
// loop_thread_ is the ONLY consumer of inbox_. Once it has exited, that is exactly the state.

#include <catch2/catch_test_macros.hpp>

#include <components/log/log.hpp>
#include <components/tests/temp_dir.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/pmr.hpp>
#include <services/index/manager_index.hpp>

#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <actor-zeta/mailbox/make_message.hpp>

using services::index::manager_index_t;

namespace {

    struct manager_index_fixture_t {
        manager_index_fixture_t()
            : log_(initialization_logger("manager_index_enqueue", test_temp_path("manager_index_enqueue/logs")))
            , scheduler_(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , manager_(actor_zeta::spawn<manager_index_t>(&resource_,
                                                          scheduler_,
                                                          log_,
                                                          test_temp_path("manager_index_enqueue/db"))) {}

        ~manager_index_fixture_t() {
            manager_.reset();
            scheduler_->stop();
            delete scheduler_;
        }

        std::pmr::memory_resource& resource() { return resource_; }

        core::pmr::otterbrix_resource resource_;
        log_t log_;
        core::non_thread_scheduler::scheduler_test_t* scheduler_{nullptr};
        services::index::manager_index_ptr manager_;
    };

} // namespace

TEST_CASE_METHOD(manager_index_fixture_t, "services::index::manager_index::enqueue reports a closed loop") {
    // The loop thread is the only consumer. With it gone, nothing in inbox_ will ever run.
    manager_->stop_loop();

    auto [msg, future] = actor_zeta::detail::make_message<void>(
        &resource(),
        actor_zeta::mailbox::make_message_id(
            actor_zeta::msg_id<manager_index_t, &manager_index_t::mark_table_dropped>),
        components::session::session_id_t{},
        components::catalog::oid_t{42},
        uint64_t{1});

    auto [needs_schedule, delivery] = manager_->enqueue_impl(std::move(msg));

    REQUIRE_FALSE(needs_schedule);
    // RED before the fix: enqueue_impl released the message into inbox_ and returned `success`
    // unconditionally, so a caller could not tell an accepted message from a dropped one.
    REQUIRE(delivery == actor_zeta::detail::enqueue_result::queue_closed);
    // And the refusal must have destroyed the message, which is what cancels the slot. Without
    // that the future stays neither ready nor failed — the shape of an await that never returns.
    REQUIRE(future.failed());
    REQUIRE(future.error() == std::make_error_code(std::errc::operation_canceled));
}

TEST_CASE_METHOD(manager_index_fixture_t, "services::index::manager_index::enqueue accepts while the loop runs") {
    // Same call on a live manager still reports success — the refusal above is about the closed
    // loop, not about this message being unacceptable.
    auto [msg, future] = actor_zeta::detail::make_message<void>(
        &resource(),
        actor_zeta::mailbox::make_message_id(
            actor_zeta::msg_id<manager_index_t, &manager_index_t::mark_table_dropped>),
        components::session::session_id_t{},
        components::catalog::oid_t{42},
        uint64_t{1});

    auto [needs_schedule, delivery] = manager_->enqueue_impl(std::move(msg));

    REQUIRE_FALSE(needs_schedule);
    REQUIRE(delivery == actor_zeta::detail::enqueue_result::success);
    future.detach();
}
