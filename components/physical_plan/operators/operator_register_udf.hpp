#pragma once

#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <actor-zeta/detail/future.hpp>

#include <functional>
#include <memory>
#include <vector>

namespace components::operators {

    // Operator implementation of manager_dispatcher_t::register_udf.
    //
    // Steps:
    //   1. resolve_function_by_name across all namespaces (cross-namespace
    //      conflict detection — bail with success_=false if any match).
    //   2. fan out the function to every executor's local function_registry_
    //      via the injected `executor_register_fn`, collect uids and verify
    //      all agree on a single non-invalid uid.
    //   3. mirror the function into function_registry_t::get_default() so
    //      validate_logical_plan lookups (which probe the default registry)
    //      can find it.
    //   4. allocate one OID + write pg_proc + pg_depend rows so the function
    //      survives restart (the registry is hydrated from pg_proc at startup).
    //
    // The function_ptr is held as std::shared_ptr (via the logical node) so the
    // operator can fan out copies via get_copy() without consuming the payload.
    //
    // The executor fan-out is injected as a callable rather than a list of
    // addresses because the dispatcher needs to enqueue the executor actor on
    // the scheduler whenever a send returns needs_sched=true — and that
    // scheduler/executor handle is not available inside the operator. Each
    // call returns one unique_future<function_uid>; the operator co_awaits
    // the futures in order to preserve the "all executors agree" invariant.
    class operator_register_udf_t final : public read_only_operator_t {
    public:
        // Number of executor fan-out calls = executor_count. The functor is
        // invoked with (session, function_copy, executor_index); it must be
        // safe to invoke up to `executor_count` times.
        using executor_register_fn_t = std::function<actor_zeta::unique_future<components::compute::function_uid>(
            components::session::session_id_t session,
            components::compute::function_ptr function_copy,
            std::size_t executor_index)>;

        operator_register_udf_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 std::shared_ptr<components::compute::function> function,
                                 std::size_t executor_count,
                                 executor_register_fn_t executor_register_fn);

        // True iff the registration succeeded across every executor and the
        // pg_proc/pg_depend rows were appended. Caller (dispatcher) reads this
        // to fulfil the bool unique_future<> the public API exposes.
        bool success() const noexcept { return success_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::shared_ptr<components::compute::function> function_;
        std::size_t executor_count_;
        executor_register_fn_t executor_register_fn_;
        bool success_{false};
    };

} // namespace components::operators
