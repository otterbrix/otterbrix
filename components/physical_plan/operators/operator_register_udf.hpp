#pragma once

#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Operator implementation of manager_dispatcher_t::register_udf.
    //
    // The executor fan-out is NOT performed here. The dispatcher (which owns the
    // executor addresses + scheduler and is the only place that can honour
    // needs_sched on a send) issues the per-executor register_udf sends itself,
    // co_awaits each unique_future, collects the resulting function_uid values,
    // and hands them to this operator as a plain, pre-collected vector. That
    // keeps every callable / type-erased indirection (std::function) and every
    // shared owner (std::shared_ptr) out of the operator.
    //
    // Steps performed by the operator:
    //   1. resolve_function_by_name across all namespaces (cross-namespace
    //      conflict detection — bail with success_=false if any match).
    //   2. validate the pre-collected per-executor uids: every executor must
    //      have agreed on a single, non-invalid uid (the "all executors agree"
    //      invariant). The dispatcher is responsible for dropping any executor
    //      that returned an error before building the vector — an empty vector
    //      means "no executors / nothing to mirror by uid".
    //   3. mirror the function into function_registry_t::get_default() so
    //      validate_logical_plan lookups (which probe the default registry)
    //      can find it, reusing the agreed LOCAL uid so the global counter and
    //      the per-executor counters never diverge.
    //   4. allocate one OID + write pg_proc + pg_depend rows so the function
    //      survives restart (the registry is hydrated from pg_proc at startup).
    //
    // The function payload is owned here as the canonical function_ptr (unique):
    // the operator deep-copies it via get_copy() for the default-registry mirror
    // and reads name()/get_signatures() for the pg_proc encode step.
    class operator_register_udf_t final : public read_only_operator_t {
    public:
        // Pre-collected per-executor registration uids gathered by the dispatcher.
        // One non-invalid, mutually-equal uid per executor on success; an empty
        // vector when there are no executors to mirror by uid.
        using executor_uids_t = std::pmr::vector<components::compute::function_uid>;

        operator_register_udf_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::compute::function_ptr function,
                                executor_uids_t executor_uids);

        // True iff the registration succeeded across every executor and the
        // pg_proc/pg_depend rows were appended. Caller (dispatcher) reads this
        // to fulfil the bool unique_future<> the public API exposes.
        bool success() const noexcept { return success_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::compute::function_ptr function_;
        executor_uids_t executor_uids_;
        bool success_{false};
    };

} // namespace components::operators
