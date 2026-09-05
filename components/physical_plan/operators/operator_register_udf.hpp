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
    // Steps performed by the operator. EVERY STEP THAT CAN REFUSE COMES BEFORE THE
    // ONE STEP THAT MUTATES, so a refusal leaves the process exactly as it found it:
    //   1. resolve_function_by_name across all namespaces (cross-namespace
    //      conflict detection — refuse with already_exists on any match, and
    //      refuse with the read's own error if the catalog could not be read).
    //   2. validate the pre-collected per-executor uids: every executor must
    //      have agreed on a single, non-invalid uid (the "all executors agree"
    //      invariant). The dispatcher is responsible for dropping any executor
    //      that returned an error before building the vector — an empty vector
    //      means "no executors / nothing to mirror by uid".
    //   3. allocate the ONE OID the pg_proc row will carry, and refuse the
    //      statement if the round did not deliver it.
    //   4. resolve the target namespace (list_namespaces + resolve_namespace)
    //      and write the pg_proc + pg_depend rows — the function's durable
    //      identity, which pg_depend and every later catalog lookup key on.
    //   5. mirror the function into function_registry_t::get_default() so
    //      validate_logical_plan lookups (which probe the default registry)
    //      can find it, reusing the agreed LOCAL uid so the global counter and
    //      the per-executor counters never diverge.
    //
    // Step 5 is the operator's ONLY mutation and it is LAST on purpose. It used to
    // sit ahead of the namespace resolution in step 4, and a catalog read can
    // refuse (scan_table answers a result_wrapper_t), so an unreadable
    // pg_namespace left the process-global registry answering for a function the
    // catalog has no row for: present for every plan-validation lookup in this
    // process, absent from every durable record of what exists.
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

        // Sourceless SINK leaf (no data pipeline, no children): all work — the
        // cross-namespace conflict read, the default-registry mirror and the
        // pg_proc/pg_depend writes — runs in await_async_and_resume. The dispatcher
        // drives this operator's async finalize directly (a single
        // await_async_and_resume).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::compute::function_ptr function_;
        executor_uids_t executor_uids_;
        bool success_{false};
    };

} // namespace components::operators
