#pragma once

#include <boost/intrusive_ptr.hpp>

#include <actor-zeta/detail/future.hpp>
#include <core/result_wrapper.hpp>

// Includes operator_data.hpp, not operator.hpp (which includes context.hpp and would close a
// cycle); operator_t is forward-declared instead and its pointer spelled boost::intrusive_ptr<operator_t>.
#include <components/physical_plan/operators/operator_data.hpp>

namespace components::operators {
    class operator_t;
} // namespace components::operators

namespace components::pipeline {

    class context_t;

    // Intra-actor seam, not mailbox state: operator_t is a boost::intrusive_ref_counter, not a
    // basic_actor, so calling back through this pointer is a plain call, not actor<->actor sharing.
    // Rule-14: an abstract interface, not std::function; the executor (services/collection/executor.hpp)
    // publishes itself onto context_t::runner, so an operator calls ctx->runner->run_subplan(...).
    struct subplan_runner_t {
        subplan_runner_t() = default;
        subplan_runner_t(const subplan_runner_t&) = delete;
        subplan_runner_t& operator=(const subplan_runner_t&) = delete;
        subplan_runner_t(subplan_runner_t&&) = delete;
        subplan_runner_t& operator=(subplan_runner_t&&) = delete;
        virtual ~subplan_runner_t() = default;

        // Same routing seam as execute_sub_plan_; caller owns `ctx` (built for this sub-plan).
        [[nodiscard]] virtual actor_zeta::unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
        run_subplan(boost::intrusive_ptr<components::operators::operator_t> root, context_t* ctx) = 0;
    };

} // namespace components::pipeline
