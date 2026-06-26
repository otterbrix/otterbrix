#pragma once

#include <boost/intrusive_ptr.hpp>

#include <actor-zeta/detail/future.hpp>
#include <core/result_wrapper.hpp>

// chunks_vector_t (the streaming pipeline's batch container). operator_data.hpp
// is a LOW-LEVEL header that does NOT pull in context.hpp, so including it here
// keeps subplan_runner.hpp cycle-free even though context.hpp forward-declares
// (and never includes) this file. operator_ptr itself is NOT taken from
// operator.hpp (which DOES include context.hpp -> would close the cycle):
// operator_t is forward-declared and the pointer spelled as
// boost::intrusive_ptr<operator_t> (== operator_t::ptr == operator_ptr).
#include <components/physical_plan/operators/operator_data.hpp>

namespace components::operators {
    class operator_t;
} // namespace components::operators

namespace components::pipeline {

    class context_t;

    // INTRA-actor sub-plan execution seam. The executor_t actor owns and runs its
    // operators synchronously inside its own coroutine; an operator is NOT an actor
    // (operator_t is a boost::intrusive_ref_counter, not a basic_actor), so calling
    // back into the owning executor through this interface pointer is an intra-actor
    // call, not cross-actor shared state (rules 10/11/13 are about ACTOR<->ACTOR).
    //
    // Rule-14 clean: an abstract virtual interface, NOT a std::function. The executor
    // implements it (services/collection/executor.hpp) and publishes itself onto
    // pipeline::context_t::runner before driving a plan; an operator that needs to run
    // a child sub-plan reads ctx->runner->run_subplan(...).
    struct subplan_runner_t {
        subplan_runner_t() = default;
        subplan_runner_t(const subplan_runner_t&) = delete;
        subplan_runner_t& operator=(const subplan_runner_t&) = delete;
        subplan_runner_t(subplan_runner_t&&) = delete;
        subplan_runner_t& operator=(subplan_runner_t&&) = delete;
        virtual ~subplan_runner_t() = default;

        // Run a prepared sub-plan root to completion through the streaming executor
        // (the same routing seam execute_sub_plan_ uses: streaming pipeline vs.
        // materialized on_execute + async-await loop) and return its output chunks.
        // The caller owns `ctx` (a pipeline::context_t built for this sub-plan).
        [[nodiscard]] virtual actor_zeta::unique_future<
            core::result_wrapper_t<components::operators::chunks_vector_t>>
        run_subplan(boost::intrusive_ptr<components::operators::operator_t> root, context_t* ctx) = 0;
    };

} // namespace components::pipeline
