#pragma once

#include <boost/intrusive_ptr.hpp>

#include <actor-zeta/detail/future.hpp>
#include <core/result_wrapper.hpp>

// Includes operator_data.hpp, not operator.hpp (which includes context.hpp and would close a
// cycle); operator_t is forward-declared instead and its pointer spelled boost::intrusive_ptr<operator_t>.
#include <components/physical_plan/operators/operator_data.hpp>

#include <cstdint>
#include <string>

namespace components::operators {
    class operator_t;
} // namespace components::operators

namespace components::pipeline {

    class context_t;

    // What FETCH read from a portal: its rows; for MOVE no rows, and how many it passed.
    struct portal_rows_t {
        components::operators::chunks_vector_t chunks;
        uint64_t rows{0};
    };

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

        // Runs `root` whole as a piece of the calling statement; caller owns `ctx` (built for this sub-plan).
        [[nodiscard]] virtual actor_zeta::unique_future<core::result_wrapper_t<components::operators::chunks_vector_t>>
        run_subplan(boost::intrusive_ptr<components::operators::operator_t> root, context_t* ctx) = 0;

        // FETCH / MOVE of a portal of this executor, named by `portal_id` (0 when the session has none of the
        // cursor's `name`, which only the errors print).
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<portal_rows_t>>
        fetch_portal(context_t* ctx, uint64_t portal_id, std::string name, uint64_t count, bool is_move) {
            return fetch_portal_impl(ctx, portal_id, std::move(name), count, is_move);
        }
        // CLOSE of a portal of this executor; CLOSE ALL (no id, no name) closes every portal of the session here.
        [[nodiscard]] actor_zeta::unique_future<core::error_t>
        close_portal(context_t* ctx, uint64_t portal_id, std::string name) {
            return close_portal_impl(ctx, portal_id, std::move(name));
        }

    private:
        [[nodiscard]] virtual actor_zeta::unique_future<core::result_wrapper_t<portal_rows_t>>
        fetch_portal_impl(context_t* ctx, uint64_t portal_id, std::string name, uint64_t count, bool is_move) = 0;
        [[nodiscard]] virtual actor_zeta::unique_future<core::error_t>
        close_portal_impl(context_t* ctx, uint64_t portal_id, std::string name) = 0;
    };

} // namespace components::pipeline
