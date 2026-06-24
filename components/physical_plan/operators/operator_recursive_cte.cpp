#include "operator_recursive_cte.hpp"
#include "operator_data.hpp"

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::operators {

    // Upper bound on fixpoint iterations: a clean error instead of an unbounded hang if
    // a recursive term keeps producing rows (cyclic graph data with no terminating filter).
    // Generous so legitimate deep hierarchies are never clipped.
    static constexpr std::size_t kMaxRecursionDepth = 10000;

    operator_recursive_cte_t::operator_recursive_cte_t(std::pmr::memory_resource* resource, log_t log, bool all)
        : read_only_operator_t(resource, std::move(log), operator_type::recursive_cte)
        , all_(all) {}

    void operator_recursive_cte_t::reset_recursive_subtree(const operator_ptr& op) {
        if (!op) {
            return;
        }
        // Generic reuse reset (state_ -> created, output_ -> null) PLUS the streaming
        // bookkeeping rewind (scan re-OPEN, cte_scan re-walk, hash-join index rebuild).
        // prepared_ is left set, so the cte_scan's captured working_set_ pointer survives
        // (no re-create, no dangle).
        op->reset_for_reuse();
        op->reset_pipeline_state();
        reset_recursive_subtree(op->left());
        reset_recursive_subtree(op->right());
    }

    void operator_recursive_cte_t::on_execute_impl(pipeline::context_t* /*context*/) {
        // Defer the fixpoint to await_async_and_resume: it co_awaits run_subplan, which a
        // synchronous on_execute_impl cannot do. Arm async_wait so the materialized drive's
        // find_waiting_operator loop dispatches await_async_and_resume (mirrors a DML sink).
        async_wait();
    }

    actor_zeta::unique_future<void> operator_recursive_cte_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto err = co_await drive_fixpoint_(ctx);
        if (err.contains_error()) {
            set_error(err);
            mark_failed();
            co_return;
        }
        mark_executed();
        co_return;
    }

    actor_zeta::unique_future<core::error_t>
    operator_recursive_cte_t::drive_fixpoint_(pipeline::context_t* ctx) {
        auto* res = resource_;

        if (!anchor_) {
            co_return core::error_t::no_error();
        }
        if (!ctx->runner) {
            co_return core::error_t{core::error_code_t::physical_plan_error,
                                    std::pmr::string{"recursive_cte: no sub-plan runner", res}};
        }

        chunks_vector_t result(res);

        // 1. Anchor (the non-recursive UNION-ALL term). Seed both `result` and the
        //    working set with its rows. run_subplan returns COPIES, so they are ours to own.
        {
            auto anchor_res = co_await ctx->runner->run_subplan(anchor_, ctx);
            if (anchor_res.has_error()) {
                co_return anchor_res.error();
            }
            chunks_vector_t seed(res);
            for (auto& chunk : anchor_res.value()) {
                if (chunk.size() > 0) {
                    result.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                    seed.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                }
            }
            working_set_ = make_operator_data(res, std::move(seed));
        }

        // 2. Fixpoint: each pass re-runs the recursive term over the CURRENT working set
        //    (the rows the previous pass produced), appends its output to `result` (UNION
        //    ALL — no dedup), and repoints the working set to that fresh output. Terminate
        //    when a pass yields no rows. The recursion-depth guard prevents an unbounded
        //    hang on cyclic data.
        if (recursive_) {
            std::size_t depth = 0;
            while (working_set_ && working_set_->size() > 0) {
                if (++depth > kMaxRecursionDepth) {
                    co_return core::error_t{core::error_code_t::physical_plan_error,
                                            std::pmr::string{"recursive_cte: maximum recursion depth exceeded", res}};
                }

                // Fresh run of the recursive-term sub-plan over the repointed working set.
                reset_recursive_subtree(recursive_);
                auto step = co_await ctx->runner->run_subplan(recursive_, ctx);
                if (step.has_error()) {
                    co_return step.error();
                }

                chunks_vector_t next(res);
                for (auto& chunk : step.value()) {
                    if (chunk.size() > 0) {
                        result.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                        next.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                    }
                }
                if (next.empty()) {
                    break;
                }
                working_set_ = make_operator_data(res, std::move(next));
            }
        }

        // Preserve the legacy empty-result schema guard: an empty CTE still emits one
        // schema'd 0-row chunk (so a downstream projection/aggregate sees the columns).
        if (result.empty() && anchor_->output() && !anchor_->output()->chunks().empty()) {
            result.emplace_back(res, anchor_->output()->chunks().front().types(), 0);
        }

        output_ = make_operator_data(res, std::move(result));
        co_return core::error_t::no_error();
    }

} // namespace components::operators
