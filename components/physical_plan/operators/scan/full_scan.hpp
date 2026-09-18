#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/storage/storage.hpp>
#include <components/table/column_state.hpp>
#include <core/result_wrapper.hpp>

namespace components::operators {

    // Compiled eagerly here so the filter carries its execution graph across the mailbox.
    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters,
                        const components::graph_execution_context& context);

    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::limit_t limit,
                  std::vector<size_t> projected_cols = {});

        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::limit_t& limit() const { return limit_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }

        // Safe to await here sequentially only because this coroutine is nested, not a behavior() handler.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Re-OPENs the scan from the head per recursive-CTE re-run; reset_for_reuse() covers state_/output_.
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            guard_types_.clear();
        }

        // An un-released cursor permanently gates compact() on its table; nothing agent-side reclaims it otherwise.
        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return cursor_id_ != 0 && !drained_; }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        // Empty but schema'd, so a downstream OUTER join can NULL-pad and a scalar aggregate can still emit COUNT=0.
        vector::data_chunk_t make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types);

        // OFFSET is applied by operator_limit above the scan, so this never needs to skip rows itself.
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;

        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
    };

} // namespace components::operators
