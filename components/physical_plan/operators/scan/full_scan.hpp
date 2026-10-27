#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/storage/storage.hpp>
#include <components/table/column_state.hpp>
#include <components/table/pushed_filter.hpp>
#include <components/table/storage_address.hpp>
#include <core/result_wrapper.hpp>

#include <services/disk/storage_contract.hpp>

namespace components::operators {

    // The predicate as it travels to the disk agent, copied onto `target` (the agent side's resource);
    // the agent compiles it. nullptr when there is nothing to filter.
    core::result_wrapper_t<std::unique_ptr<table::pushed_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        std::pmr::memory_resource* target,
                        const expressions::compare_expression_ptr& expression,
                        const logical_plan::storage_parameters* parameters,
                        const components::graph_execution_context& context);

    // A scan of one table at its storage. `expression` is a predicate the disk evaluates while it reads (only a
    // table of otterbrix's own disk gets one); the scan of any other storage reads every row and is filtered above.
    class full_scan final : public read_only_operator_t {
    public:
        full_scan(std::pmr::memory_resource* resource,
                  log_t log,
                  components::catalog::oid_t table_oid,
                  table::storage_address_t address,
                  std::pmr::string name,
                  const expressions::compare_expression_ptr& expression,
                  logical_plan::row_cap_t cap,
                  std::vector<size_t> projected_cols = {});

        const expressions::compare_expression_ptr& expression() const { return expression_; }
        const logical_plan::row_cap_t& cap() const { return cap_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }
        // The types of the columns the plan reads, from the catalog: every page is checked against them, and an
        // answer of no row has them.
        void set_page_types(std::pmr::vector<types::complex_logical_type> types) { page_types_ = std::move(types); }
        const table::storage_address_t& address() const noexcept { return address_; }

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t* ctx) override;

        // Re-OPENs the scan from the head per recursive-CTE re-run; reset_for_reuse() covers state_/output_.
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            first_batch_.reset();
        }

        // An un-released cursor permanently gates compact() on its table; nothing agent-side reclaims it otherwise.
        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return cursor_id_ != 0 && !drained_; }

    private:
        // Opens the cursor as soon as the executor opens the plan, so the scans of a plan read at the same time.
        actor_zeta::unique_future<core::error_t> open_impl(pipeline::context_t* ctx) override;

        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }
        std::pmr::string explain_label_impl() const override;

        // Empty but schema'd, so a downstream OUTER join can NULL-pad and a scalar aggregate can still emit COUNT=0.
        // OFFSET is applied by operator_limit above the scan, so this never needs to skip rows itself.
        core::result_wrapper_t<std::optional<vector::data_chunk_t>>
        emit_or_skip(std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        table::storage_address_t address_;
        std::pmr::string name_;
        expressions::compare_expression_ptr expression_;
        const logical_plan::row_cap_t cap_;
        std::vector<size_t> projected_cols_;

        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        std::pmr::vector<types::complex_logical_type> page_types_{resource_};
        std::unique_ptr<vector::data_chunk_t> first_batch_;
    };

} // namespace components::operators
