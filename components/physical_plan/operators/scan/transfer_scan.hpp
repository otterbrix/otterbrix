#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/storage_address.hpp>

#include <memory>
#include <vector>

namespace components::operators {

    // Every row of one table at its storage, without a predicate.
    class transfer_scan final : public read_only_operator_t {
    public:
        // Empty projected_cols means read all columns.
        transfer_scan(std::pmr::memory_resource* resource,
                      components::catalog::oid_t table_oid,
                      table::storage_address_t address,
                      std::pmr::string name,
                      logical_plan::row_cap_t cap,
                      std::vector<size_t> projected_cols = {});

        const logical_plan::row_cap_t& cap() const { return cap_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }
        // The types of the columns the plan reads, from the catalog: every page is checked against them, and an
        // answer of no row has them.
        void set_page_types(std::pmr::vector<types::complex_logical_type> types) { page_types_ = std::move(types); }

        // Awaits live in this nested coroutine, not a behavior() handler, so N cross-actor awaits
        // don't lost-wakeup; a no-table sentinel scan emits ONE placeholder row instead of draining empty.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t* ctx) override;

        // Lets a re-driven sub-plan re-OPEN from the stream head (recursive-CTE fixpoint re-run).
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            first_batch_.reset();
        }

        // An abandoned cursor gates compact() on its table — see operator_t::release_cursor.
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

        // OFFSET is applied by operator_limit above; a scan never skips rows.
        core::result_wrapper_t<std::optional<vector::data_chunk_t>>
        emit_or_skip(std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        table::storage_address_t address_;
        std::pmr::string name_;
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
