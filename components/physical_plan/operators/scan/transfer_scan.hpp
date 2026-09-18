#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <vector>

namespace components::operators {

    class transfer_scan final : public read_only_operator_t {
    public:
        // Empty projected_cols means read all columns.
        transfer_scan(std::pmr::memory_resource* resource,
                      components::catalog::oid_t table_oid,
                      logical_plan::limit_t limit,
                      std::vector<size_t> projected_cols = {});

        const logical_plan::limit_t& limit() const { return limit_; }
        const std::vector<size_t>& projected_cols() const noexcept { return projected_cols_; }

        // Awaits live in this nested coroutine, not a behavior() handler, so N cross-actor awaits
        // don't lost-wakeup; a no-table sentinel scan emits ONE placeholder row instead of draining empty.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Lets a re-driven sub-plan re-OPEN from the stream head (recursive-CTE fixpoint re-run).
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            guard_types_loaded_ = false;
            cursor_id_ = 0;
            guard_types_.clear();
        }

        // Same abandoned-cursor hazard as full_scan — see operator_t::release_cursor.
        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return cursor_id_ != 0 && !drained_; }

    private:
        void explain_impl(const explain_sink& s) const override {
            explain_begin(s, table_oid_);
            s.end();
        }

        vector::data_chunk_t make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types);

        // OFFSET is applied by operator_limit above; every scan receives offset()==0 here.
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch);

        components::catalog::oid_t table_oid_;
        const logical_plan::limit_t limit_;
        std::vector<size_t> projected_cols_;

        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        bool guard_types_loaded_{false};
        uint64_t cursor_id_{0};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
    };

} // namespace components::operators