#pragma once

#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // The rows of an INSERT into a host relation, as the host receives them: every column cast to its declared
    // type (assignment casts, resolved by validation) and put at its declared position under its declared name.
    class operator_assign_cast_t final : public read_only_operator_t {
    public:
        operator_assign_cast_t(std::pmr::memory_resource* resource,
                               log_t log,
                               logical_plan::insert_column_bindings_t bindings);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::streaming; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

    private:
        // Part of the write it feeds, as a scan's filter is part of the scan: EXPLAIN shows its input directly.
        void explain_impl(const explain_sink& s) const override {
            if (left_) {
                left_->explain(s);
            }
        }

        logical_plan::insert_column_bindings_t bindings_;
    };

} // namespace components::operators
