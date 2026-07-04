#include "operator_group_merge.hpp"

#include <components/types/logical_value.hpp>

namespace components::operators {

    operator_group_merge_t::operator_group_merge_t(std::pmr::memory_resource* resource,
                                                   log_t log,
                                                   bool scalar,
                                                   std::pmr::vector<types::complex_logical_type> output_types,
                                                   std::vector<std::pair<std::string, std::string>> aggs)
        : read_only_operator_t(resource, std::move(log), operator_type::group_merge)
        , scalar_(scalar)
        , output_types_(std::move(output_types))
        , aggs_(std::move(aggs)) {}

    core::error_t operator_group_merge_t::push(pipeline::context_t* /*ctx*/,
                                               vector::data_chunk_t&& input,
                                               chunks_vector_t& out) {
        // Identity passthrough: the single owning agent already returned FINAL rows.
        if (input.size() > 0) {
            saw_rows_ = true;
        }
        out.push_back(std::move(input));
        return core::error_t::no_error();
    }

    core::error_t operator_group_merge_t::finalize(pipeline::context_t* /*ctx*/, chunks_vector_t& out) {
        // Grouped aggregate over no rows emits nothing; a scalar aggregate MUST still
        // emit its single row. The agent's empty-slice finalize normally supplies it
        // (so saw_rows_ is true and this is a no-op); synthesize only when no row
        // arrived at all. Without output_types the row cannot be typed — bail (the
        // agent-side row is then the only source, as before).
        if (saw_rows_ || !scalar_ || output_types_.empty()) {
            return core::error_t::no_error();
        }
        vector::data_chunk_t row(resource_, output_types_, 1);
        for (uint64_t j = 0; j < row.column_count(); ++j) {
            // Scalar aggregate: no key columns, so column j pairs with aggs_[j].
            const bool is_count = j < aggs_.size() && aggs_[j].second == "count";
            if (is_count) {
                // COUNT over zero rows is 0 (a zero-initialized value of the column type).
                row.data[j].set_value(0, types::logical_value_t(resource_, output_types_[j]));
            } else {
                row.data[j].set_null(0, true);
            }
            if (j < aggs_.size()) {
                row.data[j].set_type_alias(aggs_[j].first);
            }
        }
        row.set_cardinality(1);
        out.push_back(std::move(row));
        return core::error_t::no_error();
    }

} // namespace components::operators
