#include "operator_check_constraint.hpp"

#include <components/cursor/cursor.hpp>

namespace components::operators {

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t                      log,
        std::vector<std::string>   not_null_columns)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns)) {}

    void operator_check_constraint_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) return;
        output_ = left_->output();

        const auto& chunk = output_->data_chunk();

        for (const auto& col_name : not_null_columns_) {
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                if (chunk.data[col].type().alias() != col_name) continue;
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (!chunk.data[col].validity().row_is_valid(row)) {
                        set_error("NOT NULL constraint violated for column: " + col_name);
                        return;
                    }
                }
                break;
            }
        }
    }

} // namespace components::operators