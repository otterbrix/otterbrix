#include "operator_not_null_check.hpp"

#include <components/cursor/cursor.hpp>

namespace components::operators {

    operator_not_null_check_t::operator_not_null_check_t(std::pmr::memory_resource* resource,
                                                          log_t log,
                                                          std::vector<std::string> not_null_columns)
        : read_write_operator_t(resource, log, operator_type::not_null_check)
        , not_null_columns_(std::move(not_null_columns)) {}

    void operator_not_null_check_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) return;
        output_ = left_->output();
        // NOT NULL enforcement performed in executor intercept_dml_io via column definitions.
        // The column names carried here serve as documentation; full eval in Etap 5 cleanup.
    }

} // namespace components::operators
