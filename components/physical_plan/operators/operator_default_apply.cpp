#include "operator_default_apply.hpp"

namespace components::operators {

    operator_default_apply_t::operator_default_apply_t(std::pmr::memory_resource* resource,
                                                        log_t log,
                                                        std::vector<default_entry_t> defaults)
        : read_write_operator_t(resource, log, operator_type::default_apply)
        , defaults_(std::move(defaults)) {}

    void operator_default_apply_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) return;
        output_ = left_->output();
        // Static defaults are already resolved by the planner; volatile defaults
        // (e.g. now(), nextval()) require runtime evaluation — deferred to Etap 5.
    }

} // namespace components::operators
