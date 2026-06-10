#include "operator_cte_scan.hpp"

namespace components::operators {

    operator_cte_scan_t::operator_cte_scan_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             operator_data_ptr* working_set)
        : read_only_operator_t(resource, std::move(log), operator_type::cte_scan)
        , working_set_(working_set) {}

    void operator_cte_scan_t::on_execute_impl(pipeline::context_t*) {
        if (working_set_ && *working_set_) {
            output_ = *working_set_;
        }
    }

} // namespace components::operators
