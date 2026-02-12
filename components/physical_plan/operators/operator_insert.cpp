#include "operator_insert.hpp"

namespace components::operators {

    operator_insert::operator_insert(std::pmr::memory_resource* resource, log_t* log,
                                     collection_full_name_t name)
        : read_write_operator_t(resource, log, std::move(name), operator_type::insert) {}

    void operator_insert::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // Insert logic (schema adoption, column expansion, dedup, append) is now handled
        // by executor via send(disk_address_, &manager_disk_t::storage_append).
        // This operator just passes data from the left child (raw_data).
        if (left_ && left_->output()) {
            output_ = left_->output();
            modified_ = operators::make_operator_write_data(resource());
        }
    }

} // namespace components::operators
