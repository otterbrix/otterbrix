#include "primary_key_scan.hpp"

namespace components::operators {

    primary_key_scan::primary_key_scan(std::pmr::memory_resource* resource,
                                       collection_full_name_t name)
        : read_only_operator_t(resource, nullptr, operator_type::primary_key_scan)
        , name_(std::move(name))
        , rows_(resource, types::logical_type::BIGINT) {}

    void primary_key_scan::append(size_t id) {
        rows_.set_value(size_++, types::logical_value_t(resource(), static_cast<int64_t>(id)));
    }

    void primary_key_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // Primary key scan is now handled by executor via:
        //   send(disk_address_, &manager_disk_t::storage_fetch) → data_chunk
        // This operator is a no-op; data is injected via inject_output().
    }

} // namespace components::operators
