#include "transfer_scan.hpp"

namespace components::operators {

    transfer_scan::transfer_scan(std::pmr::memory_resource* resource, collection_full_name_t name,
                                  logical_plan::limit_t limit)
        : read_only_operator_t(resource, nullptr, operator_type::transfer_scan)
        , name_(std::move(name))
        , limit_(limit) {}

    void transfer_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        // Transfer scan is now handled by executor via:
        //   send(disk_address_, &manager_disk_t::storage_scan) → data_chunk
        // This operator is a no-op; data is injected via inject_output().
    }

} // namespace components::operators
