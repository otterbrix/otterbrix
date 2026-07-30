#pragma once

#include "node.hpp"

#include <cstddef>

namespace components::logical_plan {

    // Pipeline replacement for inline manager_disk_t::allocate_oids_batch
    // calls in the dispatcher. The node carries the requested count; Pass 1's
    // operator_allocate_oids_t scans/allocates from the disk-side oid_generator
    // atomically and publishes the resulting OIDs on its own data channel
    // (output()), which is where the DDL planner reads them from.
    class node_allocate_oids_t final : public node_t {
    public:
        explicit node_allocate_oids_t(std::pmr::memory_resource* resource, std::size_t count);

        std::size_t count() const noexcept { return count_; }

    private:
        std::string to_string_impl() const override;

        std::size_t count_;
    };

    using node_allocate_oids_ptr = boost::intrusive_ptr<node_allocate_oids_t>;

    node_allocate_oids_ptr make_node_allocate_oids(std::pmr::memory_resource* resource, std::size_t count);

} // namespace components::logical_plan