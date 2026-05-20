#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <cstddef>
#include <vector>

namespace components::logical_plan {

    // Pipeline replacement for inline manager_disk_t::allocate_oids_batch
    // calls in the dispatcher. The node carries the requested count; Pass 1's
    // operator_allocate_oids_t scans/allocates from the disk-side oid_generator
    // atomically and stamps the resulting vector of OIDs on this node so the
    // DDL planner can read them via oids().
    class node_allocate_oids_t final : public node_t {
    public:
        explicit node_allocate_oids_t(std::pmr::memory_resource* resource, std::size_t count);

        std::size_t count() const noexcept { return count_; }

        const std::vector<components::catalog::oid_t>& oids() const noexcept { return oids_; }
        void set_oids(std::vector<components::catalog::oid_t> v) { oids_ = std::move(v); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::size_t count_;
        std::vector<components::catalog::oid_t> oids_;
    };

    using node_allocate_oids_ptr = boost::intrusive_ptr<node_allocate_oids_t>;

    node_allocate_oids_ptr make_node_allocate_oids(std::pmr::memory_resource* resource, std::size_t count);

} // namespace components::logical_plan