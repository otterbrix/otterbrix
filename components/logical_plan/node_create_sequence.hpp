#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <cstdint>
#include <limits>

namespace components::logical_plan {

    class node_create_sequence_t final : public node_t {
    public:
        node_create_sequence_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& name,
                               int64_t start = 1,
                               int64_t increment = 1,
                               int64_t min_value = 1,
                               int64_t max_value = std::numeric_limits<int64_t>::max());

        int64_t start() const { return start_; }
        int64_t increment() const { return increment_; }
        int64_t min_value() const { return min_value_; }
        int64_t max_value() const { return max_value_; }

        // Namespace OID set by the enrich phase (dispatcher resolves the namespace
        // name via catalog_view and calls set_namespace_oid before handing the plan
        // to the planner). Returns INVALID_OID (0) when not yet enriched.
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        int64_t start_;
        int64_t increment_;
        int64_t min_value_;
        int64_t max_value_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_sequence_ptr = boost::intrusive_ptr<node_create_sequence_t>;
    node_create_sequence_ptr make_node_create_sequence(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& name,
                                                       int64_t start = 1,
                                                       int64_t increment = 1,
                                                       int64_t min_value = 1,
                                                       int64_t max_value = std::numeric_limits<int64_t>::max());

} // namespace components::logical_plan
