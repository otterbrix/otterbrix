#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        explicit node_drop_index_t(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::string& name);

        const std::string& name() const noexcept;

        // Resolved by enrich_logical_plan. INVALID_OID means the index was not
        // found (DROP INDEX IF EXISTS no-ops; DROP INDEX returns silent success
        // matching the prior inline-executor behavior).
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
    };

    using node_drop_index_ptr = boost::intrusive_ptr<node_drop_index_t>;
    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name);

} // namespace components::logical_plan