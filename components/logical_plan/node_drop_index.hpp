#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        explicit node_drop_index_t(std::pmr::memory_resource* resource);

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        // Runtime label for the index actor dispatch (manager_index_t keys
        // engine entries by (table_oid, name)). Stamped by enrich from the
        // sibling catalog_resolve_table_t; never user-typed via the ctor.
        const std::string& runtime_index_name() const noexcept { return runtime_index_name_; }
        void set_runtime_index_name(std::string name) { runtime_index_name_ = std::move(name); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
        std::string runtime_index_name_;
    };

    using node_drop_index_ptr = boost::intrusive_ptr<node_drop_index_t>;
    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource);

} // namespace components::logical_plan
