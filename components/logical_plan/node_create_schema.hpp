#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>
#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    // CREATE SCHEMA [connection.][database.]schema (no relation): from SQL a simple name, created in the place's
    // database (a connection's database included); a host's plan names the database, a connection's one with its
    // connection.
    class node_create_schema_t final : public node_t {
    public:
        node_create_schema_t(std::pmr::memory_resource* resource, qualified_name_t schema, bool if_not_exists = false);

        bool if_not_exists() const noexcept { return if_not_exists_; }

        components::catalog::oid_t database_oid() const noexcept { return database_oid_; }
        void set_database_oid(components::catalog::oid_t oid) noexcept { database_oid_ = oid; }

        // The oid the planner gave the schema.
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        bool if_not_exists_;
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_schema_ptr = boost::intrusive_ptr<node_create_schema_t>;
    node_create_schema_ptr
    make_node_create_schema(std::pmr::memory_resource* resource, qualified_name_t schema, bool if_not_exists = false);

} // namespace components::logical_plan
