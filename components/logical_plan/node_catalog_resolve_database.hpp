#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/identifier_types.hpp>

#include <string>

namespace components::logical_plan {

    // Leaf carrying the catalog dependency "resolve database 'X'". pg_database
    // (OID=19) is distinct from pg_namespace (OID=20); the resolved oid is the
    // routing key for multi-database WAL workers. database_oid defaults to
    // INVALID_OID so "not yet resolved" is observable downstream.
    class node_catalog_resolve_database_t final : public node_t {
    public:
        explicit node_catalog_resolve_database_t(std::pmr::memory_resource* resource, core::dbname_t dbname);

        const std::string& dbname() const noexcept { return dbname_; }
        components::catalog::oid_t database_oid() const noexcept { return database_oid_; }
        void set_database_oid(components::catalog::oid_t oid) noexcept { database_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
    };

    using node_catalog_resolve_database_ptr = boost::intrusive_ptr<node_catalog_resolve_database_t>;

    node_catalog_resolve_database_ptr make_node_catalog_resolve_database(std::pmr::memory_resource* resource,
                                                                         core::dbname_t dbname);

} // namespace components::logical_plan
