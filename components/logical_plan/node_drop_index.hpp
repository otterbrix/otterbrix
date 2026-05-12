#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        // Phase 10.D: ctor takes role-named strings; schemaname/uuid are SQL parser
        // display fields verified by sql/test/test_create_drop.cpp.
        explicit node_drop_index_t(std::pmr::memory_resource* resource,
                                   std::string dbname,
                                   std::string relname,
                                   std::string indexname,
                                   std::string schemaname = {},
                                   std::string uuid = {});

        const std::string& name() const noexcept;

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        // Phase 9.W/10.D: role-named accessors. DROP INDEX user-typed identifiers; routing
        // uses index_oid stamped by enrich.
        const std::string& indexname() const noexcept { return indexname_; }
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }
        // Parser-display only — verified by sql/test/test_create_drop.cpp.
        const std::string& schemaname() const noexcept { return schemaname_; }
        const std::string& uuid() const noexcept { return uuid_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::string indexname_;
        std::string schemaname_;
        std::string uuid_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
    };

    using node_drop_index_ptr = boost::intrusive_ptr<node_drop_index_t>;
    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             std::string dbname,
                                             std::string relname,
                                             std::string indexname,
                                             std::string schemaname = {},
                                             std::string uuid = {});

} // namespace components::logical_plan
