#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    class node_drop_collection_t final : public node_t {
    public:
        // Phase 10.D: ctor takes role-named strings instead of cfn struct.
        // schemaname/uuid are SQL parser display fields (verified by
        // sql/test/test_create_drop.cpp); routing uses table_oid stamped by enrich.
        explicit node_drop_collection_t(std::pmr::memory_resource* resource,
                                        std::string dbname,
                                        std::string relname,
                                        std::string schemaname = {},
                                        std::string uuid = {});

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

        // Phase 9.W/10.D: role-named accessors. DROP TABLE user-typed identifiers; routing
        // uses table_oid()/namespace_oid() stamped by enrich.
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
        std::string schemaname_;
        std::string uuid_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_collection_ptr = boost::intrusive_ptr<node_drop_collection_t>;
    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource,
                                                       std::string dbname,
                                                       std::string relname,
                                                       std::string schemaname = {},
                                                       std::string uuid = {});

} // namespace components::logical_plan
