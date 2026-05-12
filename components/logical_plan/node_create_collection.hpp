#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        // Phase 10.D: ctor takes role-named strings instead of cfn struct.
        // schemaname/uuid are SQL parser display fields (e.g., for 4-part
        // identifier `uuid.db.schema.tbl` — verified by sql/test/test_create_drop.cpp);
        // they are NOT used for routing (which goes through namespace_oid stamped by enrich).
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          std::string dbname,
                                          std::string relname,
                                          std::string schemaname = {},
                                          std::string uuid = {},
                                          bool disk_storage = false);

        node_create_collection_t(std::pmr::memory_resource* resource,
                                 std::string dbname,
                                 std::string relname,
                                 std::vector<table::column_definition_t> column_definitions,
                                 std::vector<table::table_constraint_t> constraints,
                                 std::string schemaname = {},
                                 std::string uuid = {},
                                 bool disk_storage = false);

        std::pmr::vector<types::complex_logical_type> schema() const;

        std::vector<table::column_definition_t>& column_definitions();
        const std::vector<table::column_definition_t>& column_definitions() const;
        const std::vector<table::table_constraint_t>& constraints() const;

        bool is_disk_storage() const { return disk_storage_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // Phase 9.W/10.D: role-named accessors. CREATE TABLE writes pg_class.relname (the
        // table name) and pg_namespace.nspname (the parent namespace name) at DDL time.
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
        std::vector<table::column_definition_t> column_definitions_;
        std::vector<table::table_constraint_t> constraints_;
        bool disk_storage_{false};
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           std::string schemaname = {},
                                                           std::string uuid = {});

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           std::string dbname,
                                                           std::string relname,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage = false,
                                                           std::string schemaname = {},
                                                           std::string uuid = {});

} // namespace components::logical_plan
