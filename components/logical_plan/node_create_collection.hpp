#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          core::relname_t relname,
                                          bool if_not_exists = false);

        node_create_collection_t(std::pmr::memory_resource* resource,
                                 core::relname_t relname,
                                 std::vector<table::column_definition_t> column_definitions,
                                 std::vector<table::table_constraint_t> constraints,
                                 bool if_not_exists = false);

        std::pmr::vector<types::complex_logical_type> schema() const;

        std::vector<table::column_definition_t>& column_definitions();
        const std::vector<table::column_definition_t>& column_definitions() const;
        const std::vector<table::table_constraint_t>& constraints() const;

        bool if_not_exists() const noexcept { return if_not_exists_; }

        char relkind() const noexcept { return relkind_; }
        void set_relkind(char relkind) noexcept { relkind_ = relkind; }

        // relkind 'f' (a recorded remote description) only: the written uid/schema slots of its remote name,
        // and what enrich binds from it — the server and, once recorded, the server's namespace for the schema.
        const std::string& uid_slot() const noexcept { return uid_; }
        const std::string& schema_slot() const noexcept { return schema_; }
        void set_remote_slots(std::string uid, std::string schema) {
            uid_ = std::move(uid);
            schema_ = std::move(schema);
        }
        // The remote path's database (empty for schema.table) and schema, as pg_foreign_namespace keys them.
        const std::string& remote_db() const noexcept { return dbname_; }
        const std::string& remote_schema() const noexcept { return schema_; }
        components::catalog::oid_t server_oid() const noexcept { return server_oid_; }
        void set_server_oid(components::catalog::oid_t oid) noexcept { server_oid_ = oid; }
        components::catalog::oid_t remote_namespace_oid() const noexcept { return remote_namespace_oid_; }
        void set_remote_namespace_oid(components::catalog::oid_t oid) noexcept { remote_namespace_oid_ = oid; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        const std::string& relname() const noexcept { return relname_; }

        // Namespace the table is created in, as written. Kept on the node so enrich
        // binds it to a resolved namespace entry by name and stamps namespace_oid().
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<table::column_definition_t> column_definitions_;
        std::vector<table::table_constraint_t> constraints_;
        bool if_not_exists_{false};
        char relkind_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        std::string uid_;
        std::string schema_;
        components::catalog::oid_t server_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t remote_namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           core::relname_t relname,
                                                           bool if_not_exists = false);

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           core::relname_t relname,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool if_not_exists = false);

    // What a connector's describe records in the cache: `path` is the canonical path inside `server`
    // (schema.table or db.schema.table); the result is a relkind 'f' table in the server's namespace for that
    // schema, written on the canonical DDL path.
    core::result_wrapper_t<node_ptr> make_node_record_remote_table(std::pmr::memory_resource* resource,
                                                                   std::string server,
                                                                   std::vector<std::string> path,
                                                                   std::vector<table::column_definition_t> columns);

} // namespace components::logical_plan
