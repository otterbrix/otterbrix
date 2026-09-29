#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>

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
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
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

} // namespace components::logical_plan
