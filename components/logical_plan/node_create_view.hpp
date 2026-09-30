#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_catalog_resolve.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>

#include <memory_resource>

namespace components::logical_plan {

    using components::catalog::view_dependency_t;

    // CREATE VIEW v AS <body>: the body is child[0] and runs the canonical path (resolve, expansion of the views it
    // names, the host's name resolution, validation). The executor stamps what the body was bound to; the planner
    // writes it: pg_class, pg_attribute (the output columns), pg_rewrite (the text), pg_rewrite_ref (the bindings),
    // pg_depend (the dependencies).
    class node_create_view_t final : public node_t {
    public:
        node_create_view_t(std::pmr::memory_resource* resource, core::viewname_t viewname, core::query_sql_t query_sql);

        const std::string& query_sql() const { return query_sql_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        const std::string& viewname() const noexcept { return viewname_; }

        // Namespace the view is created in, as written. Kept on the node so enrich
        // binds it to a resolved namespace entry by name and stamps namespace_oid().
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }

        node_ptr body() const noexcept { return children_.empty() ? nullptr : children_.front(); }

        // CREATE OR REPLACE VIEW; replaced_oid() is the view it replaces, INVALID_OID when there is none yet.
        bool replace() const noexcept { return replace_; }
        void set_replace(bool replace) noexcept { replace_ = replace; }
        components::catalog::oid_t replaced_oid() const noexcept { return replaced_oid_; }
        void set_replaced_oid(components::catalog::oid_t oid) noexcept { replaced_oid_ = oid; }

        // Non-const: the planner stamps the minted attoids back onto them.
        std::pmr::vector<table::column_definition_t>& columns() noexcept { return columns_; }
        const std::pmr::vector<table::column_definition_t>& columns() const noexcept { return columns_; }
        void set_columns(std::pmr::vector<table::column_definition_t> columns) { columns_ = std::move(columns); }

        const std::pmr::vector<view_binding_t>& bindings() const noexcept { return bindings_; }
        void set_bindings(std::pmr::vector<view_binding_t> bindings) { bindings_ = std::move(bindings); }

        const std::pmr::vector<view_dependency_t>& dependencies() const noexcept { return dependencies_; }
        void set_dependencies(std::pmr::vector<view_dependency_t> dependencies) {
            dependencies_ = std::move(dependencies);
        }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string viewname_;
        std::string query_sql_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        bool replace_{false};
        components::catalog::oid_t replaced_oid_{components::catalog::INVALID_OID};
        std::pmr::vector<table::column_definition_t> columns_;
        std::pmr::vector<view_binding_t> bindings_;
        std::pmr::vector<view_dependency_t> dependencies_;
    };

    using node_create_view_ptr = boost::intrusive_ptr<node_create_view_t>;
    node_create_view_ptr
    make_node_create_view(std::pmr::memory_resource* resource, core::viewname_t viewname, core::query_sql_t query_sql);

} // namespace components::logical_plan
