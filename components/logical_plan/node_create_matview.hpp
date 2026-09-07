#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/catalog_write.hpp>
#include <components/table/column_definition.hpp>

#include <vector>

namespace components::logical_plan {

    // CREATE MATERIALIZED VIEW mv AS SELECT ... (PostgreSQL-canonical, relkind='m'); lowers to:
    //   sequence_t(create_collection(relkind='m'),
    //              catalog writes × N (pg_class + pg_attribute + pg_rewrite + pg_depend),
    //              insert_t(target=mv_oid, source=body_plan))
    class node_create_matview_t final : public node_t {
    public:
        node_create_matview_t(std::pmr::memory_resource* resource,
                              core::matviewname_t matviewname,
                              core::body_sql_t body_sql);

        const std::string& matviewname() const noexcept { return matviewname_; }
        const std::string& body_sql() const noexcept { return body_sql_; }

        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& source_dbname() const noexcept { return source_dbname_; }
        void set_source_dbname(std::string dbname) { source_dbname_ = std::move(dbname); }
        const std::string& source_relname() const noexcept { return source_relname_; }
        void set_source_relname(std::string relname) { source_relname_ = std::move(relname); }

        // child[0], so it's visible to tree walks (planner, physical_plan_gen); nullptr if not set yet.
        node_ptr body_plan() const noexcept { return children_.empty() ? nullptr : children_.front(); }
        void set_body_plan(node_ptr plan);

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // The body's FROM-clause table; stamped by enrich from the resolved (source_dbname, source_relname) entry.
        components::catalog::oid_t source_table_oid() const noexcept { return source_table_oid_; }
        void set_source_table_oid(components::catalog::oid_t oid) noexcept { source_table_oid_ = oid; }

        // Output schema derived by enrich; planner reads it for build_create_table_writes.
        const std::vector<table::column_definition_t>& inferred_columns() const noexcept { return inferred_columns_; }
        // Non-const: build_create_table_writes stamps attoid back here; plan-gen copies it onward.
        std::vector<table::column_definition_t>& inferred_columns() noexcept { return inferred_columns_; }
        void set_inferred_columns(std::vector<table::column_definition_t> cols) { inferred_columns_ = std::move(cols); }

        components::catalog::oid_t matview_oid() const noexcept { return matview_oid_; }
        void set_matview_oid(components::catalog::oid_t oid) noexcept { matview_oid_ = oid; }

        const std::vector<components::catalog::catalog_write_t>& catalog_writes() const noexcept {
            return catalog_writes_;
        }
        void set_catalog_writes(std::vector<components::catalog::catalog_write_t> w) { catalog_writes_ = std::move(w); }
        std::vector<components::catalog::catalog_write_t> take_catalog_writes() { return std::move(catalog_writes_); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string matviewname_;
        std::string body_sql_;
        std::string dbname_;
        std::string source_dbname_;
        std::string source_relname_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t source_table_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t matview_oid_{components::catalog::INVALID_OID};
        std::vector<table::column_definition_t> inferred_columns_;
        std::vector<components::catalog::catalog_write_t> catalog_writes_;
    };

    using node_create_matview_ptr = boost::intrusive_ptr<node_create_matview_t>;

    node_create_matview_ptr make_node_create_matview(std::pmr::memory_resource* resource,
                                                     core::matviewname_t matviewname,
                                                     core::body_sql_t body_sql);

} // namespace components::logical_plan
