#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/logical_plan/identifier_types.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    // Per-column metadata mirrored from pg_attribute
    // (relkind='r') or pg_computed_column (relkind='g'), reconstructed at
    // Pass 1 time by operator_resolve_table_t. Carries the full surface
    // enrich_plan / validate_schema read via the plan-tree idx.
    struct resolved_column_metadata_t {
        std::string attname;
        types::complex_logical_type type;
        std::int32_t attnum{0};
        // Storage chunk column index — position in storage_t::scan_batched output.
        // For relkind='r' this is attnum-1. For relkind='g' it can differ because
        // storage retains tombstoned columns between VACUUMs. -1 = unknown
        // (plan-gen falls back to pass-through).
        std::int32_t chunk_position{-1};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
        bool attnotnull{false};
        bool atthasdefault{false};
        std::string attdefspec; // serialized default expression
        std::string atttypspec; // serialized type spec
    };

    struct resolved_table_metadata_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        std::vector<resolved_column_metadata_t> columns;
        // pg_rewrite.ev_action body SQL, populated by operator_resolve_table_t for
        // relkind 'v' (regular view) and 'm' (matview — used by REFRESH). Empty
        // for other relkinds. Consumed by dispatcher Phase 1.5 rewrite_views.
        std::string view_sql;
    };

    // Full type metadata stamped by operator_resolve_type_t.
    // Carries decoded complex_logical_type + raw typdefspec + namespace.
    struct resolved_type_metadata_t {
        components::catalog::oid_t type_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        components::types::complex_logical_type type;
        std::string typdefspec;
    };

    // Discriminator for the catalog-resolve leaf node. 'namespace' is a
    // C++ keyword, hence namespace_.
    enum class resolve_kind : uint8_t
    {
        table,
        namespace_,
        database,
        type,
        constraint
    };

    // Direction for FK + CHECK constraint resolution (resolve_kind::constraint).
    // outgoing    — scan pg_constraint by conrelid (INSERT/UPDATE). Stamps
    //               fks() (contype='f', child=target) + check_exprs() (contype='c').
    // referencing — scan pg_constraint by confrelid (DELETE). Stamps fks()
    //               (contype='f', parent=target) including child table info.
    enum class resolve_direction : uint8_t
    {
        outgoing,
        referencing
    };

    // Flat catalog-dependency leaf node carrying the resolve kind plus the
    // role-named payload each kind uses. Each kind is lowered to the
    // corresponding operator_resolve_*_t during physical plan generation
    // (create_plan switches on kind()); resolution flows through the standard
    // pipeline (logical_plan → planner → optimizer → physical_plan_generator
    // → executor).
    //
    // The node carries no children/expressions and emits no tuples; it is a pure
    // resolved-dependency marker. The resolve operators stamp the resolved OID /
    // metadata back via the operator's back-pointer; the dispatcher/executor read
    // those stamps via plan_resolve_index_t.
    //
    // Field usage by kind:
    //   table       — dbname_ / relname_ / namespace_oid_ / table_oid() (base) /
    //                 resolved_metadata_
    //   namespace_  — dbname_ / namespace_oid_
    //   database    — dbname_ / database_oid_
    //   type        — dbname_ / type_name_ / type_oid_ / resolved_type_metadata_
    //   constraint  — target_ (sibling node, must be kind()==table) / direction_ /
    //                 fks_ / check_exprs_
    class node_catalog_resolve_t final : public node_t {
    public:
        node_catalog_resolve_t(std::pmr::memory_resource* resource, resolve_kind kind);

        resolve_kind kind() const noexcept { return kind_; }

        // table / namespace_ / database / type
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(core::dbname_t dbname) { dbname_ = std::move(static_cast<std::string&>(dbname)); }

        // table
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(core::relname_t relname) { relname_ = std::move(static_cast<std::string&>(relname)); }

        // type
        const std::string& type_name() const noexcept { return type_name_; }
        void set_type_name(core::typename_t type_name) {
            type_name_ = std::move(static_cast<std::string&>(type_name));
        }
        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        // table / namespace_
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // database
        components::catalog::oid_t database_oid() const noexcept { return database_oid_; }
        void set_database_oid(components::catalog::oid_t oid) noexcept { database_oid_ = oid; }

        // table — full table metadata reconstructed by operator_resolve_table_t
        // at Pass 1 time. Empty optional means the resolve operator did not find
        // the table (or hasn't run yet).
        const std::optional<resolved_table_metadata_t>& resolved_metadata() const noexcept {
            return resolved_metadata_;
        }
        void set_resolved_metadata(resolved_table_metadata_t md) { resolved_metadata_ = std::move(md); }

        // type — stamped by operator_resolve_type_t after pg_type read +
        // typdefspec decode. Empty optional means the resolve did not find a
        // matching type. Named distinctly from the table optional above so both
        // payloads can coexist on the merged node.
        const std::optional<resolved_type_metadata_t>& resolved_type_metadata() const noexcept {
            return resolved_type_metadata_;
        }
        void set_resolved_type_metadata(resolved_type_metadata_t md) { resolved_type_metadata_ = std::move(md); }

        // constraint — back-pointer to a sibling resolve node (must be
        // kind()==table); its Pass 1 stamp provides the target's table_oid.
        node_catalog_resolve_t* target() const noexcept { return target_; }
        void set_target(node_catalog_resolve_t* target) noexcept { target_ = target; }
        resolve_direction direction() const noexcept { return direction_; }
        void set_direction(resolve_direction direction) noexcept { direction_ = direction; }

        const std::vector<components::catalog::fk_info_t>& fks() const noexcept { return fks_; }
        void set_fks(std::vector<components::catalog::fk_info_t> v) { fks_ = std::move(v); }

        const std::vector<std::pair<std::string, std::string>>& check_exprs() const noexcept { return check_exprs_; }
        void set_check_exprs(std::vector<std::pair<std::string, std::string>> v) { check_exprs_ = std::move(v); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const resolve_kind kind_;
        std::string dbname_;
        std::string relname_;
        std::string type_name_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t database_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        std::optional<resolved_table_metadata_t> resolved_metadata_;
        std::optional<resolved_type_metadata_t> resolved_type_metadata_;
        node_catalog_resolve_t* target_{nullptr};
        resolve_direction direction_{resolve_direction::outgoing};
        std::vector<components::catalog::fk_info_t> fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_;
    };

    using node_catalog_resolve_ptr = boost::intrusive_ptr<node_catalog_resolve_t>;

    node_catalog_resolve_ptr make_node_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                             core::dbname_t dbname,
                                                             core::relname_t relname);

    node_catalog_resolve_ptr make_node_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                                                 core::dbname_t dbname);

    node_catalog_resolve_ptr make_node_catalog_resolve_database(std::pmr::memory_resource* resource,
                                                                core::dbname_t dbname);

    node_catalog_resolve_ptr make_node_catalog_resolve_type(std::pmr::memory_resource* resource,
                                                            core::dbname_t dbname,
                                                            core::typename_t type_name);

    node_catalog_resolve_ptr make_node_catalog_resolve_constraint(std::pmr::memory_resource* resource,
                                                                  node_catalog_resolve_t* target,
                                                                  resolve_direction direction);

} // namespace components::logical_plan
