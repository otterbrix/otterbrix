#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/logical_plan/identifier_types.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
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

    // One catalog lookup: the request fields the transformer fills in, and the
    // result fields the matching operator_resolve_*_t stamps back in place.
    //
    // Field usage by the owning node's kind:
    //   table       — dbname / relname → namespace_oid, table_md
    //   namespace_  — dbname → namespace_oid
    //   database    — dbname → database_oid
    //   type        — dbname / type_name → type_oid, type_md
    //   constraint  — target indexes the TABLE node's entries, direction
    //                 → fks, check_exprs, unique_constraints, pk_columns
    struct resolve_entry_t {
        static constexpr std::size_t no_target = static_cast<std::size_t>(-1);

        // --- request ---
        std::string dbname;
        std::string relname;
        std::string type_name;
        resolve_direction direction{resolve_direction::outgoing};
        // Constraint entries only: index into the TABLE node's entries_ naming the
        // table whose constraints this entry gathers
        std::size_t target{no_target};

        // --- result, stamped by the resolve operator ---
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t database_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid{components::catalog::INVALID_OID};
        // Empty optional means the operator did not find the target (or has not run).
        std::optional<resolved_table_metadata_t> table_md;
        std::optional<resolved_type_metadata_t> type_md;
        std::vector<components::catalog::fk_info_t> fks;
        std::vector<std::pair<std::string, std::string>> check_exprs;
        // UNIQUE / PRIMARY KEY column groups (contype 'u'/'p'), one ordered local
        // column-name list per constraint. Read by enrich to stamp the INSERT/UPDATE
        // node so operator_unique_constraint_t can enforce them.
        std::vector<std::vector<std::string>> unique_constraints;
        // PRIMARY KEY column names (contype 'p' only, flattened). PRIMARY KEY implies
        // NOT NULL, but pg_attribute.attnotnull is only written for column-level
        // constraints at CREATE TABLE — ALTER TABLE ADD PRIMARY KEY / a table-level PK
        // never back-fills it. Enrich merges these into the DML node's not_null_cols.
        std::vector<std::string> pk_columns;

        bool operator==(const resolve_entry_t& other) const noexcept;
    };

    // Catalog-dependency node: ONE per resolve kind for the WHOLE execution plan,
    // carrying every lookup of that kind the plan needs
    class node_catalog_resolve_t final : public node_t {
    public:
        node_catalog_resolve_t(std::pmr::memory_resource* resource, resolve_kind kind);

        resolve_kind kind() const noexcept { return kind_; }

        const std::pmr::vector<resolve_entry_t>& entries() const noexcept { return entries_; }
        std::pmr::vector<resolve_entry_t>& entries() noexcept { return entries_; }
        bool empty() const noexcept { return entries_.empty(); }

        // Appends `entry` unless an equivalent request is already present
        std::size_t add(resolve_entry_t entry);
        std::size_t find(std::string_view dbname, std::string_view name) const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const resolve_kind kind_;
        std::pmr::vector<resolve_entry_t> entries_;
    };

    using node_catalog_resolve_ptr = boost::intrusive_ptr<node_catalog_resolve_t>;

    node_catalog_resolve_ptr make_node_catalog_resolve(std::pmr::memory_resource* resource, resolve_kind kind);

    // Every catalog lookup an execution plan depends on
    struct catalog_resolves_t {
        node_catalog_resolve_ptr database;
        node_catalog_resolve_ptr namespaces;
        node_catalog_resolve_ptr tables;
        node_catalog_resolve_ptr types;
        node_catalog_resolve_ptr constraints;

        // The slot for `kind`, created empty on first use. Non-const so the
        // transformer can register entries.
        node_catalog_resolve_t& ensure(std::pmr::memory_resource* resource, resolve_kind kind);

        [[nodiscard]] bool empty() const noexcept;

        // --- direct lookups (replace plan_resolve_index_t) ---
        // The entry naming this target, or nullptr. An empty name never matches, so
        // a consumer that names nothing simply binds nothing.
        [[nodiscard]] const resolve_entry_t* namespace_entry(std::string_view dbname) const noexcept;
        [[nodiscard]] const resolve_entry_t* table_entry(std::string_view dbname,
                                                         std::string_view relname) const noexcept;
        [[nodiscard]] const resolve_entry_t* type_entry(std::string_view dbname,
                                                        std::string_view type_name) const noexcept;

        [[nodiscard]] components::catalog::oid_t namespace_oid(std::string_view dbname) const noexcept;
        [[nodiscard]] const resolved_table_metadata_t* table_md(std::string_view dbname,
                                                                std::string_view relname) const noexcept;
        [[nodiscard]] const resolved_table_metadata_t* table_md(components::catalog::oid_t table_oid) const noexcept;
        [[nodiscard]] const resolved_type_metadata_t* type_md(std::string_view dbname,
                                                              std::string_view type_name) const noexcept;
        // The constraint entry gathered for `table_oid` in `direction`, or nullptr.
        // Constraint entries reach their table through `target`, so this resolves the
        // index into the tables node rather than keying on a duplicated oid.
        [[nodiscard]] const resolve_entry_t* constraints_for(components::catalog::oid_t table_oid,
                                                             resolve_direction direction) const noexcept;
    };

} // namespace components::logical_plan
