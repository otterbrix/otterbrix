#pragma once

#include "node.hpp"
#include "table_storage.hpp"

#include <components/base/identifier_types.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/unique_key.hpp>
#include <components/catalog/view_binding.hpp>
#include <components/types/types.hpp>

#include <cstdint>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::logical_plan {
    // Built by operator_resolve_table_t; read by enrich_plan / validate_schema.
    struct resolved_column_metadata_t {
        std::string attname;
        types::complex_logical_type type;
        std::int32_t attnum{0};
        // storage_t::scan_batched position; attnum-1 for relkind='r' but differs for 'g' (VACUUM
        // keeps tombstones); -1 means unknown, so plan-gen falls back to pass-through.
        std::int32_t chunk_position{-1};
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
        bool attnotnull{false};
        bool atthasdefault{false};
        std::string attdefspec;
        std::string atttypspec;
    };

    using components::catalog::view_binding_t;

    struct resolved_table_metadata_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        std::vector<resolved_column_metadata_t> columns;
        // pg_rewrite.ev_action SQL for relkind 'v'/'m'; consumed by dispatcher Phase 1.5 rewrite_views.
        std::string view_sql;
        // relkind 'v': pg_rewrite_ref rows.
        std::vector<view_binding_t> view_bindings;
        // The schema the table is in: [connection.]database.schema. A connection's table (its database's
        // pg_database.datconnection is set) keeps its rows on the backend.
        std::string connection;
        std::string database;
        std::string schema;
        bool is_remote() const noexcept { return !connection.empty(); }
        // A connection's table: the storage the connector made for this statement, owned by a resolve entry.
        table_storage_t* storage{nullptr};
    };

    // Stamped by operator_resolve_type_t.
    struct resolved_type_metadata_t {
        components::catalog::oid_t type_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        components::types::complex_logical_type type;
        std::string typdefspec;
    };

    // namespace_ is spelled that way because namespace is a C++ keyword.
    enum class resolve_kind : uint8_t
    {
        table,
        namespace_,
        database,
        type,
        constraint
    };

    // outgoing scans pg_constraint by conrelid (INSERT/UPDATE) into fks()/check_exprs();
    // referencing scans by confrelid (DELETE) into fks() with parent=target.
    enum class resolve_direction : uint8_t
    {
        outgoing,
        referencing
    };

    // What CREATE VIEW bound a view body name to: a relation, read by its pg_class oid and never looked up by name.
    struct view_pin_t {
        enum class kind_t : std::uint8_t
        {
            none,
            relation
        };
        kind_t kind{kind_t::none};
        // kind == relation.
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        // The view whose body carries the pin, for the stale refusal.
        core::viewname_t view;
    };

    // Request fields are filled in by the transformer; result fields are stamped by operator_resolve_*_t.
    struct resolve_entry_t {
        static constexpr std::size_t no_target = static_cast<std::size_t>(-1);

        // [connection.][database.][schema.]relname as written. An empty database is the database (and connection)
        // of the session's place; an empty schema of a relation or type is main.pg_catalog and then the place's
        // schema, of a namespace the place's schema. Three parts name a local database, four a connection's.
        std::string connection;
        std::string database;
        std::string schema;
        std::string relname;
        std::string type_name;
        components::catalog::oid_t relation_oid{components::catalog::INVALID_OID};
        resolve_direction direction{resolve_direction::outgoing};
        // Constraint entries only: indexes the TABLE node's entries_ for the table it constrains.
        std::size_t target{no_target};
        // Table entries of an unqualified REFERENCES target: indexes the TABLE node's entries_ for the table that
        // owns the key — the target is looked up in the schema that table was found in.
        std::size_t namespace_of{no_target};
        // Constraint entries only: gathers (conname, oid) without enforcement decode, so DROP
        // CONSTRAINT can repair an invalid catalog state (e.g. doubled PRIMARY KEY) instead of refusing it.
        bool names_only{false};
        // A view body name: what CREATE VIEW bound it to. Not part of the request identity.
        view_pin_t pin;
        // A connection's table's storage for the statement, made by the connector; owned by the first entry of the
        // oid.
        table_storage_ptr storage{nullptr, core::pmr::polymorphic_deleter_t{nullptr, 0, 0}};

        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t database_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid{components::catalog::INVALID_OID};
        // Empty optional means the operator did not find the target (or has not run).
        std::optional<resolved_table_metadata_t> table_md;
        std::optional<resolved_type_metadata_t> type_md;
        std::vector<components::catalog::fk_info_t> fks;
        std::vector<std::pair<std::string, std::string>> check_exprs;
        // UNIQUE/PRIMARY KEY column groups (contype 'u'/'p'); enrich stamps these for
        // operator_unique_constraint_t to enforce.
        std::vector<catalog::unique_key_t> unique_constraints;
        // PRIMARY KEY column names (contype 'p', flattened); pg_attribute.attnotnull is never
        // backfilled by ALTER TABLE ADD PRIMARY KEY, so enrich merges these into not_null_cols instead.
        std::vector<std::string> pk_columns;
        // Every pg_constraint row (conname, oid); enrich stamps DROP CONSTRAINT subcommands from it.
        std::vector<std::pair<std::string, components::catalog::oid_t>> constraint_oids;

        bool operator==(const resolve_entry_t& other) const noexcept;
    };

    // One per resolve kind for the whole execution plan.
    class node_catalog_resolve_t final : public node_t {
    public:
        node_catalog_resolve_t(std::pmr::memory_resource* resource, resolve_kind kind);

        resolve_kind kind() const noexcept { return kind_; }

        const std::pmr::vector<resolve_entry_t>& entries() const noexcept { return entries_; }
        std::pmr::vector<resolve_entry_t>& entries() noexcept { return entries_; }
        bool empty() const noexcept { return entries_.empty(); }

        // Appends `entry` unless an equivalent request is already present
        std::size_t add(resolve_entry_t entry);
        std::size_t find(const qualified_name_t& written) const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const resolve_kind kind_;
        std::pmr::vector<resolve_entry_t> entries_;
    };

    using node_catalog_resolve_ptr = boost::intrusive_ptr<node_catalog_resolve_t>;

    node_catalog_resolve_ptr make_node_catalog_resolve(std::pmr::memory_resource* resource, resolve_kind kind);

    // A REFERENCES target and the table that owns the key, as written.
    struct referenced_table_t {
        qualified_name_t owner;
        qualified_name_t target;
    };

    struct catalog_resolves_t {
        node_catalog_resolve_ptr database;
        node_catalog_resolve_ptr namespaces;
        node_catalog_resolve_ptr tables;
        node_catalog_resolve_ptr types;
        node_catalog_resolve_ptr constraints;
        // Every REFERENCES target with its owner: one in another database is refused.
        std::vector<referenced_table_t> referenced_tables;
        // The session's place the statement completes short names with; set by the executor before resolve.
        components::catalog::place_t place;

        // Creates the slot for `kind` empty on first use; non-const so the transformer can register entries.
        node_catalog_resolve_t& ensure(std::pmr::memory_resource* resource, resolve_kind kind);

        [[nodiscard]] bool empty() const noexcept;

        // Entry naming this target, or nullptr; an empty name never matches, so nothing is bound. A database is
        // named by its connection (empty: a local database) and its name.
        [[nodiscard]] const resolve_entry_t* database_entry(std::string_view connection,
                                                            std::string_view database) const noexcept;
        [[nodiscard]] const resolve_entry_t* namespace_entry(const qualified_name_t& written) const noexcept;
        [[nodiscard]] const resolve_entry_t* table_entry(const qualified_name_t& written) const noexcept;
        [[nodiscard]] const resolve_entry_t* type_entry(const qualified_name_t& written) const noexcept;

        [[nodiscard]] components::catalog::oid_t database_oid(std::string_view connection,
                                                              std::string_view database) const noexcept;
        [[nodiscard]] components::catalog::oid_t namespace_oid(const qualified_name_t& written) const noexcept;
        [[nodiscard]] const resolved_table_metadata_t* table_md(const qualified_name_t& written) const noexcept;
        [[nodiscard]] const resolved_table_metadata_t* table_md(components::catalog::oid_t table_oid) const noexcept;
        [[nodiscard]] const resolved_type_metadata_t* type_md(const qualified_name_t& written) const noexcept;
        // Constraint entry for `table_oid` in `direction`, or nullptr, reached through `target` rather
        // than a duplicated oid; names_only entries are skipped since they would enforce nothing.
        [[nodiscard]] const resolve_entry_t* constraints_for(components::catalog::oid_t table_oid,
                                                             resolve_direction direction) const noexcept;
        // The oid of the constraint `name` of `table_oid` (any outgoing entry, full or names_only), INVALID_OID when the
        // table has none of that name: DROP CONSTRAINT's lookup.
        [[nodiscard]] components::catalog::oid_t constraint_oid(components::catalog::oid_t table_oid,
                                                                std::string_view name) const noexcept;
    };
} // namespace components::logical_plan
