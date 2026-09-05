#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <string>

namespace components::logical_plan {

    enum class drop_target_kind : uint8_t
    {
        database,
        collection,
        type,
        sequence,
        view,
        macro,
        index
    };

    // Flat DROP node carrying the target kind plus the role-named name and OID
    // fields each variant uses.
    //
    // Field usage by kind:
    //   dbname_ / relname_  — the user-typed target name; how enrich finds this
    //                         node's entry among the plan's resolved tables
    //   index_name_    — index only (the index is its own pg_class row, so DROP
    //                    INDEX names two things: the parent table and the index)
    //   namespace_oid_ — collection, database, index
    //   type_oid_      — type
    //   index_oid_     — index
    //   table_oid()    — collection, view, sequence, macro, index (base field)
    //   runtime_index_name_ — index only
    // The OIDs are stamped by enrich_logical_plan from the plan's resolved
    // catalog entries; they are INVALID_OID at parse time.
    class node_drop_t final : public node_t {
    public:
        node_drop_t(std::pmr::memory_resource* resource, drop_target_kind kind);

        drop_target_kind kind() const noexcept { return kind_; }

        // Target name, as written. Kept on the node so enrich binds it to a
        // resolved entry by name — no positional coupling to any other node.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(std::string relname) { relname_ = std::move(relname); }
        const std::string& index_name() const noexcept { return index_name_; }
        void set_index_name(std::string name) { index_name_ = std::move(name); }

        // namespace_oid: collection / database / index
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // type_oid: type
        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        // index_oid: index
        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        // RESTRICT/CASCADE. The dynamic cascade operator already refuses a
        // dependent-blocked drop under restrict_; the transformer does not copy
        // DropStmt.behavior yet, so today every DROP carries the DEFAULT — kept
        // at cascade_ (the pre-existing always-CASCADE shape) because flipping
        // it to PostgreSQL's restrict_ changes every DROP TABLE's semantics and
        // is the owner's call.
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }

        // `DROP ... IF EXISTS`. The grammar carries DropStmt.missing_ok; the
        // planner reads this flag when the target did not resolve: a missing
        // target refuses the statement UNLESS the user wrote IF EXISTS — the one
        // form PostgreSQL lets pass. Defaults to false (the loud path), so a
        // programmatic plan that never sets it gets the refusal.
        bool missing_ok() const noexcept { return missing_ok_; }
        void set_missing_ok(bool v) noexcept { missing_ok_ = v; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const drop_target_kind kind_;
        std::string dbname_;
        std::string relname_;
        std::string index_name_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
        bool missing_ok_{false};
    };

    using node_drop_ptr = boost::intrusive_ptr<node_drop_t>;
    node_drop_ptr make_node_drop(std::pmr::memory_resource* resource, drop_target_kind kind);

} // namespace components::logical_plan
