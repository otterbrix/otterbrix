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

    // Merged DROP node. Replaces the seven per-target drop nodes
    // (drop_collection / drop_database / drop_index / drop_type /
    // drop_sequence / drop_view / drop_macro) with a single flat node that
    // carries the target kind plus the role-named OID fields each variant used.
    //
    // Field usage by kind:
    //   namespace_oid_ — collection, database, index
    //   type_oid_      — type
    //   index_oid_     — index
    //   table_oid()    — collection, view, sequence, macro, index (base field)
    //   runtime_index_name_ — index only
    // The OIDs are stamped by enrich_logical_plan from the sibling
    // catalog_resolve_* nodes; they are INVALID_OID at parse time.
    class node_drop_t final : public node_t {
    public:
        node_drop_t(std::pmr::memory_resource* resource, drop_target_kind kind);

        drop_target_kind kind() const noexcept { return kind_; }

        // namespace_oid: collection / database / index
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // type_oid: type
        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        // index_oid: index
        components::catalog::oid_t index_oid() const noexcept { return index_oid_; }
        void set_index_oid(components::catalog::oid_t oid) noexcept { index_oid_ = oid; }

        // Runtime label for the index actor dispatch (manager_index_t keys
        // engine entries by (table_oid, name)). Stamped by enrich from the
        // sibling catalog_resolve_table_t; never user-typed via the ctor.
        const std::string& runtime_index_name() const noexcept { return runtime_index_name_; }
        void set_runtime_index_name(std::string name) { runtime_index_name_ = std::move(name); }

        // No setter — DROP is always-CASCADE; RESTRICT/CASCADE is not wired
        // from the parser. Preserves the pre-merge behavior where behavior_
        // was fixed at its default.
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        const drop_target_kind kind_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t index_oid_{components::catalog::INVALID_OID};
        std::string runtime_index_name_;
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_ptr = boost::intrusive_ptr<node_drop_t>;
    node_drop_ptr make_node_drop(std::pmr::memory_resource* resource, drop_target_kind kind);

} // namespace components::logical_plan
