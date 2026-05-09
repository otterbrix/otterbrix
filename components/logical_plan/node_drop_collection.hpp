#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    class node_drop_collection_t final : public node_t {
    public:
        explicit node_drop_collection_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        // Resolved OIDs + behavior — populated by enrich_logical_plan before the
        // planner runs. The planner consumes table_oid when rewriting this node
        // into a node_dynamic_cascade_delete_t (Phase 2 #49); namespace_oid is
        // kept for symmetry with create_collection_t and any future caller that
        // needs the parent namespace. Default behavior is CASCADE, matching the
        // legacy ddl.cpp BFS that always used cascade_.
        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        void set_table_oid(components::catalog::oid_t oid) noexcept { table_oid_ = oid; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t table_oid_{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_collection_ptr = boost::intrusive_ptr<node_drop_collection_t>;
    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection);

} // namespace components::logical_plan
