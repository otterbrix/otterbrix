#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    class node_drop_macro_t final : public node_t {
    public:
        explicit node_drop_macro_t(std::pmr::memory_resource* resource, const collection_full_name_t& name);

        // Resolved pg_class OID + behavior — populated by enrich_logical_plan before
        // the planner runs. The planner rewrites this node into a
        // node_dynamic_cascade_delete_t seeded at (pg_class, relation_oid).
        components::catalog::oid_t relation_oid() const noexcept { return relation_oid_; }
        void set_relation_oid(components::catalog::oid_t oid) noexcept { relation_oid_ = oid; }

        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        void set_behavior(components::catalog::drop_behavior_t b) noexcept { behavior_ = b; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t relation_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_macro_ptr = boost::intrusive_ptr<node_drop_macro_t>;
    node_drop_macro_ptr make_node_drop_macro(std::pmr::memory_resource* resource, const collection_full_name_t& name);

} // namespace components::logical_plan
