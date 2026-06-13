#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    // Planner-emitted DDL leaf: drives a CASCADE/RESTRICT walk over pg_depend
    // starting from a (seed_classid, seed_objid) seed and deletes the transitive
    // closure inline, so DROP runs through the executor pipeline like any other plan.
    //
    // The classid identifies which catalog owns the seed object — pg_namespace
    // for DROP DATABASE, pg_class for DROP TABLE/SEQUENCE/VIEW/MACRO, pg_type
    // for DROP TYPE, etc. The behavior selects RESTRICT vs CASCADE semantics
    // (see catalog::drop_behavior_t and cascade_planner.cpp).
    class node_dynamic_cascade_delete_t final : public node_t {
    public:
        node_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                      components::catalog::oid_t seed_classid,
                                      components::catalog::oid_t seed_objid,
                                      components::catalog::drop_behavior_t behavior);

        components::catalog::oid_t seed_classid() const noexcept { return seed_classid_; }
        components::catalog::oid_t seed_objid() const noexcept { return seed_objid_; }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t seed_classid_;
        components::catalog::oid_t seed_objid_;
        components::catalog::drop_behavior_t behavior_;
    };

    using node_dynamic_cascade_delete_ptr = boost::intrusive_ptr<node_dynamic_cascade_delete_t>;

} // namespace components::logical_plan
