#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <cstdint>
#include <string>

namespace components::logical_plan {

    // One object of a connection's database a DROP takes, found by the executor before it plans (dependents first):
    // the connector's DROP of it is a step of the plan before the catalog's rows go.
    struct remote_object_t {
        enum class kind_t : std::uint8_t
        {
            table,  // `oid`: the table
            column, // `oid`: the column's attoid, `table_oid` its table, which the DROP keeps
            schema  // `oid`: the schema, `schema` its names
        };
        kind_t kind;
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::catalog::place_t schema;
    };

    // Planner-emitted DDL leaf: drives a CASCADE/RESTRICT walk over pg_depend
    // starting from a (seed_classid, seed_objid) seed and deletes the transitive
    // closure inline, so DROP runs through the executor pipeline like any other plan.
    //
    // The classid identifies which catalog owns the seed object — pg_namespace
    // for DROP DATABASE, pg_class for DROP TABLE/SEQUENCE/VIEW/MACRO, pg_type
    // for DROP TYPE, etc. The behavior selects RESTRICT vs CASCADE semantics
    // (see catalog::drop_behavior_t and cascade_planner.cpp). target() names the
    // seed as the statement wrote it; with the classid and relkind it is what the RESTRICT refusal says.
    class node_dynamic_cascade_delete_t final : public node_t {
    public:
        node_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                      components::catalog::oid_t seed_classid,
                                      components::catalog::oid_t seed_objid,
                                      components::catalog::drop_behavior_t behavior,
                                      qualified_name_t target,
                                      char relkind);

        components::catalog::oid_t seed_classid() const noexcept { return seed_classid_; }
        components::catalog::oid_t seed_objid() const noexcept { return seed_objid_; }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }
        char relkind() const noexcept { return relkind_; }

        // What the DROP takes of connections' databases, in drop order, stamped by the executor: each is a step of
        // the connector before the catalog's rows go.
        const std::pmr::vector<remote_object_t>& remote_drops() const noexcept { return remote_drops_; }
        void set_remote_drops(std::pmr::vector<remote_object_t> drops) { remote_drops_ = std::move(drops); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t seed_classid_;
        components::catalog::oid_t seed_objid_;
        components::catalog::drop_behavior_t behavior_;
        char relkind_;
        std::pmr::vector<remote_object_t> remote_drops_{resource()};
    };

    using node_dynamic_cascade_delete_ptr = boost::intrusive_ptr<node_dynamic_cascade_delete_t>;

} // namespace components::logical_plan
