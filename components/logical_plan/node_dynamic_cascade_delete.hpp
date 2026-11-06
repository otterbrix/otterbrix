#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/table/storage_address.hpp>

#include <cstdint>
#include <string>

namespace components::logical_plan {

    // A table of a connection's database a DROP reaches (it goes, or a column of it goes) and its storage: found by
    // the executor before the statement plans. Any other table's storage is otterbrix's disk.
    struct storage_target_t {
        components::catalog::oid_t table_oid{components::catalog::INVALID_OID};
        components::table::storage_address_t address;
        qualified_name_t name;
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

        const std::pmr::vector<storage_target_t>& storage_targets() const noexcept { return storage_targets_; }
        void set_storage_targets(std::pmr::vector<storage_target_t> targets) { storage_targets_ = std::move(targets); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t seed_classid_;
        components::catalog::oid_t seed_objid_;
        components::catalog::drop_behavior_t behavior_;
        char relkind_;
        std::pmr::vector<storage_target_t> storage_targets_{resource()};
    };

    using node_dynamic_cascade_delete_ptr = boost::intrusive_ptr<node_dynamic_cascade_delete_t>;

} // namespace components::logical_plan
