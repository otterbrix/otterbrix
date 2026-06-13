#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Universal cascade-delete operator. Walks pg_depend at runtime starting
    // from a (seed_classid, seed_objid) seed and deletes the transitive
    // closure inline using catalog::plan_drop.
    //
    // Behavior:
    //   - RESTRICT: walks pg_depend; on first 'n' (normal external) dependency,
    //     surfaces the blocking oid via set_error and skips deletion.
    //   - CASCADE:  walks pg_depend, computes topological drop order via
    //     catalog::plan_drop, then for each step deletes the matching catalog
    //     rows via disk.delete_pg_catalog_rows. For pg_class objects (relkind='r')
    //     it additionally drops the table storage and unregisters the index entry.
    //
    // The operator is a leaf — it has no upstream operator (left_/right_ are
    // not used). It produces no rows (output_ stays nullptr).
    class operator_dynamic_cascade_delete_t final : public read_write_operator_t {
    public:
        operator_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                          log_t log,
                                          components::catalog::oid_t seed_classid,
                                          components::catalog::oid_t seed_objid,
                                          components::catalog::drop_behavior_t behavior);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t seed_classid_;
        components::catalog::oid_t seed_objid_;
        components::catalog::drop_behavior_t behavior_;
    };

} // namespace components::operators
