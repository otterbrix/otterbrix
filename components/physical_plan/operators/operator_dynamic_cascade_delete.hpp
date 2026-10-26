#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/logical_plan/node_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/catalog_name_lookup.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Drops (seed_classid, seed_objid) with everything that depends on it: RESTRICT refuses a normal dependent
    // anywhere in the closure, CASCADE deletes the catalog rows children-first and tombstones the storage of the
    // relations among them. The refusal names the seed: its classid, `target`, `relkind` of the relation (the
    // column's relation for pg_attribute) and `column`. Shared by DROP, DROP COLUMN and the host API's unregister_udf.
    // The tables of a connection's database have no storage here, so none is tombstoned for them.
    actor_zeta::unique_future<core::error_t> drop_with_dependents(std::pmr::memory_resource* resource,
                                                                  pipeline::context_t* ctx,
                                                                  components::catalog::oid_t seed_classid,
                                                                  components::catalog::oid_t seed_objid,
                                                                  components::catalog::drop_behavior_t behavior,
                                                                  const qualified_name_t& target,
                                                                  char relkind,
                                                                  const core::columnname_t& column);

    // The catalog rows of the column `attoid` of `table_oid`: its pg_attribute row and its own pg_depend edges go; a
    // column of a table otterbrix stores (`stored`) leaves a tombstone row the storage and COMMIT read. What depends
    // on the column is the caller's (drop_with_dependents).
    actor_zeta::unique_future<core::error_t> drop_column_rows(std::pmr::memory_resource* resource,
                                                              pipeline::context_t* ctx,
                                                              components::catalog::oid_t table_oid,
                                                              components::catalog::oid_t attoid,
                                                              bool stored);

    // What a DROP of (seed_classid, seed_objid) reaches of the databases of connections, found before it runs (the
    // RESTRICT refusal of drop_with_dependents included), in drop order: dependents first.
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<logical_plan::remote_object_t>>>
    drop_closure(std::pmr::memory_resource* resource,
                 pipeline::context_t* ctx,
                 components::catalog::oid_t seed_classid,
                 components::catalog::oid_t seed_objid,
                 components::catalog::drop_behavior_t behavior,
                 const qualified_name_t& target,
                 char relkind,
                 const core::columnname_t& column);

    // How the pg_proc rows of a function go. with_dependents: the way any DROP takes its dependents
    // (unregister_udf). keep_dependents: the rows and the edges they own (objid) only; the edges of what depends on
    // them (refobjid) stay, because register_udf writes the rows again under the same oids.
    enum class function_rows_drop_t
    {
        with_dependents,
        keep_dependents
    };

    actor_zeta::unique_future<core::error_t>
    drop_function_rows(std::pmr::memory_resource* resource,
                       pipeline::context_t* ctx,
                       const std::pmr::vector<components::catalog::oid_t>& function_oids,
                       function_rows_drop_t mode,
                       components::catalog::drop_behavior_t behavior,
                       const std::string& function_name);

    // Universal cascade-delete operator. Walks pg_depend at runtime starting
    // from a (seed_classid, seed_objid) seed and deletes the transitive
    // closure inline using catalog::plan_drop.
    //
    // Behavior:
    //   - RESTRICT: walks pg_depend; on a normal dependent anywhere in the closure
    //     (catalog::plan_drop) surfaces the refusal via set_error and skips deletion.
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
                                          components::catalog::drop_behavior_t behavior,
                                          qualified_name_t target,
                                          char relkind);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t seed_classid_;
        components::catalog::oid_t seed_objid_;
        components::catalog::drop_behavior_t behavior_;
        qualified_name_t target_;
        char relkind_;
    };

} // namespace components::operators
