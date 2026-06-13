#include "operator_dynamic_cascade_delete.hpp"

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {

        // Encode (classid, objid) into a single uint64 for use as map key /
        // visited-set element.
        inline std::uint64_t encode_key(catalog::oid_t cls, catalog::oid_t oid) noexcept {
            return (static_cast<std::uint64_t>(cls) << 32) | static_cast<std::uint64_t>(oid);
        }

        // Per-classid catalog-row delete fan-out. For each step in the
        // cascade plan we re-issue the same set of (table, oid_col_idx, oid)
        // deletes the planner would emit for explicit drops.
        struct per_step_delete_t {
            catalog::oid_t catalog_table_oid;
            std::int64_t oid_col_idx;
        };

        std::pmr::vector<per_step_delete_t> deletes_for_classid(std::pmr::memory_resource* resource,
                                                                catalog::oid_t classid) {
            using namespace catalog::well_known_oid;
            std::pmr::vector<per_step_delete_t> out(resource);
            if (classid == pg_class_table) {
                out.push_back({pg_index_table, 0});           // pg_index.indexrelid
                out.push_back({pg_index_table, 1});           // pg_index.indrelid
                out.push_back({pg_sequence_table, 0});        // pg_sequence.seqrelid
                out.push_back({pg_rewrite_table, 2});         // pg_rewrite.ev_class
                out.push_back({pg_attribute_table, 1});       // pg_attribute.attrelid
                out.push_back({pg_computed_column_table, 0}); // pg_computed_column.relid (relkind='g' tables)
                out.push_back({pg_constraint_table, 2});      // pg_constraint.conrelid
                out.push_back({pg_constraint_table, 4});      // pg_constraint.confrelid
                out.push_back({pg_depend_table, 1});          // pg_depend.objid
                out.push_back({pg_depend_table, 3});          // pg_depend.refobjid
                out.push_back({pg_class_table, 0});           // pg_class.oid (last)
            } else if (classid == pg_constraint_table) {
                out.push_back({pg_constraint_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_type_table) {
                out.push_back({pg_type_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_proc_table) {
                out.push_back({pg_proc_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            } else if (classid == pg_namespace_table) {
                out.push_back({pg_namespace_table, 0});
                out.push_back({pg_depend_table, 1});
                out.push_back({pg_depend_table, 3});
            }
            return out;
        }

    } // namespace

    operator_dynamic_cascade_delete_t::operator_dynamic_cascade_delete_t(std::pmr::memory_resource* resource,
                                                                         log_t log,
                                                                         catalog::oid_t seed_classid,
                                                                         catalog::oid_t seed_objid,
                                                                         catalog::drop_behavior_t behavior)
        // Tagged as dynamic_cascade_delete; the executor's generic-DDL path
        // treats it as a write-only no-output step (same convention as
        // operator_drop_index_t / operator_primitive_delete_t).
        : read_write_operator_t(resource, std::move(log), operator_type::dynamic_cascade_delete)
        , seed_classid_(seed_classid)
        , seed_objid_(seed_objid)
        , behavior_(behavior) {}

    void operator_dynamic_cascade_delete_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Output stays nullptr; the executor distinguishes "no output" from
        // "execution skipped" via mark_executed() in await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_dynamic_cascade_delete_t::await_async_and_resume(pipeline::context_t* ctx) {
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // INVALID_OID seed: resolve never produced a target, nothing to do.
        if (seed_objid_ == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }

        constexpr catalog::oid_t kPgDepend = catalog::well_known_oid::pg_depend_table;

        // Async BFS over pg_depend(refclassid, refobjid). dep_graph doubles as
        // the visited set: a present key means "already expanded".
        std::pmr::unordered_map<std::uint64_t, std::pmr::vector<catalog::dependency_t>> dep_graph(resource_);
        std::pmr::vector<std::uint64_t> stack(resource_);
        stack.push_back(encode_key(seed_classid_, seed_objid_));

        while (!stack.empty()) {
            const auto k = stack.back();
            stack.pop_back();
            if (dep_graph.count(k))
                continue;

            const auto ref_cls = static_cast<catalog::oid_t>(k >> 32);
            const auto ref_oid = static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);

            types::logical_value_t cls_lv(resource_, ref_cls);
            types::logical_value_t oid_lv(resource_, ref_oid);
            std::pmr::vector<std::string> rd_keys(resource_);
            rd_keys.emplace_back("refclassid");
            rd_keys.emplace_back("refobjid");
            std::pmr::vector<types::logical_value_t> rd_vals(resource_);
            rd_vals.emplace_back(cls_lv);
            rd_vals.emplace_back(oid_lv);
            auto [_rd, rdf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_key,
                                               exec_ctx,
                                               kPgDepend,
                                               std::move(rd_keys),
                                               components::operators::make_key_chunk(resource_, std::move(rd_vals)));
            auto dep_batches = co_await std::move(rdf);

            std::pmr::vector<catalog::dependency_t> deps(resource_);
            for (auto& chunk : dep_batches) {
                if (chunk.column_count() < 5)
                    continue;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    catalog::dependency_t d;
                    d.classid = static_cast<catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                    d.objid = static_cast<catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                    auto deptype_v = chunk.value(4, i);
                    const auto dv = deptype_v.is_null() ? std::string_view{"n"} : deptype_v.value<std::string_view>();
                    d.deptype = dv.empty() ? 'n' : dv[0];
                    deps.push_back(d);
                    stack.push_back(encode_key(d.classid, d.objid));
                }
            }
            dep_graph.insert_or_assign(k, std::move(deps));
        }

        // plan_drop: RESTRICT returns restrict_blocked on the first 'n' (normal
        // external) dependency; CASCADE computes the topological drop order and
        // reports cycle_detected (blocking_oid = offending oid) on a cycle.
        const auto plan = catalog::plan_drop(
            resource_,
            seed_classid_,
            seed_objid_,
            behavior_,
            [&dep_graph](std::pmr::memory_resource* mr,
                         catalog::oid_t cls,
                         catalog::oid_t oid) -> std::pmr::vector<catalog::dependency_t> {
                auto it = dep_graph.find(encode_key(cls, oid));
                if (it == dep_graph.end()) {
                    return std::pmr::vector<catalog::dependency_t>{mr};
                }
                return std::pmr::vector<catalog::dependency_t>{it->second.begin(), it->second.end(), mr};
            });
        // Free dep_graph as soon as plan is built — it can hold significant memory
        // for deep cascades and the rest of this coroutine only needs `plan`.
        dep_graph.clear();

        if (plan.status == catalog::ddl_status::restrict_blocked) {
            // Surface the blocked status to the executor. TODO: structured
            // error cursor — for now the string carries enough info for the
            // dispatcher's catch-all to map back to make_ddl_error_cursor.
            std::string msg = "DROP RESTRICT: object has dependents (blocking oid ";
            msg += std::to_string(plan.blocking_oid) + ")";
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        if (plan.status == catalog::ddl_status::cycle_detected) {
            std::string msg = "DROP: pg_depend cycle detected at oid ";
            msg += std::to_string(plan.blocking_oid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // Record table_oids of storage-backed (relkind 'r'/'g') pg_class objects
        // BEFORE deleting their pg_class rows: once a row is gone we can no longer
        // tell storage-backed objects from pure-catalog ones (sequence/view/macro/type).
        struct pending_storage_drop_t {
            catalog::oid_t table_oid{catalog::INVALID_OID};
        };
        std::pmr::vector<pending_storage_drop_t> pending_storage_drops(resource_);

        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;

        // pg_class relkind probe: each storage step's read is keyed by its own
        // step.objid and only decides whether the oid is storage-backed, so no read
        // feeds another iteration's read (probes are independent). plan.steps is fixed
        // before this loop, so the probe oids are all known up front — gather the
        // pg_class-keyed step oids in step order and run ONE batched read_chunks_by_keys
        // keyed on "oid", then map result[i] back to the i-th probed step.
        std::pmr::vector<catalog::oid_t> probe_oids(resource_);
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> probe_key_rows(resource_);
        for (const auto& step : plan.steps) {
            if (step.classid != catalog::well_known_oid::pg_class_table)
                continue;
            probe_oids.push_back(step.objid);
            std::pmr::vector<types::logical_value_t> row(resource_);
            row.emplace_back(types::logical_value_t(resource_, step.objid));
            probe_key_rows.push_back(std::move(row));
        }
        if (!probe_oids.empty()) {
            // Read pg_class rows for these oids to inspect relkind: (oid, relname, relnamespace,
            // relkind, ...). Storage routing is by table_oid only — relname/nspname are no longer
            // needed. result[i] = matched chunks for probe_oids[i], in input order.
            std::pmr::vector<std::string> pc_keys(resource_);
            pc_keys.emplace_back("oid");
            auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_keys,
                                               exec_ctx,
                                               kPgClass,
                                               std::move(pc_keys),
                                               components::operators::make_keys_chunk(resource_, probe_key_rows));
            std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>> pc_results =
                co_await std::move(pcf);

            for (std::size_t k = 0; k < probe_oids.size() && k < pc_results.size(); ++k) {
                const auto& pc_batches = pc_results[k];
                if (pc_batches.empty() || pc_batches[0].size() == 0 || pc_batches[0].column_count() < 4)
                    continue;

                const auto rk = pc_batches[0].value(3, 0);
                const auto rkv = rk.is_null() ? std::string_view{"r"} : rk.value<std::string_view>();
                const char relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];

                // Only regular and computing tables back actual storage. Index/
                // sequence/view/macro/composite-type entries are pure catalog
                // bookkeeping: deleting the pg_class row is sufficient.
                if (relkind != catalog::relkind::regular && relkind != catalog::relkind::computed)
                    continue;

                pending_storage_drops.push_back({probe_oids[k]});
            }
        }

        // execute the catalog-row deletes in the planned order.
        // Over-deletion is safe: scans that find no matching rows for a
        // given (table, col, oid) tuple are silent no-ops. This matches
        // build_drop_sequence's behaviour in the old dispatcher path.
        // deletes_for_classid is a pure local helper and plan.steps is fixed
        // before this loop, so no spec depends on an intervening read; collect
        // every (table, col, oid) delete into one batched call.
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> catalog_specs(resource_);
        for (const auto& step : plan.steps) {
            for (auto& d : deletes_for_classid(resource_, step.classid)) {
                catalog_specs.push_back({d.catalog_table_oid, d.oid_col_idx, step.objid});
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(d.catalog_table_oid);
            }
        }
        if (!catalog_specs.empty()) {
            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                             exec_ctx,
                                             std::move(catalog_specs));
            co_await std::move(df);
        }

        // Mark the storage + index entry dropped per table, but DO NOT physically
        // tear them down here. The mark_table_dropped / mark_storage_dropped_many
        // tombstones record (oid, dropped_at) for the next horizon-advance GC
        // sweep; the actual drop_storage + unregister_collection now fire only at
        // COMMIT time (operator_commit_transaction, after the txn_publish barrier).
        //
        // drop_storage and unregister_collection are deliberately
        // NOT sent here. A DROP inside a txn must be REVERTIBLE until COMMIT — an
        // explicit-txn ROLLBACK (operator_abort_transaction → storage_drop_aborted
        // / table_drop_aborted) un-marks the tombstones and the table survives — so
        // the backing .otbx and the index engine must still exist at abort time.
        // And other sessions must keep READING the table until publish, which the
        // un-removed storage + still-registered collection allow (the tombstone is
        // GC-invisible until on_horizon_advanced after publish).
        //
        // dropped_at = txn_id: the real commit_id isn't known yet, but txn_id is a
        // monotone upper bound that the GC predicate (dropped_at < new_horizon)
        // handles correctly once every snapshot older than this DROP has closed.
        // txn=0 (auto-commit/bootstrap) records 0, matching catalog-scan rebuild.
        const uint64_t dropped_at = ctx->txn.transaction_id;
        bool any_storage_drop = false;
        // Two-phase fan-out: send the per-table index mark (mark_table_dropped) without
        // awaiting in the loop and collect the dropped storage oids; then issue ONE
        // batched disk mark (mark_storage_dropped_many — every oid in this cascade shares
        // the same dropped_at) and await every future afterwards. No intra-target drop
        // ordering is required (the marks have no ordered follow-up here; the physical
        // drop_storage / unregister_collection run at COMMIT), so awaiting below is
        // completion-sync only and batching the disk mark cannot reorder anything.
        std::pmr::vector<actor_zeta::unique_future<void>> drop_futures(resource_);
        drop_futures.reserve(pending_storage_drops.size() + 1);
        std::pmr::vector<catalog::oid_t> dropped_storage_oids(resource_);
        dropped_storage_oids.reserve(pending_storage_drops.size());
        for (auto& sd : pending_storage_drops) {
            any_storage_drop = true;
            // DROP back-channel: record the dropped storage oid for the COMMIT
            // drain's value-space remap (operator_commit_transaction keys the
            // DROP-GC remap AND the post-publish drop_storage/unregister off the
            // ACTUAL drops in the drain).
            if (ctx->txn.transaction_id != 0) {
                ctx->dropped_storage_oids.push_back(sd.table_oid);
            }
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_mti, mtif] = actor_zeta::send(ctx->index_address,
                                                     &services::index::manager_index_t::mark_table_dropped,
                                                     ctx->session,
                                                     sd.table_oid,
                                                     dropped_at);
                drop_futures.push_back(std::move(mtif));
            }
            dropped_storage_oids.push_back(sd.table_oid);
        }
        if (!dropped_storage_oids.empty()) {
            auto [_msd, msdf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::mark_storage_dropped_many,
                                                 ctx->session,
                                                 std::move(dropped_storage_oids),
                                                 dropped_at);
            drop_futures.push_back(std::move(msdf));
        }
        for (auto& f : drop_futures) {
            co_await std::move(f);
        }

        // Flip the dispatcher's selective-broadcast flags so the next horizon
        // advance fans on_horizon_advanced out to disk + index, draining the
        // dropped queues we just populated. Fire-and-forget; the sender is the
        // dispatcher (executor's parent_address_, see executor.cpp).
        if (any_storage_drop && ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            constexpr uint8_t DISK_KIND = 1;
            constexpr uint8_t INDEX_KIND = 2;
            [[maybe_unused]] auto disk_mark =
                actor_zeta::send(ctx->current_message_sender,
                                 &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                 DISK_KIND);
            [[maybe_unused]] auto index_mark =
                actor_zeta::send(ctx->current_message_sender,
                                 &services::dispatcher::manager_dispatcher_t::on_drop_resource_marked,
                                 INDEX_KIND);
        }

        // No output — DROP statements return an affected-rows-style cursor
        // in the dispatcher; this operator only mutates state.
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
