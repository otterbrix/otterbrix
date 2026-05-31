#include "operator_dynamic_cascade_delete.hpp"

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
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
        // visited-set element. Mirrors the encoding used in the original
        // dispatcher BFS (services/dispatcher/ddl.cpp drop_database).
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

        // INVALID_OID seed → resolve never produced a target; nothing to do.
        // This mirrors the `if (rns.found)` / `if (rt.found)` guards in the
        // existing dispatcher BFS.
        if (seed_objid_ == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }

        constexpr catalog::oid_t kPgDepend = catalog::well_known_oid::pg_depend_table;

        // async BFS over pg_depend(refclassid, refobjid). The walk
        // is identical to the four copies in services/dispatcher/ddl.cpp
        // (drop_database / drop_collection / drop_sequence|view|macro).
        //
        // dep_graph also serves as the visited set: presence of a key
        // signals "already expanded". This avoids a second container and
        // keeps the asymptotics the same as the original BFS.
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
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgDepend,
                                               std::move(rd_keys),
                                               std::move(rd_vals));
            auto dep_rows = co_await std::move(rdf);

            std::pmr::vector<catalog::dependency_t> deps(resource_);
            deps.reserve(dep_rows.size());
            for (const auto& row : dep_rows) {
                if (row.size() < 5)
                    continue;
                catalog::dependency_t d;
                d.classid = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                d.objid = static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
                const auto dv = row[4].is_null() ? std::string_view{"n"} : row[4].value<std::string_view>();
                d.deptype = dv.empty() ? 'n' : dv[0];
                deps.push_back(d);
                stack.push_back(encode_key(d.classid, d.objid));
            }
            dep_graph.insert_or_assign(k, std::move(deps));
        }

        // feed the closure into catalog::plan_drop. For RESTRICT,
        // plan_drop returns immediately with status=restrict_blocked when a
        // 'n' (normal external) dependency is present. For CASCADE it
        // computes the topological drop order; cycles are surfaced via
        // status=cycle_detected (blocking_oid carries the offending oid).
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

        // for every pg_class object we are about to drop that
        // backs an actual table (relkind='r'/'g'), record the table_oid
        // BEFORE we delete its pg_class row. The pg_class scan happens
        // here (rather than after the deletes) because once the row is
        // gone we can no longer distinguish storage-backed objects from
        // pure-catalog ones (sequence/view/macro/composite type).
        struct pending_storage_drop_t {
            catalog::oid_t table_oid{catalog::INVALID_OID};
        };
        std::pmr::vector<pending_storage_drop_t> pending_storage_drops(resource_);

        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;

        for (const auto& step : plan.steps) {
            if (step.classid != catalog::well_known_oid::pg_class_table)
                continue;

            // Read pg_class row for this oid to inspect relkind: (oid, relname, relnamespace, relkind, ...)
            // Storage routing is by table_oid only — relname/nspname are no longer needed.
            types::logical_value_t pcoid_lv(resource_, step.objid);
            std::pmr::vector<std::string> pc_keys(resource_);
            pc_keys.emplace_back("oid");
            std::pmr::vector<types::logical_value_t> pc_vals(resource_);
            pc_vals.emplace_back(pcoid_lv);
            auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_rows_by_key,
                                               exec_ctx,
                                               kPgClass,
                                               std::move(pc_keys),
                                               std::move(pc_vals));
            auto pc_rows = co_await std::move(pcf);
            if (pc_rows.empty() || pc_rows[0].size() < 4)
                continue;
            const auto& row = pc_rows[0];

            const auto rkv = row[3].is_null() ? std::string_view{"r"} : row[3].value<std::string_view>();
            const char relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];

            // Only regular and computing tables back actual storage. Index/
            // sequence/view/macro/composite-type entries are pure catalog
            // bookkeeping: deleting the pg_class row is sufficient.
            if (relkind != catalog::relkind::regular && relkind != catalog::relkind::computed) {
                continue;
            }

            pending_storage_drops.push_back({step.objid});
        }

        // execute the catalog-row deletes in the planned order.
        // Over-deletion is safe: scans that find no matching rows for a
        // given (table, col, oid) tuple are silent no-ops. This matches
        // build_drop_sequence's behaviour in the old dispatcher path.
        for (const auto& step : plan.steps) {
            for (auto& d : deletes_for_classid(resource_, step.classid)) {
                auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                 exec_ctx,
                                                 d.catalog_table_oid,
                                                 d.oid_col_idx,
                                                 step.objid);
                co_await std::move(df);
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(d.catalog_table_oid);
            }
        }

        // for each table we identified above, drop the on-disk
        // storage and unregister the in-memory index entry. Order matters:
        //   (a) mark_table_dropped / mark_storage_dropped record the (oid,
        //       commit_id) pairs into manager_index_t::dropped_table_agents_
        //       and the per-agent dropped_storages_ slices owned by
        //       agent_disk_t (the manager-side mirror is deleted;
        //       mark_storage_dropped routes the entry into the owning agent
        //       slice) for the next horizon-advance GC sweep.
        //       They must run BEFORE unregister_collection /
        //       drop_storage because mark_storage_dropped reads the live
        //       storages_ entry to derive the .otbx path + sidecars — once
        //       drop_storage erases that entry the path is lost.
        //   (b) unregister_collection then drop_storage perform the existing
        //       immediate cleanup; unregister first so any concurrent
        //       index_address consumers stop referencing the collection before
        //       the storage actor frees it.
        // We pass ctx->txn.transaction_id as the dropped_at_commit_id. The
        // real post-commit id is not known at execute time; the txn_id is a
        // monotone upper-bound that the GC predicate
        // (dropped_at < new_horizon) treats correctly — the horizon eventually
        // crosses the txn_id once all snapshots that started before this DROP
        // have closed. For txn=0 (auto-commit / bootstrap path) we record 0,
        // matching the catalog scan rebuild convention.
        const uint64_t dropped_at = ctx->txn.transaction_id;
        bool any_storage_drop = false;
        for (auto& sd : pending_storage_drops) {
            any_storage_drop = true;
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_mti, mtif] = actor_zeta::send(ctx->index_address,
                                                     &services::index::manager_index_t::mark_table_dropped,
                                                     ctx->session,
                                                     sd.table_oid,
                                                     dropped_at);
                co_await std::move(mtif);
                auto [_ui, uif] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::unregister_collection,
                                                   ctx->session,
                                                   sd.table_oid);
                co_await std::move(uif);
            }
            auto [_msd, msdf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::mark_storage_dropped,
                                                 ctx->session,
                                                 sd.table_oid,
                                                 dropped_at);
            co_await std::move(msdf);
            auto [_ds, dsf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::drop_storage,
                                               ctx->session,
                                               sd.table_oid);
            co_await std::move(dsf);
        }

        // flip the dispatcher's selective-broadcast flags so the next
        // horizon advance fans on_horizon_advanced out to disk + index, which
        // drain the dropped_storages_ / dropped_table_agents_ queues we just
        // populated. Fire-and-forget: dispatcher acks the receipt by setting
        // disk_has_dropped_ / index_has_dropped_ inside its own mailbox.
        // current_message_sender carries the dispatcher (executor's
        // parent_address_) — see pipeline::context_t construction in
        // services/collection/executor.cpp.
        if (any_storage_drop &&
            ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
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
