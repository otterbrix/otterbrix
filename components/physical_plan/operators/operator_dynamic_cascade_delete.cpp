#include "operator_dynamic_cascade_delete.hpp"

#include <components/base/collection_full_name.hpp>
#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <services/disk/manager_disk.hpp>
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
            return (static_cast<std::uint64_t>(cls) << 32) |
                   static_cast<std::uint64_t>(oid);
        }

        // Per-classid catalog-row delete fan-out. For each step in the
        // cascade plan we re-issue the same set of (table, oid_col_idx, oid)
        // deletes that build_drop_sequence() in the dispatcher used to emit.
        // Mirroring that table here keeps the operator self-contained, so
        // task #49 can simply replace the dispatcher BFS with a logical
        // node_dynamic_cascade_delete_t and let the planner route to us.
        struct per_step_delete_t {
            collection_full_name_t catalog_table;
            std::int64_t           oid_col_idx;
        };

        std::vector<per_step_delete_t> deletes_for_classid(catalog::oid_t classid) {
            using namespace catalog::well_known_oid;
            const collection_full_name_t kPgClass     {"pg_catalog", "main", "pg_class"};
            const collection_full_name_t kPgAttribute {"pg_catalog", "main", "pg_attribute"};
            const collection_full_name_t kPgConstraint{"pg_catalog", "main", "pg_constraint"};
            const collection_full_name_t kPgIndex     {"pg_catalog", "main", "pg_index"};
            const collection_full_name_t kPgSequence  {"pg_catalog", "main", "pg_sequence"};
            const collection_full_name_t kPgRewrite   {"pg_catalog", "main", "pg_rewrite"};
            const collection_full_name_t kPgDepend    {"pg_catalog", "main", "pg_depend"};
            const collection_full_name_t kPgType      {"pg_catalog", "main", "pg_type"};
            const collection_full_name_t kPgProc      {"pg_catalog", "main", "pg_proc"};
            const collection_full_name_t kPgNamespace {"pg_catalog", "main", "pg_namespace"};
            const collection_full_name_t kPgComputedColumn{"pg_catalog", "main", "pg_computed_column"};

            std::vector<per_step_delete_t> out;
            if (classid == pg_class_table) {
                out.push_back({kPgIndex,           0}); // pg_index.indexrelid
                out.push_back({kPgIndex,           1}); // pg_index.indrelid
                out.push_back({kPgSequence,        0}); // pg_sequence.seqrelid
                out.push_back({kPgRewrite,         2}); // pg_rewrite.ev_class
                out.push_back({kPgAttribute,       1}); // pg_attribute.attrelid
                out.push_back({kPgComputedColumn,  0}); // pg_computed_column.relid (relkind='g' tables)
                out.push_back({kPgConstraint,      2}); // pg_constraint.conrelid
                out.push_back({kPgConstraint,      4}); // pg_constraint.confrelid
                out.push_back({kPgDepend,          1}); // pg_depend.objid
                out.push_back({kPgDepend,          3}); // pg_depend.refobjid
                out.push_back({kPgClass,           0}); // pg_class.oid (last)
            } else if (classid == pg_constraint_table) {
                out.push_back({kPgConstraint, 0});
                out.push_back({kPgDepend,     1});
                out.push_back({kPgDepend,     3});
            } else if (classid == pg_type_table) {
                out.push_back({kPgType,       0});
                out.push_back({kPgDepend,     1});
                out.push_back({kPgDepend,     3});
            } else if (classid == pg_proc_table) {
                out.push_back({kPgProc,       0});
                out.push_back({kPgDepend,     1});
                out.push_back({kPgDepend,     3});
            } else if (classid == pg_namespace_table) {
                out.push_back({kPgNamespace,  0});
                out.push_back({kPgDepend,     1});
                out.push_back({kPgDepend,     3});
            }
            return out;
        }

    } // namespace

    operator_dynamic_cascade_delete_t::operator_dynamic_cascade_delete_t(
        std::pmr::memory_resource*           resource,
        log_t                                 log,
        catalog::oid_t                       seed_classid,
        catalog::oid_t                       seed_objid,
        catalog::drop_behavior_t             behavior)
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

        const collection_full_name_t kPgDepend{"pg_catalog", "main", "pg_depend"};

        // Step 1 — async BFS over pg_depend(refclassid, refobjid). The walk
        // is identical to the four copies in services/dispatcher/ddl.cpp
        // (drop_database / drop_collection / drop_sequence|view|macro).
        //
        // dep_graph also serves as the visited set: presence of a key
        // signals "already expanded". This avoids a second container and
        // keeps the asymptotics the same as the original BFS.
        std::unordered_map<std::uint64_t, std::vector<catalog::dependency_t>> dep_graph;
        std::vector<std::uint64_t> stack;
        stack.push_back(encode_key(seed_classid_, seed_objid_));

        while (!stack.empty()) {
            const auto k = stack.back();
            stack.pop_back();
            if (dep_graph.count(k)) continue;

            const auto ref_cls = static_cast<catalog::oid_t>(k >> 32);
            const auto ref_oid = static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);

            types::logical_value_t cls_lv(resource_, ref_cls);
            types::logical_value_t oid_lv(resource_, ref_oid);
            auto [_rd, rdf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx,
                kPgDepend,
                std::vector<std::string>{"refclassid", "refobjid"},
                std::vector<types::logical_value_t>{cls_lv, oid_lv});
            auto dep_rows = co_await std::move(rdf);

            std::vector<catalog::dependency_t> deps;
            deps.reserve(dep_rows.size());
            for (const auto& row : dep_rows) {
                if (row.size() < 5) continue;
                catalog::dependency_t d;
                d.classid    = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                d.objid      = static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
                const auto dv = row[4].is_null()
                                    ? std::string_view{"n"}
                                    : row[4].value<std::string_view>();
                d.deptype    = dv.empty() ? 'n' : dv[0];
                deps.push_back(d);
                stack.push_back(encode_key(d.classid, d.objid));
            }
            dep_graph[k] = std::move(deps);
        }

        // Step 2 — feed the closure into catalog::plan_drop. For RESTRICT,
        // plan_drop returns immediately with status=restrict_blocked when a
        // 'n' (normal external) dependency is present. For CASCADE it
        // computes the topological drop order; cycles surface as
        // status=cycle_detected with the offending oid.
        const auto plan = catalog::plan_drop(
            seed_classid_, seed_objid_, behavior_,
            [&dep_graph](catalog::oid_t cls, catalog::oid_t oid)
                -> std::vector<catalog::dependency_t> {
                auto it = dep_graph.find(encode_key(cls, oid));
                return it != dep_graph.end() ? it->second
                                              : std::vector<catalog::dependency_t>{};
            });

        if (plan.status != catalog::ddl_status::ok) {
            // Surface the blocked/cycle status to the executor. Phase #49
            // upgrades this to a structured error cursor — for now the
            // string carries enough info for the dispatcher's catch-all to
            // map back to make_ddl_error_cursor.
            std::string msg = (plan.status == catalog::ddl_status::restrict_blocked)
                ? "DROP RESTRICT: object has dependents (blocking oid "
                : "DROP CASCADE: pg_depend cycle detected (offending oid ";
            msg += std::to_string(plan.blocking_oid) + ")";
            set_error(std::move(msg));
            mark_executed();
            co_return;
        }

        // Step 3 — for every pg_class object we are about to drop that
        // backs an actual table (relkind='r'/'g'), pre-resolve its
        // (database, schema, name) tuple BEFORE we delete its pg_class row.
        // We cannot resolve it after the row is gone. We piggyback on the
        // same dep_graph entries: each (pg_class, oid) in the plan needs a
        // pg_class row read joined with pg_namespace for nspname.
        struct pending_storage_drop_t {
            collection_full_name_t name;
            char                   relkind{'r'};
        };
        std::vector<pending_storage_drop_t> pending_storage_drops;

        const collection_full_name_t kPgClass    {"pg_catalog", "main", "pg_class"};
        const collection_full_name_t kPgNamespace{"pg_catalog", "main", "pg_namespace"};

        for (const auto& step : plan.steps) {
            if (step.classid != catalog::well_known_oid::pg_class_table) continue;

            // Read pg_class row for this oid: (oid, relname, relnamespace, relkind, relstoragemode)
            types::logical_value_t pcoid_lv(resource_, step.objid);
            auto [_pc, pcf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgClass,
                std::vector<std::string>{"oid"},
                std::vector<types::logical_value_t>{pcoid_lv});
            auto pc_rows = co_await std::move(pcf);
            if (pc_rows.empty() || pc_rows[0].size() < 4) continue;
            const auto& row = pc_rows[0];

            std::string relname{row[1].value<std::string_view>()};
            const auto relnamespace = static_cast<catalog::oid_t>(
                row[2].value<std::uint32_t>());
            const auto rkv = row[3].is_null()
                                 ? std::string_view{"r"}
                                 : row[3].value<std::string_view>();
            const char relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];

            // Only regular and computing tables back actual storage. Index/
            // sequence/view/macro/composite-type entries are pure catalog
            // bookkeeping: deleting the pg_class row is sufficient.
            if (relkind != catalog::relkind::regular &&
                relkind != catalog::relkind::computed) {
                continue;
            }

            // Read pg_namespace.nspname for the relnamespace oid.
            types::logical_value_t nsoid_lv(resource_, relnamespace);
            auto [_ns, nsf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::read_rows_by_key,
                exec_ctx, kPgNamespace,
                std::vector<std::string>{"oid"},
                std::vector<types::logical_value_t>{nsoid_lv});
            auto ns_rows = co_await std::move(nsf);
            std::string nspname;
            if (!ns_rows.empty() && ns_rows[0].size() >= 2) {
                nspname = std::string{ns_rows[0][1].value<std::string_view>()};
            }
            // SQL-created tables use the namespace as the database key in
            // collection_full_name_t (rangevar_to_collection convention used
            // by storage and the index actor).
            pending_storage_drops.push_back({
                collection_full_name_t{nspname, "", std::move(relname)},
                relkind});
        }

        // Step 4 — execute the catalog-row deletes in the planned order.
        // Over-deletion is safe: scans that find no matching rows for a
        // given (table, col, oid) tuple are silent no-ops. This matches
        // build_drop_sequence's behaviour in the old dispatcher path.
        for (const auto& step : plan.steps) {
            for (auto& d : deletes_for_classid(step.classid)) {
                auto [_d, df] = actor_zeta::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::delete_pg_catalog_rows,
                    exec_ctx, d.catalog_table, d.oid_col_idx, step.objid);
                co_await std::move(df);
                if (ctx->txn.transaction_id != 0) ctx->pg_catalog_delete_tables.insert(d.catalog_table);
            }
        }

        // Step 5 — for each table we identified above, drop the on-disk
        // storage and unregister the in-memory index entry. Order matters:
        // unregister first so any concurrent index_address consumers stop
        // referencing the collection before the storage actor frees it.
        for (auto& sd : pending_storage_drops) {
            if (ctx->index_address != actor_zeta::address_t::empty_address()) {
                auto [_ui, uif] = actor_zeta::send(
                    ctx->index_address,
                    &services::index::manager_index_t::unregister_collection,
                    ctx->session, sd.name);
                co_await std::move(uif);
            }
            auto [_ds, dsf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::drop_storage,
                ctx->session, sd.name);
            co_await std::move(dsf);
        }

        // No output — DROP statements return an affected-rows-style cursor
        // in the dispatcher; this operator only mutates state.
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators
