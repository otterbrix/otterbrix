#include "dispatcher.hpp"
#include "catalog_view.hpp"
#include "resolve_type.hpp"

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_primitive_delete.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;

namespace services::dispatcher {

    namespace catalog = components::catalog;

    namespace {

        std::size_t estimate_ddl_oid_count(const node_ptr& node) {
            switch (node->type()) {
                case node_type::create_database_t:   return 1; // namespace_oid
                case node_type::create_sequence_t:   return 1; // seq_oid
                case node_type::create_view_t:       return 2; // view_oid + rule_oid
                case node_type::create_macro_t:      return 2; // macro_oid + rule_oid
                case node_type::create_constraint_t: return 1; // constraint_oid
                case node_type::create_index_t:      return 1; // index_oid (executor path)
                case node_type::create_collection_t: {
                    auto* cc = static_cast<const node_create_collection_t*>(node.get());
                    return 1 + cc->column_definitions().size(); // table_oid + col_oids
                }
                default:                             return 0;
            }
        }

        // Build a node_sequence of node_primitive_delete leaves from a pre-computed
        // cascade_plan_t. Each step is expanded to the full set of catalog table row
        // deletions for that object's classid. Over-deletion is safe: scans that find
        // no matching rows for a given (table, col, oid) tuple are silent no-ops.
        node_ptr build_drop_sequence(std::pmr::memory_resource* r,
                                      const catalog::cascade_plan_t& plan) {
            using namespace catalog::well_known_oid;
            const collection_full_name_t kPgClass      {"pg_catalog", "main", "pg_class"};
            const collection_full_name_t kPgAttribute  {"pg_catalog", "main", "pg_attribute"};
            const collection_full_name_t kPgConstraint {"pg_catalog", "main", "pg_constraint"};
            const collection_full_name_t kPgIndex      {"pg_catalog", "main", "pg_index"};
            const collection_full_name_t kPgSequence   {"pg_catalog", "main", "pg_sequence"};
            const collection_full_name_t kPgRewrite    {"pg_catalog", "main", "pg_rewrite"};
            const collection_full_name_t kPgDepend     {"pg_catalog", "main", "pg_depend"};
            const collection_full_name_t kPgType       {"pg_catalog", "main", "pg_type"};
            const collection_full_name_t kPgProc       {"pg_catalog", "main", "pg_proc"};
            const collection_full_name_t kPgNamespace  {"pg_catalog", "main", "pg_namespace"};

            auto seq = boost::intrusive_ptr(new node_sequence_t(r));
            auto add = [&](collection_full_name_t tbl, std::int64_t col, catalog::oid_t oid) {
                seq->append_child(boost::intrusive_ptr(
                    new node_primitive_delete_t(r, std::move(tbl), col, oid)));
            };

            for (const auto& step : plan.steps) {
                const auto oid = step.objid;
                if (step.classid == pg_class_table) {
                    add(kPgIndex,      0, oid); // pg_index.indexrelid
                    add(kPgIndex,      1, oid); // pg_index.indrelid
                    add(kPgSequence,   0, oid); // pg_sequence.seqrelid
                    add(kPgRewrite,    2, oid); // pg_rewrite.ev_class
                    add(kPgAttribute,  1, oid); // pg_attribute.attrelid
                    add(kPgConstraint, 2, oid); // pg_constraint.conrelid
                    add(kPgConstraint, 4, oid); // pg_constraint.confrelid
                    add(kPgDepend,     1, oid); // pg_depend.objid
                    add(kPgDepend,     3, oid); // pg_depend.refobjid
                    add(kPgClass,      0, oid); // pg_class.oid (last)
                } else if (step.classid == pg_constraint_table) {
                    add(kPgConstraint, 0, oid);
                    add(kPgDepend,     1, oid);
                    add(kPgDepend,     3, oid);
                } else if (step.classid == pg_type_table) {
                    add(kPgType,       0, oid);
                    add(kPgDepend,     1, oid);
                    add(kPgDepend,     3, oid);
                } else if (step.classid == pg_proc_table) {
                    add(kPgProc,       0, oid);
                    add(kPgDepend,     1, oid);
                    add(kPgDepend,     3, oid);
                } else if (step.classid == pg_namespace_table) {
                    add(kPgNamespace,  0, oid); // pg_namespace.oid (last)
                    add(kPgDepend,     1, oid);
                    add(kPgDepend,     3, oid);
                }
            }
            return seq;
        }


    } // anonymous namespace

    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    execute_ddl(components::session::session_id_t session,
                node_ptr logical_plan,
                components::table::transaction_data txn_data,
                catalog_view_t& view,
                ddl_context_t dctx) {
        components::execution_context_t ddl_ctx{session, txn_data, {}};

        catalog::oid_batch_t oid_batch;
        if (const std::size_t need = estimate_ddl_oid_count(logical_plan); need > 0) {
            auto [_, fut] = actor_zeta::send(dctx.disk_address,
                                             &disk::manager_disk_t::allocate_oids_batch,
                                             need);
            oid_batch.oids = co_await std::move(fut);
        }

        switch (logical_plan->type()) {
            case node_type::create_database_t: {
                const catalog::oid_t ns_oid = oid_batch.allocate();
                auto writes = catalog::build_create_namespace_writes(
                    dctx.resource, std::string(logical_plan->database_name()), ns_oid);
                for (auto& w : writes) {
                    auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                      &disk::manager_disk_t::append_pg_catalog_row,
                                                      ddl_ctx, std::move(w.table), std::move(w.row));
                    co_await std::move(wf);
                }
                break;
            }
            case node_type::drop_database_t: {
                auto db_name = logical_plan->database_name();
                for (const auto& coll : dctx.collections) {
                    if (coll.database == db_name) {
                        if (dctx.index_address != actor_zeta::address_t::empty_address()) {
                            auto [_ui, uif] = actor_zeta::send(dctx.index_address,
                                                               &index::manager_index_t::unregister_collection,
                                                               session, coll);
                            co_await std::move(uif);
                        }
                        auto [_ds, dsf] = actor_zeta::send(dctx.disk_address,
                                                            &disk::manager_disk_t::drop_storage,
                                                            session, coll);
                        co_await std::move(dsf);
                    }
                }
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, db_name, std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    // BFS pg_depend from (pg_namespace, ns_oid) → plan_drop → execute.
                    const collection_full_name_t kPgDepend{"pg_catalog", "main", "pg_depend"};
                    const auto encode_key = [](catalog::oid_t cls, catalog::oid_t oid) -> std::uint64_t {
                        return (std::uint64_t(cls) << 32) | oid;
                    };
                    std::unordered_map<std::uint64_t, std::vector<catalog::dependency_t>> dep_graph;
                    std::vector<std::uint64_t> bfs;
                    bfs.push_back(encode_key(catalog::well_known_oid::pg_namespace_table, rns.oid));
                    while (!bfs.empty()) {
                        const auto k = bfs.back(); bfs.pop_back();
                        if (dep_graph.count(k)) continue;
                        const catalog::oid_t ref_cls = static_cast<catalog::oid_t>(k >> 32);
                        const catalog::oid_t ref_oid = static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);
                        components::types::logical_value_t cls_lv(dctx.resource, ref_cls);
                        components::types::logical_value_t oid_lv(dctx.resource, ref_oid);
                        auto [_rd, rdf] = actor_zeta::send(
                            dctx.disk_address,
                            &disk::manager_disk_t::read_rows_by_key,
                            ddl_ctx, kPgDepend,
                            std::vector<std::string>{"refclassid", "refobjid"},
                            std::vector<components::types::logical_value_t>{cls_lv, oid_lv});
                        auto dep_rows = co_await std::move(rdf);
                        std::vector<catalog::dependency_t> deps;
                        for (const auto& row : dep_rows) {
                            if (row.size() < 5) continue;
                            catalog::dependency_t d;
                            d.classid    = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                            d.objid      = static_cast<catalog::oid_t>(row[1].value<std::uint32_t>());
                            d.refclassid = ref_cls;
                            d.refobjid   = ref_oid;
                            const auto dv = row[4].is_null() ? std::string_view{"n"} : row[4].value<std::string_view>();
                            d.deptype    = dv.empty() ? 'n' : dv[0];
                            deps.push_back(d);
                            bfs.push_back(encode_key(d.classid, d.objid));
                        }
                        dep_graph[k] = std::move(deps);
                    }
                    const auto ns_plan = catalog::plan_drop(
                        catalog::well_known_oid::pg_namespace_table, rns.oid,
                        catalog::drop_behavior_t::cascade_,
                        [&dep_graph, &encode_key](catalog::oid_t cls, catalog::oid_t oid)
                            -> std::vector<catalog::dependency_t> {
                            auto it = dep_graph.find(encode_key(cls, oid));
                            return it != dep_graph.end() ? it->second : std::vector<catalog::dependency_t>{};
                        });
                    if (ns_plan.status != catalog::ddl_status::ok) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        disk::ddl_result_t err;
                        err.status       = ns_plan.status;
                        err.blocking_oid = ns_plan.blocking_oid;
                        co_return make_ddl_error_cursor(dctx.resource, err);
                    }
                    auto drop_seq = build_drop_sequence(dctx.resource, ns_plan);
                    for (auto& child : drop_seq->children()) {
                        auto* pd = static_cast<node_primitive_delete_t*>(child.get());
                        auto [_d, df] = actor_zeta::send(
                            dctx.disk_address,
                            &disk::manager_disk_t::delete_pg_catalog_rows,
                            ddl_ctx,
                            pd->catalog_table(),
                            pd->oid_col_idx(),
                            pd->target_oid());
                        co_await std::move(df);
                    }
                }
                break;
            }
            case node_type::drop_collection_t: {
                if (dctx.index_address != actor_zeta::address_t::empty_address()) {
                    auto [_ui, uif] = actor_zeta::send(dctx.index_address,
                                                       &index::manager_index_t::unregister_collection,
                                                       session, logical_plan->collection_full_name());
                    co_await std::move(uif);
                }
                auto [_ds, dsf] = actor_zeta::send(dctx.disk_address,
                                                   &disk::manager_disk_t::drop_storage,
                                                   session, logical_plan->collection_full_name());
                co_await std::move(dsf);
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx, rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        // R1: cascade plan built here (executor layer), disk = pure storage.
                        // Pre-fetch dependency subgraph from pg_depend via BFS.
                        const collection_full_name_t kPgDepend{"pg_catalog", "main", "pg_depend"};
                        const auto encode_key = [](catalog::oid_t cls,
                                                    catalog::oid_t oid) -> std::uint64_t {
                            return (std::uint64_t(cls) << 32) | oid;
                        };
                        std::unordered_map<std::uint64_t,
                                           std::vector<catalog::dependency_t>> dep_graph;
                        std::vector<std::uint64_t> bfs;
                        bfs.push_back(encode_key(catalog::well_known_oid::pg_class_table, rt.oid));
                        while (!bfs.empty()) {
                            const auto k = bfs.back(); bfs.pop_back();
                            if (dep_graph.count(k)) continue;
                            const catalog::oid_t ref_cls =
                                static_cast<catalog::oid_t>(k >> 32);
                            const catalog::oid_t ref_oid =
                                static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);
                            components::types::logical_value_t cls_lv(dctx.resource, ref_cls);
                            components::types::logical_value_t oid_lv(dctx.resource, ref_oid);
                            auto [_rd, rdf] = actor_zeta::send(
                                dctx.disk_address,
                                &disk::manager_disk_t::read_rows_by_key,
                                ddl_ctx, kPgDepend,
                                std::vector<std::string>{"refclassid", "refobjid"},
                                std::vector<components::types::logical_value_t>{cls_lv, oid_lv});
                            auto dep_rows = co_await std::move(rdf);
                            std::vector<catalog::dependency_t> deps;
                            for (const auto& row : dep_rows) {
                                if (row.size() < 5) continue;
                                catalog::dependency_t d;
                                d.classid    = static_cast<catalog::oid_t>(
                                    row[0].value<std::uint32_t>());
                                d.objid      = static_cast<catalog::oid_t>(
                                    row[1].value<std::uint32_t>());
                                d.refclassid = ref_cls;
                                d.refobjid   = ref_oid;
                                const auto dv = row[4].is_null()
                                    ? std::string_view{"n"}
                                    : row[4].value<std::string_view>();
                                d.deptype    = dv.empty() ? 'n' : dv[0];
                                deps.push_back(d);
                                bfs.push_back(encode_key(d.classid, d.objid));
                            }
                            dep_graph[k] = std::move(deps);
                        }

                        // Build cascade plan — pure catalog logic, no disk writes.
                        const auto plan = catalog::plan_drop(
                            catalog::well_known_oid::pg_class_table, rt.oid,
                            catalog::drop_behavior_t::cascade_,
                            [&dep_graph, &encode_key](catalog::oid_t cls,
                                                       catalog::oid_t oid)
                                -> std::vector<catalog::dependency_t> {
                                auto it = dep_graph.find(encode_key(cls, oid));
                                return it != dep_graph.end()
                                    ? it->second
                                    : std::vector<catalog::dependency_t>{};
                            });
                        if (plan.status != catalog::ddl_status::ok) {
                            disk::ddl_result_t err;
                            err.status       = plan.status;
                            err.blocking_oid = plan.blocking_oid;
                            co_return make_ddl_error_cursor(dctx.resource, err);
                        }

                        // Execute pre-built drop sequence (disk = pure storage).
                        auto drop_seq = build_drop_sequence(dctx.resource, plan);
                        for (auto& child : drop_seq->children()) {
                            auto* pd = static_cast<node_primitive_delete_t*>(child.get());
                            auto [_d, df] = actor_zeta::send(
                                dctx.disk_address,
                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                ddl_ctx,
                                pd->catalog_table(),
                                pd->oid_col_idx(),
                                pd->target_oid());
                            co_await std::move(df);
                        }
                    }
                }
                break;
            }
            case node_type::create_sequence_t: {
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* seq = static_cast<const node_create_sequence_t*>(logical_plan.get());
                    const catalog::oid_t seq_oid = oid_batch.allocate();
                    auto writes = catalog::build_create_sequence_writes(
                        dctx.resource,
                        std::string(logical_plan->collection_name()), rns.oid, seq_oid,
                        seq->start(), seq->increment(), seq->min_value(), seq->max_value(),
                        /*cycle=*/false);
                    for (auto& w : writes) {
                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                          ddl_ctx, std::move(w.table), std::move(w.row));
                        co_await std::move(wf);
                    }
                }
                break;
            }
            case node_type::create_view_t: {
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* vw = static_cast<const node_create_view_t*>(logical_plan.get());
                    const catalog::oid_t view_oid = oid_batch.allocate();
                    const catalog::oid_t rule_oid = oid_batch.allocate();
                    auto writes = catalog::build_create_view_writes(
                        dctx.resource,
                        std::string(logical_plan->collection_name()), rns.oid, view_oid, rule_oid,
                        std::string(vw->query_sql()));
                    for (auto& w : writes) {
                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                          ddl_ctx, std::move(w.table), std::move(w.row));
                        co_await std::move(wf);
                    }
                }
                break;
            }
            case node_type::create_macro_t: {
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* mc = static_cast<const node_create_macro_t*>(logical_plan.get());
                    const catalog::oid_t macro_oid = oid_batch.allocate();
                    const catalog::oid_t rule_oid = oid_batch.allocate();
                    auto writes = catalog::build_create_macro_writes(
                        dctx.resource,
                        std::string(logical_plan->collection_name()), rns.oid, macro_oid, rule_oid,
                        std::string(mc->body_sql()));
                    for (auto& w : writes) {
                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                          ddl_ctx, std::move(w.table), std::move(w.row));
                        co_await std::move(wf);
                    }
                }
                break;
            }
            case node_type::drop_sequence_t:
            case node_type::drop_view_t:
            case node_type::drop_macro_t: {
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        // BFS pg_depend from (pg_class, rt.oid) → cascade plan → execute.
                        const collection_full_name_t kPgDepend{"pg_catalog", "main", "pg_depend"};
                        const auto encode_key = [](catalog::oid_t cls,
                                                    catalog::oid_t oid) -> std::uint64_t {
                            return (std::uint64_t(cls) << 32) | oid;
                        };
                        std::unordered_map<std::uint64_t,
                                           std::vector<catalog::dependency_t>> dep_graph;
                        std::vector<std::uint64_t> bfs;
                        bfs.push_back(encode_key(catalog::well_known_oid::pg_class_table, rt.oid));
                        while (!bfs.empty()) {
                            const auto k = bfs.back(); bfs.pop_back();
                            if (dep_graph.count(k)) continue;
                            const catalog::oid_t ref_cls =
                                static_cast<catalog::oid_t>(k >> 32);
                            const catalog::oid_t ref_oid =
                                static_cast<catalog::oid_t>(k & 0xFFFFFFFFu);
                            components::types::logical_value_t cls_lv(dctx.resource, ref_cls);
                            components::types::logical_value_t oid_lv(dctx.resource, ref_oid);
                            auto [_rd, rdf] = actor_zeta::send(
                                dctx.disk_address,
                                &disk::manager_disk_t::read_rows_by_key,
                                ddl_ctx, kPgDepend,
                                std::vector<std::string>{"refclassid", "refobjid"},
                                std::vector<components::types::logical_value_t>{cls_lv, oid_lv});
                            auto dep_rows = co_await std::move(rdf);
                            std::vector<catalog::dependency_t> deps;
                            for (const auto& row : dep_rows) {
                                if (row.size() < 5) continue;
                                catalog::dependency_t d;
                                d.classid    = static_cast<catalog::oid_t>(
                                    row[0].value<std::uint32_t>());
                                d.objid      = static_cast<catalog::oid_t>(
                                    row[1].value<std::uint32_t>());
                                d.refclassid = ref_cls;
                                d.refobjid   = ref_oid;
                                const auto dv = row[4].is_null()
                                    ? std::string_view{"n"}
                                    : row[4].value<std::string_view>();
                                d.deptype    = dv.empty() ? 'n' : dv[0];
                                deps.push_back(d);
                                bfs.push_back(encode_key(d.classid, d.objid));
                            }
                            dep_graph[k] = std::move(deps);
                        }

                        const auto plan = catalog::plan_drop(
                            catalog::well_known_oid::pg_class_table, rt.oid,
                            catalog::drop_behavior_t::cascade_,
                            [&dep_graph, &encode_key](catalog::oid_t cls,
                                                       catalog::oid_t oid)
                                -> std::vector<catalog::dependency_t> {
                                auto it = dep_graph.find(encode_key(cls, oid));
                                return it != dep_graph.end()
                                    ? it->second
                                    : std::vector<catalog::dependency_t>{};
                            });
                        if (plan.status != catalog::ddl_status::ok) {
                            disk::ddl_result_t err;
                            err.status       = plan.status;
                            err.blocking_oid = plan.blocking_oid;
                            co_return make_ddl_error_cursor(dctx.resource, err);
                        }

                        auto drop_seq = build_drop_sequence(dctx.resource, plan);
                        for (auto& child : drop_seq->children()) {
                            auto* pd = static_cast<node_primitive_delete_t*>(child.get());
                            auto [_d, df] = actor_zeta::send(
                                dctx.disk_address,
                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                ddl_ctx,
                                pd->catalog_table(),
                                pd->oid_col_idx(),
                                pd->target_oid());
                            co_await std::move(df);
                        }
                    }
                }
                break;
            }
            case node_type::alter_table_t: {
                auto* alter = static_cast<node_alter_table_t*>(logical_plan.get());
                const auto& _coll = logical_plan->collection_full_name();
                const auto* ns = view.try_get_namespace(
                    _coll.database.empty() ? _coll.schema : _coll.database);
                if (ns) {
                    const auto* rt = co_await view.get_table(ddl_ctx, ns->oid, _coll.collection);
                    if (rt) {
                        for (const auto& sub : alter->subcommands()) {
                            switch (sub.kind) {
                                case alter_table_kind::add_column: {
                                    auto col = sub.column;
                                    resolve_builtin(col.type());
                                    // Pre-scan pg_attribute to find max(attnum) for this table.
                                    const collection_full_name_t pg_attr_c{"pg_catalog", "main", "pg_attribute"};
                                    components::types::logical_value_t toid_lv(dctx.resource, rt->oid);
                                    auto [_pa, paf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::read_rows_by_key,
                                                                        ddl_ctx, pg_attr_c,
                                                                        std::vector<std::string>{"attrelid"},
                                                                        std::vector<components::types::logical_value_t>{toid_lv});
                                    auto attr_rows = co_await std::move(paf);
                                    std::int32_t next_attnum = 1;
                                    for (const auto& row : attr_rows) {
                                        if (row.size() < 5 || row[4].is_null()) continue;
                                        auto n = row[4].value<std::int32_t>();
                                        if (n >= next_attnum) next_attnum = n + 1;
                                    }
                                    // Allocate attoid.
                                    auto [_oa, oaf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::allocate_oids_batch,
                                                                        std::size_t{1});
                                    catalog::oid_batch_t att_batch;
                                    att_batch.oids = co_await std::move(oaf);
                                    const catalog::oid_t attoid = att_batch.allocate();
                                    // Build and write pg_attribute row.
                                    std::string typspec = catalog::encode_type_spec(col.type());
                                    std::string defspec;
                                    if (col.has_default_value())
                                        defspec = catalog::encode_default_spec(col.default_value());
                                    // Derive atttypid from the column's logical type so subsequent
                                    // resolve_table calls return a real type rather than UNKNOWN.
                                    // Mirrors build_create_table_writes' atttypid derivation.
                                    const catalog::oid_t atttypid = (col.atttypid() != catalog::INVALID_OID)
                                                                        ? col.atttypid()
                                                                        : catalog::builtin_type_to_oid(col.type().type());
                                    auto att_row = catalog::build_pg_attribute_row(
                                        dctx.resource, attoid, rt->oid, std::string(col.name()),
                                        atttypid, next_attnum,
                                        col.is_not_null(), col.has_default_value(),
                                        false, typspec, defspec);
                                    auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                                      &disk::manager_disk_t::append_pg_catalog_row,
                                                                      ddl_ctx, pg_attr_c, std::move(att_row));
                                    co_await std::move(wf);
                                    // Storage side-effect: update in-memory schema (no catalog writes).
                                    auto [_da, daf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::ddl_add_column,
                                                                        ddl_ctx,
                                                                        rt->oid,
                                                                        std::move(col));
                                    if (auto r = co_await std::move(daf); r.failed()) {
                                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                                        co_return make_ddl_error_cursor(dctx.resource, r);
                                    }
                                    break;
                                }
                                case alter_table_kind::drop_column: {
                                    // Step 1: read pg_attribute row for (table_oid, column_name).
                                    const collection_full_name_t pg_attr_c  {"pg_catalog", "main", "pg_attribute"};
                                    const collection_full_name_t pg_dep_c   {"pg_catalog", "main", "pg_depend"};
                                    const collection_full_name_t pg_idx_c   {"pg_catalog", "main", "pg_index"};
                                    const collection_full_name_t pg_class_c {"pg_catalog", "main", "pg_class"};
                                    const collection_full_name_t pg_con_c   {"pg_catalog", "main", "pg_constraint"};
                                    components::types::logical_value_t toid_lv(dctx.resource, rt->oid);
                                    auto [_pa, paf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::read_rows_by_key,
                                                                        ddl_ctx, pg_attr_c,
                                                                        std::vector<std::string>{"attrelid"},
                                                                        std::vector<components::types::logical_value_t>{toid_lv});
                                    auto attr_rows = co_await std::move(paf);
                                    catalog::oid_t attoid = catalog::INVALID_OID;
                                    std::int32_t attnum = 0;
                                    catalog::oid_t atttypid = catalog::INVALID_OID;
                                    bool att_not_null = false, att_has_default = false;
                                    std::string att_typspec, att_defspec;
                                    for (const auto& row : attr_rows) {
                                        if (row.size() < 10 || row[2].is_null()) continue;
                                        if (row[2].value<std::string_view>() != sub.column_name) continue;
                                        if (!row[7].is_null() && row[7].value<bool>()) continue; // already dropped
                                        attoid          = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                                        atttypid        = row[3].is_null() ? catalog::INVALID_OID : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                                        attnum          = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
                                        att_not_null    = !row[5].is_null() && row[5].value<bool>();
                                        att_has_default = !row[6].is_null() && row[6].value<bool>();
                                        if (!row[8].is_null()) att_typspec = std::string(row[8].value<std::string_view>());
                                        if (!row[9].is_null()) att_defspec = std::string(row[9].value<std::string_view>());
                                        break;
                                    }
                                    if (attoid == catalog::INVALID_OID) break; // column not found, nothing to do
                                    // Step 2: read pg_depend for deps on this column (refclassid=pg_attribute, refobjid=attoid).
                                    components::types::logical_value_t att_cls_lv(dctx.resource,
                                        catalog::well_known_oid::pg_attribute_table);
                                    components::types::logical_value_t att_oid_lv(dctx.resource, attoid);
                                    auto [_pd, pdf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::read_rows_by_key,
                                                                        ddl_ctx, pg_dep_c,
                                                                        std::vector<std::string>{"refclassid", "refobjid"},
                                                                        std::vector<components::types::logical_value_t>{att_cls_lv, att_oid_lv});
                                    auto dep_rows = co_await std::move(pdf);
                                    // Step 3: CASCADE — drop dependent indexes and constraints.
                                    for (const auto& dep_row : dep_rows) {
                                        if (dep_row.size() < 2 || dep_row[0].is_null() || dep_row[1].is_null()) continue;
                                        const auto dep_cls = static_cast<catalog::oid_t>(dep_row[0].value<std::uint32_t>());
                                        const auto dep_oid = static_cast<catalog::oid_t>(dep_row[1].value<std::uint32_t>());
                                        if (dep_cls == catalog::well_known_oid::pg_class_table) {
                                            // Drop index: pg_index + pg_depend + pg_class rows.
                                            auto [_i1, i1f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_idx_c, std::int64_t{0}, dep_oid);
                                            co_await std::move(i1f);
                                            auto [_i2, i2f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_dep_c, std::int64_t{1}, dep_oid);
                                            co_await std::move(i2f);
                                            auto [_i3, i3f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_dep_c, std::int64_t{3}, dep_oid);
                                            co_await std::move(i3f);
                                            auto [_i4, i4f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_class_c, std::int64_t{0}, dep_oid);
                                            co_await std::move(i4f);
                                        } else if (dep_cls == catalog::well_known_oid::pg_constraint_table) {
                                            // Drop constraint: pg_constraint + pg_depend rows.
                                            auto [_c1, c1f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_con_c, std::int64_t{0}, dep_oid);
                                            co_await std::move(c1f);
                                            auto [_c2, c2f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_dep_c, std::int64_t{1}, dep_oid);
                                            co_await std::move(c2f);
                                            auto [_c3, c3f] = actor_zeta::send(dctx.disk_address,
                                                &disk::manager_disk_t::delete_pg_catalog_rows,
                                                ddl_ctx, pg_dep_c, std::int64_t{3}, dep_oid);
                                            co_await std::move(c3f);
                                        }
                                    }
                                    // Step 4: delete original pg_attribute row, insert tombstone.
                                    auto [_d, df] = actor_zeta::send(dctx.disk_address,
                                                                      &disk::manager_disk_t::delete_pg_catalog_rows,
                                                                      ddl_ctx, pg_attr_c, std::int64_t{0}, attoid);
                                    co_await std::move(df);
                                    auto tombstone = catalog::build_pg_attribute_row(
                                        dctx.resource, attoid, rt->oid, std::string(sub.column_name),
                                        atttypid, attnum, att_not_null, att_has_default, true,
                                        att_typspec, att_defspec);
                                    auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                                      &disk::manager_disk_t::append_pg_catalog_row,
                                                                      ddl_ctx, pg_attr_c, std::move(tombstone));
                                    co_await std::move(wf);
                                    break;
                                }
                                case alter_table_kind::rename_column: {
                                    // Read all pg_attribute rows for this table, find the one matching old name.
                                    const collection_full_name_t pg_attr_c{"pg_catalog", "main", "pg_attribute"};
                                    components::types::logical_value_t toid_lv(dctx.resource, rt->oid);
                                    auto [_pa, paf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::read_rows_by_key,
                                                                        ddl_ctx, pg_attr_c,
                                                                        std::vector<std::string>{"attrelid"},
                                                                        std::vector<components::types::logical_value_t>{toid_lv});
                                    auto attr_rows = co_await std::move(paf);
                                    catalog::oid_t attoid = catalog::INVALID_OID;
                                    std::int32_t attnum = 0;
                                    catalog::oid_t atttypid = catalog::INVALID_OID;
                                    bool att_not_null = false, att_has_default = false;
                                    std::string att_typspec, att_defspec;
                                    for (const auto& row : attr_rows) {
                                        if (row.size() < 10 || row[2].is_null()) continue;
                                        if (row[2].value<std::string_view>() != sub.column_name) continue;
                                        if (!row[7].is_null() && row[7].value<bool>()) continue; // dropped
                                        attoid       = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
                                        atttypid     = row[3].is_null() ? catalog::INVALID_OID : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                                        attnum       = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
                                        att_not_null    = !row[5].is_null() && row[5].value<bool>();
                                        att_has_default = !row[6].is_null() && row[6].value<bool>();
                                        if (!row[8].is_null()) att_typspec = std::string(row[8].value<std::string_view>());
                                        if (!row[9].is_null()) att_defspec = std::string(row[9].value<std::string_view>());
                                        break;
                                    }
                                    if (attoid != catalog::INVALID_OID) {
                                        auto [_d, df] = actor_zeta::send(dctx.disk_address,
                                                                          &disk::manager_disk_t::delete_pg_catalog_rows,
                                                                          ddl_ctx, pg_attr_c, std::int64_t{0}, attoid);
                                        co_await std::move(df);
                                        auto new_row = catalog::build_pg_attribute_row(
                                            dctx.resource, attoid, rt->oid, std::string(sub.new_column_name),
                                            atttypid, attnum, att_not_null, att_has_default, false,
                                            att_typspec, att_defspec);
                                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                                          ddl_ctx, pg_attr_c, std::move(new_row));
                                        co_await std::move(wf);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
                break;
            }
            case node_type::create_constraint_t: {
                auto* cstr = static_cast<node_create_constraint_t*>(logical_plan.get());
                const auto& _coll2 = logical_plan->collection_full_name();
                const auto* ns2 = view.try_get_namespace(
                    _coll2.database.empty() ? _coll2.schema : _coll2.database);
                if (ns2) {
                    const auto* rt2 = co_await view.get_table(ddl_ctx, ns2->oid, _coll2.collection);
                    if (rt2) {
                        components::catalog::oid_t ref_oid = components::catalog::INVALID_OID;
                        std::vector<components::catalog::oid_t> fk_col_attoids;
                        std::vector<components::catalog::oid_t> ref_col_attoids;
                        if (cstr->kind() == constraint_kind::foreign_key && !cstr->ref_collection().empty()) {
                            const auto* rrt = co_await view.get_table(ddl_ctx, ns2->oid,
                                                                       cstr->ref_collection().collection);
                            if (rrt) {
                                ref_oid = rrt->oid;
                                // Resolve child (local) column names → attoids.
                                for (const auto& col_name : cstr->columns()) {
                                    for (const auto& ci : rt2->columns) {
                                        if (ci.attname == col_name) {
                                            fk_col_attoids.push_back(ci.attoid);
                                            break;
                                        }
                                    }
                                }
                                // Resolve parent (referenced) column names → attoids.
                                for (const auto& col_name : cstr->ref_columns()) {
                                    for (const auto& ci : rrt->columns) {
                                        if (ci.attname == col_name) {
                                            ref_col_attoids.push_back(ci.attoid);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if (cstr->kind() == constraint_kind::check && cstr->check_expr().empty()) {
                            if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                            co_return make_cursor(dctx.resource,
                                error_code_t::other_error,
                                "CHECK constraint expression is empty or contains unsupported "
                                "constructs (functions, subqueries, and CASE expressions are not "
                                "allowed; valid: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                "column references, and constants)");
                        }
                        const catalog::oid_t constraint_oid = oid_batch.allocate();
                        auto writes = catalog::build_create_constraint_writes(
                            dctx.resource,
                            std::string(cstr->name()), rt2->oid, constraint_oid,
                            static_cast<char>(cstr->kind()), ref_oid,
                            std::move(fk_col_attoids), std::move(ref_col_attoids),
                            cstr->match_type(), cstr->del_action(), cstr->upd_action(),
                            std::string(cstr->check_expr()));
                        for (auto& w : writes) {
                            auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                              &disk::manager_disk_t::append_pg_catalog_row,
                                                              ddl_ctx, std::move(w.table), std::move(w.row));
                            co_await std::move(wf);
                        }
                    }
                }
                break;
            }
            case node_type::sequence_t: {
                for (auto& child : logical_plan->children()) {
                    if (child->type() == node_type::create_collection_t) {
                        // Physical storage creation only — catalog rows are in sibling
                        // primitive_write_t children produced by planner::rewrite_create_table.
                        auto* cc = static_cast<node_create_collection_t*>(child.get());
                        const auto& coll = cc->collection_full_name();
                        auto cols = cc->column_definitions(); // already resolved by enrich_plan
                        if (cols.empty()) {
                            auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                                &disk::manager_disk_t::create_storage,
                                                                session, coll);
                            co_await std::move(csf);
                        } else {
                            auto storage_cols = cols;
                            if (cc->is_disk_storage()) {
                                auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                                    &disk::manager_disk_t::create_storage_disk,
                                                                    session, coll,
                                                                    std::move(storage_cols));
                                co_await std::move(csf);
                            } else {
                                auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                                    &disk::manager_disk_t::create_storage_with_columns,
                                                                    session, coll,
                                                                    std::move(storage_cols));
                                co_await std::move(csf);
                            }
                        }
                        if (dctx.index_address != actor_zeta::address_t::empty_address()) {
                            auto [_ri, rif] = actor_zeta::send(dctx.index_address,
                                                                &index::manager_index_t::register_collection,
                                                                session, coll);
                            co_await std::move(rif);
                        }
                    } else if (child->type() == node_type::primitive_write_t) {
                        auto* pw = static_cast<node_primitive_write_t*>(child.get());
                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                          ddl_ctx,
                                                          pw->catalog_table(),
                                                          std::move(pw->row()));
                        co_await std::move(wf);
                    } else if (child->type() == node_type::primitive_delete_t) {
                        auto* pd = static_cast<node_primitive_delete_t*>(child.get());
                        auto [_d, df] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::delete_pg_catalog_rows,
                                                          ddl_ctx,
                                                          pd->catalog_table(),
                                                          pd->oid_col_idx(),
                                                          pd->target_oid());
                        co_await std::move(df);
                    }
                }
                break;
            }
            default:
                break;
        }

        auto [_f, ff] = actor_zeta::send(dctx.disk_address,
                                          &disk::manager_disk_t::flush,
                                          session, wal::id_t{0});
        co_await std::move(ff);

        if (dctx.wal_address != actor_zeta::address_t::empty_address()) {
            auto db = logical_plan->database_name();
            auto [_c, cf] = actor_zeta::send(dctx.wal_address,
                                              &wal::manager_wal_replicate_t::commit_txn,
                                              session,
                                              txn_data.transaction_id,
                                              wal::wal_sync_mode::FULL,
                                              std::string(db.empty() ? "default" : db));
            co_await std::move(cf);
        }

        if (dctx.wal_address != actor_zeta::address_t::empty_address() &&
            dctx.disk_address != actor_zeta::address_t::empty_address()) {
            auto [_ac, acf] = actor_zeta::send(dctx.wal_address,
                                                &wal::manager_wal_replicate_t::auto_checkpoint_wal_id,
                                                session);
            auto ac_wal_id = co_await std::move(acf);
            if (ac_wal_id > wal::id_t{0}) {
                auto [_cp, cpf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::checkpoint_all,
                                                    session, ac_wal_id);
                auto safe_trunc = co_await std::move(cpf);
                if (safe_trunc > wal::id_t{0}) {
                    auto [_tr, trf] = actor_zeta::send(dctx.wal_address,
                                                        &wal::manager_wal_replicate_t::truncate_before,
                                                        session, safe_trunc);
                    co_await std::move(trf);
                }
            }
        }

        if (txn_data.transaction_id != 0) {
            uint64_t commit_id = dctx.txn_manager.commit(session);
            if (dctx.disk_address != actor_zeta::address_t::empty_address() && commit_id > 0) {
                components::execution_context_t cpa_ctx{session, txn_data, {}};
                auto [_cpa, cpaf] = actor_zeta::send(dctx.disk_address,
                                                      &disk::manager_disk_t::commit_pg_catalog_appends,
                                                      cpa_ctx, commit_id);
                co_await std::move(cpaf);
            }
        }

        co_return make_cursor(dctx.resource, operation_status_t::success);
    }

} // namespace services::dispatcher