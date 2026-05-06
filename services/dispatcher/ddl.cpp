#include "dispatcher.hpp"
#include "catalog_view.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_primitive_write.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/planner/planner.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;

namespace services::dispatcher {

    namespace {

        std::size_t estimate_ddl_oid_count(const node_ptr& /*node*/) {
            return 0;
        }

    } // anonymous namespace

    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    execute_ddl(components::session::session_id_t session,
                node_ptr logical_plan,
                components::table::transaction_data txn_data,
                catalog_view_t& view,
                const ddl_context_t& dctx) {
        components::execution_context_t ddl_ctx{session, txn_data, {}};

        if (const std::size_t need = estimate_ddl_oid_count(logical_plan); need > 0) {
            auto [_, fut] = actor_zeta::send(dctx.disk_address,
                                             &disk::manager_disk_t::allocate_oids_batch,
                                             need);
            components::catalog::oid_batch_t oid_batch;
            oid_batch.oids = co_await std::move(fut);
            components::planner::planner_t planner;
            logical_plan = planner.create_plan(dctx.resource, std::move(logical_plan), std::move(oid_batch));
        }

        switch (logical_plan->type()) {
            case node_type::create_database_t: {
                auto [_d, df] = actor_zeta::send(dctx.disk_address,
                                                  &disk::manager_disk_t::ddl_create_namespace,
                                                  ddl_ctx,
                                                  logical_plan->database_name());
                if (auto r = co_await std::move(df); r.failed()) {
                    if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                    co_return make_ddl_error_cursor(dctx.resource, r);
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
                    auto [_dn, dnf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::ddl_drop_namespace,
                                                        ddl_ctx, rns.oid,
                                                        disk::drop_behavior_t::cascade_);
                    if (auto r = co_await std::move(dnf); r.failed()) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        co_return make_ddl_error_cursor(dctx.resource, r);
                    }
                }
                break;
            }
            case node_type::create_collection_t: {
                auto cc = boost::static_pointer_cast<node_create_collection_t>(logical_plan);
                if (cc->column_definitions().empty()) {
                    auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                       &disk::manager_disk_t::create_storage,
                                                       session,
                                                       logical_plan->collection_full_name());
                    co_await std::move(csf);
                } else {
                    std::vector<components::table::column_definition_t> storage_columns =
                        cc->column_definitions();
                    for (auto& col : storage_columns) {
                        auto& col_type = col.type();
                        if (col_type.type() == components::types::logical_type::UNKNOWN) {
                            co_await view.get_type(ddl_ctx,
                                                          components::catalog::well_known_oid::public_namespace,
                                                          std::string(col_type.type_name()));
                            co_await view.get_type(ddl_ctx,
                                                          components::catalog::well_known_oid::pg_catalog_namespace,
                                                          std::string(col_type.type_name()));
                            const auto* rt = view.try_get_type(
                                components::catalog::well_known_oid::public_namespace,
                                std::string_view(col_type.type_name()));
                            if (!rt) {
                                rt = view.try_get_type(
                                    components::catalog::well_known_oid::pg_catalog_namespace,
                                    std::string_view(col_type.type_name()));
                            }
                            if (rt) {
                                std::string alias = col_type.has_alias() ? col_type.alias() : std::string{};
                                col_type = rt->type;
                                if (!alias.empty()) col_type.set_alias(alias);
                            }
                        }
                        if (col_type.type() == components::types::logical_type::STRUCT) {
                            for (auto& field : col_type.child_types()) {
                                if (field.type() == components::types::logical_type::UNKNOWN) {
                                    co_await view.get_type(ddl_ctx,
                                                                  components::catalog::well_known_oid::public_namespace,
                                                                  std::string(field.type_name()));
                                    co_await view.get_type(ddl_ctx,
                                                                  components::catalog::well_known_oid::pg_catalog_namespace,
                                                                  std::string(field.type_name()));
                                    const auto* rt = view.try_get_type(
                                        components::catalog::well_known_oid::public_namespace,
                                        std::string_view(field.type_name()));
                                    if (!rt) {
                                        rt = view.try_get_type(
                                            components::catalog::well_known_oid::pg_catalog_namespace,
                                            std::string_view(field.type_name()));
                                    }
                                    if (rt) {
                                        std::string fa = field.has_alias() ? field.alias() : std::string{};
                                        field = rt->type;
                                        if (!fa.empty()) field.set_alias(fa);
                                    }
                                }
                            }
                        }
                    }
                    if (cc->is_disk_storage()) {
                        auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                           &disk::manager_disk_t::create_storage_disk,
                                                           session,
                                                           logical_plan->collection_full_name(),
                                                           std::move(storage_columns));
                        co_await std::move(csf);
                    } else {
                        auto [_cs, csf] = actor_zeta::send(dctx.disk_address,
                                                           &disk::manager_disk_t::create_storage_with_columns,
                                                           session,
                                                           logical_plan->collection_full_name(),
                                                           std::move(storage_columns));
                        co_await std::move(csf);
                    }
                }
                if (dctx.index_address != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] = actor_zeta::send(dctx.index_address,
                                                       &index::manager_index_t::register_collection,
                                                       session, logical_plan->collection_full_name());
                    co_await std::move(rif);
                }
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx, logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    std::vector<components::table::column_definition_t> ddl_cols =
                        cc->column_definitions().empty()
                            ? cc->column_definitions()
                            : cc->column_definitions();
                    if (!ddl_cols.empty()) {
                        for (auto& col : ddl_cols) {
                            auto& col_type = col.type();
                            if (col_type.type() == components::types::logical_type::UNKNOWN) {
                                const auto* rt = view.try_get_type(
                                    components::catalog::well_known_oid::public_namespace,
                                    std::string_view(col_type.type_name()));
                                if (!rt) {
                                    rt = view.try_get_type(
                                        components::catalog::well_known_oid::pg_catalog_namespace,
                                        std::string_view(col_type.type_name()));
                                }
                                if (rt) {
                                    std::string alias = col_type.has_alias() ? col_type.alias() : std::string{};
                                    col_type = rt->type;
                                    if (!alias.empty()) col_type.set_alias(alias);
                                }
                            }
                            if (col_type.type() == components::types::logical_type::STRUCT) {
                                for (auto& field : col_type.child_types()) {
                                    if (field.type() == components::types::logical_type::UNKNOWN) {
                                        const auto* rt = view.try_get_type(
                                            components::catalog::well_known_oid::public_namespace,
                                            std::string_view(field.type_name()));
                                        if (!rt) {
                                            rt = view.try_get_type(
                                                components::catalog::well_known_oid::pg_catalog_namespace,
                                                std::string_view(field.type_name()));
                                        }
                                        if (rt) {
                                            std::string fa = field.has_alias() ? field.alias() : std::string{};
                                            field = rt->type;
                                            if (!fa.empty()) field.set_alias(fa);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    const char rk = ddl_cols.empty() ? relkind::computed : relkind::regular;
                    auto [_dt, dtf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::ddl_create_table,
                                                        ddl_ctx, rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::move(ddl_cols), rk);
                    if (auto r = co_await std::move(dtf); r.failed()) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        co_return make_ddl_error_cursor(dctx.resource, r);
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
                        auto [_dd, ddf] = actor_zeta::send(dctx.disk_address,
                                                            &disk::manager_disk_t::ddl_drop_table,
                                                            ddl_ctx, rt.oid,
                                                            disk::drop_behavior_t::cascade_);
                        auto drop_r = co_await std::move(ddf);
                        if (drop_r.failed())
                            co_return make_ddl_error_cursor(dctx.resource, drop_r);
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
                    auto [_dd, ddf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::ddl_create_sequence,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        seq->start(), seq->increment(),
                                                        seq->min_value(), seq->max_value(),
                                                        /*cycle=*/false);
                    if (auto r = co_await std::move(ddf); r.failed()) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        co_return make_ddl_error_cursor(dctx.resource, r);
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
                    auto [_dd, ddf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::ddl_create_view,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::string(vw->query_sql()));
                    if (auto r = co_await std::move(ddf); r.failed()) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        co_return make_ddl_error_cursor(dctx.resource, r);
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
                    auto [_dd, ddf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::ddl_create_macro,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::string(mc->body_sql()));
                    if (auto r = co_await std::move(ddf); r.failed()) {
                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                        co_return make_ddl_error_cursor(dctx.resource, r);
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
                        auto method = logical_plan->type() == node_type::drop_sequence_t
                                          ? &disk::manager_disk_t::ddl_drop_sequence
                                      : logical_plan->type() == node_type::drop_view_t
                                          ? &disk::manager_disk_t::ddl_drop_view
                                          : &disk::manager_disk_t::ddl_drop_macro;
                        auto [_dd, ddf] = actor_zeta::send(dctx.disk_address,
                                                            method, ddl_ctx, rt.oid,
                                                            disk::drop_behavior_t::cascade_);
                        auto drop_r = co_await std::move(ddf);
                        if (drop_r.failed())
                            co_return make_ddl_error_cursor(dctx.resource, drop_r);
                    }
                }
                break;
            }
            case node_type::alter_table_t: {
                auto* alter = static_cast<node_alter_table_t*>(logical_plan.get());
                auto [_rn, rnf] = actor_zeta::send(dctx.disk_address,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(dctx.disk_address,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        for (const auto& sub : alter->subcommands()) {
                            switch (sub.kind) {
                                case alter_table_kind::add_column: {
                                    auto [_da, daf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::ddl_add_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        sub.column);
                                    if (auto r = co_await std::move(daf); r.failed()) {
                                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                                        co_return make_ddl_error_cursor(dctx.resource, r);
                                    }
                                    break;
                                }
                                case alter_table_kind::drop_column: {
                                    auto [_dc, dcf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::ddl_drop_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        std::string(sub.column_name),
                                                                        disk::drop_behavior_t::cascade_);
                                    if (auto r = co_await std::move(dcf); r.failed()) {
                                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                                        co_return make_ddl_error_cursor(dctx.resource, r);
                                    }
                                    break;
                                }
                                case alter_table_kind::rename_column: {
                                    auto [_dr, drf] = actor_zeta::send(dctx.disk_address,
                                                                        &disk::manager_disk_t::ddl_rename_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        std::string(sub.column_name),
                                                                        std::string(sub.new_column_name));
                                    if (auto r = co_await std::move(drf); r.failed()) {
                                        if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                                        co_return make_ddl_error_cursor(dctx.resource, r);
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
                auto [_rn2, rnf2] = actor_zeta::send(dctx.disk_address,
                                                      &disk::manager_disk_t::resolve_namespace,
                                                      ddl_ctx,
                                                      logical_plan->database_name(),
                                                      std::uint64_t{0});
                auto rns2 = co_await std::move(rnf2);
                if (rns2.found) {
                    auto [_rt2, rtf2] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::resolve_table,
                                                          ddl_ctx,
                                                          rns2.oid,
                                                          logical_plan->collection_name(),
                                                          std::uint64_t{0});
                    auto rt2 = co_await std::move(rtf2);
                    if (rt2.found) {
                        components::catalog::oid_t ref_oid = components::catalog::INVALID_OID;
                        if (cstr->kind() == constraint_kind::foreign_key && !cstr->ref_collection().empty()) {
                            auto [_rrt, rrtf] = actor_zeta::send(dctx.disk_address,
                                                                   &disk::manager_disk_t::resolve_table,
                                                                   ddl_ctx,
                                                                   rns2.oid,
                                                                   std::string(cstr->ref_collection().collection),
                                                                   std::uint64_t{0});
                            auto rrt = co_await std::move(rrtf);
                            if (rrt.found) ref_oid = rrt.oid;
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
                        auto [_dc, dcf] = actor_zeta::send(dctx.disk_address,
                                                            &disk::manager_disk_t::ddl_create_constraint,
                                                            ddl_ctx,
                                                            rt2.oid,
                                                            std::string(cstr->name()),
                                                            static_cast<char>(cstr->kind()),
                                                            ref_oid,
                                                            std::vector<components::catalog::oid_t>{},
                                                            std::vector<components::catalog::oid_t>{},
                                                            cstr->match_type(),
                                                            cstr->del_action(),
                                                            cstr->upd_action(),
                                                            std::string(cstr->check_expr()));
                        if (auto r = co_await std::move(dcf); r.failed()) {
                            if (txn_data.transaction_id != 0) dctx.txn_manager.abort(session);
                            co_return make_ddl_error_cursor(dctx.resource, r);
                        }
                    }
                }
                break;
            }
            case node_type::sequence_t: {
                for (auto& child : logical_plan->children()) {
                    if (child->type() == node_type::primitive_write_t) {
                        auto* pw = static_cast<node_primitive_write_t*>(child.get());
                        auto [_w, wf] = actor_zeta::send(dctx.disk_address,
                                                          &disk::manager_disk_t::append_pg_catalog_row,
                                                          ddl_ctx,
                                                          pw->catalog_table(),
                                                          std::move(pw->row()));
                        co_await std::move(wf);
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