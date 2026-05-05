#include "executor.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <components/logical_plan/forward.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>

using namespace components::cursor;

namespace {

// Walk through planner-added constraint wrapper nodes (check_constraint,
// sequence) to find the base DML node type.
// Needed because the planner may wrap insert/update/delete with constraint nodes,
// changing the top-level type from insert_t to e.g. check_constraint_t.
components::logical_plan::node_type find_effective_dml_type(
    const components::logical_plan::node_ptr& plan) {
    using namespace components::logical_plan;
    auto* n = plan.get();
    while (n) {
        switch (n->type()) {
        case node_type::check_constraint_t:
            if (!n->children().empty()) {
                n = n->children().front().get();
                continue;
            }
            return n->type();
        default:
            return n->type();
        }
    }
    return node_type::unused;
}

} // namespace

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage,
                   components::logical_plan::limit_t limit)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage)
        , limit(limit) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           actor_zeta::address_t index_address,
                           components::table::transaction_manager_t* txn_manager,
                           log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
        , txn_manager_(txn_manager)
        , log_(log)
        , pending_void_(resource) {
        register_default_functions(function_registry_);
    }

    actor_zeta::behavior_t executor_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();
        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::execute_ddl_plan>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_ddl_plan, msg);
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::register_udf>: {
                co_await actor_zeta::dispatch(this, &executor_t::register_udf, msg);
                break;
            }
            default:
                break;
        }
    }

    void executor_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->available()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
    }

    auto executor_t::make_type() const noexcept -> const char* { return "executor"; }

    static execute_result_t make_ddl_error(std::pmr::memory_resource* res, const services::disk::ddl_result_t& r) {
        std::string msg;
        if (r.status == services::disk::ddl_status::restrict_blocked) {
            msg = "cannot drop: other objects depend on it (blocking oid " +
                  std::to_string(static_cast<unsigned>(r.blocking_oid)) + ")";
        } else if (r.status == services::disk::ddl_status::cycle_detected) {
            msg = "cannot drop: dependency cycle detected (at oid " +
                  std::to_string(static_cast<unsigned>(r.blocking_oid)) + ")";
        } else if (r.status == services::disk::ddl_status::not_found) {
            msg = "object does not exist";
        } else {
            msg = "DDL operation failed";
        }
        return execute_result_t{make_cursor(res, error_code_t::other_error, std::move(msg)), {}};
    }

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan(components::session::session_id_t session,
                             components::logical_plan::node_ptr logical_plan,
                             components::logical_plan::storage_parameters parameters,
                             services::context_storage_t context_storage,
                             components::table::transaction_data txn) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        // Handle index operations directly from the logical plan (no physical operator needed)
        using namespace components::logical_plan;
        if (logical_plan->type() == node_type::create_index_t) {
            auto* node_ci = static_cast<node_create_index_t*>(logical_plan.get());
            auto coll_name = logical_plan->collection_full_name();

            if (!coll_name.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
                auto [_ix, ixf] = actor_zeta::send(index_address_,
                                                   &index::manager_index_t::create_index,
                                                   session,
                                                   coll_name,
                                                   index::index_name_t(node_ci->name()),
                                                   node_ci->keys(),
                                                   node_ci->type());
                auto id_index = co_await std::move(ixf);

                if (id_index == components::index::INDEX_ID_UNDEFINED) {
                    trace(log_, "executor: index {} already exists, returning error", node_ci->name());
                    co_return execute_result_t{
                        make_cursor(resource(), error_code_t::index_create_fail, "index already exists"),
                        {}};
                }

                // Backfill: populate new index with existing data from storage
                auto [_tr, trf] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::storage_total_rows, session, coll_name);
                auto total_rows = co_await std::move(trf);

                if (total_rows > 0) {
                    auto [_ss, ssf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::storage_scan_segment,
                                                       session,
                                                       coll_name,
                                                       int64_t{0},
                                                       total_rows);
                    auto scan_data = co_await std::move(ssf);

                    if (scan_data) {
                        auto count = scan_data->size();
                        auto [_ir, irf] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::insert_rows,
                                                           index::execution_context_t{session, txn, coll_name},
                                                           std::move(scan_data),
                                                           uint64_t{0},
                                                           count);
                        co_await std::move(irf);
                    }
                }

                // Persist index metadata in pg_class + pg_index + pg_depend so the index
                // is rediscoverable across restarts via catalog_view_t / resolve_table.
                if (disk_address_ != actor_zeta::address_t::empty_address()) {
                    components::execution_context_t ddl_ctx{session, txn, coll_name};
                    auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_namespace,
                                                        ddl_ctx,
                                                        std::string(coll_name.database),
                                                        std::uint64_t{0});
                    auto rns = co_await std::move(rnf);
                    if (rns.found) {
                        auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::resolve_table,
                                                            ddl_ctx,
                                                            rns.oid,
                                                            std::string(coll_name.collection),
                                                            std::uint64_t{0});
                        auto rt = co_await std::move(rtf);
                        if (rt.found) {
                            std::vector<std::string> column_names;
                            column_names.reserve(node_ci->keys().size());
                            for (auto& key : node_ci->keys()) {
                                column_names.emplace_back(key.as_string());
                            }
                            auto [_di, dif] = actor_zeta::send(disk_address_,
                                                                &disk::manager_disk_t::ddl_create_index,
                                                                ddl_ctx,
                                                                rns.oid,
                                                                rt.oid,
                                                                std::string(node_ci->name()),
                                                                std::move(column_names));
                            auto created = co_await std::move(dif);
                            // Decision 3: backfill completed above (insert_rows + index build) —
                            // flip indisvalid=true so the optimizer starts using this index.
                            if (created.created_oid != components::catalog::INVALID_OID) {
                                auto [_sv, svf] = actor_zeta::send(disk_address_,
                                                                    &disk::manager_disk_t::ddl_index_set_valid,
                                                                    ddl_ctx,
                                                                    created.created_oid,
                                                                    true);
                                if (auto r = co_await std::move(svf); r.failed())
                                    trace(log_, "ddl_index_set_valid failed: status={}, blocker={}", static_cast<int>(r.status), r.blocking_oid);
                            }
                        }
                    }
                }
            }
            co_return execute_result_t{make_cursor(resource(), operation_status_t::success), {}};
        }

        if (logical_plan->type() == node_type::drop_index_t) {
            auto* node_di = static_cast<node_drop_index_t*>(logical_plan.get());
            auto coll_name = logical_plan->collection_full_name();

            if (!coll_name.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
                auto [_ix, ixf] = actor_zeta::send(index_address_,
                                                   &index::manager_index_t::drop_index,
                                                   session,
                                                   coll_name,
                                                   index::index_name_t(node_di->name()));
                co_await std::move(ixf);
            }
            // pg_index/pg_class teardown for the dropped index. We resolve by name to get
            // the index oid, then ddl_drop_index handles the cascade.
            if (disk_address_ != actor_zeta::address_t::empty_address()) {
                components::execution_context_t ddl_ctx{session, txn, coll_name};
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    std::string(coll_name.database),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_ri, rif] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        std::string(node_di->name()),
                                                        std::uint64_t{0});
                    auto ri = co_await std::move(rif);
                    if (ri.found) {
                        auto [_di, dif] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::ddl_drop_index,
                                                            ddl_ctx,
                                                            ri.oid,
                                                            disk::drop_behavior_t::cascade_);
                        if (auto r = co_await std::move(dif); r.failed())
                            co_return make_ddl_error(resource(), r);
                    }
                }
            }
            co_return execute_result_t{make_cursor(resource(), operation_status_t::success), {}};
        }

        // Determine if this is a DML operation.
        // find_effective_dml_type unwraps planner-added constraint wrapper nodes so
        // that is_dml is correct even when the plan is wrapped by check_constraint etc.
        const auto effective_type = find_effective_dml_type(logical_plan);
        bool is_dml = (effective_type == node_type::insert_t || effective_type == node_type::update_t ||
                       effective_type == node_type::delete_t);

        // Step 1: Begin transaction for DML (executor owns full lifecycle)
        // Direct call to txn_manager_ avoids static_cast to dispatcher and bypasses actor mutex.
        // Safe: txn methods only touch txn_manager_ (own mutex) + are synchronous.
        components::table::transaction_data txn_data = txn;
        if (is_dml) {
            txn_data = txn_manager_->begin_transaction(session).data();
            trace(log_, "executor::execute_plan: began txn {}", txn_data.transaction_id);
        }

        auto limit = components::logical_plan::limit_t::unlimit();
        for (const auto& child : logical_plan->children()) {
            if (child->type() == components::logical_plan::node_type::limit_t) {
                limit = static_cast<components::logical_plan::node_limit_t*>(child.get())->limit();
            }
        }

        components::operators::operator_ptr plan =
            planner::create_plan(context_storage, function_registry_, logical_plan, limit, &parameters);

        if (!plan) {
            if (is_dml) {
                txn_manager_->abort(session);
            }
            co_return execute_result_t{
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"),
                {}};
        }

        plan->set_as_root();

        auto plan_data = traverse_plan_(std::move(plan), std::move(parameters), std::move(context_storage));
        plan_data.limit = limit;

        // Step 2: Execute physical plan
        auto result = co_await execute_sub_plan_(session, std::move(plan_data), txn_data);

        if (is_dml && result.cursor->is_success()) {
            // CHECK constraint enforcement is now handled by operator_check_constraint_t
            // (planner-inserted into the physical plan). No post-execution disk round-trip needed.

            // Step 3: WAL DATA (physical format — stores post-compute data for direct replay)
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                auto& cname = result.wal_collection;
                switch (effective_type) {
                    case node_type::insert_t: {
                        if (result.append_row_count == 0 || !result.wal_insert_data) {
                            trace(log_, "executor::execute_plan: INSERT produced 0 rows, skipping WAL");
                            break;
                        }
                        trace(log_, "executor::execute_plan: WAL physical_insert");
                        auto [_w1, wf1] = actor_zeta::send(wal_address_,
                                                           &wal::manager_wal_replicate_t::write_physical_insert,
                                                           session,
                                                           std::string(cname.database),
                                                           std::string(cname.collection),
                                                           std::move(result.wal_insert_data),
                                                           static_cast<uint64_t>(result.append_row_start),
                                                           result.append_row_count,
                                                           txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf1);
                        auto [_d1, df1] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                        pending_void_.push_back(std::move(df1));
                        break;
                    }
                    case node_type::update_t: {
                        trace(log_, "executor::execute_plan: WAL physical_update");
                        auto upd_count = static_cast<uint64_t>(result.wal_row_ids.size());
                        auto [_w2, wf2] = actor_zeta::send(wal_address_,
                                                           &wal::manager_wal_replicate_t::write_physical_update,
                                                           session,
                                                           std::string(cname.database),
                                                           std::string(cname.collection),
                                                           std::move(result.wal_row_ids),
                                                           std::move(result.wal_update_data),
                                                           upd_count,
                                                           txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf2);
                        auto [_d2, df2] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                        pending_void_.push_back(std::move(df2));
                        break;
                    }
                    case node_type::delete_t: {
                        trace(log_, "executor::execute_plan: WAL physical_delete");
                        auto count = static_cast<uint64_t>(result.wal_row_ids.size());
                        auto [_w3, wf3] = actor_zeta::send(wal_address_,
                                                           &wal::manager_wal_replicate_t::write_physical_delete,
                                                           session,
                                                           std::string(cname.database),
                                                           std::string(cname.collection),
                                                           std::move(result.wal_row_ids),
                                                           count,
                                                           txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf3);
                        auto [_d3, df3] =
                            actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                        pending_void_.push_back(std::move(df3));
                        break;
                    }
                    default:
                        break;
                }
            }

            // Step 4: Commit transaction
            uint64_t commit_id = txn_manager_->commit(session);
            trace(log_, "executor::execute_plan: committed txn {}, commit_id {}", txn_data.transaction_id, commit_id);

            // Step 5: Commit side-effects on storage and index
            auto coll_name = logical_plan->collection_full_name();
            if (result.append_row_count > 0 && commit_id > 0) {
                components::execution_context_t ctx{session, txn_data, coll_name};
                auto [_ca, caf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_commit_append,
                                                   ctx,
                                                   commit_id,
                                                   result.append_row_start,
                                                   result.append_row_count);
                co_await std::move(caf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ci, cif] =
                        actor_zeta::send(index_address_, &index::manager_index_t::commit_insert, ctx, commit_id);
                    co_await std::move(cif);
                }
            }
            if (result.delete_txn_id != 0 && commit_id > 0) {
                components::execution_context_t del_ctx{session, txn_data, coll_name};
                auto [_cd, cdf] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::storage_commit_delete, del_ctx, commit_id);
                co_await std::move(cdf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_cdi, cdif] =
                        actor_zeta::send(index_address_, &index::manager_index_t::commit_delete, del_ctx, commit_id);
                    co_await std::move(cdif);
                }
                // Fire-and-forget auto-GC check
                auto lowest = txn_manager_->lowest_active_start_time();
                auto [gc_sched, gc_fut] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::maybe_cleanup, del_ctx, lowest);
                pending_void_.push_back(std::move(gc_fut));
            }

            // Step 6: WAL COMMIT marker
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                auto [_wc, wcf] = actor_zeta::send(wal_address_,
                                                   &wal::manager_wal_replicate_t::commit_txn,
                                                   session,
                                                   txn_data.transaction_id,
                                                   wal::wal_sync_mode::NORMAL,
                                                   std::string(coll_name.database));
                co_await std::move(wcf);
            }

            co_return execute_result_t{std::move(result.cursor), std::move(result.updates)};

        } else if (is_dml && result.cursor->is_error()) {
            // Abort path
            trace(log_, "executor::execute_plan: DML error, aborting txn");
            auto coll_name = logical_plan->collection_full_name();
            if (result.append_row_count > 0) {
                components::execution_context_t abort_ctx{session, txn_data, coll_name};
                auto [_ra, raf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_revert_append,
                                                   abort_ctx,
                                                   result.append_row_start,
                                                   result.append_row_count);
                co_await std::move(raf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] =
                        actor_zeta::send(index_address_, &index::manager_index_t::revert_insert, abort_ctx);
                    co_await std::move(rif);
                }
            }
            txn_manager_->abort(session);
        }

        co_return execute_result_t{std::move(result.cursor), std::move(result.updates)};
    }

    // Variant E: route DDL to disk's ddl_* under a real transaction so the pg_catalog
    // mutations carry MVCC + WAL trace. Begin a txn, dispatch by plan->type, commit.

    executor_t::unique_future<execute_result_t>
    executor_t::execute_ddl_plan(components::session::session_id_t session,
                                  components::logical_plan::node_ptr logical_plan,
                                  components::table::transaction_data /*txn*/) {
        using namespace components::logical_plan;
        debug(log_, "executor::execute_ddl_plan ENTRY, session: {}, type: {}",
              session.data(), to_string(logical_plan->type()));

        components::table::transaction_data txn_data{0, 0};
        if (txn_manager_ != nullptr) {
            txn_data = txn_manager_->begin_transaction(session).data();
        }
        components::execution_context_t ddl_ctx{session, txn_data, {}};
        auto ok_cursor = [&] {
            return execute_result_t{make_cursor(resource(), operation_status_t::success), {}};
        };
        auto on_ddl_fail = [&](const services::disk::ddl_result_t& r) -> execute_result_t {
            if (txn_manager_ != nullptr && txn_data.transaction_id != 0)
                txn_manager_->abort(session);
            return make_ddl_error(resource(), r);
        };

        switch (logical_plan->type()) {
            case node_type::create_database_t: {
                auto [_d, df] = actor_zeta::send(disk_address_,
                                                  &disk::manager_disk_t::ddl_create_namespace,
                                                  ddl_ctx,
                                                  logical_plan->database_name());
                if (auto r = co_await std::move(df); r.failed())
                    co_return on_ddl_fail(r);
                break;
            }
            case node_type::drop_database_t: {
                auto db_name = logical_plan->database_name();
                // Cascade-drop user-side storages (the dispatcher's collections_ set is
                // not visible from here; rely on pg_class scan via resolve to find them).
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    db_name,
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_dn, dnf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_drop_namespace,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        disk::drop_behavior_t::cascade_);
                    if (auto r = co_await std::move(dnf); r.failed())
                        co_return on_ddl_fail(r);
                }
                break;
            }
            case node_type::create_collection_t: {
                auto cc = boost::static_pointer_cast<node_create_collection_t>(logical_plan);
                // Create the user-side storage entry (disk).
                if (cc->column_definitions().empty()) {
                    auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                       &disk::manager_disk_t::create_storage,
                                                       session,
                                                       logical_plan->collection_full_name());
                    co_await std::move(csf);
                } else {
                    std::vector<components::table::column_definition_t> storage_columns =
                        cc->column_definitions();
                    if (cc->is_disk_storage()) {
                        auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                           &disk::manager_disk_t::create_storage_disk,
                                                           session,
                                                           logical_plan->collection_full_name(),
                                                           std::move(storage_columns));
                        co_await std::move(csf);
                    } else {
                        auto [_cs, csf] = actor_zeta::send(disk_address_,
                                                           &disk::manager_disk_t::create_storage_with_columns,
                                                           session,
                                                           logical_plan->collection_full_name(),
                                                           std::move(storage_columns));
                        co_await std::move(csf);
                    }
                }
                // Register in index manager.
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::register_collection,
                                                       session,
                                                       logical_plan->collection_full_name());
                    co_await std::move(rif);
                }
                // pg_catalog mutation.
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    std::vector<components::table::column_definition_t> ddl_cols = cc->column_definitions();
                    const char rk = ddl_cols.empty()
                        ? components::catalog::relkind::computed
                        : components::catalog::relkind::regular;
                    auto [_dt, dtf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_create_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::move(ddl_cols),
                                                        rk);
                    if (auto r = co_await std::move(dtf); r.failed())
                        co_return on_ddl_fail(r);
                }
                break;
            }
            case node_type::drop_collection_t: {
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ui, uif] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::unregister_collection,
                                                       session,
                                                       logical_plan->collection_full_name());
                    co_await std::move(uif);
                }
                auto [_ds, dsf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::drop_storage,
                                                   session,
                                                   logical_plan->collection_full_name());
                co_await std::move(dsf);
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        auto [_dd, ddf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::ddl_drop_table,
                                                            ddl_ctx,
                                                            rt.oid,
                                                            disk::drop_behavior_t::cascade_);
                        if (auto r = co_await std::move(ddf); r.failed())
                            co_return on_ddl_fail(r);
                    }
                }
                break;
            }
            case node_type::create_sequence_t: {
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* seq = static_cast<const components::logical_plan::node_create_sequence_t*>(logical_plan.get());
                    auto [_dd, ddf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_create_sequence,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        seq->start(), seq->increment(),
                                                        seq->min_value(), seq->max_value(),
                                                        /*cycle=*/false);
                    if (auto r = co_await std::move(ddf); r.failed())
                        co_return on_ddl_fail(r);
                }
                break;
            }
            case node_type::create_view_t: {
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* vw = static_cast<const components::logical_plan::node_create_view_t*>(logical_plan.get());
                    auto [_dd, ddf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_create_view,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::string(vw->query_sql()));
                    if (auto r = co_await std::move(ddf); r.failed())
                        co_return on_ddl_fail(r);
                }
                break;
            }
            case node_type::create_macro_t: {
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto* mc = static_cast<const components::logical_plan::node_create_macro_t*>(logical_plan.get());
                    auto [_dd, ddf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::ddl_create_macro,
                                                        ddl_ctx, rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::string(mc->body_sql()));
                    if (auto r = co_await std::move(ddf); r.failed())
                        co_return on_ddl_fail(r);
                }
                break;
            }
            case node_type::drop_sequence_t:
            case node_type::drop_view_t:
            case node_type::drop_macro_t: {
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        std::string(logical_plan->collection_name()),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        auto method = logical_plan->type() == node_type::drop_sequence_t
                                          ? &disk::manager_disk_t::ddl_drop_sequence
                                      : logical_plan->type() == node_type::drop_view_t
                                          ? &disk::manager_disk_t::ddl_drop_view
                                          : &disk::manager_disk_t::ddl_drop_macro;
                        auto [_dd, ddf] = actor_zeta::send(disk_address_,
                                                            method,
                                                            ddl_ctx,
                                                            rt.oid,
                                                            disk::drop_behavior_t::cascade_);
                        if (auto r = co_await std::move(ddf); r.failed())
                            co_return on_ddl_fail(r);
                    }
                }
                break;
            }
            case node_type::create_constraint_t: {
                auto* cstr = static_cast<components::logical_plan::node_create_constraint_t*>(logical_plan.get());
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        components::catalog::oid_t ref_oid = components::catalog::INVALID_OID;
                        if (cstr->kind() == components::logical_plan::constraint_kind::foreign_key &&
                            !cstr->ref_collection().empty()) {
                            // Resolve referenced table — same namespace assumed; future work could
                            // accept a fully-qualified ref_collection.
                            auto [_rrt, rrtf] = actor_zeta::send(disk_address_,
                                                                   &disk::manager_disk_t::resolve_table,
                                                                   ddl_ctx,
                                                                   rns.oid,
                                                                   std::string(cstr->ref_collection().collection),
                                                                   std::uint64_t{0});
                            auto rrt = co_await std::move(rrtf);
                            if (rrt.found) {
                                ref_oid = rrt.oid;
                            }
                        }
                        // P2.5: reject CHECK expressions with unsupported node types
                        if (cstr->kind() == components::logical_plan::constraint_kind::check
                            && cstr->check_expr().empty()) {
                            if (txn_manager_ != nullptr && txn_data.transaction_id != 0)
                                txn_manager_->abort(session);
                            co_return execute_result_t{
                                make_cursor(resource(), error_code_t::other_error,
                                    "CHECK constraint expression is empty or contains unsupported "
                                    "constructs (functions, subqueries, and CASE expressions are not "
                                    "allowed; valid: comparisons, AND/OR/NOT, IS NULL/IS NOT NULL, "
                                    "column references, and constants)"),
                                {}};
                        }
                        auto [_dc, dcf] = actor_zeta::send(disk_address_,
                                                            &disk::manager_disk_t::ddl_create_constraint,
                                                            ddl_ctx,
                                                            rt.oid,
                                                            std::string(cstr->name()),
                                                            static_cast<char>(cstr->kind()),
                                                            ref_oid,
                                                            std::vector<components::catalog::oid_t>{},
                                                            std::vector<components::catalog::oid_t>{},
                                                            cstr->match_type(),
                                                            cstr->del_action(),
                                                            cstr->upd_action(),
                                                            std::string(cstr->check_expr()));
                        if (auto r = co_await std::move(dcf); r.failed())
                            co_return on_ddl_fail(r);
                    }
                }
                break;
            }
            case node_type::alter_table_t: {
                auto* alter = static_cast<components::logical_plan::node_alter_table_t*>(logical_plan.get());
                auto [_rn, rnf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::resolve_namespace,
                                                    ddl_ctx,
                                                    logical_plan->database_name(),
                                                    std::uint64_t{0});
                auto rns = co_await std::move(rnf);
                if (rns.found) {
                    auto [_rt, rtf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::resolve_table,
                                                        ddl_ctx,
                                                        rns.oid,
                                                        logical_plan->collection_name(),
                                                        std::uint64_t{0});
                    auto rt = co_await std::move(rtf);
                    if (rt.found) {
                        // Iterate every subcommand under one transaction; partial failures
                        // roll back via WAL abort_txn since we share txn_data across all sub-DDLs.
                        for (const auto& sub : alter->subcommands()) {
                            switch (sub.kind) {
                                case components::logical_plan::alter_table_kind::add_column: {
                                    auto [_da, daf] = actor_zeta::send(disk_address_,
                                                                        &disk::manager_disk_t::ddl_add_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        sub.column);
                                    if (auto r = co_await std::move(daf); r.failed())
                                        co_return on_ddl_fail(r);
                                    break;
                                }
                                case components::logical_plan::alter_table_kind::drop_column: {
                                    auto [_dc, dcf] = actor_zeta::send(disk_address_,
                                                                        &disk::manager_disk_t::ddl_drop_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        std::string(sub.column_name),
                                                                        disk::drop_behavior_t::cascade_);
                                    if (auto r = co_await std::move(dcf); r.failed())
                                        co_return on_ddl_fail(r);
                                    break;
                                }
                                case components::logical_plan::alter_table_kind::rename_column: {
                                    auto [_dr, drf] = actor_zeta::send(disk_address_,
                                                                        &disk::manager_disk_t::ddl_rename_column,
                                                                        ddl_ctx,
                                                                        rt.oid,
                                                                        std::string(sub.column_name),
                                                                        std::string(sub.new_column_name));
                                    if (auto r = co_await std::move(drf); r.failed())
                                        co_return on_ddl_fail(r);
                                    break;
                                }
                            }
                        }
                    }
                }
                break;
            }
            default:
                break;
        }

        auto [_f, ff] = actor_zeta::send(disk_address_,
                                          &disk::manager_disk_t::flush,
                                          session,
                                          services::wal::id_t{0});
        co_await std::move(ff);

        // commit_txn → WAL (FULL fsync) so the DDL records are durable before returning.
        // Two markers per DDL: pg_catalog (where ddl_* writes its rows) and the user db
        // (where storage create/drop happened). Both must be committed for replay to
        // pick up the records.
        if (wal_address_ != actor_zeta::address_t::empty_address()) {
            auto [_pc, pcf] = actor_zeta::send(wal_address_,
                                                &wal::manager_wal_replicate_t::commit_txn,
                                                session,
                                                txn_data.transaction_id,
                                                wal::wal_sync_mode::FULL,
                                                std::string("pg_catalog"));
            co_await std::move(pcf);

            auto db = logical_plan->database_name();
            if (!db.empty() && db != "pg_catalog") {
                auto [_c, cf] = actor_zeta::send(wal_address_,
                                                  &wal::manager_wal_replicate_t::commit_txn,
                                                  session,
                                                  txn_data.transaction_id,
                                                  wal::wal_sync_mode::FULL,
                                                  std::string(db));
                co_await std::move(cf);
            }
        }
        if (txn_manager_ != nullptr && txn_data.transaction_id != 0) {
            txn_manager_->commit(session);
        }
        co_return ok_cursor();
    }

    executor_t::unique_future<function_result_t> executor_t::register_udf(components::session::session_id_t session,
                                                                          components::compute::function_ptr function) {
        trace(log_, "executor::register_udf, session: {}, {}", session.data(), function->name());
        std::string name = function->name();
        auto signatures = function->get_signatures();
        auto res = function_registry_.add_function(std::move(function));
        if (res.status() == components::compute::compute_status::ok()) {
            co_return res.value();
        }
        co_return components::compute::invalid_function_uid;
    }

    plan_t executor_t::traverse_plan_(components::operators::operator_ptr&& plan,
                                      components::logical_plan::storage_parameters&& parameters,
                                      services::context_storage_t&& context_storage) {
        std::stack<components::operators::operator_ptr> look_up;
        std::stack<components::operators::operator_ptr> sub_plans;
        look_up.push(plan);
        while (!look_up.empty()) {
            auto check_op = look_up.top();
            while (check_op->right() == nullptr) {
                check_op = check_op->left();
                if (check_op == nullptr) {
                    break;
                }
            }
            sub_plans.push(look_up.top());
            look_up.pop();
            if (check_op != nullptr) {
                look_up.push(check_op->right());
                look_up.push(check_op->left());
            }
        }

        trace(log_, "executor::subplans count {}", sub_plans.size());

        return plan_t{std::move(sub_plans), parameters, std::move(context_storage)};
    }

    executor_t::unique_future<sub_plan_result_t>
    executor_t::execute_sub_plan_(components::session::session_id_t session,
                                  plan_t plan_data,
                                  components::table::transaction_data txn) {
        cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t accumulated_updates(resource());
        sub_plan_result_t result_tracking;

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan");
                break;
            }

            components::pipeline::context_t pipeline_context{session,
                                                             address(),
                                                             parent_address_,
                                                             &function_registry_,
                                                             plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;
            pipeline_context.txn = txn;

            // Prepare the operator tree (connects children in aggregation, etc.)
            plan->prepare();

            // Execute the plan tree (scan operators send I/O requests and enter waiting state)
            plan->on_execute(&pipeline_context);

            if (plan->has_error()) {
                cursor = make_cursor(resource(), error_code_t::create_physical_plan_error, plan->error_message());
                break;
            }

            // Await all waiting operators (multiple scans in a join, etc.)
            while (!plan->is_executed()) {
                auto waiting_op = plan->find_waiting_operator();
                if (!waiting_op) {
                    error(log_,
                          "Plan not executed and no waiting operator! session: {}, plan type: {}",
                          session.data(),
                          static_cast<int>(plan->type()));
                    cursor = make_cursor(resource(),
                                         error_code_t::create_physical_plan_error,
                                         "operator failed to complete execution");
                    break;
                }
                trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                if (waiting_op->type() == components::operators::operator_type::insert ||
                    waiting_op->type() == components::operators::operator_type::remove ||
                    waiting_op->type() == components::operators::operator_type::update) {
                    result_tracking = co_await intercept_dml_io_(waiting_op, &pipeline_context);
                } else {
                    co_await waiting_op->await_async_and_resume(&pipeline_context);
                }
                trace(log_, "executor: after await completed");
                // Re-execute: completed scan allows parent to proceed, may find next waiting scan
                plan->on_execute(&pipeline_context);
            }
            if (cursor && cursor->is_error())
                break;

            switch (plan->type()) {
                case components::operators::operator_type::insert: {
                    trace(log_, "executor::execute_plan : operators::operator_type::insert");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->data_chunk()));
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                case components::operators::operator_type::remove: {
                    trace(log_, "executor::execute_plan : operators::operator_type::remove");
                    if (plan->modified()) {
                        for (auto& [key, val] : plan->modified()->updated_types_map()) {
                            accumulated_updates[key] += val;
                        }
                    }
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->data_chunk()));
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                case components::operators::operator_type::update: {
                    trace(log_, "executor::execute_plan : operators::operator_type::update");
                    if (plan->output()) {
                        cursor = make_cursor(resource(), std::move(plan->output()->data_chunk()));
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                default: {
                    trace(log_,
                          "executor::execute_plan : operator_type={}, session: {}",
                          static_cast<int>(plan->type()),
                          session.data());

                    if (plan->is_root()) {
                        if (plan->output()) {
                            auto& chunk = plan->output()->data_chunk();
                            // Apply post-sort limit
                            if (plan_data.limit.limit() > 0 &&
                                static_cast<int>(chunk.size()) > plan_data.limit.limit()) {
                                chunk.set_cardinality(static_cast<uint64_t>(plan_data.limit.limit()));
                            }
                            cursor = make_cursor(resource(), std::move(chunk));
                        } else {
                            cursor = make_cursor(resource(), operation_status_t::success);
                        }
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }
            }

            if (cursor->is_error()) {
                break;
            }

            if (pipeline_context.has_pending_disk_futures()) {
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
            }

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        result_tracking.cursor = std::move(cursor);
        result_tracking.updates = std::move(accumulated_updates);
        co_return std::move(result_tracking);
    }

    executor_t::unique_future<sub_plan_result_t>
    executor_t::intercept_dml_io_(components::operators::operator_t::ptr waiting_op,
                                  components::pipeline::context_t* ctx) {
        using namespace components::operators;
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        sub_plan_result_t result;
        result.wal_row_ids = std::pmr::vector<int64_t>(resource());

        switch (waiting_op->type()) {
            case operator_type::insert: {
                auto& out_chunk = waiting_op->output()->data_chunk();
                auto* ins = static_cast<operator_insert*>(waiting_op.get());
                components::execution_context_t exec_ctx{ctx->session, ctx->txn, ins->collection_name()};

                // Capture WAL data BEFORE storage_append moves it
                result.wal_insert_data =
                    std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                out_chunk.copy(*result.wal_insert_data, 0);
                result.wal_collection = ins->collection_name();

                // storage_append (handles schema adoption, _id dedup)
                auto data_copy = std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                out_chunk.copy(*data_copy, 0);
                auto [_a, af] = actor_zeta::send(disk_address_,
                                                 &disk::manager_disk_t::storage_append,
                                                 exec_ctx,
                                                 std::move(data_copy));
                auto [start_row, actual_count] = co_await std::move(af);

                result.append_row_start = static_cast<int64_t>(start_row);
                result.append_row_count = actual_count;

                if (actual_count == 0) {
                    result.wal_insert_data.reset();
                    waiting_op->set_output(nullptr);
                    waiting_op->mark_executed();
                    break;
                }

                // Mirror to index (txn-aware)
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto idx_data = std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                    out_chunk.copy(*idx_data, 0);
                    auto [_ix, ixf] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::insert_rows,
                                                       exec_ctx,
                                                       std::move(idx_data),
                                                       static_cast<uint64_t>(start_row),
                                                       actual_count);
                    co_await std::move(ixf);
                }

                // Build result chunk
                data_chunk_t res_chunk(resource(), {}, actual_count);
                res_chunk.set_cardinality(actual_count);
                waiting_op->set_output(make_operator_data(resource(), std::move(res_chunk)));
                waiting_op->mark_executed();
                break;
            }

            case operator_type::remove: {
                auto* del_op = static_cast<operator_delete*>(waiting_op.get());
                auto& ids = waiting_op->modified()->ids();
                result.delete_count = waiting_op->modified()->size();
                size_t modified_size = result.delete_count;
                components::execution_context_t exec_ctx{ctx->session, ctx->txn, del_op->collection_name()};

                // Capture WAL data: row IDs for physical delete
                result.wal_row_ids.reserve(modified_size);
                for (size_t i = 0; i < modified_size; i++) {
                    result.wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
                }
                result.wal_collection = del_op->collection_name();

                // storage_delete_rows
                vector_t row_ids(resource(), components::types::logical_type::BIGINT, modified_size);
                for (size_t i = 0; i < modified_size; i++) {
                    row_ids.data<int64_t>()[i] = static_cast<int64_t>(ids[i]);
                }
                auto [_d, df] = actor_zeta::send(disk_address_,
                                                 &disk::manager_disk_t::storage_delete_rows,
                                                 exec_ctx,
                                                 std::move(row_ids),
                                                 static_cast<uint64_t>(modified_size));
                co_await std::move(df);

                result.delete_txn_id = ctx->txn.transaction_id;

                // Mirror to index
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    if (auto scan_out = waiting_op->left() ? waiting_op->left()->output() : nullptr) {
                        auto& sc = scan_out->data_chunk();
                        auto idx_data = std::make_unique<data_chunk_t>(resource(), sc.types(), sc.size());
                        sc.copy(*idx_data, 0);
                        auto idx_ids = std::pmr::vector<int64_t>(resource());
                        idx_ids.reserve(modified_size);
                        for (size_t i = 0; i < modified_size; i++) {
                            idx_ids.emplace_back(sc.row_ids.data<int64_t>()[i]);
                        }
                        auto [_ix, ixf] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::delete_rows,
                                                           exec_ctx,
                                                           std::move(idx_data),
                                                           std::move(idx_ids));
                        co_await std::move(ixf);
                    }
                }

                // Build result (need types from storage)
                auto [_t, tf] = actor_zeta::send(disk_address_,
                                                 &disk::manager_disk_t::storage_types,
                                                 ctx->session,
                                                 del_op->collection_name());
                auto types = co_await std::move(tf);
                data_chunk_t chunk(resource(), types, modified_size);
                chunk.set_cardinality(modified_size);
                waiting_op->set_output(make_operator_data(resource(), std::move(chunk)));
                waiting_op->mark_executed();
                break;
            }

            case operator_type::update: {
                auto* upd = static_cast<operator_update*>(waiting_op.get());
                auto& out_chunk = waiting_op->output()->data_chunk();
                components::execution_context_t exec_ctx{ctx->session, ctx->txn, upd->collection_name()};

                // Capture WAL data: row_ids + updated data for physical update
                result.wal_row_ids.reserve(out_chunk.size());
                for (uint64_t i = 0; i < out_chunk.size(); i++) {
                    result.wal_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                }
                result.wal_update_data =
                    std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                out_chunk.copy(*result.wal_update_data, 0);
                result.wal_collection = upd->collection_name();

                // storage_update (MVCC: delete old rows + insert new rows)
                vector_t row_ids(resource(), components::types::logical_type::BIGINT, out_chunk.size());
                for (uint64_t i = 0; i < out_chunk.size(); i++) {
                    row_ids.data<int64_t>()[i] = out_chunk.row_ids.data<int64_t>()[i];
                }
                auto data_copy = std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                out_chunk.copy(*data_copy, 0);
                auto [_u, uf] = actor_zeta::send(disk_address_,
                                                 &disk::manager_disk_t::storage_update,
                                                 exec_ctx,
                                                 std::move(row_ids),
                                                 std::move(data_copy));
                auto [upd_row_start, upd_row_count] = co_await std::move(uf);
                result.append_row_start = upd_row_start;
                result.append_row_count = upd_row_count;
                result.delete_txn_id = ctx->txn.transaction_id;

                // Mirror to index (old+new data)
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    if (auto scan_out = waiting_op->left() ? waiting_op->left()->output() : nullptr) {
                        auto& sc = scan_out->data_chunk();
                        auto old_data = std::make_unique<data_chunk_t>(resource(), sc.types(), sc.size());
                        sc.copy(*old_data, 0);
                        auto new_data = std::make_unique<data_chunk_t>(resource(), out_chunk.types(), out_chunk.size());
                        out_chunk.copy(*new_data, 0);
                        auto idx_ids = std::pmr::vector<int64_t>(resource());
                        idx_ids.reserve(out_chunk.size());
                        for (size_t i = 0; i < out_chunk.size(); i++) {
                            idx_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                        }
                        auto [_ix, ixf] = actor_zeta::send(index_address_,
                                                           &index::manager_index_t::update_rows,
                                                           exec_ctx,
                                                           std::move(old_data),
                                                           std::move(new_data),
                                                           std::move(idx_ids),
                                                           static_cast<int64_t>(upd_row_start));
                        co_await std::move(ixf);
                    }
                }

                // output_ already set by on_execute_impl (contains updated rows)
                waiting_op->mark_executed();
                break;
            }

            default:
                break;
        }
        co_return std::move(result);
    }

} // namespace services::collection::executor
