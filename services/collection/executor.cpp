#include "executor.hpp"

#include <components/context/execution_context.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

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
        , pending_void_(resource)
        , pending_execute_(resource) {}

    actor_zeta::behavior_t executor_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &executor_t::execute_plan, msg);
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
        for (auto it = pending_execute_.begin(); it != pending_execute_.end();) {
            if (it->available()) {
                it = pending_execute_.erase(it);
            } else {
                ++it;
            }
        }
    }

    auto executor_t::make_type() const noexcept -> const char* { return "executor"; }

    executor_t::unique_future<execute_result_t> executor_t::execute_plan(
        components::session::session_id_t session,
        components::logical_plan::node_ptr logical_plan,
        components::logical_plan::storage_parameters parameters,
        services::context_storage_t context_storage,
        components::table::transaction_data txn
    ) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        // Handle index operations directly from the logical plan (no physical operator needed)
        using namespace components::logical_plan;
        if (logical_plan->type() == node_type::create_index_t) {
            auto* node_ci = static_cast<node_create_index_t*>(logical_plan.get());
            auto coll_name = logical_plan->collection_full_name();

            if (!coll_name.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
                // WAL durability for create_index
                if (wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto ci_ptr = boost::static_pointer_cast<node_create_index_t>(logical_plan);
                    auto [_w, wf] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::create_index, session, ci_ptr);
                    co_await std::move(wf);
                }

                auto [_ix, ixf] = actor_zeta::send(index_address_,
                    &index::manager_index_t::create_index,
                    session, coll_name,
                    index::index_name_t(node_ci->name()),
                    node_ci->keys(),
                    node_ci->type());
                auto id_index = co_await std::move(ixf);

                if (id_index == components::index::INDEX_ID_UNDEFINED) {
                    trace(log_, "executor: index {} already exists, returning error", node_ci->name());
                    co_return execute_result_t{
                        make_cursor(resource(), error_code_t::index_create_fail, "index already exists"),
                        {}
                    };
                }

                // Backfill: populate new index with existing data from storage
                auto [_tr, trf] = actor_zeta::send(disk_address_,
                    &disk::manager_disk_t::storage_total_rows, session, coll_name);
                auto total_rows = co_await std::move(trf);

                if (total_rows > 0) {
                    auto [_ss, ssf] = actor_zeta::send(disk_address_,
                        &disk::manager_disk_t::storage_scan_segment,
                        session, coll_name, int64_t{0}, total_rows);
                    auto scan_data = co_await std::move(ssf);

                    if (scan_data) {
                        auto count = scan_data->size();
                        auto [_ir, irf] = actor_zeta::send(index_address_,
                            &index::manager_index_t::insert_rows,
                            session, coll_name, std::move(scan_data),
                            uint64_t{0}, count);
                        co_await std::move(irf);
                    }
                }
            }
            co_return execute_result_t{
                make_cursor(resource(), operation_status_t::success),
                {}
            };
        }

        if (logical_plan->type() == node_type::drop_index_t) {
            auto* node_di = static_cast<node_drop_index_t*>(logical_plan.get());
            auto coll_name = logical_plan->collection_full_name();

            if (!coll_name.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
                // WAL durability for drop_index
                if (wal_address_ != actor_zeta::address_t::empty_address()) {
                    auto di_ptr = boost::static_pointer_cast<node_drop_index_t>(logical_plan);
                    auto [_w, wf] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::drop_index, session, di_ptr);
                    co_await std::move(wf);
                }

                auto [_ix, ixf] = actor_zeta::send(index_address_,
                    &index::manager_index_t::drop_index,
                    session, coll_name,
                    index::index_name_t(node_di->name()));
                co_await std::move(ixf);
            }
            co_return execute_result_t{
                make_cursor(resource(), operation_status_t::success),
                {}
            };
        }

        // Determine if this is a DML operation
        bool is_dml = (logical_plan->type() == node_type::insert_t ||
                       logical_plan->type() == node_type::update_t ||
                       logical_plan->type() == node_type::delete_t);

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

        components::operators::operator_ptr plan = planner::create_plan(context_storage, logical_plan, limit);

        if (!plan) {
            if (is_dml) {
                txn_manager_->abort(session);
            }
            co_return execute_result_t{
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"),
                {}
            };
        }

        plan->set_as_root();

        auto wal_params = components::logical_plan::make_parameter_node(resource());
        wal_params->set_parameters(parameters);

        auto plan_data = traverse_plan_(std::move(plan), std::move(parameters), std::move(context_storage));

        // Step 2: Execute physical plan
        auto result = co_await execute_sub_plan_(session, std::move(plan_data), txn_data);

        if (is_dml && result.cursor->is_success()) {
            // Step 3: WAL DATA
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                switch (logical_plan->type()) {
                    case node_type::insert_t: {
                        trace(log_, "executor::execute_plan: WAL insert_many");
                        auto insert = boost::static_pointer_cast<node_insert_t>(logical_plan);
                        auto [_w1, wf1] = actor_zeta::send(wal_address_,
                            &wal::manager_wal_replicate_t::insert_many, session, insert, txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf1);
                        auto [_d1, df1] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::flush, session, wal_id);
                        pending_void_.push_back(std::move(df1));
                        break;
                    }
                    case node_type::update_t: {
                        trace(log_, "executor::execute_plan: WAL update_many");
                        auto update = boost::static_pointer_cast<node_update_t>(logical_plan);
                        auto [_w2, wf2] = actor_zeta::send(wal_address_,
                            &wal::manager_wal_replicate_t::update_many, session, update, wal_params, txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf2);
                        auto [_d2, df2] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::flush, session, wal_id);
                        pending_void_.push_back(std::move(df2));
                        break;
                    }
                    case node_type::delete_t: {
                        trace(log_, "executor::execute_plan: WAL delete_many");
                        auto delete_node = boost::static_pointer_cast<node_delete_t>(logical_plan);
                        auto [_w3, wf3] = actor_zeta::send(wal_address_,
                            &wal::manager_wal_replicate_t::delete_many, session, delete_node, wal_params, txn_data.transaction_id);
                        auto wal_id = co_await std::move(wf3);
                        auto [_d3, df3] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::flush, session, wal_id);
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
                    ctx, commit_id,
                    result.append_row_start, result.append_row_count);
                co_await std::move(caf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ci, cif] = actor_zeta::send(index_address_,
                        &index::manager_index_t::commit_insert,
                        ctx, commit_id);
                    co_await std::move(cif);
                }
            }
            if (result.delete_txn_id != 0 && commit_id > 0) {
                components::execution_context_t del_ctx{session, txn_data, coll_name};
                auto [_cd, cdf] = actor_zeta::send(disk_address_,
                    &disk::manager_disk_t::storage_commit_delete,
                    del_ctx, commit_id);
                co_await std::move(cdf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_cdi, cdif] = actor_zeta::send(index_address_,
                        &index::manager_index_t::commit_delete,
                        del_ctx, commit_id);
                    co_await std::move(cdif);
                }
                // Fire-and-forget auto-GC check
                auto lowest = txn_manager_->lowest_active_start_time();
                auto [gc_sched, gc_fut] = actor_zeta::send(disk_address_,
                    &disk::manager_disk_t::maybe_cleanup,
                    del_ctx, lowest);
                pending_void_.push_back(std::move(gc_fut));
            }

            // Step 6: WAL COMMIT marker
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                auto [_wc, wcf] = actor_zeta::send(wal_address_,
                    &wal::manager_wal_replicate_t::commit_txn, session, txn_data.transaction_id);
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
                    result.append_row_start, result.append_row_count);
                co_await std::move(raf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] = actor_zeta::send(index_address_,
                        &index::manager_index_t::revert_insert,
                        abort_ctx);
                    co_await std::move(rif);
                }
            }
            txn_manager_->abort(session);
        }

        co_return execute_result_t{std::move(result.cursor), std::move(result.updates)};
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

    executor_t::unique_future<sub_plan_result_t> executor_t::execute_sub_plan_(
        components::session::session_id_t session,
        plan_t plan_data,
        components::table::transaction_data txn) {

        cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t accumulated_updates(resource());
        int64_t append_row_start = 0;
        uint64_t append_row_count = 0;
        uint64_t delete_txn_id = 0;

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan");
                break;
            }

            components::pipeline::context_t pipeline_context{session, address(), parent_address_, plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;
            pipeline_context.txn = txn;

            // Prepare the operator tree (connects children in aggregation, etc.)
            plan->prepare();

            // Execute the plan tree (scan operators send I/O requests and enter waiting state)
            plan->on_execute(&pipeline_context);

            // Await all waiting operators (multiple scans in a join, etc.)
            while (!plan->is_executed()) {
                auto waiting_op = plan->find_waiting_operator();
                if (!waiting_op) {
                    error(log_, "Plan not executed and no waiting operator! session: {}, plan type: {}",
                          session.data(), static_cast<int>(plan->type()));
                    cursor = make_cursor(resource(), error_code_t::create_physical_plan_error,
                                         "operator failed to complete execution");
                    break;
                }
                trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                co_await waiting_op->await_async_and_resume(&pipeline_context);
                trace(log_, "executor: after await_async_and_resume completed");
                // Re-execute: completed scan allows parent to proceed, may find next waiting scan
                plan->on_execute(&pipeline_context);
            }
            if (cursor && cursor->is_error()) break;

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

                case components::operators::operator_type::raw_data:
                case components::operators::operator_type::join:
                case components::operators::operator_type::aggregate: {
                    if (plan->type() == components::operators::operator_type::aggregate) {
                        trace(log_, "executor::execute_plan : operators::operator_type::aggregate, session: {}",
                              session.data());
                    } else if (plan->type() == components::operators::operator_type::join) {
                        trace(log_, "executor::execute_plan : operators::operator_type::join, session: {}",
                              session.data());
                    } else {
                        trace(log_, "executor::execute_plan : operators::operator_type::raw_data, session: {}",
                              session.data());
                    }

                    if (plan->is_root()) {
                        if (plan->output()) {
                            cursor = make_cursor(resource(), std::move(plan->output()->data_chunk()));
                        } else {
                            cursor = make_cursor(resource(), operation_status_t::success);
                        }
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                default:
                    cursor = make_cursor(resource(), operation_status_t::success);
                    break;
            }

            if (cursor->is_error()) break;

            if (pipeline_context.has_pending_disk_futures()) {
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
            }

            // Accumulate MVCC tracking from DML operators (stored in operator fields)
            if (plan->type() == components::operators::operator_type::insert) {
                auto* ins = static_cast<components::operators::operator_insert*>(plan.get());
                if (ins->append_row_count() > 0) {
                    append_row_start = ins->append_row_start();
                    append_row_count = ins->append_row_count();
                }
            }
            if (plan->type() == components::operators::operator_type::remove) {
                auto* del = static_cast<components::operators::operator_delete*>(plan.get());
                if (del->delete_txn_id() != 0) {
                    delete_txn_id = del->delete_txn_id();
                }
            }

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        co_return sub_plan_result_t{std::move(cursor), std::move(accumulated_updates),
                                     append_row_start, append_row_count, delete_txn_id};
    }

} // namespace services::collection::executor
