#include "executor.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/planner/planner.hpp>
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
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>
// operator_{insert,delete,update}.hpp no longer included — Phase 5 moved
// the static_cast'd intercept_dml_io_ branches into each operator's
// await_async_and_resume so the executor only sees the base operator_t.
#include <components/physical_plan/operators/predicates/predicate.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>

using namespace components::cursor;

namespace {

// Walk through planner-added constraint wrapper nodes (check_constraint,
// sequence) to find the base DML node type.
// Needed because the planner may wrap insert/update/delete with constraint nodes,
// changing the top-level type from insert_t to e.g. check_constraint_t.
//
// Phase 7.x: dispatcher may wrap INSERT into a sequence_t when the target is
// a relkind='g' (computing/dynamic-schema) table — e.g. sequence_t(insert,
// computed_field_register) once P7.2 lands. Recurse through that wrapper so
// the executor still recognizes the plan as DML and runs the
// begin_transaction / commit-side pg_catalog swap path. Restricted to
// sequence_t whose first child is a DML node (insert/update/delete) so DDL
// sequence_t plans (CREATE COLLECTION etc.) keep their existing dml=false
// treatment.
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
        case node_type::sequence_t:
            if (!n->children().empty()) {
                const auto first = n->children().front()->type();
                if (first == node_type::insert_t || first == node_type::update_t ||
                    first == node_type::delete_t) {
                    n = n->children().front().get();
                    continue;
                }
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

    executor_t::unique_future<execute_result_t>
    executor_t::execute_plan(components::session::session_id_t session,
                             components::logical_plan::node_ptr logical_plan,
                             components::logical_plan::storage_parameters parameters,
                             services::context_storage_t context_storage,
                             components::table::transaction_data txn) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        using namespace components::logical_plan;

        // CREATE INDEX / DROP INDEX are now lowered by the planner into
        // sequence_t(primitive_write/delete × N, create_index_t/drop_index_t)
        // and run through the standard physical-plan pipeline (see
        // components/physical_plan_generator/impl/create_plan_sequence.cpp).
        // No special-case branches required here.

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

            // Phase 5: WAL physical_{insert,update,delete} writes used to live
            // here, but were moved into operator_{insert,delete,update}::
            // await_async_and_resume so each DML operator is self-contained
            // (storage_append + WAL + index mirror happen together inside the
            // operator coroutine, before mark_executed). The flush future is
            // pushed onto pipeline_context.pending_disk_futures and awaited
            // by execute_sub_plan_, ordering guarantees stay identical.

            // Step 4: Commit transaction
            uint64_t commit_id = txn_manager_->commit(session);
            trace(log_, "executor::execute_plan: committed txn {}, commit_id {}", txn_data.transaction_id, commit_id);

            // Step 5: Commit side-effects on storage and index.
            // Phase 8.G: routing is by table_oid only — never read cfn from
            // logical_plan, since wrappers (sequence_t etc.) have no cfn and
            // would shadow the inner DML node's identity (root cause of #132).
            // result.dml_table_oid is stamped by the inner DML operator; it
            // identifies the storage/index target unambiguously.
            if (result.dml_append_row_count > 0 && commit_id > 0) {
                components::execution_context_t ctx{session, txn_data, result.dml_table_oid};
                auto [_ca, caf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_commit_append,
                                                   ctx,
                                                   result.dml_table_oid,
                                                   commit_id,
                                                   result.dml_append_row_start,
                                                   result.dml_append_row_count);
                co_await std::move(caf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ci, cif] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::commit_insert,
                                                       ctx,
                                                       result.dml_table_oid,
                                                       commit_id);
                    co_await std::move(cif);
                }
            }
            if (result.dml_delete_txn_id != 0 && commit_id > 0) {
                components::execution_context_t del_ctx{session, txn_data, result.dml_table_oid};
                auto [_cd, cdf] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::storage_commit_delete, del_ctx, result.dml_table_oid, commit_id);
                co_await std::move(cdf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_cdi, cdif] = actor_zeta::send(index_address_,
                                                         &index::manager_index_t::commit_delete,
                                                         del_ctx,
                                                         result.dml_table_oid,
                                                         commit_id);
                    co_await std::move(cdif);
                }
                // Fire-and-forget auto-GC check
                auto lowest = txn_manager_->lowest_active_start_time();
                auto [gc_sched, gc_fut] =
                    actor_zeta::send(disk_address_, &disk::manager_disk_t::maybe_cleanup, del_ctx, lowest);
                pending_void_.push_back(std::move(gc_fut));
            }

            // Phase 5b Wave 4-D / Phase 7: flip MVCC tags for pg_catalog
            // rows written during this DML fragment (e.g. pg_computed_column
            // appends produced by operator_computed_field_register_t once
            // P7.2 wires it). Same txn; same commit_id; swap inline so the
            // rows become visible to the next reader without dispatcher
            // round-trips. Without this block the catalog rows would carry
            // insert_id == txn_id and be permanently invisible (since
            // txn_manager has already removed the active txn entry by the
            // time of return).
            if (commit_id > 0) {
                if (!result.pg_catalog_appends.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::storage_commit_appends,
                                                        pgc_ctx, commit_id,
                                                        std::move(result.pg_catalog_appends));
                    co_await std::move(paf);
                    result.pg_catalog_appends.clear();
                }
                if (!result.pg_catalog_delete_tables.empty()) {
                    components::execution_context_t pgc_ctx{session, txn_data, {}};
                    auto [_pd, pdf] = actor_zeta::send(disk_address_,
                                                        &disk::manager_disk_t::storage_commit_deletes,
                                                        pgc_ctx, commit_id,
                                                        std::move(result.pg_catalog_delete_tables));
                    co_await std::move(pdf);
                    result.pg_catalog_delete_tables.clear();
                }
            }

            // Step 6: WAL COMMIT marker
            if (wal_address_ != actor_zeta::address_t::empty_address()) {
                // Phase 8.E: WAL workers keyed by database_oid. Single-worker
                // model uses main_database for all DML in this phase; multi-database
                // routing follows once CREATE DATABASE allocates per-namespace workers.
                constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                auto [_wc, wcf] = actor_zeta::send(wal_address_,
                                                   &wal::manager_wal_replicate_t::commit_txn,
                                                   session,
                                                   txn_data.transaction_id,
                                                   wal::wal_sync_mode::NORMAL,
                                                   db_oid);
                co_await std::move(wcf);
            }

            co_return execute_result_t{std::move(result.cursor),
                                       std::move(result.updates),
                                       std::move(result.pg_catalog_appends),
                                       std::move(result.pg_catalog_delete_tables)};

        } else if (is_dml && result.cursor->is_error()) {
            // Abort path
            trace(log_, "executor::execute_plan: DML error, aborting txn");
            // Phase 8.G: oid-only routing on abort path; same rationale as commit path.
            if (result.dml_append_row_count > 0) {
                components::execution_context_t abort_ctx{session, txn_data, result.dml_table_oid};
                auto [_ra, raf] = actor_zeta::send(disk_address_,
                                                   &disk::manager_disk_t::storage_revert_append,
                                                   abort_ctx,
                                                   result.dml_table_oid,
                                                   result.dml_append_row_start,
                                                   result.dml_append_row_count);
                co_await std::move(raf);
                if (index_address_ != actor_zeta::address_t::empty_address()) {
                    auto [_ri, rif] = actor_zeta::send(index_address_,
                                                       &index::manager_index_t::revert_insert,
                                                       abort_ctx,
                                                       result.dml_table_oid);
                    co_await std::move(rif);
                }
            }
            // Phase 5b Wave 4-D: revert pg_catalog appends written in this fragment
            // before aborting the txn (otherwise the rows linger with insert_id =
            // txn_id and are unreachable). delete_tables tombstones with delete_id
            // = txn_id are reverted by storage's abort path on txn_manager_->abort.
            if (!result.pg_catalog_appends.empty()) {
                components::execution_context_t pgc_ctx{session, txn_data, {}};
                auto [_pa, paf] = actor_zeta::send(disk_address_,
                                                    &disk::manager_disk_t::storage_revert_appends,
                                                    pgc_ctx,
                                                    std::move(result.pg_catalog_appends));
                co_await std::move(paf);
                result.pg_catalog_appends.clear();
            }
            result.pg_catalog_delete_tables.clear();
            txn_manager_->abort(session);
        }

        co_return execute_result_t{std::move(result.cursor),
                                   std::move(result.updates),
                                   std::move(result.pg_catalog_appends),
                                   std::move(result.pg_catalog_delete_tables)};
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
            pipeline_context.wal_address = wal_address_;
            pipeline_context.txn = txn;
            // VACUUM/MVCC GC threshold (Phase 3 #52). operator_vacuum_t reads
            // this to gate manager_disk_t::vacuum_all + manager_index_t::
            // cleanup_all_versions. Computed up-front so the operator does not
            // need a back-channel to txn_manager_.
            pipeline_context.lowest_active_start_time = txn_manager_->lowest_active_start_time();

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
                // Phase 5: DML operators (insert/remove/update) now self-contain
                // WAL + storage + index I/O inside their await_async_and_resume.
                // No more switch-based intercept_dml_io_ — every operator is
                // dispatched uniformly via the same coroutine entry point.
                co_await waiting_op->await_async_and_resume(&pipeline_context);
                // Propagate errors set during async resume (fk_check, fk_cascade,
                // DML on disk failure, etc.)
                if (waiting_op->has_error()) {
                    cursor = make_cursor(resource(),
                                         error_code_t::create_physical_plan_error,
                                         waiting_op->error_message());
                    break;
                }
                trace(log_, "executor: after await completed");
                // Re-execute: completed scan allows parent to proceed, may find next waiting scan
                plan->on_execute(&pipeline_context);
            }
            if (cursor && cursor->is_error())
                break;

            // Detect errors set asynchronously in operators (e.g. fk_cascade root with RESTRICT).
            if (plan->has_error()) {
                cursor = make_cursor(resource(),
                                     error_code_t::create_physical_plan_error,
                                     plan->error_message());
                break;
            }

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

            // Phase 5b: lift pg_catalog swap info from this fragment's pipeline
            // context into the per-call sub_plan_result_t accumulated across
            // sub-plan iterations. execute_plan then forwards them into the
            // returned execute_result_t for the dispatcher to aggregate onto
            // transaction_t.
            for (auto& a : pipeline_context.pg_catalog_appends) {
                result_tracking.pg_catalog_appends.push_back(std::move(a));
            }
            for (auto& d : pipeline_context.pg_catalog_delete_tables) {
                result_tracking.pg_catalog_delete_tables.insert(std::move(d));
            }
            pipeline_context.pg_catalog_appends.clear();
            pipeline_context.pg_catalog_delete_tables.clear();

            // Phase 5: lift DML swap-info recorded by operator_insert /
            // operator_delete / operator_update inside await_async_and_resume.
            // execute_plan reads these to drive storage_commit_append /
            // storage_commit_delete after txn_manager_->commit. Last DML
            // fragment wins per call (matches the previous intercept_dml_io_
            // semantics, where each call rewrote result_tracking).
            if (pipeline_context.dml_append_row_count > 0) {
                result_tracking.dml_append_row_start = pipeline_context.dml_append_row_start;
                result_tracking.dml_append_row_count = pipeline_context.dml_append_row_count;
                result_tracking.dml_table_oid        = pipeline_context.dml_table_oid;
            }
            if (pipeline_context.dml_delete_txn_id != 0) {
                result_tracking.dml_delete_txn_id = pipeline_context.dml_delete_txn_id;
                result_tracking.dml_table_oid     = pipeline_context.dml_table_oid;
            }
            pipeline_context.dml_append_row_start = 0;
            pipeline_context.dml_append_row_count = 0;
            pipeline_context.dml_delete_txn_id    = 0;
            pipeline_context.dml_table_oid        = components::catalog::INVALID_OID;

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        result_tracking.cursor = std::move(cursor);
        result_tracking.updates = std::move(accumulated_updates);
        co_return std::move(result_tracking);
    }

} // namespace services::collection::executor
