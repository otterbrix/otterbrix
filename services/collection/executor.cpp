#include "executor.hpp"

#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
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
                           log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , index_address_(std::move(index_address))
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
        services::context_storage_t context_storage
    ) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        // Handle index operations directly from the logical plan (no physical operator needed)
        using namespace components::logical_plan;
        if (logical_plan->type() == node_type::create_index_t) {
            auto* node_ci = static_cast<node_create_index_t*>(logical_plan.get());
            auto coll_name = logical_plan->collection_full_name();

            if (!coll_name.empty() && index_address_ != actor_zeta::address_t::empty_address()) {
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

        auto limit = components::logical_plan::limit_t::unlimit();
        for (const auto& child : logical_plan->children()) {
            if (child->type() == components::logical_plan::node_type::limit_t) {
                limit = static_cast<components::logical_plan::node_limit_t*>(child.get())->limit();
            }
        }

        components::operators::operator_ptr plan = planner::create_plan(context_storage, logical_plan, limit);

        if (!plan) {
            co_return execute_result_t{
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"),
                {}
            };
        }

        plan->set_as_root();

        auto wal_params = components::logical_plan::make_parameter_node(resource());
        wal_params->set_parameters(parameters);

        auto plan_data = traverse_plan_(std::move(plan), std::move(parameters), std::move(context_storage));

        auto result = co_await execute_sub_plan_(session, std::move(plan_data));

        if (result.cursor->is_success() && wal_address_ != actor_zeta::address_t::empty_address()) {
            using namespace components::logical_plan;
            switch (logical_plan->type()) {
                case node_type::insert_t: {
                    trace(log_, "executor::execute_plan: WAL insert_many");
                    auto insert = boost::static_pointer_cast<node_insert_t>(logical_plan);
                    auto [_w1, wf1] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::insert_many, session, insert);
                    auto wal_id = co_await std::move(wf1);
                    auto [_d1, df1] = actor_zeta::send(disk_address_,
                        &disk::manager_disk_t::flush, session, wal_id);
                    // Defer flush — WAL already has the data, disk persistence can complete asynchronously
                    pending_void_.push_back(std::move(df1));
                    break;
                }
                case node_type::update_t: {
                    trace(log_, "executor::execute_plan: WAL update_many");
                    auto update = boost::static_pointer_cast<node_update_t>(logical_plan);
                    auto [_w2, wf2] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::update_many, session, update, wal_params);
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
                        &wal::manager_wal_replicate_t::delete_many, session, delete_node, wal_params);
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

        co_return result;
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

    executor_t::unique_future<execute_result_t> executor_t::execute_sub_plan_(
        components::session::session_id_t session,
        plan_t plan_data) {

        cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t accumulated_updates(resource());

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan");
                break;
            }

            auto coll_name = [&]() -> collection_full_name_t {
                switch (plan->type()) {
                    case components::operators::operator_type::insert:
                        return static_cast<components::operators::operator_insert&>(*plan).collection_name();
                    case components::operators::operator_type::remove:
                        return static_cast<components::operators::operator_delete&>(*plan).collection_name();
                    case components::operators::operator_type::update:
                        return static_cast<components::operators::operator_update&>(*plan).collection_name();
                    default:
                        return {};
                }
            }();

            components::pipeline::context_t pipeline_context{session, address(), parent_address_, plan_data.parameters};
            pipeline_context.disk_address = disk_address_;
            pipeline_context.index_address = index_address_;

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

                    auto output = plan->output();
                    if (output && output->size() > 0 && !coll_name.empty()) {
                        auto name = coll_name;
                        auto& out_chunk = output->data_chunk();

                        // Mirror to storage (handles schema adoption + column expansion)
                        auto data_copy = std::make_unique<components::vector::data_chunk_t>(
                            resource(), out_chunk.types(), out_chunk.size());
                        out_chunk.copy(*data_copy, 0);
                        auto [_a, af] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::storage_append,
                            session, name, std::move(data_copy));
                        auto [start_row, actual_count] = co_await std::move(af);

                        if (actual_count == 0) {
                            // All rows were duplicates — nothing inserted
                            cursor = make_cursor(resource(), operation_status_t::success);
                            break;
                        }

                        // Mirror to manager_index_t (dual write)
                        if (index_address_ != actor_zeta::address_t::empty_address()) {
                            auto data_for_index = std::make_unique<components::vector::data_chunk_t>(
                                resource(), out_chunk.types(), out_chunk.size());
                            out_chunk.copy(*data_for_index, 0);
                            auto [_ix, ixf] = actor_zeta::send(index_address_,
                                &index::manager_index_t::insert_rows,
                                session, name, std::move(data_for_index),
                                static_cast<uint64_t>(start_row),
                                actual_count);
                            co_await std::move(ixf);
                        }

                        // Persist to disk (write_data_chunk for disk agent)
                        auto data_for_disk = std::make_unique<components::vector::data_chunk_t>(
                            resource(), out_chunk.types(), out_chunk.size());
                        out_chunk.copy(*data_for_disk, 0);
                        auto [_wr, wrf] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::write_data_chunk,
                            session, name.database, name.collection,
                            std::move(data_for_disk));
                        co_await std::move(wrf);

                        // Build result cursor with actual inserted count
                        components::vector::data_chunk_t result_chunk(resource(), {}, actual_count);
                        result_chunk.set_cardinality(actual_count);
                        cursor = make_cursor(resource(), std::move(result_chunk));
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                case components::operators::operator_type::remove: {
                    trace(log_, "executor::execute_plan : operators::operator_type::remove");

                    // Accumulate updated types
                    if (plan->modified()) {
                        for (auto& [key, val] : plan->modified()->updated_types_map()) {
                            accumulated_updates[key] += val;
                        }
                    }

                    auto modified_data = plan->modified();
                    size_t modified_size = modified_data ? modified_data->size() : 0;

                    if (modified_size > 0 && !coll_name.empty()) {
                        auto name = coll_name;
                        auto& ids = modified_data->ids();

                        // Mirror deletion to storage
                        components::vector::vector_t row_ids(resource(),
                            components::types::logical_type::BIGINT, modified_size);
                        for (size_t i = 0; i < modified_size; i++) {
                            row_ids.set_value(i,
                                components::types::logical_value_t{resource(),
                                    static_cast<int64_t>(ids[i])});
                        }
                        auto [_d, df] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::storage_delete_rows,
                            session, name, std::move(row_ids), modified_size);
                        co_await std::move(df);

                        // Mirror deletion to manager_index_t (dual write)
                        if (index_address_ != actor_zeta::address_t::empty_address()) {
                            auto scan_output = plan->left() ? plan->left()->output() : nullptr;
                            if (scan_output) {
                                auto& scan_chunk = scan_output->data_chunk();
                                auto data_for_index = std::make_unique<components::vector::data_chunk_t>(
                                    resource(), scan_chunk.types(), scan_chunk.size());
                                scan_chunk.copy(*data_for_index, 0);
                                auto index_row_ids = std::pmr::vector<size_t>(resource());
                                index_row_ids.reserve(modified_size);
                                for (size_t i = 0; i < modified_size; i++) {
                                    // Use chunk-local indices: data_for_index is a copy of scan_chunk,
                                    // so row i in data_for_index corresponds to ids[i] in storage.
                                    // The index engine uses row_id to access column.value(row_id),
                                    // so it must be a valid index within the data chunk.
                                    index_row_ids.push_back(i);
                                }
                                auto [_ix, ixf] = actor_zeta::send(index_address_,
                                    &index::manager_index_t::delete_rows,
                                    session, name, std::move(data_for_index),
                                    std::move(index_row_ids));
                                co_await std::move(ixf);
                            }
                        }

                        // Persist removal to disk
                        auto ids_copy = std::pmr::vector<size_t>(resource());
                        ids_copy.reserve(modified_size);
                        for (size_t i = 0; i < modified_size; i++) {
                            ids_copy.push_back(ids[i]);
                        }
                        auto [_rm, rmf] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::remove_documents,
                            session, name.database, name.collection,
                            std::move(ids_copy));
                        co_await std::move(rmf);
                    }

                    // Build result cursor
                    if (!coll_name.empty()) {
                        auto [_t, tf] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::storage_types, session, coll_name);
                        auto types = co_await std::move(tf);
                        components::vector::data_chunk_t chunk(resource(), types, modified_size);
                        chunk.set_cardinality(modified_size);
                        cursor = make_cursor(resource(), std::move(chunk));
                    } else {
                        cursor = make_cursor(resource(), operation_status_t::success);
                    }
                    break;
                }

                case components::operators::operator_type::update: {
                    trace(log_, "executor::execute_plan : operators::operator_type::update");

                    auto output = plan->output();
                    auto modified = plan->modified();

                    if (output && modified && modified->size() > 0 && !coll_name.empty()) {
                        auto name = coll_name;
                        auto& out_chunk = output->data_chunk();

                        // Mirror update to storage
                        components::vector::vector_t row_ids(resource(),
                            components::types::logical_type::BIGINT, out_chunk.size());
                        for (uint64_t i = 0; i < out_chunk.size(); i++) {
                            row_ids.set_value(i,
                                components::types::logical_value_t{resource(),
                                    out_chunk.row_ids.data<int64_t>()[i]});
                        }
                        auto data_copy = std::make_unique<components::vector::data_chunk_t>(
                            resource(), out_chunk.types(), out_chunk.size());
                        out_chunk.copy(*data_copy, 0);

                        auto [_u, uf] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::storage_update,
                            session, name, std::move(row_ids),
                            std::move(data_copy));
                        co_await std::move(uf);

                        // Mirror update to manager_index_t (dual write)
                        if (index_address_ != actor_zeta::address_t::empty_address()) {
                            auto scan_output = plan->left() ? plan->left()->output() : nullptr;
                            if (scan_output) {
                                auto& scan_chunk = scan_output->data_chunk();
                                auto old_data_for_index = std::make_unique<components::vector::data_chunk_t>(
                                    resource(), scan_chunk.types(), scan_chunk.size());
                                scan_chunk.copy(*old_data_for_index, 0);
                                auto new_data_for_index = std::make_unique<components::vector::data_chunk_t>(
                                    resource(), out_chunk.types(), out_chunk.size());
                                out_chunk.copy(*new_data_for_index, 0);
                                auto& mod_ids = modified->ids();
                                auto index_row_ids = std::pmr::vector<size_t>(resource());
                                index_row_ids.reserve(mod_ids.size());
                                for (size_t i = 0; i < mod_ids.size(); i++) {
                                    index_row_ids.push_back(mod_ids[i]);
                                }
                                auto [_ix, ixf] = actor_zeta::send(index_address_,
                                    &index::manager_index_t::update_rows,
                                    session, name,
                                    std::move(old_data_for_index),
                                    std::move(new_data_for_index),
                                    std::move(index_row_ids));
                                co_await std::move(ixf);
                            }
                        }

                        // Persist removal to disk (old data will be replaced)
                        auto& ids = modified->ids();
                        auto ids_to_remove = std::pmr::vector<size_t>(resource());
                        ids_to_remove.reserve(ids.size());
                        for (size_t i = 0; i < ids.size(); i++) {
                            ids_to_remove.push_back(ids[i]);
                        }
                        auto [_rm, rmf] = actor_zeta::send(disk_address_,
                            &disk::manager_disk_t::remove_documents,
                            session, name.database, name.collection,
                            std::move(ids_to_remove));
                        co_await std::move(rmf);

                        cursor = make_cursor(resource(), std::move(out_chunk));
                    } else if (output) {
                        cursor = make_cursor(resource(), std::move(output->data_chunk()));
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

            plan_data.sub_plans.pop();
        }

        trace(log_, "executor::execute_sub_plan finished, success: {}", cursor->is_success());
        co_return execute_result_t{std::move(cursor), std::move(accumulated_updates)};
    }

} // namespace services::collection::executor
