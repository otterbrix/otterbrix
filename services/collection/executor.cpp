#include "executor.hpp"

#include <services/wal/manager_wal_replicate.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/index/index_engine.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/base/operators/operator_add_index.hpp>
#include <components/physical_plan/base/operators/operator_drop_index.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/executor.hpp>
#include <services/disk/index_agent_disk.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::collection::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(std::pmr::memory_resource* resource,
                           actor_zeta::address_t parent_address,
                           actor_zeta::address_t wal_address,
                           actor_zeta::address_t disk_address,
                           log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , parent_address_(std::move(parent_address))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address))
        , plans_(this->resource())
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
        components::catalog::used_format_t data_format
    ) {
        trace(log_, "executor::execute_plan, session: {}", session.data());

        auto limit = components::logical_plan::limit_t::unlimit();
        for (const auto& child : logical_plan->children()) {
            if (child->type() == components::logical_plan::node_type::limit_t) {
                limit = static_cast<components::logical_plan::node_limit_t*>(child.get())->limit();
            }
        }

        // TODO: this does not handle cross documents/columns operations
        components::base::operators::operator_ptr plan;
        if (data_format == components::catalog::used_format_t::documents) {
            plan = collection::planner::create_plan(context_storage, logical_plan, limit);
        } else if (data_format == components::catalog::used_format_t::columns) {
            plan = table::planner::create_plan(context_storage, logical_plan, limit);
        }

        if (!plan) {
            co_return execute_result_t{
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"),
                {}
            };
        }

        plan->set_as_root();

        auto wal_params = components::logical_plan::make_parameter_node(resource());
        wal_params->set_parameters(parameters);

        traverse_plan_(session, std::move(plan), std::move(parameters), std::move(context_storage));

        auto result = co_await execute_sub_plan_(session);

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
                    co_await std::move(df1);
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
                    co_await std::move(df2);
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
                    co_await std::move(df3);
                    break;
                }
                default:
                    break;
            }
        }

        co_return result;
    }

    void executor_t::traverse_plan_(const components::session::session_id_t& session,
                                    components::collection::operators::operator_ptr&& plan,
                                    components::logical_plan::storage_parameters&& parameters,
                                    services::context_storage_t&& context_storage) {
        std::stack<components::collection::operators::operator_ptr> look_up;
        std::stack<components::collection::operators::operator_ptr> sub_plans;
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

        plans_.emplace(session, plan_t{std::move(sub_plans), parameters, std::move(context_storage)});
    }

    executor_t::unique_future<execute_result_t> executor_t::execute_sub_plan_(
        const components::session::session_id_t& session) {

        auto& plan_data = plans_.at(session);
        cursor_t_ptr cursor;
        components::base::operators::operator_write_data_t::updated_types_map_t accumulated_updates(resource());

        while (!plan_data.sub_plans.empty()) {
            auto plan = plan_data.sub_plans.top();
            trace(log_, "executor::execute_sub_plan, session: {}", session.data());

            if (!plan) {
                cursor = make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan");
                break;
            }

            auto collection = plan->context();
            if (collection && collection->dropped()) {
                cursor = make_cursor(resource(), error_code_t::collection_dropped, "collection dropped");
                break;
            }

            components::pipeline::context_t pipeline_context{session, address(), parent_address_, plan_data.parameters};
            plan->on_execute(&pipeline_context);

            trace(log_, "executor: after on_execute, is_executed={}", plan->is_executed());
            if (!plan->is_executed()) {
                auto waiting_op = plan->find_waiting_operator();
                if (waiting_op) {
                    trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                    co_await waiting_op->await_async_and_resume(&pipeline_context);
                    trace(log_, "executor: after await_async_and_resume completed");

                    if (waiting_op->is_executed()) {
                        trace(log_, "executor: waiting op completed, re-executing root plan");
                        plan->on_execute(&pipeline_context);
                    }
                }

                if (!plan->is_executed()) {
                    error(log_, "Plan not executed after co_await! session: {}, plan type: {}",session.data(), static_cast<int>(plan->type()));
                    cursor = make_cursor(resource(), error_code_t::create_physical_plan_error,"operator failed to complete execution");
                    break;
                }
            }

            switch (plan->type()) {
                case components::collection::operators::operator_type::add_index: {
                    auto* add_op = static_cast<components::base::operators::operator_add_index*>(plan.get());

                    auto disk_address = co_await std::move(add_op->disk_future());

                    auto id_index = add_op->id_index();

                    if (id_index == components::index::INDEX_ID_UNDEFINED) {
                        trace(log_, "executor: index {} already exists, returning error", add_op->index_name());
                        cursor = make_cursor(resource(), error_code_t::index_create_fail, "index already exists");
                        break;
                    }

                    components::index::set_disk_agent(collection->index_engine(),
                        id_index, disk_address, collection->disk());
                    components::index::insert(collection->index_engine(),
                        id_index, collection->document_storage());

                    if (disk_address != actor_zeta::address_t::empty_address()) {
                        auto* index = components::index::search_index(collection->index_engine(), id_index);
                        auto range = index->keys();
                        std::vector<std::pair<components::document::value_t, document_id_t>> values;
                        values.reserve(collection->document_storage().size());
                        for (auto it = range.first; it != range.second; ++it) {
                            const auto& key_tmp = *it;
                            const std::string& key = key_tmp.as_string();
                            for (const auto& doc : collection->document_storage()) {
                                values.emplace_back(doc.second->get_value(key), doc.first);
                            }
                        }
                        auto [_idxins, idxf] = actor_zeta::send(collection->disk(),
                                         &services::disk::manager_disk_t::index_insert_many,
                                         session,
                                         add_op->index_name(),
                                         std::move(values));
                        co_await std::move(idxf);
                    }
                    cursor = make_cursor(resource(), operation_status_t::success);
                    break;
                }

                case components::collection::operators::operator_type::drop_index: {
                    auto* drop_op = static_cast<components::base::operators::operator_drop_index*>(plan.get());
                    cursor = drop_op->error_cursor()
                        ? drop_op->error_cursor()
                        : make_cursor(resource(), operation_status_t::success);
                    break;
                }

                case components::collection::operators::operator_type::insert:
                    cursor = co_await insert_document_impl_(session, collection, std::move(plan));
                    break;

                case components::collection::operators::operator_type::remove: {
                    if (plan->modified()) {
                        for (auto& [key, val] : plan->modified()->updated_types_map()) {
                            accumulated_updates[key] += val;
                        }
                    }
                    cursor = co_await delete_document_impl_(session, collection, std::move(plan));
                    break;
                }

                case components::collection::operators::operator_type::update:
                    cursor = co_await update_document_impl_(session, collection, std::move(plan));
                    break;

                case components::collection::operators::operator_type::raw_data:
                case components::collection::operators::operator_type::join:
                case components::collection::operators::operator_type::aggregate:
                    cursor = co_await aggregate_document_impl_(session, collection, std::move(plan));
                    break;

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
        plans_.erase(session);
        co_return execute_result_t{std::move(cursor), std::move(accumulated_updates)};
    }

    executor_t::unique_future<cursor_t_ptr> executor_t::aggregate_document_impl_(
        const components::session::session_id_t& session,
        context_collection_t* collection,
        components::collection::operators::operator_ptr plan) {

        if (plan->type() == components::collection::operators::operator_type::aggregate) {
            trace(log_, "executor::execute_plan : operators::operator_type::agreggate, session: {}", session.data());
        } else if (plan->type() == components::collection::operators::operator_type::join) {
            trace(log_, "executor::execute_plan : operators::operator_type::join, session: {}", session.data());
        } else {
            trace(log_, "executor::execute_plan : operators::operator_type::raw_data, session: {}", session.data());
        }

        if (plan->is_root()) {
            if (!collection) {
                if (plan->output()->uses_data_chunk()) {
                    co_return make_cursor(resource(), std::move(plan->output()->data_chunk()));
                } else {
                    co_return make_cursor(resource(), std::move(plan->output()->documents()));
                }
            } else if (collection->uses_datatable()) {
                components::vector::data_chunk_t chunk(resource(), collection->table_storage().table().copy_types());
                if (plan->output()) {
                    chunk = std::move(plan->output()->data_chunk());
                }
                co_return make_cursor(resource(), std::move(chunk));
            } else {
                std::pmr::vector<document_ptr> docs;
                if (plan->output()) {
                    docs = std::move(plan->output()->documents());
                }
                co_return make_cursor(resource(), std::move(docs));
            }
        } else {
            co_return make_cursor(resource(), operation_status_t::success);
        }
    }

    executor_t::unique_future<cursor_t_ptr> executor_t::update_document_impl_(
        const components::session::session_id_t& session,
        context_collection_t* collection,
        components::collection::operators::operator_ptr plan) {

        trace(log_, "executor::execute_plan : operators::operator_type::update");

        auto output = plan->output();
        auto modified = plan->modified();

        if (collection->uses_datatable()) {
            if (output) {
                auto ids_to_remove = modified->ids();
                auto data_chunk = std::move(output->data_chunk());
                auto [_rm1, rmf1] = actor_zeta::send(collection->disk(),
                                 &services::disk::manager_disk_t::remove_documents,
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 std::move(ids_to_remove));
                co_await std::move(rmf1);
                co_return make_cursor(resource(), std::move(data_chunk));
            } else {
                if (modified) {
                    auto ids_to_remove = modified->ids();
                    size_t cardinality = std::get<std::pmr::vector<size_t>>(ids_to_remove).size();
                    auto [_rm2, rmf2] = actor_zeta::send(collection->disk(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::move(ids_to_remove));
                    co_await std::move(rmf2);
                    components::vector::data_chunk_t chunk(resource(),
                                                           collection->table_storage().table().copy_types());
                    chunk.set_cardinality(cardinality);
                    // TODO: fill chunk with modified rows
                    co_return make_cursor(resource(), std::move(chunk));
                } else {
                    auto [_rm3, rmf3] = actor_zeta::send(collection->disk(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<size_t>{resource()});
                    co_await std::move(rmf3);
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
        } else {
            if (output) {
                auto new_id = components::document::get_document_id(output->documents().front());
                std::pmr::vector<document_id_t> ids{resource()};
                std::pmr::vector<document_ptr> documents{resource()};
                ids.emplace_back(new_id);
                auto [_rm4, rmf4] = actor_zeta::send(collection->disk(),
                                 &services::disk::manager_disk_t::remove_documents,
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 ids);
                co_await std::move(rmf4);
                for (const auto& id : ids) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
                co_return make_cursor(resource(), std::move(documents));
            } else {
                if (modified) {
                    auto ids_to_remove = modified->ids();
                    std::pmr::vector<document_ptr> documents(resource());
                    for (const auto& id :
                         std::get<std::pmr::vector<components::document::document_id_t>>(ids_to_remove)) {
                        documents.emplace_back(collection->document_storage().at(id));
                    }
                    auto [_rm5, rmf5] = actor_zeta::send(collection->disk(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::move(ids_to_remove));
                    co_await std::move(rmf5);
                    co_return make_cursor(resource(), std::move(documents));
                } else {
                    auto [_rm6, rmf6] = actor_zeta::send(collection->disk(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<document_id_t>{resource()});
                    co_await std::move(rmf6);
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
        }
    }

    executor_t::unique_future<cursor_t_ptr> executor_t::insert_document_impl_(
        const components::session::session_id_t& session,
        context_collection_t* collection,
        components::collection::operators::operator_ptr plan) {
        
        
        auto output = plan->output();
        auto modified = plan->modified();

        trace(log_,
              "executor::execute_plan : operators::operator_type::insert {}",
              output ? output->size() : 0);
        // TODO: disk support for data_table
        if (!output || output->uses_documents()) {
            auto docs_to_write = output ? std::move(output->documents())
                                        : std::pmr::vector<document_ptr>{resource()};
            auto [_wr1, wrf1] = actor_zeta::send(collection->disk(),
                             &services::disk::manager_disk_t::write_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             std::move(docs_to_write));
            co_await std::move(wrf1);
            std::pmr::vector<document_ptr> documents(resource());
            if (modified) {
                for (const auto& id :
                     std::get<std::pmr::vector<components::document::document_id_t>>(modified->ids())) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
            } else {
                for (const auto& doc : collection->document_storage()) {
                    documents.emplace_back(doc.second);
                }
            }
            co_return make_cursor(resource(), std::move(documents));
        } else {
            size_t size = 0;
            if (modified) {
                size = std::get<std::pmr::vector<size_t>>(modified->ids()).size();
            } else {
                size = collection->table_storage().table().calculate_size();
            }
            components::vector::data_chunk_t chunk(resource(), {}, size);
            chunk.set_cardinality(size);
            co_return make_cursor(resource(), std::move(chunk));
        }
    }

    executor_t::unique_future<cursor_t_ptr> executor_t::delete_document_impl_(
        const components::session::session_id_t& session,
        context_collection_t* collection,
        components::collection::operators::operator_ptr plan) {

        trace(log_, "executor::execute_plan : operators::operator_type::remove");

        auto modified_data = plan->modified();
        size_t modified_size = modified_data ? modified_data->size() : 0;

        if (collection->uses_datatable()) {
            auto ids_to_remove = modified_data ? modified_data->ids() : std::pmr::vector<size_t>{resource()};
            auto [_del1, delf1] = actor_zeta::send(collection->disk(),
                             &services::disk::manager_disk_t::remove_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             std::move(ids_to_remove));
            co_await std::move(delf1);
            components::vector::data_chunk_t chunk(resource(), collection->table_storage().table().copy_types(), modified_size);
            chunk.set_cardinality(modified_size);
            co_return make_cursor(resource(), std::move(chunk));
        } else {
            auto ids_to_remove = modified_data ? modified_data->ids() : std::pmr::vector<document_id_t>{resource()};
            auto [_del2, delf2] = actor_zeta::send(collection->disk(),
                             &services::disk::manager_disk_t::remove_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             std::move(ids_to_remove));
            co_await std::move(delf2);
            std::pmr::vector<document_ptr> documents(resource());
            documents.resize(modified_size);
            co_return make_cursor(resource(), std::move(documents));
        }
    }

} // namespace services::collection::executor