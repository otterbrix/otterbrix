#include "executor.hpp"

#include <components/index/index_engine.hpp>
#include <components/physical_plan/base/operators/operator_add_index.hpp>
#include <components/physical_plan/base/operators/operator_drop_index.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/excutor.hpp>
#include <services/disk/index_agent_disk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/memory_storage/memory_storage.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::collection::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(std::pmr::memory_resource* resource, services::memory_storage_t* memory_storage, log_t&& log)
        : actor_zeta::basic_actor<executor_t>{resource}
        , memory_storage_(memory_storage->address())
        , plans_(this->resource())
        , log_(log) {}

    void executor_t::behavior(actor_zeta::mailbox::message* msg) {
        // Poll completed coroutines first (per PROMISE_FUTURE_GUIDE.md)
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<executor_t, &executor_t::execute_plan>: {
                // CRITICAL: Store pending coroutine! execute_plan uses promise/future
                auto future = actor_zeta::dispatch(this, &executor_t::execute_plan, msg);
                if (!future.available()) {
                    pending_execute_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<executor_t, &executor_t::create_documents>: {
                auto future = actor_zeta::dispatch(this, &executor_t::create_documents, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            default:
                break;
        }
    }

    // Poll and clean up completed coroutines (per PROMISE_FUTURE_GUIDE.md)
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

        // TODO: this does not handle cross documents/columns operations
        components::base::operators::operator_ptr plan;
        if (data_format == components::catalog::used_format_t::documents) {
            plan = collection::planner::create_plan(context_storage,
                                                    logical_plan,
                                                    components::logical_plan::limit_t::unlimit());
        } else if (data_format == components::catalog::used_format_t::columns) {
            plan = table::planner::create_plan(context_storage,
                                               logical_plan,
                                               components::logical_plan::limit_t::unlimit());
        }

        if (!plan) {
            // Return error directly via future (NOT callback!)
            co_return execute_result_t{
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"),
                {}
            };
        }

        plan->set_as_root();
        traverse_plan_(session, std::move(plan), std::move(parameters), std::move(context_storage));

        // Directly await result from execute_sub_plan_ (without intermediate promise)
        co_return co_await execute_sub_plan_(session);
    }

    executor_t::unique_future<void> executor_t::create_documents(
        session_id_t session,
        context_collection_t* collection,
        std::pmr::vector<document_ptr> documents
    ) {
        trace(log_,
              "executor_t::create_documents: {}::{}, count: {}",
              collection->name().database,
              collection->name().collection,
              documents.size());
        for (const auto& doc : documents) {
            collection->document_storage().emplace(components::document::get_document_id(doc), doc);
        }
        // With futures, caller gets notified via co_await, no callback needed
        co_return;
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

        // Store plans - execute_sub_plan_ will be called directly via co_await
        plans_.emplace(session, plan_t{std::move(sub_plans), parameters, std::move(context_storage)});
    }

    // Coroutine-based execute_sub_plan_ - returns execute_result_t directly
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

            components::pipeline::context_t pipeline_context{session, address(), memory_storage_, plan_data.parameters};
            plan->on_execute(&pipeline_context);

            trace(log_, "executor: after on_execute, is_executed={}", plan->is_executed());
            if (!plan->is_executed()) {
                // Find the operator that's actually waiting (could be a child operator like index_scan)
                auto waiting_op = plan->find_waiting_operator();
                if (waiting_op) {
                    trace(log_, "executor: found waiting operator, type={}", static_cast<int>(waiting_op->type()));
                    // Call await_async_and_resume on the WAITING operator (not the root)
                    auto task = waiting_op->await_async_and_resume(&pipeline_context);
                    trace(log_, "executor: after await_async_and_resume, task.done()={}", task.done());

                    // After the waiting operator completes, re-execute the root plan to propagate results
                    if (task.done() && waiting_op->is_executed()) {
                        trace(log_, "executor: waiting op completed, re-executing root plan");
                        plan->on_execute(&pipeline_context);
                    }
                }

                // If still not executed, fall through to old mechanism
                if (!plan->is_executed()) {
                    sessions::make_session(
                        collection->sessions(),
                        session,
                        sessions::suspend_plan_t{memory_storage_, std::move(plan), std::move(pipeline_context)});
                    cursor = make_cursor(resource(), operation_status_t::success);
                    break;
                }
            }

            switch (plan->type()) {
                case components::collection::operators::operator_type::add_index: {
                    auto* add_op = static_cast<components::base::operators::operator_add_index*>(plan.get());

                    // co_await on disk future to get index agent address
                    auto disk_address = co_await std::move(add_op->disk_future());

                    // Inline create_index_finish logic:
                    auto& create_index_session = sessions::find(collection->sessions(), session, add_op->index_name())
                        .get<sessions::create_index_t>();

                    // Check if index already existed (id_index == INDEX_ID_UNDEFINED means index wasn't created)
                    if (create_index_session.id_index == components::index::INDEX_ID_UNDEFINED) {
                        // Index already exists - return error
                        trace(log_, "executor: index {} already exists, returning error", add_op->index_name());
                        sessions::remove(collection->sessions(), session, add_op->index_name());
                        cursor = make_cursor(resource(), error_code_t::index_create_fail, "index already exists");
                        break;
                    }

                    components::index::set_disk_agent(collection->index_engine(),
                        create_index_session.id_index, disk_address);
                    components::index::insert(collection->index_engine(),
                        create_index_session.id_index, collection->document_storage());

                    // Fill index_disk if disk is enabled
                    if (disk_address != actor_zeta::address_t::empty_address()) {
                        auto* index = components::index::search_index(collection->index_engine(), create_index_session.id_index);
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
                        actor_zeta::send(disk_address,
                                         address(),
                                         &services::disk::index_agent_disk_t::insert_many,
                                         session,
                                         values);
                    }
                    sessions::remove(collection->sessions(), session, add_op->index_name());
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
                    // Extract updates BEFORE moving plan (critical for schema versioning!)
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

            // Await all pending disk index operations (future pattern)
            // Note: pragma suppresses GCC false positive for moved-from futures
            // See docs/gcc-maybe-uninitialized-false-positive.md for details
            if (pipeline_context.has_pending_disk_futures()) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
                auto disk_futures = pipeline_context.take_pending_disk_futures();
                for (auto& fut : disk_futures) {
                    co_await std::move(fut);
                }
#pragma GCC diagnostic pop
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
            trace(log_, "executor::execute_plan : operators::operator_type::agreggate");
        } else if (plan->type() == components::collection::operators::operator_type::join) {
            trace(log_, "executor::execute_plan : operators::operator_type::join");
        } else {
            trace(log_, "executor::execute_plan : operators::operator_type::raw_data");
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

        if (collection->uses_datatable()) {
            if (plan->output()) {
                actor_zeta::otterbrix::send(collection->disk(),
                                 address(),
                                 &services::disk::manager_disk_t::remove_documents,
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 plan->modified()->ids());
                co_return make_cursor(resource(), std::move(plan->output()->data_chunk()));
            } else {
                if (plan->modified()) {
                    actor_zeta::otterbrix::send(collection->disk(),
                                     address(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    components::vector::data_chunk_t chunk(resource(),
                                                           collection->table_storage().table().copy_types());
                    chunk.set_cardinality(std::get<std::pmr::vector<size_t>>(plan->modified()->ids()).size());
                    // TODO: fill chunk with modified rows
                    co_return make_cursor(resource(), std::move(chunk));
                } else {
                    actor_zeta::otterbrix::send(collection->disk(),
                                     address(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<size_t>{resource()});
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
        } else {
            if (plan->output()) {
                auto new_id = components::document::get_document_id(plan->output()->documents().front());
                std::pmr::vector<document_id_t> ids{resource()};
                std::pmr::vector<document_ptr> documents{resource()};
                ids.emplace_back(new_id);
                actor_zeta::otterbrix::send(collection->disk(),
                                 address(),
                                 &services::disk::manager_disk_t::remove_documents,
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 ids);
                for (const auto& id : ids) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
                co_return make_cursor(resource(), std::move(documents));
            } else {
                if (plan->modified()) {
                    std::pmr::vector<document_ptr> documents(resource());
                    for (const auto& id :
                         std::get<std::pmr::vector<components::document::document_id_t>>(plan->modified()->ids())) {
                        documents.emplace_back(collection->document_storage().at(id));
                    }
                    actor_zeta::otterbrix::send(collection->disk(),
                                     address(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    co_return make_cursor(resource(), std::move(documents));
                } else {
                    actor_zeta::otterbrix::send(collection->disk(),
                                     address(),
                                     &services::disk::manager_disk_t::remove_documents,
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<document_id_t>{resource()});
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
        }
    }

    executor_t::unique_future<cursor_t_ptr> executor_t::insert_document_impl_(
        const components::session::session_id_t& session,
        context_collection_t* collection,
        components::collection::operators::operator_ptr plan) {

        trace(log_,
              "executor::execute_plan : operators::operator_type::insert {}",
              plan->output() ? plan->output()->size() : 0);
        // TODO: disk support for data_table
        if (!plan->output() || plan->output()->uses_documents()) {
            actor_zeta::otterbrix::send(collection->disk(),
                             address(),
                             &services::disk::manager_disk_t::write_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             plan->output() ? std::move(plan->output()->documents())
                                            : std::pmr::vector<document_ptr>{resource()});
            std::pmr::vector<document_ptr> documents(resource());
            if (plan->modified()) {
                for (const auto& id :
                     std::get<std::pmr::vector<components::document::document_id_t>>(plan->modified()->ids())) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
            } else {
                for (const auto& doc : collection->document_storage()) {
                    documents.emplace_back(doc.second);
                }
            }
            co_return make_cursor(resource(), std::move(documents));
        } else {
            actor_zeta::otterbrix::send(collection->disk(),
                             address(),
                             &services::disk::manager_disk_t::write_data_chunk,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             plan->output() ? std::move(plan->output()->data_chunk())
                                            : components::vector::data_chunk_t{resource(), {}});
            size_t size = 0;
            if (plan->modified()) {
                size = std::get<std::pmr::vector<size_t>>(plan->modified()->ids()).size();
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

        if (collection->uses_datatable()) {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<size_t>{resource()};
            actor_zeta::otterbrix::send(collection->disk(),
                             address(),
                             &services::disk::manager_disk_t::remove_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            size_t size = plan->modified()->size();
            components::vector::data_chunk_t chunk(resource(), collection->table_storage().table().copy_types(), size);
            chunk.set_cardinality(size);
            // TODO: handle updated_types_map for delete
            co_return make_cursor(resource(), std::move(chunk));
        } else {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<document_id_t>{resource()};
            actor_zeta::otterbrix::send(collection->disk(),
                             address(),
                             &services::disk::manager_disk_t::remove_documents,
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            std::pmr::vector<document_ptr> documents(resource());
            documents.resize(plan->modified()->size());
            // TODO: handle updated_types_map for delete
            co_return make_cursor(resource(), std::move(documents));
        }
    }

} // namespace services::collection::executor