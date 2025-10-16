#include "executor.hpp"

#include <components/index/disk/route.hpp>
#include <components/physical_plan/collection/operators/operator_delete.hpp>
#include <components/physical_plan/collection/operators/operator_insert.hpp>
#include <components/physical_plan/collection/operators/operator_update.hpp>
#include <components/physical_plan/collection/operators/scan/primary_key_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/system_command.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/memory_storage/route.hpp>

using namespace components::cursor;

namespace services::collection::executor {

    plan_t::plan_t(std::stack<components::collection::operators::operator_ptr>&& sub_plans,
                   components::logical_plan::storage_parameters parameters,
                   services::context_storage_t&& context_storage)
        : sub_plans(std::move(sub_plans))
        , parameters(parameters)
        , context_storage_(context_storage) {}

    executor_t::executor_t(services::memory_storage_t* memory_storage, log_t&& log)
        : actor_zeta::basic_actor<executor_t>{memory_storage}
        , memory_storage_(memory_storage->address())
        , plans_(resource())
        , log_(log)
        , execute_plan_(
              actor_zeta::make_behavior(resource(), handler_id(route::execute_plan), this, &executor_t::execute_plan))
        , create_documents_(actor_zeta::make_behavior(resource(),
                                                      handler_id(route::create_documents),
                                                      this,
                                                      &executor_t::create_documents))
        , create_index_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success_create),
                                                         this,
                                                         &executor_t::create_index_finish))
        , create_index_finish_index_exist_(actor_zeta::make_behavior(resource(),
                                                                     handler_id(index::route::error),
                                                                     this,
                                                                     &executor_t::create_index_finish_index_exist))
        , index_modify_finish_(actor_zeta::make_behavior(resource(),
                                                         handler_id(index::route::success),
                                                         this,
                                                         &executor_t::index_modify_finish))
        , index_find_finish_(actor_zeta::make_behavior(resource(),
                                                       handler_id(index::route::success_find),
                                                       this,
                                                       &executor_t::index_find_finish)) {}

    actor_zeta::behavior_t executor_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case handler_id(route::create_documents): {
                    create_documents_(msg);
                    break;
                }
                case handler_id(index::route::success_create): {
                    create_index_finish_(msg);
                    break;
                }
                case handler_id(index::route::error): {
                    create_index_finish_index_exist_(msg);
                    break;
                }
                case handler_id(index::route::success): {
                    index_modify_finish_(msg);
                    break;
                }
                case handler_id(index::route::success_find): {
                    index_find_finish_(msg);
                    break;
                }
            }
        });
    }

    auto executor_t::make_type() const noexcept -> const char* const { return "executor"; }

    void executor_t::execute_plan(const components::session::session_id_t& session,
                                  components::logical_plan::node_ptr logical_plan,
                                  components::logical_plan::storage_parameters parameters,
                                  services::context_storage_t&& context_storage,
                                  components::catalog::used_format_t data_format) {
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
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"));
            return;
        }
        plan->set_as_root();
        traverse_plan_(session, std::move(plan), std::move(parameters), std::move(context_storage));
    }

    void executor_t::create_documents(const components::session::session_id_t& session,
                                      context_collection_t* collection,
                                      const std::pmr::vector<document_ptr>& documents) {
        trace(log_,
              "executor_t::create_documents: {}::{}, count: {}",
              collection->name().database,
              collection->name().collection,
              documents.size());
        //components::pipeline::context_t pipeline_context{session, address(), components::logical_plan::storage_parameters{}};
        //insert_(&pipeline_context, documents);
        for (const auto& doc : documents) {
            collection->document_storage().emplace(components::document::get_document_id(doc), doc);
        }
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::create_documents_finish), session);
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
        // start execution chain by sending first avaliable sub_plan
        auto current_plan =
            (plans_.emplace(session, plan_t{std::move(sub_plans), parameters, std::move(context_storage)}))
                .first->second.sub_plans.top();
        execute_sub_plan_(session, current_plan, parameters);
    }

    void executor_t::execute_sub_plan_(const components::session::session_id_t& session,
                                       components::collection::operators::operator_ptr plan,
                                       components::logical_plan::storage_parameters parameters) {
        trace(log_, "executor::execute_sub_plan, session: {}", session.data());
        if (!plan) {
            execute_sub_plan_finish_(
                session,
                make_cursor(resource(), error_code_t::create_physical_plan_error, "invalid query plan"));
            return;
        }
        auto collection = plan->context();
        if (collection && collection->dropped()) {
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), error_code_t::collection_dropped, "collection dropped"));
            return;
        }
        components::pipeline::context_t pipeline_context{session, address(), memory_storage_, parameters};
        plan->on_execute(&pipeline_context);
        if (!plan->is_executed()) {
            sessions::make_session(
                collection->sessions(),
                session,
                sessions::suspend_plan_t{memory_storage_, std::move(plan), std::move(pipeline_context)});
            return;
        }

        switch (plan->type()) {
            case components::collection::operators::operator_type::insert: {
                insert_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::remove: {
                delete_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::update: {
                update_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::raw_data:
            case components::collection::operators::operator_type::join:
            case components::collection::operators::operator_type::aggregate: {
                aggregate_document_impl(session, collection, std::move(plan));
                return;
            }
            case components::collection::operators::operator_type::add_index:
            case components::collection::operators::operator_type::drop_index: {
                // nothing to do
                return;
            }
            default: {
                execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                return;
            }
        }
    }

    void executor_t::execute_sub_plan_finish_(
        const components::session::session_id_t& session,
        cursor_t_ptr result,
        components::base::operators::operator_write_data_t::updated_types_map_t updates) {
        if (result->is_error() || !plans_.contains(session)) {
            execute_plan_finish_(session, std::move(result), std::move(updates));
        }
        auto& plan = plans_.at(session);
        if (plan.sub_plans.size() == 1) {
            execute_plan_finish_(session, std::move(result), std::move(updates));
        } else {
            assert(!plan.sub_plans.empty() && "executor_t:execute_sub_plan_finish_: sub plans execution failed");
            plan.sub_plans.pop();
            execute_sub_plan_(session, plan.sub_plans.top(), plan.parameters);
        }
    }

    void
    executor_t::execute_plan_finish_(const components::session::session_id_t& session,
                                     cursor_t_ptr&& cursor,
                                     components::base::operators::operator_write_data_t::updated_types_map_t updates) {
        trace(log_, "executor::execute_plan_finish, success: {}", cursor->is_success());
        if (updates.empty()) {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             std::move(cursor));
        } else {
            actor_zeta::send(memory_storage_,
                             address(),
                             handler_id(memory_storage::route::execute_plan_delete_finish),
                             session,
                             std::move(cursor),
                             std::move(updates));
        }
        plans_.erase(session);
    }

    void executor_t::aggregate_document_impl(const components::session::session_id_t& session,
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
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->data_chunk())));
                } else {
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->documents())));
                }
            } else if (collection->uses_datatable()) {
                components::vector::data_chunk_t chunk(resource(), collection->table_storage().table().copy_types());
                if (plan->output()) {
                    chunk = std::move(plan->output()->data_chunk());
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
            } else {
                std::pmr::vector<document_ptr> docs;
                if (plan->output()) {
                    docs = std::move(plan->output()->documents());
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(docs)));
            }
        } else {
            execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
        }
    }

    void executor_t::update_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::update");

        if (collection->uses_datatable()) {
            if (plan->output()) {
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 plan->modified()->ids());
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(plan->output()->data_chunk())));
            } else {
                if (plan->modified()) {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    components::vector::data_chunk_t chunk(resource(),
                                                           collection->table_storage().table().copy_types());
                    chunk.set_cardinality(std::get<std::pmr::vector<size_t>>(plan->modified()->ids()).size());
                    // TODO: fill chunk with modified rows
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
                } else {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<size_t>{resource()});
                    execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                }
            }
        } else {
            if (plan->output()) {
                auto new_id = components::document::get_document_id(plan->output()->documents().front());
                std::pmr::vector<document_id_t> ids{resource()};
                std::pmr::vector<document_ptr> documents{resource()};
                ids.emplace_back(new_id);
                actor_zeta::send(collection->disk(),
                                 address(),
                                 disk::handler_id(disk::route::remove_documents),
                                 session,
                                 collection->name().database,
                                 collection->name().collection,
                                 ids);
                for (const auto& id : ids) {
                    documents.emplace_back(collection->document_storage().at(id));
                }
                execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
            } else {
                if (plan->modified()) {
                    std::pmr::vector<document_ptr> documents(resource());
                    for (const auto& id :
                         std::get<std::pmr::vector<components::document::document_id_t>>(plan->modified()->ids())) {
                        documents.emplace_back(collection->document_storage().at(id));
                    }
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     plan->modified()->ids());
                    execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
                } else {
                    actor_zeta::send(collection->disk(),
                                     address(),
                                     disk::handler_id(disk::route::remove_documents),
                                     session,
                                     collection->name().database,
                                     collection->name().collection,
                                     std::pmr::vector<document_id_t>{resource()});
                    execute_sub_plan_finish_(session, make_cursor(resource(), operation_status_t::success));
                }
            }
        }
    }

    void executor_t::insert_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_,
              "executor::execute_plan : operators::operator_type::insert {}",
              plan->output() ? plan->output()->size() : 0);
        // TODO: disk support for data_table
        if (!plan->output() || plan->output()->uses_documents()) {
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::write_documents),
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
            execute_sub_plan_finish_(session, make_cursor(resource(), std::move(documents)));
        } else {
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::write_documents),
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
            execute_sub_plan_finish_(session, make_cursor(resource(), std::move(chunk)));
        }
    }

    void executor_t::delete_document_impl(const components::session::session_id_t& session,
                                          context_collection_t* collection,
                                          components::collection::operators::operator_ptr plan) {
        trace(log_, "executor::execute_plan : operators::operator_type::remove");

        if (collection->uses_datatable()) {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<size_t>{resource()};
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            size_t size = plan->modified()->size();
            components::vector::data_chunk_t chunk(resource(), collection->table_storage().table().copy_types(), size);
            chunk.set_cardinality(size);
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), std::move(chunk)),
                                     std::move(plan->modified()->updated_types_map()));
        } else {
            auto modified = plan->modified() ? plan->modified()->ids() : std::pmr::vector<document_id_t>{resource()};
            actor_zeta::send(collection->disk(),
                             address(),
                             disk::handler_id(disk::route::remove_documents),
                             session,
                             collection->name().database,
                             collection->name().collection,
                             modified);
            std::pmr::vector<document_ptr> documents(resource());
            documents.resize(plan->modified()->size());
            execute_sub_plan_finish_(session,
                                     make_cursor(resource(), std::move(documents)),
                                     std::move(plan->modified()->updated_types_map()));
        }
    }

} // namespace services::collection::executor