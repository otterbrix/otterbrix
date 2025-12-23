#include "memory_storage.hpp"
#include "route.hpp"
#include <cassert>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/system_command.hpp>
#include <core/tracy/tracy.hpp>
#include <services/collection/collection.hpp>
#include <utility>

using namespace components::cursor;

namespace services {

    using namespace services::memory_storage;
    using namespace core::pmr;

    memory_storage_t::load_buffer_t::load_buffer_t(std::pmr::memory_resource* resource)
        : collections(resource) {}
    memory_storage_t::memory_storage_t(std::pmr::memory_resource* o_resource,
                                       actor_zeta::scheduler_raw scheduler,
                                       log_t& log)
        : actor_zeta::cooperative_supervisor<memory_storage_t>(o_resource)
        , e_(scheduler)
        , databases_(resource())
        , collections_(resource())
        , log_(log.clone())
        , sync_(
              actor_zeta::make_behavior(resource(), core::handler_id(core::route::sync), this, &memory_storage_t::sync))
        , load_(actor_zeta::make_behavior(resource(), handler_id(route::load), this, &memory_storage_t::load))
        , size_(actor_zeta::make_behavior(resource(),
                                          collection::handler_id(collection::route::size),
                                          this,
                                          &memory_storage_t::size))
        , create_documents_finish_(
              actor_zeta::make_behavior(resource(),
                                        collection::handler_id(collection::route::create_documents_finish),
                                        this,
                                        &memory_storage_t::create_documents_finish))
        , execute_plan_(actor_zeta::make_behavior(resource(),
                                                  handler_id(route::execute_plan),
                                                  this,
                                                  &memory_storage_t::execute_plan))
        , execute_plan_finish_(actor_zeta::make_behavior(resource(),
                                                         collection::handler_id(collection::route::execute_plan_finish),
                                                         this,
                                                         &memory_storage_t::execute_plan_finish))
        , execute_plan_delete_finish_(actor_zeta::make_behavior(resource(),
                                                                handler_id(route::execute_plan_delete_finish),
                                                                this,
                                                                &memory_storage_t::execute_plan_delete_finish))

    {
        ZoneScoped;
        trace(log_, "memory_storage start thread pool");
        //TODO MIGHT BE CHANGED
        executor_address_ = (spawn_actor(
            [this](services::collection::executor::executor_t* ptr) {
                executor_ = collection::executor::executor_ptr(ptr, actor_zeta::pmr::deleter_t(resource()));
            },
            log_.clone()));
    }

    memory_storage_t::~memory_storage_t() {
        ZoneScoped;
        trace(log_, "delete memory_storage");
    }

    auto memory_storage_t::make_type() const noexcept -> const char* { return "memory_storage"; }

    actor_zeta::scheduler::scheduler_abstract_t* memory_storage_t::make_scheduler() noexcept { return e_; }

    actor_zeta::behavior_t memory_storage_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case core::handler_id(core::route::sync): {
                    sync_(msg);
                    break;
                }
                case handler_id(route::load): {
                    load_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size): {
                    size_(msg);
                    break;
                }
                case collection::handler_id(collection::route::create_documents_finish): {
                    create_documents_finish_(msg);
                    break;
                }
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case collection::handler_id(collection::route::execute_plan_finish): {
                    execute_plan_finish_(msg);
                    break;
                }
                case handler_id(route::execute_plan_delete_finish): {
                    execute_plan_delete_finish_(msg);
                    break;
                }
            }
        });
    }

    void memory_storage_t::sync(const address_pack& pack) {
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
    }

    void memory_storage_t::execute_plan(const components::session::session_id_t& session,
                                        components::logical_plan::node_ptr logical_plan,
                                        components::logical_plan::storage_parameters parameters,
                                        components::catalog::used_format_t used_format) {
        using components::logical_plan::node_type;

        switch (logical_plan->type()) {
            case node_type::create_database_t:
                create_database_(session, std::move(logical_plan));
                break;
            case node_type::drop_database_t:
                drop_database_(session, std::move(logical_plan));
                break;
            case node_type::create_collection_t:
                create_collection_(session, std::move(logical_plan));
                break;
            case node_type::drop_collection_t:
                drop_collection_(session, std::move(logical_plan));
                break;
            default:
                execute_plan_impl(session, std::move(logical_plan), std::move(parameters), used_format);
                break;
        }
    }

    void memory_storage_t::size(const components::session::session_id_t& session, collection_full_name_t&& name) {
        trace(log_, "collection {}::{}::size", name.database, name.collection);
        auto collection = collections_.at(name).get();
        if (collection->dropped()) {
            actor_zeta::send(current_message()->sender(),
                             address(),
                             handler_id(collection::route::size_finish),
                             session,
                             make_cursor(resource(), error_code_t::collection_dropped));
        } else {
            if (collection->uses_datatable()) {
                size_t size = collection->table_storage().table().calculate_size();
                components::vector::data_chunk_t chunk(resource(),
                                                       collection->table_storage().table().copy_types(),
                                                       size);
                chunk.set_cardinality(size);
                actor_zeta::send(current_message()->sender(),
                                 address(),
                                 handler_id(collection::route::size_finish),
                                 session,
                                 make_cursor(resource(), std::move(chunk)));
            } else {
                std::pmr::vector<components::document::document_ptr> documents(resource());
                for (const auto& doc : collection->document_storage()) {
                    documents.emplace_back(doc.second);
                }
                actor_zeta::send(current_message()->sender(),
                                 address(),
                                 handler_id(collection::route::size_finish),
                                 session,
                                 make_cursor(resource(), std::move(documents)));
            }
        }
    }

    void memory_storage_t::load(const components::session::session_id_t& session, const disk::result_load_t& result) {
        trace(log_, "memory_storage_t:load");
        load_buffer_ = std::make_unique<load_buffer_t>(resource());
        auto count_collections = std::accumulate(
            (*result).begin(),
            (*result).end(),
            0ul,
            [](size_t sum, const disk::result_database_t& database) { return sum + database.collections.size(); });
        if (count_collections > 0) {
            sessions_.emplace(session, session_t{nullptr, current_message()->sender(), count_collections});
        }
        for (const auto& database : (*result)) {
            debug(log_, "memory_storage_t:load:create_database: {}", database.name);
            databases_.insert(database.name);
            for (const auto& collection : database.collections) {
                debug(log_, "memory_storage_t:load:create_collection: {}", collection.name);
                collection_full_name_t name(database.name, collection.name);
                auto context = new collection::context_collection_t(resource(), name, manager_disk_, log_.clone());
                collections_.emplace(name, context);
                load_buffer_->collections.emplace_back(name);
                debug(log_, "memory_storage_t:load:fill_documents: {}", collection.documents.size());
                actor_zeta::send(executor_address_,
                                 address(),
                                 collection::handler_id(collection::route::create_documents),
                                 session,
                                 context,
                                 collection.documents);
            }
        }
        if (count_collections == 0) {
            actor_zeta::send(current_message()->sender(), address(), handler_id(route::load_finish), session);
            load_buffer_.reset();
        }
    }

    void memory_storage_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    void memory_storage_t::create_database_(const components::session::session_id_t& session,
                                            components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_database {}", logical_plan->database_name());
        databases_.insert(logical_plan->database_name());
        actor_zeta::send(current_message()->sender(),
                         this->address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         make_cursor(resource(), operation_status_t::success));
    }

    void memory_storage_t::drop_database_(const components::session::session_id_t& session,
                                          components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_database {}", logical_plan->database_name());
        databases_.erase(logical_plan->database_name());
        actor_zeta::send(current_message()->sender(),
                         this->address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         make_cursor(resource(), operation_status_t::success));
    }

    void memory_storage_t::create_collection_(const components::session::session_id_t& session,
                                              components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_collection {}", logical_plan->collection_full_name().to_string());
        auto create_collection_plan =
            reinterpret_cast<const components::logical_plan::node_create_collection_ptr&>(logical_plan);
        if (create_collection_plan->schema().empty()) {
            collections_.emplace(logical_plan->collection_full_name(),
                                 new collection::context_collection_t(resource(),
                                                                      logical_plan->collection_full_name(),
                                                                      manager_disk_,
                                                                      log_.clone()));
        } else {
            std::vector<components::table::column_definition_t> columns;
            columns.reserve(create_collection_plan->schema().size());
            for (const auto& type : create_collection_plan->schema()) {
                columns.emplace_back(type.alias(), type);
            }
            collections_.emplace(logical_plan->collection_full_name(),
                                 new collection::context_collection_t(resource(),
                                                                      logical_plan->collection_full_name(),
                                                                      std::move(columns),
                                                                      manager_disk_,
                                                                      log_.clone()));
        }
        auto cursor = make_cursor(resource(), operation_status_t::success);
        actor_zeta::send(current_message()->sender(),
                         this->address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         cursor);
    }

    void memory_storage_t::drop_collection_(const components::session::session_id_t& session,
                                            components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_collection {}", logical_plan->collection_full_name().to_string());
        sessions_.emplace(session, session_t{logical_plan, current_message()->sender(), 1});
        actor_zeta::send(current_message()->sender(),
                         address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         collections_.at(logical_plan->collection_full_name())->drop()
                             ? make_cursor(resource(), operation_status_t::success)
                             : make_cursor(resource(), error_code_t::other_error, "collection not dropped"));
        sessions_.erase(session);
        collections_.erase(logical_plan->collection_full_name());
        trace(log_, "memory_storage_t:drop_collection_finish {}", logical_plan->collection_full_name().to_string());
    }

    void memory_storage_t::execute_plan_impl(const components::session::session_id_t& session,
                                             components::logical_plan::node_ptr logical_plan,
                                             components::logical_plan::storage_parameters parameters,
                                             components::catalog::used_format_t used_format) {
        trace(log_,
              "memory_storage_t:execute_plan_impl: collection: {}, sesion: {}",
              logical_plan->collection_full_name().to_string(),
              session.data());
        if (used_format != components::catalog::used_format_t::undefined) {
            auto dependency_tree_collections_names = logical_plan->collection_dependencies();
            context_storage_t collections_context_storage;
            while (!dependency_tree_collections_names.empty()) {
                collection_full_name_t name =
                    dependency_tree_collections_names.extract(dependency_tree_collections_names.begin()).value();
                if (name.empty()) {
                    // raw_data from ql does not belong to any collection
                    collections_context_storage.emplace(std::move(name), nullptr);
                    continue;
                }
                collections_context_storage.emplace(std::move(name), collections_.at(name).get());
            }
            sessions_.emplace(session, session_t{logical_plan, current_message()->sender(), 1});
            actor_zeta::send(executor_address_,
                             address(),
                             collection::handler_id(collection::route::execute_plan),
                             session,
                             logical_plan,
                             parameters,
                             std::move(collections_context_storage),
                             used_format);
        }
    }

    void memory_storage_t::execute_plan_finish(const components::session::session_id_t& session, cursor_t_ptr result) {
        auto& s = sessions_.at(session);
        debug(log_,
              "memory_storage_t:execute_plan_finish: session: {}, success: {}",
              session.data(),
              result->is_success());
        actor_zeta::send(s.sender, address(), handler_id(route::execute_plan_finish), session, std::move(result));
        sessions_.erase(session);
    }

    void memory_storage_t::execute_plan_delete_finish(
        const components::session::session_id_t& session,
        cursor_t_ptr result,
        components::base::operators::operator_write_data_t::updated_types_map_t updates) {
        auto& s = sessions_.at(session);
        debug(log_,
              "memory_storage_t:execute_plan_delete_finish: session: {}, success: {}",
              session.data(),
              result->is_success());
        actor_zeta::send(s.sender,
                         address(),
                         handler_id(route::execute_plan_delete_finish),
                         session,
                         std::move(result),
                         std::move(updates));
        sessions_.erase(session);
    }

    void memory_storage_t::create_documents_finish(const components::session::session_id_t& session) {
        if (!sessions_.contains(session)) {
            return;
        }
        auto& s = sessions_.at(session);
        --s.count_answers;
        debug(log_, "memory_storage_t:create_documents_finish: {}, rest: {}", session.data(), s.count_answers);
        if (s.count_answers == 0) {
            actor_zeta::send(s.sender, address(), handler_id(route::load_finish), session);
            load_buffer_.reset();
            sessions_.erase(session);
        }
    }

} // namespace services
