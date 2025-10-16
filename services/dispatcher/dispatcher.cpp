#include "dispatcher.hpp"

#include <core/system_command.hpp>
#include <core/tracy/tracy.hpp>

#include <components/document/document.hpp>
#include <components/planner/planner.hpp>

#include <services/collection/route.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/disk/route.hpp>
#include <services/memory_storage/context_storage.hpp>
#include <services/memory_storage/route.hpp>
#include <services/wal/route.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(manager_dispatcher_t* manager_dispatcher,
                               actor_zeta::address_t& mstorage,
                               actor_zeta::address_t& mwal,
                               actor_zeta::address_t& mdisk,
                               log_t& log)
        : actor_zeta::basic_actor<dispatcher_t>(manager_dispatcher)
        , load_(actor_zeta::make_behavior(resource(), core::handler_id(core::route::load), this, &dispatcher_t::load))
        , load_from_disk_result_(actor_zeta::make_behavior(resource(),
                                                           disk::handler_id(disk::route::load_finish),
                                                           this,
                                                           &dispatcher_t::load_from_disk_result))
        , load_from_memory_storage_result_(
              actor_zeta::make_behavior(resource(),
                                        memory_storage::handler_id(memory_storage::route::load_finish),
                                        this,
                                        &dispatcher_t::load_from_memory_storage_result))
        , load_from_wal_result_(actor_zeta::make_behavior(resource(),
                                                          wal::handler_id(wal::route::load_finish),
                                                          this,
                                                          &dispatcher_t::load_from_wal_result))
        , execute_plan_(
              actor_zeta::make_behavior(resource(), handler_id(route::execute_plan), this, &dispatcher_t::execute_plan))
        , execute_plan_finish_(
              actor_zeta::make_behavior(resource(),
                                        memory_storage::handler_id(memory_storage::route::execute_plan_finish),
                                        this,
                                        &dispatcher_t::execute_plan_finish))
        , execute_plan_delete_finish_(
              actor_zeta::make_behavior(resource(),
                                        memory_storage::handler_id(memory_storage::route::execute_plan_delete_finish),
                                        this,
                                        &dispatcher_t::execute_plan_delete_finish))
        , size_(actor_zeta::make_behavior(resource(),
                                          collection::handler_id(collection::route::size),
                                          this,
                                          &dispatcher_t::size))
        , size_finish_(actor_zeta::make_behavior(resource(),
                                                 collection::handler_id(collection::route::size_finish),
                                                 this,
                                                 &dispatcher_t::size_finish))
        , close_cursor_(actor_zeta::make_behavior(resource(),
                                                  collection::handler_id(collection::route::close_cursor),
                                                  this,
                                                  &dispatcher_t::close_cursor))
        , wal_success_(actor_zeta::make_behavior(resource(),
                                                 wal::handler_id(wal::route::success),
                                                 this,
                                                 &dispatcher_t::wal_success))
        , log_(log.clone())
        , catalog_(resource())
        , manager_dispatcher_(manager_dispatcher->address())
        , memory_storage_(mstorage)
        , manager_wal_(mwal)
        , manager_disk_(mdisk) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", make_type());
    }

    dispatcher_t::~dispatcher_t() { trace(log_, "delete dispatcher_t"); }

    auto dispatcher_t::make_type() const noexcept -> const char* const { return "dispatcher_t"; }

    actor_zeta::behavior_t dispatcher_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case core::handler_id(core::route::load): {
                    load_(msg);
                    break;
                }

                case disk::handler_id(disk::route::load_finish): {
                    load_from_disk_result_(msg);
                    break;
                }
                case memory_storage::handler_id(memory_storage::route::load_finish): {
                    load_from_memory_storage_result_(msg);
                    break;
                }
                case wal::handler_id(wal::route::load_finish): {
                    load_from_wal_result_(msg);
                    break;
                }
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case memory_storage::handler_id(memory_storage::route::execute_plan_finish): {
                    execute_plan_finish_(msg);
                    break;
                }
                case memory_storage::handler_id(memory_storage::route::execute_plan_delete_finish): {
                    execute_plan_delete_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size): {
                    size_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size_finish): {
                    size_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::close_cursor): {
                    close_cursor_(msg);
                    break;
                }
                case wal::handler_id(wal::route::success): {
                    wal_success_(msg);
                    break;
                }
            }
        });
    }

    void dispatcher_t::load(const components::session::session_id_t& session, actor_zeta::address_t sender) {
        trace(log_, "dispatcher_t::load, session: {}", session.data());
        load_session_ = session;
        make_session(session_to_address_, session, session_t(std::move(sender)));
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::load), session);
    }

    void dispatcher_t::load_from_disk_result(const components::session::session_id_t& session,
                                             const disk::result_load_t& result) {
        trace(log_, "dispatcher_t::load_from_disk_result, session: {}, wal_id: {}", session.data(), result.wal_id());
        if ((*result).empty()) {
            actor_zeta::send(find_session(session_to_address_, session).address(),
                             dispatcher_t::address(),
                             core::handler_id(core::route::load_finish),
                             session);
            remove_session(session_to_address_, session);
            load_result_.clear();
        } else {
            load_result_ = result;
            actor_zeta::send(memory_storage_,
                             address(),
                             memory_storage::handler_id(memory_storage::route::load),
                             session,
                             result);
        }
    }

    void dispatcher_t::load_from_memory_storage_result(const components::session::session_id_t& session) {
        trace(log_, "dispatcher_t::load_from_memory_storage_result, session: {}", session.data());
        actor_zeta::send(manager_disk_, address(), disk::handler_id(disk::route::load_indexes), session);
        actor_zeta::send(manager_wal_, address(), wal::handler_id(wal::route::load), session, load_result_.wal_id());
        for (const auto& database : (*load_result_)) {
            collection_full_name_t name;
            name.database = database.name;
            catalog_.create_namespace({database.name.c_str()});
            for (const auto& collection : database.collections) {
                auto err = catalog_.create_computing_table({resource(), {database.name, collection.name}});
                assert(!err);
            }
        }
        load_result_.clear();
    }

    void dispatcher_t::load_from_wal_result(const components::session::session_id_t& session,
                                            std::vector<services::wal::record_t>& in_records) {
        // TODO think what to do with records
        records_ = std::move(in_records);
        load_count_answers_ = records_.size();
        trace(log_,
              "dispatcher_t::load_from_wal_result, session: {}, count commands: {}",
              session.data(),
              load_count_answers_);
        if (load_count_answers_ == 0) {
            trace(log_, "dispatcher_t::load_from_wal_result - empty records_");
            actor_zeta::send(find_session(session_to_address_, session).address(),
                             dispatcher_t::address(),
                             core::handler_id(core::route::load_finish),
                             session);
            remove_session(session_to_address_, session);
            return;
        }
        last_wal_id_ = records_[load_count_answers_ - 1].id;
        for (auto& record : records_) {
            switch (record.data->type()) {
                case node_type::create_database_t: {
                    components::session::session_id_t session_database;
                    execute_plan(session_database, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::drop_database_t: {
                    components::session::session_id_t session_database;
                    execute_plan(session_database, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::create_collection_t: {
                    components::session::session_id_t session_collection;
                    execute_plan(session_collection, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::drop_collection_t: {
                    components::session::session_id_t session_collection;
                    execute_plan(session_collection, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::insert_t: {
                    components::session::session_id_t session_insert;
                    execute_plan(session_insert, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::delete_t: {
                    components::session::session_id_t session_delete;
                    execute_plan(session_delete, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::update_t: {
                    components::session::session_id_t session_update;
                    execute_plan(session_update, record.data, record.params, manager_wal_);
                    break;
                }
                case node_type::create_index_t: {
                    components::session::session_id_t session_create_index;
                    execute_plan(session_create_index, record.data, record.params, manager_wal_);
                    break;
                }
                // TODO no drop index?
                default:
                    break;
            }
        }
    }

    void dispatcher_t::execute_plan(const components::session::session_id_t& session,
                                    components::logical_plan::node_ptr plan,
                                    parameter_node_ptr params,
                                    actor_zeta::base::address_t address) {
        trace(log_, "dispatcher_t::execute_plan: session {}, {}", session.data(), plan->to_string());
        make_session(session_to_address_, session, session_t(address, plan, params));
        auto logic_plan = create_logic_plan(plan);
        table_id id(resource(), logic_plan->collection_full_name());
        cursor_t_ptr error;
        used_format_t used_format = used_format_t::undefined;

        switch (logic_plan->type()) {
            case node_type::create_database_t:
                if (!check_namespace_exists(id)) {
                    error = make_cursor(resource(), error_code_t::database_already_exists, "database already exists");
                }
                break;
            case node_type::drop_database_t:
                error = check_namespace_exists(id);
                break;
            case node_type::create_collection_t:
                if (!check_collectction_exists(id)) {
                    error =
                        make_cursor(resource(), error_code_t::collection_already_exists, "collection already exists");
                }
                break;
            case node_type::drop_collection_t:
                error = check_collectction_exists(id);
                break;
            default: {
                auto check_result = check_collections_format_(plan);
                if (check_result->is_error()) {
                    error = std::move(check_result);
                } else {
                    used_format = check_result->uses_table_data() ? used_format_t::columns : used_format_t::documents;
                }
            }
        }

        if (error) {
            execute_plan_finish(session, std::move(error));
            return;
        }

        actor_zeta::send(memory_storage_,
                         dispatcher_t::address(),
                         memory_storage::handler_id(memory_storage::route::execute_plan),
                         session,
                         std::move(logic_plan),
                         params->take_parameters(),
                         used_format);
    }

    void dispatcher_t::execute_plan_finish(const components::session::session_id_t& session, cursor_t_ptr result) {
        result_storage_[session] = result;
        auto& s = find_session(session_to_address_, session);
        auto plan = s.node();
        auto params = s.params();
        trace(log_,
              "dispatcher_t::execute_plan_finish: session {}, {}, {}",
              session.data(),
              plan.get() ? plan->to_string() : "",
              result->is_success());
        if (result->is_success()) {
            //todo: delete

            switch (plan->type()) {
                case node_type::create_database_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_database),
                                     session,
                                     plan->database_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_database),
                                         session,
                                         plan);
                        return;
                    }
                    break;
                }

                case node_type::drop_database_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    catalog_.drop_namespace(table_id(resource(), plan->collection_full_name()).get_namespace());
                    break;
                }

                case node_type::create_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::append_collection),
                                     session,
                                     plan->database_name(),
                                     plan->collection_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::create_collection),
                                         session,
                                         plan);
                        return;
                    }
                    break;
                }

                case node_type::insert_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds logical_plan");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::insert_many),
                                         session,
                                         std::move(plan));
                        return;
                    }
                    break;
                }

                case node_type::update_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds correct plan*");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::update_many),
                                         session,
                                         std::move(plan),
                                         s.params());
                        return;
                    }
                    break;
                }

                case node_type::delete_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    if (s.address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        assert(s.node() && "Doesn't holds correct plan*");
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::delete_many),
                                         session,
                                         std::move(plan),
                                         s.params());
                        return;
                    }
                    break;
                }

                case node_type::drop_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    collection_full_name_t name(plan->database_name(), plan->collection_name());
                    actor_zeta::send(manager_disk_,
                                     dispatcher_t::address(),
                                     disk::handler_id(disk::route::remove_collection),
                                     session,
                                     plan->database_name(),
                                     plan->collection_name());
                    if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    } else {
                        actor_zeta::send(manager_wal_,
                                         dispatcher_t::address(),
                                         wal::handler_id(wal::route::drop_collection),
                                         session,
                                         std::move(plan));
                        return;
                    }
                    break;
                }
                case node_type::create_index_t:
                case node_type::drop_index_t: {
                    trace(log_, "dispatcher_t::execute_plan_finish: {}", to_string(plan->type()));
                    const auto session_address = find_session(session_to_address_, session).address().get();
                    if (session_address == manager_wal_.get()) {
                        wal_success(session, last_wal_id_);
                    }
                    // We initiate session from this dispatcher,
                    // for now it means: dispatcher was called from manager_disk after initiate loading from disk.
                    if (session_address == dispatcher_t::address().get()) {
                        remove_session(session_to_address_, session);
                        return;
                    }
                    break;
                }
                default: {
                    trace(log_, "dispatcher_t::execute_plan_finish: non processed type - {}", to_string(plan->type()));
                }
            }

            //end: delete
        }
        // TODO add verification for mutable types (they should be skipped too)
        if (!load_from_wal_in_progress(session)) {
            actor_zeta::send(s.address(),
                             dispatcher_t::address(),
                             handler_id(route::execute_plan_finish),
                             session,
                             std::move(result));
            remove_session(session_to_address_, session);
            result_storage_.erase(session);
        }
    }

    void dispatcher_t::execute_plan_delete_finish(const components::session::session_id_t& session,
                                                  cursor_t_ptr cursor,
                                                  recomputed_types updates) {
        update_result_ = updates;
        execute_plan_finish(session, std::move(cursor));
    }

    void dispatcher_t::size(const components::session::session_id_t& session,
                            std::string& database_name,
                            std::string& collection,
                            actor_zeta::base::address_t sender) {
        trace(log_,
              "dispatcher_t::size: session:{}, database: {}, collection: {}",
              session.data(),
              database_name,
              collection);
        make_session(session_to_address_, session, session_t(std::move(sender)));
        auto error = check_collectction_exists({resource(), {database_name, collection}});
        if (error) {
            size_finish(session, std::move(error));
        }
        actor_zeta::send(memory_storage_,
                         dispatcher_t::address(),
                         collection::handler_id(collection::route::size),
                         session,
                         collection_full_name_t{database_name, collection});
    }

    void dispatcher_t::size_finish(const components::session::session_id_t& session, cursor_t_ptr&& result) {
        trace(log_, "dispatcher_t::size_finish session: {}, size: {}", session.data(), result->size());
        actor_zeta::send(find_session(session_to_address_, session).address(),
                         dispatcher_t::address(),
                         collection::handler_id(collection::route::size_finish),
                         session,
                         result->size());
        remove_session(session_to_address_, session);
    }

    void dispatcher_t::close_cursor(const components::session::session_id_t& session) {
        trace(log_, " dispatcher_t::close_cursor ");
        trace(log_, "Session : {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            cursor_.erase(it);
        } else {
            error(log_, "Not find session : {}", session.data());
        }
    }

    void dispatcher_t::wal_success(const components::session::session_id_t& session, services::wal::id_t wal_id) {
        trace(log_, "dispatcher_t::wal_success session : {}, wal id: {}", session.data(), wal_id);

        if (!is_session_exist(session_to_address_, session)) {
            actor_zeta::send(manager_disk_,
                             dispatcher_t::address(),
                             disk::handler_id(disk::route::flush),
                             session,
                             wal_id);
            return;
        }

        auto session_obj = find_session(session_to_address_, session);
        update_catalog(session_obj.node());
        actor_zeta::send(manager_disk_, dispatcher_t::address(), disk::handler_id(disk::route::flush), session, wal_id);

        const bool is_from_wal = session_obj.address().get() == manager_wal_.get();
        if (is_from_wal) {
            return;
        }

        trace(log_, "dispatcher_t::wal_success remove session : {}, wal id: {}", session.data(), wal_id);
        auto result = result_storage_[session];
        actor_zeta::send(session_obj.address(),
                         dispatcher_t::address(),
                         handler_id(route::execute_plan_finish),
                         session,
                         result);
        remove_session(session_to_address_, session);
        result_storage_.erase(session);
    }

    // TODO separate change logic and condition check
    bool dispatcher_t::load_from_wal_in_progress(const components::session::session_id_t& session) {
        if (find_session(session_to_address_, session).address().get() == manager_wal_.get()) {
            if (--load_count_answers_ == 0) {
                actor_zeta::send(find_session(session_to_address_, load_session_).address(),
                                 dispatcher_t::address(),
                                 core::handler_id(core::route::load_finish),
                                 load_session_);
                remove_session(session_to_address_, load_session_);
            }
            return true;
        }
        return false;
    }

    const components::catalog::catalog& dispatcher_t::current_catalog() { return catalog_; }

    components::cursor::cursor_t_ptr dispatcher_t::check_namespace_exists(const components::catalog::table_id id) {
        cursor_t_ptr error;
        if (!catalog_.namespace_exists(id.get_namespace())) {
            error = make_cursor(resource(), error_code_t::database_not_exists, "database does not exist");
        }
        return error;
    }

    components::cursor::cursor_t_ptr dispatcher_t::check_collectction_exists(const components::catalog::table_id id) {
        cursor_t_ptr error;
        if (!(error = check_namespace_exists(id)).get()) {
            bool exists = catalog_.table_exists(id);
            bool computes = catalog_.table_computes(id);
            // table can either compute or exist with schema - not both
            if (exists == computes) {
                error = make_cursor(resource(),
                                    error_code_t::collection_not_exists,
                                    (exists) ? "collection exists and computes schema at the same time"
                                             : "collection does not exist");
            }
        }

        return error;
    }
    components::cursor::cursor_t_ptr
    dispatcher_t::check_collections_format_(const components::logical_plan::node_ptr& logical_plan) {
        used_format_t used_format = used_format_t::undefined;
        cursor_t_ptr result = make_cursor(resource(), operation_status_t::success);
        auto check_format = [&](const components::logical_plan::node_t* node) {
            used_format_t check = used_format_t::undefined;
            // pull check format from collection referenced by logical_plan
            if (!node->collection_full_name().empty()) {
                table_id id(resource(), node->collection_full_name());
                if (auto res = check_collectction_exists(id); !res) {
                    check = catalog_.get_table_format(id);
                } else {
                    result = res;
                    return false;
                }
            }
            // pull/double-check check format from collection referenced by logical_plan and data stored inside node_data_t
            if (node->type() == components::logical_plan::node_type::data_t) {
                const auto* data_node = reinterpret_cast<const components::logical_plan::node_data_t*>(node);
                if (check == used_format_t::undefined) {
                    check = static_cast<used_format_t>(data_node->uses_data_chunk());
                } else if (check != static_cast<used_format_t>(data_node->uses_data_chunk())) {
                    result =
                        make_cursor(resource(),
                                    error_code_t::incompatible_storage_types,
                                    "logical plan data format is not the same as referenced collection data format");
                    return false;
                }
            }

            // compare check to previously gathered
            if (used_format == check) {
                return true;
            } else if (used_format == used_format_t::undefined) {
                used_format = check;
                return true;
            } else if (check == used_format_t::undefined) {
                return true;
            }
            result = make_cursor(resource(),
                                 error_code_t::incompatible_storage_types,
                                 "logical plan data format is not the same as referenced collection data format");
            return false;
        };

        std::queue<components::logical_plan::node_t*> look_up;
        look_up.emplace(logical_plan.get());
        while (!look_up.empty()) {
            auto plan_node = look_up.front();

            if (check_format(plan_node)) {
                for (const auto& child : plan_node->children()) {
                    look_up.emplace(child.get());
                }
                look_up.pop();
            } else {
                return result;
            }
        }

        switch (used_format) {
            case used_format_t::documents:
                return make_cursor(resource(), std::pmr::vector<document_ptr>{resource()});
            case used_format_t::columns:
                return make_cursor(resource(), components::vector::data_chunk_t{resource(), {}, 0});
            case used_format_t::undefined:
                return make_cursor(resource(), error_code_t::incompatible_storage_types, "undefined storage format");
        }
    }

    components::logical_plan::node_ptr dispatcher_t::create_logic_plan(components::logical_plan::node_ptr plan) {
        //todo: cache logical plans
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan));
    }

    void dispatcher_t::update_catalog(components::logical_plan::node_ptr node) {
        table_id id(resource(), node->collection_full_name());
        switch (node->type()) {
            case node_type::create_database_t:
                catalog_.create_namespace(id.get_namespace());
                break;
            case node_type::drop_database_t:
                catalog_.drop_namespace(id.get_namespace());
                break;
            case node_type::create_collection_t: {
                auto node_info = reinterpret_cast<node_create_collection_ptr&>(node);
                if (node_info->schema().empty()) {
                    auto err = catalog_.create_computing_table(id);
                    assert(!err);
                } else {
                    std::vector<components::types::field_description> desc;
                    desc.reserve(node_info->schema().size());
                    for (size_t i = 0; i < node_info->schema().size();
                         desc.push_back(components::types::field_description(i++)))
                        ;

                    auto sch = schema(
                        resource(),
                        components::catalog::create_struct(
                            std::vector<complex_logical_type>(node_info->schema().begin(), node_info->schema().end()),
                            std::move(desc)));
                    auto err = catalog_.create_table(id, table_metadata(resource(), std::move(sch)));
                    assert(!err);
                }
                break;
            }
            case node_type::drop_collection_t:
                if (catalog_.table_exists(id)) {
                    catalog_.drop_table(id);
                } else {
                    catalog_.drop_computing_table(id);
                }
                break;
            case node_type::insert_t: {
                if (!node->children().size() || node->children().back()->type() != node_type::data_t) {
                    // only inserts with documents are supported for now
                    break;
                }

                std::optional<std::reference_wrapper<computed_schema>> comp_sch;
                std::optional<std::reference_wrapper<const schema>> sch;
                if (catalog_.table_computes(id)) {
                    comp_sch = catalog_.get_computing_table_schema(id);
                } else {
                    sch = catalog_.get_table_schema(id);
                }

                auto node_info = reinterpret_cast<node_data_ptr&>(node->children().back());
                if (node_info->uses_documents()) {
                    for (const auto& doc : node_info->documents()) {
                        for (const auto& [key, value] : *doc->json_trie()->as_object()) {
                            auto key_val = key->get_mut()->get_string().value();
                            auto log_type = components::base::operators::type_from_json(value.get());

                            if (comp_sch.has_value()) {
                                comp_sch.value().get().append(std::pmr::string(key_val), log_type);
                            }
                            // else { // todo: type conversion tree
                            //     auto asserted_type = sch.value().get().find_field(std::pmr::string(key_val));
                            //     if (asserted_type != log_type) {
                            //         error(log_,
                            //               "Schema failure: inserted value of incorrect type into column {}",
                            //               std::string(key_val));
                            //         result_storage_[session] =
                            //             make_cursor(resource(), error_code_t::other_error, "Schema failure");
                            //     }
                            // }
                        }
                    }
                    break;
                }
            }
            case node_type::delete_t: {
                if (catalog_.table_computes(id)) {
                    auto& sch = catalog_.get_computing_table_schema(id);
                    for (const auto& [name_type, refcount] : update_result_) {
                        sch.drop_n(name_type.first, name_type.second, refcount);
                    }
                    update_result_.clear();
                }
                break;
            }
            default:
                break;
        }
    }

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::cooperative_supervisor<manager_dispatcher_t>(resource_ptr)
        , create_(actor_zeta::make_behavior(resource(), handler_id(route::create), this, &manager_dispatcher_t::create))
        , load_(actor_zeta::make_behavior(resource(),
                                          core::handler_id(core::route::load),
                                          this,
                                          &manager_dispatcher_t::load))
        , execute_plan_(actor_zeta::make_behavior(resource(),
                                                  handler_id(route::execute_plan),
                                                  this,
                                                  &manager_dispatcher_t::execute_plan))
        , size_(actor_zeta::make_behavior(resource(),
                                          collection::handler_id(collection::route::size),
                                          this,
                                          &manager_dispatcher_t::size))
        , schema_(actor_zeta::make_behavior(resource(),
                                            collection::handler_id(collection::route::schema),
                                            this,
                                            &manager_dispatcher_t::get_schema))
        , close_cursor_(actor_zeta::make_behavior(resource(),
                                                  collection::handler_id(collection::route::close_cursor),
                                                  this,
                                                  &manager_dispatcher_t::close_cursor))
        , sync_(actor_zeta::make_behavior(resource(),
                                          core::handler_id(core::route::sync),
                                          this,
                                          &manager_dispatcher_t::sync))
        , log_(log.clone())
        , e_(scheduler) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t ");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* const { return "manager_dispatcher"; }

    auto manager_dispatcher_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* { return e_; }

    actor_zeta::behavior_t manager_dispatcher_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case handler_id(route::create): {
                    create_(msg);
                    break;
                }
                case core::handler_id(core::route::load): {
                    load_(msg);
                    break;
                }
                case handler_id(route::execute_plan): {
                    execute_plan_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size): {
                    size_(msg);
                    break;
                }
                case collection::handler_id(collection::route::schema): {
                    schema_(msg);
                    break;
                }
                case collection::handler_id(collection::route::close_cursor): {
                    close_cursor_(msg);
                    break;
                }
                case core::handler_id(core::route::sync): {
                    sync_(msg);
                    break;
                }
            }
        });
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        ZoneScoped;
        std::unique_lock<spin_lock> _(lock_);
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    // mr_delete(resource(), agent)
    void manager_dispatcher_t::create(const components::session::session_id_t& session) {
        trace(log_, "manager_dispatcher_t::create session: {} ", session.data());
        auto target = spawn_actor(
            [this](dispatcher_t* ptr) {
                dispatchers_.emplace_back(dispatcher_ptr(ptr, actor_zeta::pmr::deleter_t(resource())));
            },
            memory_storage_,
            manager_wal_,
            manager_disk_,
            log_);
    }

    void manager_dispatcher_t::load(const components::session::session_id_t& session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        return actor_zeta::send(dispatcher(),
                                address(),
                                core::handler_id(core::route::load),
                                session,
                                current_message()->sender());
    }

    void manager_dispatcher_t::execute_plan(const components::session::session_id_t& session,
                                            node_ptr plan,
                                            parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());
        return actor_zeta::send(dispatcher(),
                                address(),
                                handler_id(route::execute_plan),
                                session,
                                std::move(plan),
                                std::move(params),
                                current_message()->sender());
    }

    void manager_dispatcher_t::size(const components::session::session_id_t& session,
                                    std::string& database_name,
                                    std::string& collection) {
        trace(log_,
              "manager_dispatcher_t::size session: {} , database: {}, collection name: {} ",
              session.data(),
              database_name,
              collection);
        actor_zeta::send(dispatcher(),
                         address(),
                         collection::handler_id(collection::route::size),
                         session,
                         std::move(database_name),
                         std::move(collection),
                         current_message()->sender());
    }

    void manager_dispatcher_t::get_schema(const components::session::session_id_t& session,
                                          const std::pmr::vector<std::pair<database_name_t, collection_name_t>>& ids) {
        auto& catalog = const_cast<components::catalog::catalog&>(current_catalog());
        std::pmr::vector<complex_logical_type> schemas;
        schemas.reserve(ids.size());

        for (const auto& [db, coll] : ids) {
            table_id id(resource(), {db, coll});
            if (catalog.table_exists(id)) {
                schemas.push_back(catalog.get_table_schema(id).schema_struct());
                continue;
            }

            if (catalog.table_computes(id)) {
                schemas.push_back(catalog.get_computing_table_schema(id).latest_types_struct());
                continue;
            }

            schemas.push_back(logical_type::INVALID);
        }

        actor_zeta::send(current_message()->sender(),
                         address(),
                         collection::handler_id(collection::route::schema_finish),
                         session,
                         make_cursor(resource(), std::move(schemas)));
    }

    void manager_dispatcher_t::close_cursor(const components::session::session_id_t&) {}

    const components::catalog::catalog& manager_dispatcher_t::current_catalog() {
        return dispatchers_[0]->current_catalog();
    }

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t { return dispatchers_[0]->address(); }

} // namespace services::dispatcher
