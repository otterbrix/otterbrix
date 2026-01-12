#include "dispatcher.hpp"

#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_data.hpp>

#include <core/tracy/tracy.hpp>
#include <core/executor.hpp>
#include <thread>  // for std::this_thread::yield()

#include <components/document/document.hpp>
#include <components/planner/planner.hpp>

#include <services/disk/manager_disk.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/memory_storage/context_storage.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {

    dispatcher_t::dispatcher_t(std::pmr::memory_resource* resource,
                               actor_zeta::address_t manager_dispatcher,
                               actor_zeta::address_t memory_storage,
                               actor_zeta::address_t wal_address,
                               actor_zeta::address_t disk_address,
                               log_t& log)
        : actor_zeta::basic_actor<dispatcher_t>(resource)
        , log_(log.clone())
        , catalog_(resource)
        , manager_dispatcher_(std::move(manager_dispatcher))
        , memory_storage_(std::move(memory_storage))
        , wal_address_(std::move(wal_address))
        , disk_address_(std::move(disk_address)) {
        trace(log_, "dispatcher_t::dispatcher_t start name:{}", make_type());
    }

    dispatcher_t::~dispatcher_t() { trace(log_, "delete dispatcher_t"); }

    auto dispatcher_t::make_type() const noexcept -> const char* { return "dispatcher_t"; }

    void dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        // DISABLED: poll_pending() causes use-after-free race condition
        // Race between available() (true after set_value) and final_suspend()
        // See: docs/actor-zeta-race-condition.md
        // poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<dispatcher_t, &dispatcher_t::load>: {
                // CRITICAL: Store pending coroutine! (per documentation)
                // If destroyed immediately, coroutine is destroyed → refcount underflow!
                auto future = actor_zeta::dispatch(this, &dispatcher_t::load, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<dispatcher_t, &dispatcher_t::execute_plan>: {
                // CRITICAL: Store pending coroutine!
                // execute_plan now returns cursor_t_ptr through future
                auto future = actor_zeta::dispatch(this, &dispatcher_t::execute_plan, msg);
                if (!future.available()) {
                    pending_cursor_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<dispatcher_t, &dispatcher_t::size>: {
                // size() now returns size_t via future
                auto future = actor_zeta::dispatch(this, &dispatcher_t::size, msg);
                if (!future.available()) {
                    pending_size_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<dispatcher_t, &dispatcher_t::close_cursor>: {
                auto future = actor_zeta::dispatch(this, &dispatcher_t::close_cursor, msg);
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
    void dispatcher_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->available()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_cursor_.begin(); it != pending_cursor_.end();) {
            if (it->available()) {
                it = pending_cursor_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_size_.begin(); it != pending_size_.end();) {
            if (it->available()) {
                it = pending_size_.erase(it);
            } else {
                ++it;
            }
        }
    }

    dispatcher_t::unique_future<void> dispatcher_t::load(
        components::session::session_id_t session
    ) {
        trace(log_, "dispatcher_t::load, session: {}", session.data());
        load_session_ = session;
        
        trace(log_, "dispatcher_t::load - step 1: loading from disk");
        auto disk_result = co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::load, session);

        // Step 2: Check result (former load_from_disk_result)
        trace(log_, "dispatcher_t::load - step 2: disk result received, wal_id: {}", disk_result.wal_id());

        if ((*disk_result).empty()) {
            // Empty result - finish immediately
            trace(log_, "dispatcher_t::load - empty result, finishing");
            co_return;
        }

        // Step 3: Load to memory_storage with co_await
        trace(log_, "dispatcher_t::load - step 3: loading to memory storage");
        co_await actor_zeta::otterbrix::send(memory_storage_, address(), &services::memory_storage_t::load, session, disk_result);

        // Step 4: Load indexes with co_await (wait for all indexes to be created)
        // Pass manager_dispatcher address for execute_plan callbacks
        trace(log_, "dispatcher_t::load - step 4: loading indexes with co_await");
        co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::load_indexes, session, manager_dispatcher_);

        // Step 5: Update catalog
        for (const auto& database : (*disk_result)) {
            catalog_.create_namespace({database.name.c_str()});
            for (const auto& collection : database.collections) {
                auto err = catalog_.create_computing_table({resource(), {database.name, collection.name}});
                assert(!err);
            }
        }

        // Step 6: Load WAL records with co_await (via interface contract)
        trace(log_, "dispatcher_t::load - step 6: loading WAL records via co_await");
        auto records = co_await actor_zeta::send(wal_address_, address(), &wal::manager_wal_replicate_t::load, session, disk_result.wal_id());

        // Step 7: Process WAL records (former load_from_wal_result - now inline!)
        load_count_answers_ = records.size();
        trace(log_,
              "dispatcher_t::load - step 7: processing WAL records, count: {}",
              load_count_answers_);

        if (load_count_answers_ == 0) {
            trace(log_, "dispatcher_t::load - empty WAL records, finishing");
            co_return;
        }

        last_wal_id_ = records[load_count_answers_ - 1].id;

        // WAL replay: execute records without re-writing to WAL
        // Use memory_storage directly
        for (auto& record : records) {
            trace(log_, "dispatcher_t::load - replaying WAL record id: {}", record.id);

            // Order same as execute_plan: first create_logic_plan, then switch
            auto logic_plan = create_logic_plan(record.data);

            // Determine format for operation (similar to execute_plan)
            used_format_t used_format = used_format_t::undefined;
            switch (logic_plan->type()) {
                case node_type::create_database_t:
                case node_type::drop_database_t:
                case node_type::create_collection_t:
                case node_type::drop_collection_t:
                    // These operations do not require format
                    break;
                default: {
                    // insert/delete/update/create_index require format from catalog
                    auto check_result = check_collections_format_(record.data);
                    if (!check_result->is_error()) {
                        used_format = check_result->uses_table_data() ? used_format_t::columns : used_format_t::documents;
                    }
                    break;
                }
            }

            // co_await on memory_storage for replay
            auto exec_result = co_await actor_zeta::otterbrix::send(memory_storage_, address(),
                                                          &services::memory_storage_t::execute_plan,
                                                          components::session::session_id_t{},
                                                          std::move(logic_plan),
                                                          record.params->take_parameters(),
                                                          used_format);
            // Update catalog after successful replay
            if (exec_result.cursor->is_success()) {
                update_catalog(record.data);
            }
        }

        // WAL replay completed
        trace(log_, "dispatcher_t::load - WAL replay completed");
        co_return;
    }

    dispatcher_t::unique_future<components::cursor::cursor_t_ptr> dispatcher_t::execute_plan(
        components::session::session_id_t session,
        components::logical_plan::node_ptr plan,
        parameter_node_ptr params
    ) {
        trace(log_, "dispatcher_t::execute_plan: session {}, {}", session.data(), plan->to_string());

        // Create a copy of params for WAL operations (before take_parameters() consumes them)
        // This avoids shared state (session_to_address_) which caused race conditions
        auto params_for_wal = components::logical_plan::make_parameter_node(resource());
        params_for_wal->set_parameters(params->parameters());

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
                if (!check_collection_exists(id)) {
                    error =
                        make_cursor(resource(), error_code_t::collection_already_exists, "collection already exists");
                } else {
                    const auto& n = reinterpret_cast<const node_create_collection_ptr&>(logic_plan);
                    for (auto& column_type : n->schema()) {
                        if (column_type.type() == logical_type::UNKNOWN) {
                            if (error = check_type_exists(column_type.type_name()); !error) {
                                auto proper_type = catalog_.get_type(column_type.type_name());
                                std::string alias = column_type.alias();
                                column_type = std::move(proper_type);
                                column_type.set_alias(alias);
                            }
                        }
                    }
                }
                break;
            case node_type::drop_collection_t:
                error = check_collection_exists(id);
                break;
            case node_type::create_type_t: {
                auto& n = reinterpret_cast<node_create_type_ptr&>(logic_plan);
                if (!check_type_exists(n->type().type_name())) {
                    error = make_cursor(resource(),
                                        error_code_t::schema_error,
                                        "type: \'" + n->type().alias() + "\' already exists");
                    break;
                } else {
                    if (n->type().type() == logical_type::STRUCT) {
                        for (auto& field : n->type().child_types()) {
                            if (field.type() == logical_type::UNKNOWN) {
                                error = check_type_exists(field.type_name());
                                if (error) {
                                    break;
                                } else {
                                    std::string alias = field.alias();
                                    field = catalog_.get_type(field.type_name());
                                    field.set_alias(alias);
                                }
                            }
                        }
                        if (error) {
                            break;
                        }
                    }
                    catalog_.create_type(n->type());
                    // Local operation - return result directly
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
            case node_type::drop_type_t: {
                const auto& n = boost::polymorphic_pointer_downcast<node_create_type_t>(logic_plan);
                error = check_type_exists(n->type().alias());
                if (error) {
                    break;
                } else {
                    catalog_.drop_type(n->type().alias());
                    // Local operation - return result directly
                    co_return make_cursor(resource(), operation_status_t::success);
                }
            }
            default: {
                auto check_result = check_collections_format_(plan);
                if (check_result->is_error()) {
                    error = std::move(check_result);
                } else {
                    used_format = check_result->uses_table_data() ? used_format_t::columns : used_format_t::documents;
                }
            }
        }

        // If validation error - return result directly via future
        if (error) {
            trace(log_, "dispatcher_t::execute_plan: validation error, returning directly");
            co_return std::move(error);
        }

        // ========================================================================
        // co_await on memory_storage_t::execute_plan() - get cursor via future!
        // ========================================================================
        trace(log_, "dispatcher_t::execute_plan: calling memory_storage with co_await");
        auto exec_result = co_await actor_zeta::otterbrix::send(memory_storage_, address(),
                                                      &services::memory_storage_t::execute_plan,
                                                      session,
                                                      std::move(logic_plan),
                                                      params->take_parameters(),
                                                      used_format);

        // ========================================================================
        // Process result
        // ========================================================================
        auto& result = exec_result.cursor;

        trace(log_,
              "dispatcher_t::execute_plan: result received, session {}, {}, success: {}",
              session.data(),
              plan.get() ? plan->to_string() : "",
              result->is_success());

        // Save updates for delete operations
        if (!exec_result.updates.empty()) {
            update_result_ = exec_result.updates;
        }

        if (result->is_success()) {
            switch (plan->type()) {
                case node_type::create_database_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // Queue disk command (strict durability)
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::append_database,
                                     session, plan->database_name());
                    // co_await on WAL for durability - cast to specific type for type safety
                    auto create_database = boost::static_pointer_cast<node_create_database_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::create_database, session,
                        create_database);
                    // Update catalog after successful WAL write
                    update_catalog(plan);
                    // Flush and wait for disk I/O completion (strict durability)
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::drop_database_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    catalog_.drop_namespace(table_id(resource(), plan->collection_full_name()).get_namespace());
                    break;
                }

                case node_type::create_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // Queue disk command (strict durability)
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::append_collection,
                                     session, plan->database_name(), plan->collection_name());
                    // co_await on WAL for durability - cast to specific type
                    auto create_collection = boost::static_pointer_cast<node_create_collection_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::create_collection, session, create_collection);
                    update_catalog(plan);
                    // Flush and wait for disk I/O completion (strict durability)
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::insert_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // co_await on WAL for durability - cast to specific type
                    auto insert = boost::static_pointer_cast<node_insert_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::insert_many, session, insert);
                    update_catalog(plan);
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::update_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // co_await on WAL for durability - cast to specific type
                    auto update = boost::static_pointer_cast<node_update_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::update_many, session, update, params_for_wal);
                    update_catalog(plan);
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::delete_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // co_await on WAL for durability - cast to specific type
                    auto delete_i = boost::static_pointer_cast<node_delete_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::delete_many, session, delete_i, params_for_wal);
                    update_catalog(plan);
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::drop_collection_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::remove_collection,
                                     session, plan->database_name(), plan->collection_name());
                    // co_await on WAL for durability - cast to specific type
                    auto drop_collection = boost::static_pointer_cast<node_drop_collection_t>(plan);
                    auto wal_id = co_await actor_zeta::send(wal_address_, address(),
                        &wal::manager_wal_replicate_t::drop_collection, session, drop_collection);
                    update_catalog(plan);
                    co_await actor_zeta::send(disk_address_, address(), &disk::manager_disk_t::flush, session, wal_id);
                    co_return result;
                }

                case node_type::create_index_t:
                case node_type::drop_index_t: {
                    trace(log_, "dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    // Index operations - currently without WAL
                    co_return result;
                }

                default: {
                    trace(log_, "dispatcher_t::execute_plan: non processed type - {}", to_string(plan->type()));
                }
            }
        } else {
            trace(log_, "dispatcher_t::execute_plan: error: \"{}\"", result->get_error().what);
        }

        // Return result (for read-only operations or errors)
        co_return std::move(result);
    }

    dispatcher_t::unique_future<size_t> dispatcher_t::size(
        components::session::session_id_t session,
        std::string database_name,
        std::string collection
    ) {
        trace(log_,
              "dispatcher_t::size: session:{}, database: {}, collection: {}",
              session.data(),
              database_name,
              collection);

        auto error = check_collection_exists({resource(), {database_name, collection}});
        if (error) {
            co_return size_t(0);
        }

        // co_await on memory_storage::size - get size via future
        auto result = co_await actor_zeta::otterbrix::send(memory_storage_, address(), &services::memory_storage_t::size,
                                                 session, collection_full_name_t{database_name, collection});
        co_return result;
    }

    dispatcher_t::unique_future<void> dispatcher_t::close_cursor(
        components::session::session_id_t session
    ) {
        trace(log_, "dispatcher_t::close_cursor, session: {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            cursor_.erase(it);
        } else {
            error(log_, "Not find session : {}", session.data());
        }
        co_return;
    }

    // wal_success() REMOVED - now using co_await on WAL methods in execute_plan()

    const components::catalog::catalog& dispatcher_t::current_catalog() { return catalog_; }

    components::cursor::cursor_t_ptr
    dispatcher_t::check_namespace_exists(const components::catalog::table_id id) const {
        cursor_t_ptr error;
        if (!catalog_.namespace_exists(id.get_namespace())) {
            error = make_cursor(resource(), error_code_t::database_not_exists, "database does not exist");
        }
        return error;
    }

    components::cursor::cursor_t_ptr
    dispatcher_t::check_collection_exists(const components::catalog::table_id id) const {
        cursor_t_ptr error = check_namespace_exists(id);
        if (!error) {
            bool exists = catalog_.table_exists(id);
            bool computes = catalog_.table_computes(id);
            if (exists == computes) {
                error = make_cursor(resource(),
                                    error_code_t::collection_not_exists,
                                    exists ? "collection exists and computes schema at the same time"
                                           : "collection does not exist");
            }
        }
        return error;
    }

    components::cursor::cursor_t_ptr dispatcher_t::check_type_exists(const std::string& alias) const {
        cursor_t_ptr error;
        if (!catalog_.type_exists(alias)) {
            error = make_cursor(resource(), error_code_t::schema_error, "type: \'" + alias + "\' does not exists");
        }
        return error;
    }

    components::cursor::cursor_t_ptr
    dispatcher_t::check_collections_format_(components::logical_plan::node_ptr& logical_plan) const {
        used_format_t used_format = used_format_t::undefined;
        std::pmr::vector<complex_logical_type> encountered_types{resource()};
        cursor_t_ptr result = make_cursor(resource(), operation_status_t::success);
        auto check_format = [&](components::logical_plan::node_t* node) {
            used_format_t check = used_format_t::undefined;
            if (!node->collection_full_name().empty()) {
                table_id id(resource(), node->collection_full_name());
                if (auto res = check_collection_exists(id); !res) {
                    check = catalog_.get_table_format(id);
                    if (!catalog_.table_computes(id)) {
                        for (const auto& type : catalog_.get_table_schema(id).columns()) {
                            encountered_types.emplace_back(type);
                        }
                    }
                } else {
                    result = res;
                    return false;
                }
            }
            if (node->type() == components::logical_plan::node_type::data_t) {
                auto* data_node = reinterpret_cast<components::logical_plan::node_data_t*>(node);
                if (check == used_format_t::undefined) {
                    check = static_cast<used_format_t>(data_node->uses_data_chunk());
                } else if (check != static_cast<used_format_t>(data_node->uses_data_chunk())) {
                    result =
                        make_cursor(resource(),
                                    error_code_t::incompatible_storage_types,
                                    "logical plan data format is not the same as referenced collection data format");
                    return false;
                }

                if (used_format == used_format_t::documents && check == used_format_t::columns) {
                    data_node->convert_to_documents();
                    check = used_format_t::documents;
                }

                if (data_node->uses_data_chunk()) {
                    for (auto& column : data_node->data_chunk().data) {
                        auto it = std::find_if(encountered_types.begin(),
                                               encountered_types.end(),
                                               [&column](const complex_logical_type& type) {
                                                   return type.alias() == column.type().alias();
                                               });
                        if (it != encountered_types.end() && catalog_.type_exists(it->type_name())) {
                            if (it->type() == logical_type::STRUCT) {
                                components::vector::vector_t new_column(data_node->data_chunk().resource(),
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.value(i).cast_as(*it);
                                    if (val.type().type() == logical_type::NA) {
                                        result =
                                            make_cursor(resource(),
                                                        error_code_t::schema_error,
                                                        "couldn't convert parsed ROW to type: \'" + it->alias() + "\'");
                                        return false;
                                    } else {
                                        new_column.set_value(i, val);
                                    }
                                }
                                column = std::move(new_column);
                            } else if (it->type() == logical_type::ENUM) {
                                components::vector::vector_t new_column(data_node->data_chunk().resource(),
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.data<std::string_view>()[i];
                                    auto enum_val = logical_value_t::create_enum(*it, val);
                                    if (enum_val.type().type() == logical_type::NA) {
                                        result =
                                            make_cursor(resource(),
                                                        error_code_t::schema_error,
                                                        "enum: \'" + it->alias() + "\' does not contain value: \'" +
                                                            std::string(val) + "\'");
                                        return false;
                                    } else {
                                        new_column.set_value(i, enum_val);
                                    }
                                }
                                column = std::move(new_column);
                            } else {
                                assert(false && "missing type conversion in dispatcher_t::check_collections_format_");
                            }
                        }
                    }
                }
            }

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
                return make_cursor(resource(), std::pmr::vector<components::document::document_ptr>{resource()});
            case used_format_t::columns:
                return make_cursor(resource(), components::vector::data_chunk_t{resource(), {}, 0});
            default:
                return make_cursor(resource(), error_code_t::incompatible_storage_types, "undefined storage format");
        }
    }

    components::logical_plan::node_ptr dispatcher_t::create_logic_plan(components::logical_plan::node_ptr plan) {
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
                auto node_info = boost::polymorphic_pointer_downcast<node_create_collection_t>(node);
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
                            "schema",
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
                    break;
                }

                std::optional<std::reference_wrapper<computed_schema>> comp_sch;
                if (catalog_.table_computes(id)) {
                    comp_sch = catalog_.get_computing_table_schema(id);
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
                        }
                    }
                }
                break;
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

    // ============================================================================
    // manager_dispatcher_t
    // ============================================================================

    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone()) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t ");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    void manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        // Lock required: behavior() can be called concurrently from multiple threads
        // (actor_mixin::enqueue_impl processes messages synchronously in caller's thread)
        std::lock_guard<spin_lock> guard(lock_);

        // DISABLED: poll_pending() causes use-after-free race condition
        // Race between available() (true after set_value) and final_suspend()
        // See: docs/actor-zeta-race-condition.md
        // poll_pending();

        switch (msg->command()) {
            // Note: sync is called directly, not through message passing
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::create>: {
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::create, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::load>: {
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::load, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                // CRITICAL: Store pending coroutine! execute_plan returns cursor_t_ptr
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                if (!future.available()) {
                    pending_cursor_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::size>: {
                // size() returns size_t via future
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::size, msg);
                if (!future.available()) {
                    pending_size_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::get_schema>: {
                // get_schema() returns cursor_t_ptr via future
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::get_schema, msg);
                if (!future.available()) {
                    pending_cursor_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_cursor>: {
                auto future = actor_zeta::dispatch(this, &manager_dispatcher_t::close_cursor, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            default:
                break;
        }
    }

    // Poll and clean up completed coroutines
    void manager_dispatcher_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->available()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_cursor_.begin(); it != pending_cursor_.end();) {
            if (it->available()) {
                it = pending_cursor_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_size_.begin(); it != pending_size_.end();) {
            if (it->available()) {
                it = pending_size_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void manager_dispatcher_t::sync(sync_pack pack) {
        constexpr static int memory_storage = 0;
        constexpr static int wal_address = 1;
        constexpr static int disk_address = 2;
        memory_storage_ = std::get<memory_storage>(pack);
        wal_address_ = std::get<wal_address>(pack);
        disk_address_ = std::get<disk_address>(pack);
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::create(
        components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::create session: {} ", session.data());
        // Use actor_zeta::spawn<T>(resource, args...) - resource is passed as first constructor arg
        auto ptr = actor_zeta::spawn<dispatcher_t>(resource(), address(), memory_storage_, wal_address_, disk_address_, log_);
        dispatchers_.emplace_back(std::move(ptr));
        co_return;
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::load(
        components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::load session: {}", session.data());
        // With futures, sender is not needed - caller gets notified via co_await
        auto future = actor_zeta::send(dispatcher(), address(), &dispatcher_t::load, session);
        // Always schedule dispatcher_t (workaround for actor-zeta race condition where
        // needs_scheduling() returns false when actor transitions from running to idle)
        scheduler_->enqueue(dispatchers_[0].get());
        co_await std::move(future);
        co_return;
    }

    // execute_plan returns cursor_t_ptr via future (not callback!)
    // co_await on dispatcher_t::execute_plan and return cursor to caller
    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr> manager_dispatcher_t::execute_plan(
        components::session::session_id_t session,
        node_ptr plan,
        parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());
        // Send to dispatcher and schedule it (cooperative_actor needs explicit scheduling)
        auto future = actor_zeta::send(dispatcher(), address(), &dispatcher_t::execute_plan,
                                       session, std::move(plan), std::move(params));
        // Always schedule dispatcher_t (workaround for actor-zeta race condition where
        // needs_scheduling() returns false when actor transitions from running to idle)
        scheduler_->enqueue(dispatchers_[0].get());
        auto cursor = co_await std::move(future);
        co_return cursor;
    }

    // size returns size_t via future (no callback!)
    manager_dispatcher_t::unique_future<size_t> manager_dispatcher_t::size(
        components::session::session_id_t session,
        std::string database_name,
        std::string collection) {
        trace(log_,
              "manager_dispatcher_t::size session: {} , database: {}, collection name: {} ",
              session.data(),
              database_name,
              collection);
        // Send to dispatcher and schedule it (cooperative_actor needs explicit scheduling)
        auto future = actor_zeta::send(dispatcher(), address(), &dispatcher_t::size,
                                       session, std::move(database_name), std::move(collection));
        // Always schedule dispatcher_t (workaround for actor-zeta race condition where
        // needs_scheduling() returns false when actor transitions from running to idle)
        scheduler_->enqueue(dispatchers_[0].get());
        auto result = co_await std::move(future);
        co_return result;
    }

    // get_schema returns cursor via future (no callback!)
    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr> manager_dispatcher_t::get_schema(
        components::session::session_id_t session,
        std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids) {
        trace(log_, "manager_dispatcher_t::get_schema session: {}, ids count: {}", session.data(), ids.size());
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

        // Return cursor via future (not callback!)
        co_return make_cursor(resource(), std::move(schemas));
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::close_cursor(
        components::session::session_id_t session) {
        (void)session;
        co_return;
    }

    const components::catalog::catalog& manager_dispatcher_t::current_catalog() {
        return dispatchers_[0]->current_catalog();
    }

    auto manager_dispatcher_t::dispatcher() -> actor_zeta::address_t { return dispatchers_[0]->address(); }

} // namespace services::dispatcher
