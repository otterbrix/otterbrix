#include "dispatcher.hpp"

#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_create_collection.hpp>

#include <core/tracy/tracy.hpp>
#include <core/executor.hpp>
#include <thread>
#include <chrono>

#include <components/document/document.hpp>
#include <components/planner/planner.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

#include <services/disk/manager_disk.hpp>
#include <services/collection/collection.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

using namespace components::logical_plan;
using namespace components::cursor;
using namespace components::catalog;
using namespace components::types;

namespace services::dispatcher {


    manager_dispatcher_t::manager_dispatcher_t(std::pmr::memory_resource* resource_ptr,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log,
                                               run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_dispatcher_t>()
        , resource_(resource_ptr)
        , scheduler_(scheduler)
        , log_(log.clone())
        , run_fn_(std::move(run_fn))
        , catalog_(resource_ptr)
        , databases_(resource_ptr)
        , collections_(resource_ptr)
        , pending_void_(resource_ptr)
        , pending_cursor_(resource_ptr)
        , pending_size_(resource_ptr)
        , pending_execute_(resource_ptr) {
        ZoneScoped;
        trace(log_, "manager_dispatcher_t::manager_dispatcher_t");
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
        trace(log_, "delete manager_dispatcher_t");
    }

    auto manager_dispatcher_t::make_type() const noexcept -> const char* { return "manager_dispatcher"; }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<spin_lock> guard(lock_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    actor_zeta::behavior_t manager_dispatcher_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::execute_plan>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::execute_plan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::size>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::size, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::get_schema>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::get_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_dispatcher_t, &manager_dispatcher_t::close_cursor>: {
                co_await actor_zeta::dispatch(this, &manager_dispatcher_t::close_cursor, msg);
                break;
            }
            default:
                break;
        }
    }

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
        for (auto it = pending_execute_.begin(); it != pending_execute_.end();) {
            if (it->available()) {
                it = pending_execute_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void manager_dispatcher_t::sync(sync_pack pack) {
        constexpr static int wal_idx = 0;
        constexpr static int disk_idx = 1;
        wal_address_ = std::get<wal_idx>(pack);
        disk_address_ = std::get<disk_idx>(pack);

        executor_ = actor_zeta::spawn<collection::executor::executor_t>(
            resource(), address(), wal_address_, disk_address_, log_.clone());
        executor_address_ = executor_->address();
        trace(log_, "manager_dispatcher_t: executor spawned with WAL/Disk addresses");
    }

    void manager_dispatcher_t::init_from_state(
        std::pmr::set<database_name_t> databases,
        loader::document_map_t documents,
        loader::schema_map_t /*schemas*/) {
        trace(log_, "manager_dispatcher_t::init_from_state: populating storage");

        databases_ = std::move(databases);
        trace(log_, "manager_dispatcher_t::init_from_state: initialized {} databases", databases_.size());

        for (auto& [full_name, docs] : documents) {
            debug(log_, "manager_dispatcher_t::init_from_state: creating collection {}.{}",
                  full_name.database, full_name.collection);

            auto* context = new collection::context_collection_t(
                resource(), full_name, disk_address_, log_.clone());

            auto& storage = context->document_storage();
            for (auto& doc : docs) {
                if (doc) {
                    auto doc_id = components::document::get_document_id(doc);
                    storage.emplace(doc_id, std::move(doc));
                }
            }

            collections_.emplace(full_name, context);
            debug(log_, "manager_dispatcher_t::init_from_state: collection {}.{} initialized with {} documents",
                  full_name.database, full_name.collection, storage.size());
        }

        trace(log_, "manager_dispatcher_t::init_from_state: complete - {} collections", collections_.size());
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr> manager_dispatcher_t::execute_plan(
        components::session::session_id_t session,
        node_ptr plan,
        parameter_node_ptr params) {
        trace(log_, "manager_dispatcher_t::execute_plan session: {}, {}", session.data(), plan->to_string());

        auto params_for_wal = make_parameter_node(resource());
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
                    error = make_cursor(resource(), error_code_t::collection_already_exists, "collection already exists");
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
                    error = make_cursor(resource(), error_code_t::schema_error,
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

        if (error) {
            trace(log_, "manager_dispatcher_t::execute_plan: validation error");
            co_return std::move(error);
        }

        collection::executor::execute_result_t exec_result;
        switch (logic_plan->type()) {
            case node_type::create_database_t:
                exec_result = create_database_(std::move(logic_plan));
                break;
            case node_type::drop_database_t:
                exec_result = drop_database_(std::move(logic_plan));
                break;
            case node_type::create_collection_t:
                exec_result = create_collection_(std::move(logic_plan));
                break;
            case node_type::drop_collection_t:
                exec_result = drop_collection_(std::move(logic_plan));
                break;
            default:
                exec_result = co_await execute_plan_impl(session, std::move(logic_plan),
                                                          params->take_parameters(), used_format);
                break;
        }

        auto& result = exec_result.cursor;
        trace(log_, "manager_dispatcher_t::execute_plan: result received, success: {}", result->is_success());

        if (!exec_result.updates.empty()) {
            update_result_ = exec_result.updates;
        }

        if (result->is_success()) {
            switch (plan->type()) {
                case node_type::create_database_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    auto [_d1, df1] = actor_zeta::send(disk_address_, &disk::manager_disk_t::append_database,
                                     session, plan->database_name());
                    co_await std::move(df1);
                    auto create_database = boost::static_pointer_cast<node_create_database_t>(plan);
                    auto [_w1, wf1] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::create_database, session, create_database);
                    auto wal_id = co_await std::move(wf1);
                    update_catalog(plan);
                    auto [_d2, df2] = actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                    co_await std::move(df2);
                    co_return result;
                }

                case node_type::drop_database_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    catalog_.drop_namespace(table_id(resource(), plan->collection_full_name()).get_namespace());
                    break;
                }

                case node_type::create_collection_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    auto [_c1, cf1] = actor_zeta::send(disk_address_, &disk::manager_disk_t::append_collection,
                                     session, plan->database_name(), plan->collection_name());
                    co_await std::move(cf1);
                    auto create_collection = boost::static_pointer_cast<node_create_collection_t>(plan);
                    auto [_c2, cf2] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::create_collection, session, create_collection);
                    auto wal_id = co_await std::move(cf2);
                    update_catalog(plan);
                    auto [_c3, cf3] = actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                    co_await std::move(cf3);
                    co_return result;
                }

                case node_type::insert_t:
                case node_type::update_t:
                case node_type::delete_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    update_catalog(plan);
                    co_return result;
                }

                case node_type::drop_collection_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    auto [_dr1, drf1] = actor_zeta::send(disk_address_, &disk::manager_disk_t::remove_collection,
                                     session, plan->database_name(), plan->collection_name());
                    co_await std::move(drf1);
                    auto drop_collection = boost::static_pointer_cast<node_drop_collection_t>(plan);
                    auto [_dr2, drf2] = actor_zeta::send(wal_address_,
                        &wal::manager_wal_replicate_t::drop_collection, session, drop_collection);
                    auto wal_id = co_await std::move(drf2);
                    update_catalog(plan);
                    auto [_dr3, drf3] = actor_zeta::send(disk_address_, &disk::manager_disk_t::flush, session, wal_id);
                    co_await std::move(drf3);
                    co_return result;
                }

                case node_type::create_index_t:
                case node_type::drop_index_t: {
                    trace(log_, "manager_dispatcher_t::execute_plan: {}", to_string(plan->type()));
                    co_return result;
                }

                default: {
                    trace(log_, "manager_dispatcher_t::execute_plan: non processed type - {}", to_string(plan->type()));
                }
            }
        } else {
            trace(log_, "manager_dispatcher_t::execute_plan: error: \"{}\"", result->get_error().what);
        }

        co_return std::move(result);
    }

    manager_dispatcher_t::unique_future<size_t> manager_dispatcher_t::size(
        components::session::session_id_t session,
        std::string database_name,
        std::string collection) {
        trace(log_, "manager_dispatcher_t::size session:{}, database: {}, collection: {}",
              session.data(), database_name, collection);

        auto error = check_collection_exists({resource(), {database_name, collection}});
        if (error) {
            co_return size_t(0);
        }

        collection_full_name_t name{database_name, collection};
        auto coll = collections_.at(name).get();
        if (coll->dropped()) {
            co_return size_t(0);
        }
        if (coll->uses_datatable()) {
            co_return coll->table_storage().table().calculate_size();
        } else {
            co_return coll->document_storage().size();
        }
    }

    manager_dispatcher_t::unique_future<components::cursor::cursor_t_ptr> manager_dispatcher_t::get_schema(
        components::session::session_id_t session,
        std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids) {
        trace(log_, "manager_dispatcher_t::get_schema session: {}, ids count: {}", session.data(), ids.size());
        std::pmr::vector<complex_logical_type> schemas;
        schemas.reserve(ids.size());

        for (const auto& [db, coll] : ids) {
            table_id id(resource(), {db, coll});
            if (catalog_.table_exists(id)) {
                schemas.push_back(catalog_.get_table_schema(id).schema_struct());
                continue;
            }
            if (catalog_.table_computes(id)) {
                schemas.push_back(catalog_.get_computing_table_schema(id).latest_types_struct());
                continue;
            }
            schemas.push_back(logical_type::INVALID);
        }

        co_return make_cursor(resource(), std::move(schemas));
    }

    manager_dispatcher_t::unique_future<void> manager_dispatcher_t::close_cursor(
        components::session::session_id_t session) {
        trace(log_, "manager_dispatcher_t::close_cursor, session: {}", session.data());
        auto it = cursor_.find(session);
        if (it != cursor_.end()) {
            cursor_.erase(it);
        }
        co_return;
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_database_(
        node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_database {}", logical_plan->database_name());
        databases_.insert(logical_plan->database_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_database_(
        node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_database {}", logical_plan->database_name());
        databases_.erase(logical_plan->database_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::create_collection_(
        node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:create_collection {}", logical_plan->collection_full_name().to_string());
        auto create_collection_plan =
            boost::polymorphic_pointer_downcast<node_create_collection_t>(logical_plan);
        if (create_collection_plan->schema().empty()) {
            collections_.emplace(logical_plan->collection_full_name(),
                                 new collection::context_collection_t(resource(),
                                                                      logical_plan->collection_full_name(),
                                                                      disk_address_,
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
                                                                      disk_address_,
                                                                      log_.clone()));
        }
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t manager_dispatcher_t::drop_collection_(
        node_ptr logical_plan) {
        trace(log_, "manager_dispatcher_t:drop_collection {}", logical_plan->collection_full_name().to_string());
        auto cursor = collections_.at(logical_plan->collection_full_name())->drop()
            ? make_cursor(resource(), operation_status_t::success)
            : make_cursor(resource(), error_code_t::other_error, "collection not dropped");
        collections_.erase(logical_plan->collection_full_name());
        return {std::move(cursor), {}};
    }

    manager_dispatcher_t::unique_future<collection::executor::execute_result_t>
    manager_dispatcher_t::execute_plan_impl(
        components::session::session_id_t session,
        node_ptr logical_plan,
        storage_parameters parameters,
        used_format_t used_format) {
        trace(log_, "manager_dispatcher_t:execute_plan_impl: collection: {}, session: {}",
              logical_plan->collection_full_name().to_string(), session.data());

        if (used_format == used_format_t::undefined) {
            co_return collection::executor::execute_result_t{
                make_cursor(resource(), error_code_t::other_error, "undefined format"),
                {}
            };
        }

        auto dependency_tree_collections_names = logical_plan->collection_dependencies();
        context_storage_t collections_context_storage;
        while (!dependency_tree_collections_names.empty()) {
            collection_full_name_t name =
                dependency_tree_collections_names.extract(dependency_tree_collections_names.begin()).value();
            if (name.empty()) {
                collections_context_storage.emplace(std::move(name), nullptr);
                continue;
            }
            collections_context_storage.emplace(std::move(name), collections_.at(name).get());
        }

        trace(log_, "manager_dispatcher_t:execute_plan_impl: calling executor");
        auto [needs_sched, future] = actor_zeta::otterbrix::send(executor_address_,
                                                           &collection::executor::executor_t::execute_plan,
                                                           session,
                                                           logical_plan,
                                                           parameters,
                                                           std::move(collections_context_storage),
                                                           used_format);
        if (needs_sched && executor_) {
            scheduler_->enqueue(executor_.get());
        }
        auto result = co_await std::move(future);

        trace(log_, "manager_dispatcher_t:execute_plan_impl: executor returned, success: {}",
              result.cursor->is_success());
        co_return result;
    }

    cursor_t_ptr manager_dispatcher_t::check_namespace_exists(const table_id id) const {
        cursor_t_ptr error;
        if (!catalog_.namespace_exists(id.get_namespace())) {
            error = make_cursor(resource(), error_code_t::database_not_exists, "database does not exist");
        }
        return error;
    }

    cursor_t_ptr manager_dispatcher_t::check_collection_exists(const table_id id) const {
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

    cursor_t_ptr manager_dispatcher_t::check_type_exists(const std::string& alias) const {
        cursor_t_ptr error;
        if (!catalog_.type_exists(alias)) {
            error = make_cursor(resource(), error_code_t::schema_error, "type: \'" + alias + "\' does not exists");
        }
        return error;
    }

    cursor_t_ptr manager_dispatcher_t::check_collections_format_(node_ptr& logical_plan) const {
        used_format_t used_format = used_format_t::undefined;
        std::pmr::vector<complex_logical_type> encountered_types{resource()};
        cursor_t_ptr result = make_cursor(resource(), operation_status_t::success);
        auto check_format = [&](node_t* node) {
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
            if (node->type() == node_type::data_t) {
                auto* data_node = reinterpret_cast<node_data_t*>(node);
                if (check == used_format_t::undefined) {
                    check = static_cast<used_format_t>(data_node->uses_data_chunk());
                } else if (check != static_cast<used_format_t>(data_node->uses_data_chunk())) {
                    result = make_cursor(resource(), error_code_t::incompatible_storage_types,
                                    "logical plan data format is not the same as referenced collection data format");
                    return false;
                }

                if (used_format == used_format_t::documents && check == used_format_t::columns) {
                    data_node->convert_to_documents();
                    check = used_format_t::documents;
                }

                if (data_node->uses_data_chunk()) {
                    for (auto& column : data_node->data_chunk().data) {
                        auto it = std::find_if(encountered_types.begin(), encountered_types.end(),
                                               [&column](const complex_logical_type& type) {
                                                   return type.alias() == column.type().alias();
                                               });
                        if (it != encountered_types.end() && catalog_.type_exists(it->type_name())) {
                            if (it->type() == logical_type::STRUCT) {
                                components::vector::vector_t new_column(data_node->data_chunk().resource(),
                                                                        *it, data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.value(i).cast_as(*it);
                                    if (val.type().type() == logical_type::NA) {
                                        result = make_cursor(resource(), error_code_t::schema_error,
                                                        "couldn't convert parsed ROW to type: \'" + it->alias() + "\'");
                                        return false;
                                    } else {
                                        new_column.set_value(i, val);
                                    }
                                }
                                column = std::move(new_column);
                            } else if (it->type() == logical_type::ENUM) {
                                components::vector::vector_t new_column(data_node->data_chunk().resource(),
                                                                        *it, data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.data<std::string_view>()[i];
                                    auto enum_val = logical_value_t::create_enum(*it, val);
                                    if (enum_val.type().type() == logical_type::NA) {
                                        result = make_cursor(resource(), error_code_t::schema_error,
                                                        "enum: \'" + it->alias() + "\' does not contain value: \'" +
                                                            std::string(val) + "\'");
                                        return false;
                                    } else {
                                        new_column.set_value(i, enum_val);
                                    }
                                }
                                column = std::move(new_column);
                            } else {
                                assert(false && "missing type conversion");
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
            result = make_cursor(resource(), error_code_t::incompatible_storage_types,
                                 "logical plan data format is not the same as referenced collection data format");
            return false;
        };

        std::queue<node_t*> look_up;
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

    node_ptr manager_dispatcher_t::create_logic_plan(node_ptr plan) {
        components::planner::planner_t planner;
        return planner.create_plan(resource(), std::move(plan));
    }

    void manager_dispatcher_t::update_catalog(node_ptr node) {
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
                    std::vector<field_description> desc;
                    desc.reserve(node_info->schema().size());
                    for (size_t i = 0; i < node_info->schema().size();
                         desc.push_back(field_description(i++)));

                    auto sch = schema(
                        resource(),
                        create_struct("schema",
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

} // namespace services::dispatcher
