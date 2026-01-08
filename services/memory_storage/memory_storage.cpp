#include "memory_storage.hpp"
#include <boost/polymorphic_pointer_cast.hpp>
#include <cassert>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan_generator/create_plan.hpp>
#include <core/excutor.hpp>
#include <core/tracy/tracy.hpp>
#include <services/collection/collection.hpp>
#include <utility>

using namespace components::cursor;

namespace services {

    using namespace core::pmr;

    memory_storage_t::load_buffer_t::load_buffer_t(std::pmr::memory_resource* resource)
        : collections(resource) {}

    memory_storage_t::memory_storage_t(std::pmr::memory_resource* o_resource,
                                       actor_zeta::scheduler_raw scheduler,
                                       log_t& log)
        : actor_zeta::actor::actor_mixin<memory_storage_t>()
        , resource_(o_resource)
        , scheduler_(scheduler)
        , databases_(resource())
        , collections_(resource())
        , log_(log.clone())
    {
        ZoneScoped;
        trace(log_, "memory_storage start thread pool");
        // Spawn executor using new spawn API
        executor_ = actor_zeta::spawn<collection::executor::executor_t>(resource(), this, log_.clone());
        executor_address_ = executor_->address();
    }

    memory_storage_t::~memory_storage_t() {
        ZoneScoped;
        trace(log_, "delete memory_storage");
    }

    auto memory_storage_t::make_type() const noexcept -> const char* { return "memory_storage"; }

    void memory_storage_t::behavior(actor_zeta::mailbox::message* msg) {
        // Poll completed coroutines first (per PROMISE_FUTURE_GUIDE.md)
        poll_pending();

        switch (msg->command()) {
            // Note: sync is called directly, not through message passing
            case actor_zeta::msg_id<memory_storage_t, &memory_storage_t::load>: {
                // CRITICAL: Store pending coroutine! (per documentation)
                auto future = actor_zeta::dispatch(this, &memory_storage_t::load, msg);
                if (!future.available()) {
                    pending_void_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<memory_storage_t, &memory_storage_t::execute_plan>: {
                // CRITICAL: Store pending coroutine! execute_plan uses co_await
                auto future = actor_zeta::dispatch(this, &memory_storage_t::execute_plan, msg);
                if (!future.available()) {
                    pending_execute_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<memory_storage_t, &memory_storage_t::size>: {
                // size() returns size_t, using pending_size_
                auto future = actor_zeta::dispatch(this, &memory_storage_t::size, msg);
                if (!future.available()) {
                    pending_size_.push_back(std::move(future));
                }
                break;
            }
            case actor_zeta::msg_id<memory_storage_t, &memory_storage_t::close_cursor>: {
                auto future = actor_zeta::dispatch(this, &memory_storage_t::close_cursor, msg);
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
    void memory_storage_t::poll_pending() {
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
        for (auto it = pending_size_.begin(); it != pending_size_.end();) {
            if (it->available()) {
                it = pending_size_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void memory_storage_t::sync(address_pack pack) {
        manager_dispatcher_ = std::get<static_cast<uint64_t>(unpack_rules::manager_dispatcher)>(pack);
        manager_disk_ = std::get<static_cast<uint64_t>(unpack_rules::manager_disk)>(pack);
    }

    // execute_plan() - LINEAR COROUTINE (per actor-zeta migration docs)
    // Returns result via future (NOT via callback!)
    memory_storage_t::unique_future<collection::executor::execute_result_t> memory_storage_t::execute_plan(
        components::session::session_id_t session,
        components::logical_plan::node_ptr logical_plan,
        components::logical_plan::storage_parameters parameters,
        components::catalog::used_format_t used_format) {
        using components::logical_plan::node_type;

        switch (logical_plan->type()) {
            case node_type::create_database_t:
                co_return create_database_(std::move(logical_plan));
            case node_type::drop_database_t:
                co_return drop_database_(std::move(logical_plan));
            case node_type::create_collection_t:
                co_return create_collection_(std::move(logical_plan));
            case node_type::drop_collection_t:
                co_return drop_collection_(std::move(logical_plan));
            default:
                // Use co_await to get result from executor
                co_return co_await execute_plan_impl(session, std::move(logical_plan), std::move(parameters), used_format);
        }
    }

    // size() returns size_t via future (not callback!)
    memory_storage_t::unique_future<size_t> memory_storage_t::size(
        components::session::session_id_t session,
        collection_full_name_t name) {
        trace(log_, "collection {}::{}::size", name.database, name.collection);
        auto collection = collections_.at(name).get();
        if (collection->dropped()) {
            co_return size_t(0);
        }
        if (collection->uses_datatable()) {
            co_return collection->table_storage().table().calculate_size();
        } else {
            co_return collection->document_storage().size();
        }
    }

    memory_storage_t::unique_future<void> memory_storage_t::close_cursor(
        components::session::session_id_t session,
        std::set<collection_full_name_t> collections) {
        // TODO: Implement cursor closing logic
        (void)session;
        (void)collections;
        co_return;
    }

    // load() - now LINEAR COROUTINE with promise/future
    // Waits for all create_documents completion via co_await
    memory_storage_t::unique_future<void> memory_storage_t::load(
        components::session::session_id_t session,
        disk::result_load_t result) {
        trace(log_, "memory_storage_t:load");
        load_buffer_ = std::make_unique<load_buffer_t>(resource());

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
                // co_await on each create_documents - wait for completion
                auto future = actor_zeta::otterbrix::send(executor_address_, address(),
                                                     &collection::executor::executor_t::create_documents,
                                                     session,
                                                     context,
                                                     collection.documents);
                if (future.needs_scheduling() && executor_) {
                    scheduler_->enqueue(executor_.get());
                }
                co_await std::move(future);
            }
        }

        trace(log_, "memory_storage_t:load finished");
        load_buffer_.reset();
        co_return;
    }

    // Helper methods return cursor directly (NOT via callback!)
    collection::executor::execute_result_t memory_storage_t::create_database_(
        components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_database {}", logical_plan->database_name());
        databases_.insert(logical_plan->database_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t memory_storage_t::drop_database_(
        components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_database {}", logical_plan->database_name());
        databases_.erase(logical_plan->database_name());
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t memory_storage_t::create_collection_(
        components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:create_collection {}", logical_plan->collection_full_name().to_string());
        auto create_collection_plan =
            boost::polymorphic_pointer_downcast<components::logical_plan::node_create_collection_t>(logical_plan);
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
        return {make_cursor(resource(), operation_status_t::success), {}};
    }

    collection::executor::execute_result_t memory_storage_t::drop_collection_(
        components::logical_plan::node_ptr logical_plan) {
        trace(log_, "memory_storage_t:drop_collection {}", logical_plan->collection_full_name().to_string());
        auto cursor = collections_.at(logical_plan->collection_full_name())->drop()
            ? make_cursor(resource(), operation_status_t::success)
            : make_cursor(resource(), error_code_t::other_error, "collection not dropped");
        collections_.erase(logical_plan->collection_full_name());
        trace(log_, "memory_storage_t:drop_collection_finish {}", logical_plan->collection_full_name().to_string());
        return {std::move(cursor), {}};
    }

    // execute_plan_impl - now LINEAR COROUTINE with co_await on executor
    memory_storage_t::unique_future<collection::executor::execute_result_t> memory_storage_t::execute_plan_impl(
        components::session::session_id_t session,
        components::logical_plan::node_ptr logical_plan,
        components::logical_plan::storage_parameters parameters,
        components::catalog::used_format_t used_format) {
        trace(log_,
              "memory_storage_t:execute_plan_impl: collection: {}, session: {}",
              logical_plan->collection_full_name().to_string(),
              session.data());

        if (used_format == components::catalog::used_format_t::undefined) {
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
                // raw_data from ql does not belong to any collection
                collections_context_storage.emplace(std::move(name), nullptr);
                continue;
            }
            collections_context_storage.emplace(std::move(name), collections_.at(name).get());
        }

        // Use co_await to get result from executor (NOT callback!)
        trace(log_, "memory_storage_t:execute_plan_impl: calling executor with co_await");
        auto future = actor_zeta::otterbrix::send(executor_address_, address(),
                                                           &collection::executor::executor_t::execute_plan,
                                                           session,
                                                           logical_plan,
                                                           parameters,
                                                           std::move(collections_context_storage),
                                                           used_format);
        if (future.needs_scheduling() && executor_) {
            scheduler_->enqueue(executor_.get());
        }
        auto result = co_await std::move(future);

        trace(log_, "memory_storage_t:execute_plan_impl: executor returned, success: {}", result.cursor->is_success());
        co_return result;
    }

} // namespace services