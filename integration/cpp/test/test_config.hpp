#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_sync_mode.hpp>

#include <actor-zeta/send.hpp>

#include <components/catalog/catalog_oids.hpp>

#include <chrono>
#include <thread>
#include <utility>

inline configuration::config test_create_config(const std::filesystem::path& path = std::filesystem::current_path()) {
    return configuration::config::create_config(path);
    // To change log level
    // config.log.level =log_t::level::trace;
}

inline void test_clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.main_path);
    std::filesystem::create_directories(config.main_path);
}

// Test-side CREATE TABLE: builds the same logical plan the SQL transformer
// emits (create_collection wrapped with catalog_resolve_namespace) and sends
// it through the single client channel, execute_plan.
inline components::cursor::cursor_t_ptr
test_create_collection(otterbrix::wrapper_dispatcher_t* dispatcher,
                       const otterbrix::session_id_t& session,
                       const database_name_t& database,
                       const collection_name_t& collection,
                       std::vector<components::table::column_definition_t> column_definitions = {},
                       std::vector<components::table::table_constraint_t> constraints = {}) {
    auto* resource = dispatcher->resource();
    auto node = components::sql::transform::maybe_wrap_with_catalog_resolve_namespace(
        resource,
        database,
        components::logical_plan::make_node_create_collection(resource,
                                                              core::relname_t{collection},
                                                              std::move(column_definitions),
                                                              std::move(constraints)));
    return dispatcher->execute_plan(
        session,
        components::logical_plan::execution_plan_t{resource,
                                                   std::move(node),
                                                   components::logical_plan::make_parameter_node(resource)});
}

class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    test_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {
        // Isolate the process-global UDF registry between test cases: each test
        // gets a fresh builtins-only default registry so user functions from a
        // previous test don't leak into this one (which crashed test_batch_join
        // when run after test_batch_where — a stale aggregate UDF resolved to a
        // null function at plan-gen).
        components::compute::function_registry_t::reset_default();
    }

    template<typename Fn, typename... Args>
    auto disk_invoke(Fn fn, Args&&... args) {
        auto [_, future] =
            actor_zeta::otterbrix::send(manager_disk_->address(), fn, std::forward<Args>(args)...);
        while (!future.is_ready()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return std::move(future).take_ready();
    }

    // Alias so test_probe::probe_* (services/disk/tests/catalog_probe.hpp) can drive
    // this integration fixture: probe_read() calls fx.invoke(&manager_disk_t::...).
    template<typename Fn, typename... Args>
    auto invoke(Fn fn, Args&&... args) {
        return disk_invoke(fn, std::forward<Args>(args)...);
    }

    template<typename Fn, typename... Args>
    auto wal_invoke(Fn fn, Args&&... args) {
        auto [_, future] =
            actor_zeta::otterbrix::send(manager_wal_->address(), fn, std::forward<Args>(args)...);
        while (!future.is_ready()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return std::move(future).take_ready();
    }

    template<typename Fn, typename... Args>
    auto dispatcher_invoke(Fn fn, Args&&... args) {
        auto [_, future] =
            actor_zeta::otterbrix::send(manager_dispatcher_->address(), fn, std::forward<Args>(args)...);
        while (!future.is_ready()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return std::move(future).take_ready();
    }

#if defined(DEV_MODE)
    void debug_write_committed_physical_update(components::catalog::oid_t table_oid,
                                               std::pmr::vector<int64_t> row_ids,
                                               components::vector::data_chunk_t& data) {
        auto wal_data =
            std::make_unique<components::vector::data_chunk_t>(&resource, data.types(), data.size());
        data.copy(*wal_data, 0);

        manager_disk_->direct_update_sync(table_oid, row_ids, data);
        wal_invoke(&services::wal::manager_wal_replicate_t::write_physical_update,
                   otterbrix::session_id_t(),
                   table_oid,
                   std::move(row_ids),
                   std::move(wal_data),
                   data.size(),
                   std::uint64_t{0},
                   components::catalog::well_known_oid::main_database);
        wal_invoke(&services::wal::manager_wal_replicate_t::commit_txn,
                   otterbrix::session_id_t(),
                   std::uint64_t{0},
                   services::wal::wal_sync_mode::NORMAL,
                   components::catalog::well_known_oid::main_database,
                   std::uint64_t{0});
    }

    components::table::storage::row_group_layout_kind
    debug_first_row_group_layout_kind(components::catalog::oid_t table_oid) const noexcept {
        return manager_disk_->debug_first_row_group_layout_kind_sync(table_oid);
    }

    void debug_reset_first_row_group_scan_path_counts(components::catalog::oid_t table_oid) noexcept {
        manager_disk_->debug_reset_first_row_group_scan_path_counts_sync(table_oid);
    }

    components::table::row_group_scan_path_counts_t
    debug_first_row_group_scan_path_counts(components::catalog::oid_t table_oid) const noexcept {
        return manager_disk_->debug_first_row_group_scan_path_counts_sync(table_oid);
    }
#endif
};
