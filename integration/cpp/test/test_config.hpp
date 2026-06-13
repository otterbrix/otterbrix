#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>

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
};
