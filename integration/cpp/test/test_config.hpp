#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>

#include <sstream>
#include <string>

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

// Shared integration-test helpers. Kept in a NAMED namespace (not global) so
// they never collide with the anonymous-namespace `exec`/`seed` helpers that
// several on-main test files still define locally — a global `exec` overload
// with the same signature would make every unqualified call in those files
// ambiguous. New test files opt in with `using namespace test_helpers;`.
namespace test_helpers {

    // Run one SQL statement on a fresh session and return the cursor.
    inline components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    // create_config + clear_directory + disk/wal flags in one call.
    inline configuration::config
    make_test_config(const std::filesystem::path& path, bool disk_on = false, bool wal_on = false) {
        auto config = test_create_config(path);
        test_clear_directory(config);
        config.disk.on = disk_on;
        config.wal.on = wal_on;
        return config;
    }

    // Emit `INSERT INTO <table> (<cols>) VALUES <row(0)>, <row(1)>, ...;` for `n`
    // rows, where `row(i)` returns the parenthesized tuple text for row i, and run
    // it. Callers assert on the returned cursor (success + affected size).
    template<typename RowFn>
    inline components::cursor::cursor_t_ptr seed_rows(otterbrix::wrapper_dispatcher_t* dispatcher,
                                                      const std::string& table,
                                                      const std::string& cols,
                                                      unsigned n,
                                                      RowFn&& row) {
        std::stringstream q;
        q << "INSERT INTO " << table << " (" << cols << ") VALUES ";
        for (unsigned i = 0; i < n; ++i) {
            q << row(i) << (i + 1 == n ? ";" : ", ");
        }
        return exec(dispatcher, q.str());
    }

} // namespace test_helpers
