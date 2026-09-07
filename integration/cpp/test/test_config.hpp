#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>

#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>

// `path` has no default (once deleted a test's cwd) and must be pre-qualified against the shared fixture root.
inline configuration::config test_create_config(const std::filesystem::path& path) {
    if (!integration_fixture_path_is_qualified(path)) {
        FAIL("test_create_config: '" << path.string()
                                     << "' is an unqualified fixture root: it sits directly under the shared '"
                                     << integration_fixture_shared_root().string()
                                     << "', where every test binary running at once clears and reseeds it. Build the "
                                        "path with integration_fixture_path(\"<leaf>\") -- this process's root is '"
                                     << integration_fixture_root().string() << "'.");
    }
    return configuration::config::create_config(path);
    // To change log level
    // config.log.level =log_t::level::trace;
}

// Reports I/O failure via std::error_code instead of throwing, so it doesn't read as an engine defect.
[[nodiscard]] inline std::error_code test_try_clear_directory(const configuration::config& config) {
    std::error_code ec;
    std::filesystem::remove_all(config.main_path, ec);
    if (ec) {
        return ec;
    }
    // create_directories answers "already there" with false and no error code — only `ec` says something went wrong.
    std::filesystem::create_directories(config.main_path, ec);
    return ec;
}

// Fatal to the case on failure; names the path and reason instead of an unhandled exception.
inline void test_clear_directory(const configuration::config& config) {
    const std::error_code ec = test_try_clear_directory(config);
    if (ec) {
        FAIL("test_clear_directory: could not make '" << config.main_path.string()
                                                      << "' a clean directory: " << ec.message());
    }
}

// Names a DML target as the transformer would, so register_plan_targets resolves it like a transformed plan.
inline components::logical_plan::node_ptr
test_dml_target(components::logical_plan::node_ptr node, const std::string& database, const std::string& collection) {
    using namespace components::logical_plan;
    switch (node->type()) {
        case node_type::insert_t: {
            auto* n = static_cast<node_insert_t*>(node.get());
            n->set_dbname(database);
            n->set_relname(collection);
            break;
        }
        case node_type::update_t: {
            auto* n = static_cast<node_update_t*>(node.get());
            n->set_dbname(database);
            n->set_relname(collection);
            break;
        }
        case node_type::delete_t: {
            auto* n = static_cast<node_delete_t*>(node.get());
            n->set_dbname(database);
            n->set_relname(collection);
            break;
        }
        default:
            // Everything else (aggregate/match/...) already carries its own names.
            break;
    }
    return node;
}

// Registers the namespace lookup here too, redundantly, so this stays a faithful copy of a transformed plan.
inline components::cursor::cursor_t_ptr
test_create_collection(otterbrix::wrapper_dispatcher_t* dispatcher,
                       const otterbrix::session_id_t& session,
                       const database_name_t& database,
                       const collection_name_t& collection,
                       std::vector<components::table::column_definition_t> column_definitions = {},
                       std::vector<components::table::table_constraint_t> constraints = {}) {
    auto* resource = dispatcher->resource();
    auto node = components::logical_plan::make_node_create_collection(resource,
                                                                      core::relname_t{collection},
                                                                      std::move(column_definitions),
                                                                      std::move(constraints));
    node->set_dbname(database);
    components::logical_plan::execution_plan_t plan{resource,
                                                    node,
                                                    components::logical_plan::make_parameter_node(resource)};
    components::sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, database);
    return dispatcher->execute_plan(session, std::move(plan));
}

class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    // Host customization hooks forwarded to the engine ctor chain; Null Objects here for non-federation tests.
    test_spaces(const configuration::config& config,
                services::planner::create_plan_rule_t create_plan_rule = &services::planner::no_custom_lowering,
                components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass)
        : otterbrix::base_otterbrix_t(config, create_plan_rule, optimizer_pass) {
        // Resets the UDF registry per test — a stale one once crashed test_batch_join after test_batch_where.
        components::compute::function_registry_t::reset_default();
    }
};

// Named, not global, so it can't collide with anonymous-namespace exec/seed helpers other test files define.
namespace test_helpers {

    inline components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    // No disk flag: every table is disk-backed, so `path` is where the data goes, full stop.
    inline configuration::config make_test_config(const std::filesystem::path& path, bool wal_on = false) {
        auto config = test_create_config(path);
        test_clear_directory(config);
        config.wal.on = wal_on;
        return config;
    }

    // `row(i)` returns row i's already-parenthesized tuple text.
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
