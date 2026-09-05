#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <sstream>
#include <string>
#include <system_error>

// `path` is REQUIRED. It used to default to std::filesystem::current_path(), which no call
// site in the tree ever used and which no caller could have wanted: config::create_config
// puts main_path at exactly the path it is handed (configuration.hpp, `main_path(path)`),
// and the overwhelmingly common next line is test_clear_directory(config) --
// remove_all(main_path). The default therefore stood for "delete the working directory",
// i.e. the build tree the test binary was launched from. Not EVERY fixture clears: a reopen
// fixture deliberately keeps what the first open wrote (test_declared_key_conkey_loss.cpp
// builds a second config over the same directory and does not clear it). Those are the
// minority, and they are exactly the ones for which a current_path() default would have been
// hardest to notice.
inline configuration::config test_create_config(const std::filesystem::path& path) {
    return configuration::config::create_config(path);
    // To change log level
    // config.log.level =log_t::level::trace;
}

// Make `config.main_path` exist and be empty, and REPORT an I/O failure instead of throwing
// it. The throwing overloads of remove_all / create_directories put a
// std::filesystem::filesystem_error into the body of whichever case happened to be running,
// where it reads as a defect in the engine rather than as a fixture that could not be built:
// "filesystem error: in remove_all: Directory not empty" and "... No such file or directory"
// were both observed that way, and neither was about the code under test.
//
// std::error_code and not core::error_t: this is the filesystem's own non-throwing channel,
// and a header-only test helper holds no arena to build core::error_t's pmr::string in
// (rule 14 rules out get_default_resource / new_delete_resource).
[[nodiscard]] inline std::error_code test_try_clear_directory(const configuration::config& config) {
    std::error_code ec;
    std::filesystem::remove_all(config.main_path, ec);
    if (ec) {
        return ec;
    }
    // create_directories answers "already there" with false and NO error code, which is not
    // a failure; only `ec` says anything went wrong.
    std::filesystem::create_directories(config.main_path, ec);
    return ec;
}

// The fixture as every case wants it: a clean directory, or a case that stops right here.
// A test cannot go on without its data directory, so the refusal is fatal to the case -- but
// it now names the path and the reason instead of arriving as an unhandled exception.
inline void test_clear_directory(const configuration::config& config) {
    const std::error_code ec = test_try_clear_directory(config);
    if (ec) {
        FAIL("test_clear_directory: could not make '" << config.main_path.string()
                                                      << "' a clean directory: " << ec.message());
    }
}

// Name a DML node's target the way the SQL transformer does. The executor's
// register_plan_targets picks the name up and registers the catalog lookup, so a
// hand-built test plan resolves exactly like a transformed one. Returns the node
// so it drops straight into an execution_plan_t.
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

// Test-side CREATE TABLE: builds the same logical plan the SQL transformer
// emits and sends it through the single client channel, execute_plan. The
// namespace lookup is registered on the plan, exactly as the transformer does;
// the executor's top-up would also cover it, but naming it here keeps the test
// plan a faithful copy of a transformed one.
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
    // create_plan_rule / optimizer_pass: host customization hooks forwarded to the
    // engine through the constructor chain (physgen lowering of node_extension /
    // custom nodes; a final optimizer pass). Null Objects for non-federation tests.
    test_spaces(const configuration::config& config,
                services::planner::create_plan_rule_t create_plan_rule = &services::planner::no_custom_lowering,
                components::planner::optimizer_pass_t optimizer_pass = &components::planner::no_op_pass)
        : otterbrix::base_otterbrix_t(config, create_plan_rule, optimizer_pass) {
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

    // create_config + clear_directory + the WAL flag in one call. There is no disk flag:
    // every table is disk-backed, so `path` is where the data goes, full stop.
    inline configuration::config make_test_config(const std::filesystem::path& path, bool wal_on = false) {
        auto config = test_create_config(path);
        test_clear_directory(config);
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
