#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <integration/cpp/otterbrix.hpp>

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
inline components::logical_plan::node_ptr test_dml_target(components::logical_plan::node_ptr node,
                                                          const core::dbname_t& database,
                                                          const core::relname_t& collection) {
    using components::logical_plan::node_type;
    // Everything else (aggregate/match/...) already carries its own names.
    if (node->type() == node_type::insert_t || node->type() == node_type::update_t ||
        node->type() == node_type::delete_t) {
        node->set_target(qualified_name_t{database, collection});
    }
    return node;
}

// Registers the namespace lookup here too, redundantly, so this stays a faithful copy of a transformed plan.
inline components::cursor::cursor_t_ptr
test_create_collection(otterbrix::wrapper_dispatcher_t* dispatcher,
                       const otterbrix::session_id_t& session,
                       const core::dbname_t& database,
                       const core::relname_t& collection,
                       std::vector<components::table::column_definition_t> column_definitions = {},
                       std::vector<components::table::table_constraint_t> constraints = {}) {
    auto* resource = dispatcher->resource();
    auto node = components::logical_plan::make_node_create_collection(resource,
                                                                      collection,
                                                                      std::move(column_definitions),
                                                                      std::move(constraints));
    node->set_target(qualified_name_t{database, collection});
    components::logical_plan::execution_plan_t plan{resource,
                                                    node,
                                                    components::logical_plan::make_parameter_node(resource)};
    components::sql::transform::register_catalog_resolve_namespace(resource, &plan.catalog_resolves, database.t);
    return dispatcher->execute_plan(session, std::move(plan));
}

// Fatal to the case when the engine refuses to start; a test that expects a refusal calls open() itself.
inline otterbrix::base_otterbrix_t::host_ptr test_open_engine(const configuration::config& config,
                                                             components::planner::primitives_t primitives = {}) {
    auto host = otterbrix::base_otterbrix_t::open(config, primitives);
    if (host.has_error()) {
        FAIL("the engine refused to start at '" << config.main_path.string() << "': " << host.error().what);
    }
    return std::move(host.value());
}

inline otterbrix::otterbrix_ptr test_make_otterbrix(const configuration::config& config) {
    return otterbrix::otterbrix_ptr{new otterbrix::otterbrix_t(test_open_engine(config))};
}

class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    explicit test_spaces(const configuration::config& config, components::planner::primitives_t primitives = {})
        : otterbrix::base_otterbrix_t(test_open_engine(config, primitives)) {}
};

// A test reads the catalog through SQL; only one that corrupts it on purpose writes through the disk actor.
class catalog_forging_spaces_t final : public otterbrix::base_otterbrix_t {
public:
    explicit catalog_forging_spaces_t(const configuration::config& config)
        : otterbrix::base_otterbrix_t(test_open_engine(config)) {}

    actor_zeta::address_t disk_address() const noexcept { return engine().disk_address(); }
};

// Named, not global, so it can't collide with anonymous-namespace exec/seed helpers other test files define.
namespace test_helpers {

    inline components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return dispatcher->execute_sql(otterbrix::session_id_t(), sql);
    }

    inline bool ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        return exec(dispatcher, sql)->is_success();
    }

    // A crash image: the live directory copied as it lies on disk, replacing whatever `to` held.
    inline void copy_crash_image(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::error_code ec;
        std::filesystem::remove_all(to, ec);
        if (!ec) {
            std::filesystem::create_directories(to.parent_path(), ec);
        }
        if (!ec) {
            std::filesystem::copy(from, to, std::filesystem::copy_options::recursive, ec);
        }
        if (ec) {
            FAIL("copy_crash_image: '" << from.string() << "' -> '" << to.string() << "': " << ec.message());
        }
    }

    // No disk flag and no wal flag: every table is disk-backed and every write is journalled,
    // so `path` is where the data goes, full stop.
    inline configuration::config make_test_config(const std::filesystem::path& path) {
        auto config = test_create_config(path);
        test_clear_directory(config);
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
