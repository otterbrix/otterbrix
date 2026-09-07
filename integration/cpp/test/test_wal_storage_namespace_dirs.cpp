#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/compute/function.hpp>
#include <integration/cpp/base_spaces.hpp>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <filesystem>
#include <set>
#include <string>

// Table storage shares the WAL root (config_disk.path == config_wal.path), so a table's
// per-namespace directory (${wal_root}/${relnamespace}/${oid}/table.otbx) is named after an oid
// too; classifying startup-scan directories by name alone spawned a worker for a database that
// does not exist. The fix classifies by STRUCTURE instead: a real WAL directory holds
// wal_<oid>_NNNNNN segment files, a storage namespace directory holds only <table_oid>/ dirs.

using namespace test_helpers;

namespace {

    // base_otterbrix_t's manager_wal_ is protected, so a thin subclass is the only way to read the
    // live worker count.
    class wal_probe_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit wal_probe_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            components::compute::function_registry_t::reset_default();
        }
        std::size_t wal_worker_count() const { return manager_wal_->active_worker_count(); }
    };

    bool dir_has_wal_segment(const std::filesystem::path& dir) {
        for (const auto& f : std::filesystem::directory_iterator(dir)) {
            if (!f.is_regular_file()) {
                continue;
            }
            const auto name = f.path().filename().string();
            if (name.size() >= 4 && name.compare(0, 4, "wal_") == 0) {
                return true;
            }
        }
        return false;
    }

} // namespace

TEST_CASE("integration::cpp::wal_storage_namespace_dirs::no_worker_for_a_storage_namespace_directory") {
    auto config = test_create_config(integration_fixture_path("test_wal_storage_namespace_dirs/db"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(exec(d, "CREATE DATABASE adb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE adb.t1 (id bigint);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE adb.t2 (id bigint);")->is_success());
        REQUIRE(exec(d, "INSERT INTO adb.t1 (id) VALUES (1), (2), (3);")->is_success());
        REQUIRE(exec(d, "CREATE DATABASE bdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE bdb.t (id bigint);")->is_success());
        REQUIRE(exec(d, "INSERT INTO bdb.t (id) VALUES (1);")->is_success());
    }

    std::set<components::catalog::oid_t> db_dirs;      // parse as oid AND hold a wal_ segment
    std::set<components::catalog::oid_t> storage_dirs; // parse as oid but hold NO wal_ segment
    for (const auto& e : std::filesystem::directory_iterator(config.wal.path)) {
        if (!e.is_directory()) {
            continue;
        }
        components::catalog::oid_t oid;
        if (!services::wal::parse_database_dir_name(e.path().filename().string(), oid)) {
            continue;
        }
        (dir_has_wal_segment(e.path()) ? db_dirs : storage_dirs).insert(oid);
    }

    INFO("NOT VACUOUS: at least one oid-named storage namespace directory must sit under the WAL root");
    REQUIRE_FALSE(storage_dirs.empty());
    INFO("there must be a real database WAL directory to spawn a worker for");
    REQUIRE_FALSE(db_dirs.empty());

    wal_probe_spaces_t space(config);
    const auto workers = space.wal_worker_count();
    INFO("db dirs (should each get a worker): " << db_dirs.size()
                                                << "; storage namespace dirs (should get none): " << storage_dirs.size()
                                                << "; workers spawned: " << workers);
    REQUIRE(workers == db_dirs.size());
}
