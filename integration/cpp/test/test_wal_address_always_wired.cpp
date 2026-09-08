#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <stdexcept>
#include <string>

// Two halves of one invariant: an engine built by base_spaces always has a WAL that is actually
// written to, and there is no second, quieter way to switch it off.
//
// Checking that config.wal.path is a non-empty DIRECTORY would prove nothing: wal.path and
// disk.path are the same <base>/wal (configuration.hpp), and the disk manager fills it with the
// table tree. Only a wal_* segment file proves the journal itself was reached.

namespace {
    bool wal_segment_exists(const std::filesystem::path& wal_root) {
        std::error_code ec;
        if (!std::filesystem::is_directory(wal_root, ec)) {
            return false;
        }
        for (const auto& entry : std::filesystem::recursive_directory_iterator(wal_root, ec)) {
            if (entry.is_regular_file(ec) && entry.path().filename().string().rfind("wal_", 0) == 0) {
                return true;
            }
        }
        return false;
    }
} // namespace

TEST_CASE("integration::cpp::wal_address_always_wired::a statement reaches the journal") {
    auto config = test_create_config(integration_fixture_path("test_wal_address_always_wired/wired"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE w;")->is_success());
        REQUIRE(exec("CREATE TABLE w.t (id BIGINT);")->is_success());
        REQUIRE(exec("INSERT INTO w.t (id) VALUES (1);")->is_success());
    }

    INFO("wal root: " << config.wal.path.string());
    CHECK(wal_segment_exists(config.wal.path));
}

TEST_CASE("integration::cpp::wal_address_always_wired::an empty wal path is refused at startup") {
    auto config = test_create_config(integration_fixture_path("test_wal_address_always_wired/empty_path"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    // Not "no WAL": the recovery scan and total_wal_bytes() both skip an empty path, so the
    // auto-checkpoint threshold would never fire and nothing would say why.
    config.wal.path.clear();

    REQUIRE_THROWS_AS(test_spaces{config}, std::runtime_error);
}
