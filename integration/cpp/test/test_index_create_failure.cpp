#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// CREATE INDEX must fail, not silently fall back to an in-memory index, when it cannot open its disk storage.
// Failure is injected by planting a DIRECTORY at the next oid's storage path, so open() fails with EISDIR.
TEST_CASE("integration::cpp::test_index_create_failure::unopenable_disk_index_is_an_error") {
    auto config = test_create_config(integration_fixture_path("test_index_create_failure/unopenable"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec("CREATE TABLE d.t (id bigint, k bigint);")->is_success());

    REQUIRE(exec("CREATE INDEX ok_idx ON d.t USING hash (k);")->is_success());

    std::filesystem::path ok_index_dir;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
        if (entry.is_directory() && std::filesystem::exists(entry.path() / "CURRENT")) {
            ok_index_dir = entry.path();
            break;
        }
    }
    REQUIRE_FALSE(ok_index_dir.empty());
    const auto oid_dir = ok_index_dir.parent_path();

    // Assumes the next CREATE INDEX allocates exactly one oid and no other DDL runs in between.
    const auto ok_oid = std::stoull(ok_index_dir.filename().string());
    std::filesystem::create_directories(oid_dir / std::to_string(ok_oid + 1) / "hash_index.bin");

    auto cursor = exec("CREATE INDEX bad_idx ON d.t USING hash (k);");
    INFO("a disk index that cannot open its storage must not silently become an in-memory index");
    CHECK(cursor->is_error());
}
