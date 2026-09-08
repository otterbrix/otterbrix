#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// An index whose disk storage cannot be opened must not take the engine down at startup.
// The per-index hash storage is opened by the AGENT itself, inside bitcask_index_disk_t::open()
// (reports by value; no shared handle). On failure, create() hands bootstrap_index_sync
// no agent, and the index is SKIPPED — unregistered, unpublished — rather than aborting the
// engine. The construct-and-open ctor, which asserts and aborts on the same failures, is reached
// only by the backend's own tests.
// Reachable only because indtype is now persisted: without it a restarted index always comes
// back as `single` and this hashed-storage injection never meets its target.

TEST_CASE("integration::cpp::test_index_bootstrap_failure::engine_starts_when_an_index_cannot_open") {
    auto config = test_create_config(integration_fixture_path("test_index_bootstrap_failure/restart"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;

    std::filesystem::path index_dir;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE b;")->is_success());
        REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON b.t USING hash (k);")->is_success());
        REQUIRE(exec("INSERT INTO b.t (id, k) VALUES (1, 10), (2, 20), (3, 30);")->is_success());

        // The oid-keyed layout carries no index name, so find the directory by its
        // hash_index.bin file instead.
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
            if (entry.is_directory() && std::filesystem::exists(entry.path() / "hash_index.bin")) {
                index_dir = entry.path();
                break;
            }
        }
        REQUIRE_FALSE(index_dir.empty());
    }

    // Make the storage file unopenable for the next start: disk_hash_table_t::create expects
    // a regular file, so replace it with a directory.
    const auto storage_file = index_dir / "hash_index.bin";
    std::filesystem::remove_all(storage_file);
    std::filesystem::create_directories(storage_file);
    REQUIRE(std::filesystem::is_directory(storage_file));

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        // Reaching this line at all is most of the assertion: the previous behaviour was to abort
        // inside the constructor, which takes the process with it.
        auto cur = exec("SELECT id FROM b.t WHERE k = 20;");
        INFO("an index that cannot open its storage must leave the table readable by scan");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);

        // And the data is still all there.
        auto all = exec("SELECT id FROM b.t;");
        REQUIRE(all->is_success());
        CHECK(all->size() == 3);
    }
}
