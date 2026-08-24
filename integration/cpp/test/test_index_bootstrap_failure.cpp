#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// An index whose disk storage cannot be opened must not take the engine down at startup.
//
// bootstrap_indexes_sync opens a per-table hash storage through disk_hash_table_t::create(), which
// reports by value. It must not use the direct constructor: that one asserts and aborts on exactly
// the failures this path exists to survive (unopenable file, unreadable or incompatible header),
// and an index that will not open costs a full scan, whereas aborting costs the whole engine its
// start. The try/catch that used to wrap it caught exceptions the storage no longer throws.
//
// THIS TEST CANNOT BE MADE RED, and the reason is worth recording rather than hiding. Two things
// stand between the injection and that code:
//   * the storage this bootstrap opens is <disk>/<table_oid>/hash_index.bin, while a hash index
//     actually keeps its files in <disk>/<table_oid>/<index_name>/;
//   * the branch is gated on the restored row's type being `hashed`, and the index TYPE is not
//     persisted in pg_index, so after a restart every index comes back as `single` and the branch
//     is skipped altogether.
// So it is a characterisation test: it pins that a restart survives an unopenable storage path and
// leaves the table readable, and it documents why the constructor swap it accompanies is currently
// unreachable. Persisting the index type is a recorded prerequisite; when that lands, this test
// becomes the red proof it could not be today.

TEST_CASE("integration::cpp::test_index_bootstrap_failure::engine_starts_when_an_index_cannot_open") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_bootstrap_failure/restart");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
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

        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
            if (entry.is_directory() && entry.path().filename() == "k_idx") {
                index_dir = entry.path();
                break;
            }
        }
        REQUIRE_FALSE(index_dir.empty());
    }

    // The engine is down; make the storage file unopenable for the next start. The bootstrap
    // opens the storage shared by the whole TABLE (<disk>/<table_oid>/hash_index.bin), not the
    // per-index directory beside it.
    const auto storage_file = index_dir.parent_path() / "hash_index.bin";
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
