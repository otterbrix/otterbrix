#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <string>

// An index whose disk storage cannot be opened must not take the engine down at startup.
//
// The per-index hash storage (<disk>/<table_oid>/<indexrelid>/hash_index.bin) is opened by the
// AGENT that owns it, inside bitcask_index_disk_t::create(), which reports by value; it used to
// be opened by bootstrap_indexes_sync and handed in as a shared handle (removed in C2c, rule
// 10). The DECISION stays where it was: index_agent_disk_t::create() hands bootstrap_indexes_sync
// either an agent or the reason there is none, and on a reason it SKIPS the index entirely —
// registering nothing, publishing no address, never scheduling it. An index that will not open
// costs a full scan, whereas aborting costs the whole engine its start. The factory is what
// makes that possible: the DIRECT constructor asserts and aborts on exactly the failures this
// path exists to survive (unopenable file, unreadable or incompatible header).
//
// Historically this test could not be made red: pg_index carried no indtype, so after a restart
// every index came back as `single` and the hashed branch never ran. With indtype persisted the
// injection below reaches the real code path, and this is the red proof the original
// characterisation test promised.

TEST_CASE("integration::cpp::test_index_bootstrap_failure::engine_starts_when_an_index_cannot_open") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_bootstrap_failure/restart");
    test_clear_directory(config);
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

        // The on-disk layout is oid-keyed (<disk>/<table_oid>/<indexrelid>/) and carries no
        // index name, so find the index directory by content: it owns the hash storage file.
        for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
            if (entry.is_directory() && std::filesystem::exists(entry.path() / "hash_index.bin")) {
                index_dir = entry.path();
                break;
            }
        }
        REQUIRE_FALSE(index_dir.empty());
    }

    // The engine is down; make the per-index storage file unopenable for the next start —
    // a directory where disk_hash_table_t::create expects a regular file. The agent's own
    // open is what meets it now.
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
