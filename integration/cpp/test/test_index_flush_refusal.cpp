#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>

// flush_if_needed() and the *_index_agent_t::force_flush overrides used to swallow a failed
// force_flush (return void / log-and-reply): a checkpoint downstream saw a clean fan-out from
// flush_all_indexes and truncated the WAL segments that were the index's only copy.
//
// Injection: btree_t::flush() opens `<index dir>/metadata` WRITE|FILE_CREATE; a DIRECTORY there
// fails the open (test_index_bootstrap_failure's technique — the only one, since the
// block-manager fault seam doesn't cover the B+tree's own files). The INSERT after leaves the
// tree dirty via publish_buckets before its force_flush fails.

TEST_CASE("integration::cpp::test_index_flush_refusal::checkpoint_fails_when_an_index_flush_cannot_reach_the_disk") {
    auto config = test_helpers::make_test_config(
        integration_fixture_path("test_index_flush_refusal/checkpoint"),
        /*wal_on=*/true);
    config.log.level = log_t::level::off;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE fl;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE fl.t (id bigint, k bigint);")->is_success());
    // No USING clause: the ordered (B+tree) family, which is the one whose store keeps a
    // `metadata` file.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE INDEX k_idx ON fl.t (k);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO fl.t (id, k) VALUES (1, 10), (2, 20);")->is_success());

    // The on-disk layout is oid-keyed and carries no index name, so find the tree by content.
    std::filesystem::path index_dir;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(config.disk.path)) {
        if (entry.is_regular_file() && entry.path().filename() == "metadata") {
            index_dir = entry.path().parent_path();
            break;
        }
    }
    REQUIRE_FALSE(index_dir.empty());

    const auto metadata = index_dir / "metadata";
    std::filesystem::remove_all(metadata);
    std::filesystem::create_directories(metadata);
    REQUIRE(std::filesystem::is_directory(metadata));

    // Leaves the tree dirty: the entries are applied, the flush behind them is not.
    test_helpers::exec(dispatcher, "INSERT INTO fl.t (id, k) VALUES (3, 30);");

    auto cur = test_helpers::exec(dispatcher, "CHECKPOINT;");
    INFO("a CHECKPOINT whose index flush never reached the disk must FAIL: step 4 truncates the WAL");
    REQUIRE(cur->is_error());
}
