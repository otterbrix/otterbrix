#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <string>

// FIN-0 / item 3 — A CHECKPOINT MUST NOT REPORT SUCCESS OVER AN INDEX FLUSH THAT FAILED.
//
// Two swallows sat on the same path and both are closed by the same channel.
//
//   * btree_index_disk_t::flush_if_needed() returned void and, on a failed force_flush(),
//     simply `return`ed. The threshold flush is the ONLY thing that puts an ordered index's
//     entries on the device outside an explicit force_flush, so a failure there was
//     unobservable from anywhere above it.
//   * btree_index_agent_t::force_flush / bitcask_index_agent_t::force_flush logged the
//     store's io_error and replied — the contract's return type was unique_future<void>, so
//     there was nothing else they COULD do. manager_index_t::flush_all_indexes then reported
//     a clean fan-out, and operator_checkpoint_t went on to step 4: truncate_before, which
//     drops exactly the WAL segments that were the last remaining copy of what the index
//     failed to write.
//
// THE INJECTION. core::b_plus_tree::btree_t::flush() writes the leaf list into
// `<index dir>/metadata`, opened WRITE|FILE_CREATE. Replacing that path with a DIRECTORY
// makes the open fail, which the store reports as io_error — the same technique
// test_index_bootstrap_failure uses to make an index storage unopenable, and the only one
// available here (the T3 block-manager seam does not cover the B+tree's own files).
//
// The INSERT after the break is what leaves the tree dirty: publish_buckets applies the
// entries and then fails its force_flush, and a failed flush does not reset the dirty state.
// The CHECKPOINT that follows therefore has real work to do and real reason to refuse.

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
