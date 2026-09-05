#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/index/manager_index.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

// index_type::hashed (bitcask) journals every committed statement to a durable txn log
// (apply_txn_inserts / apply_txn_deletes append bitcask.txn.log and rewrite
// bitcask.txn.applied); every other index_type takes the bulk path (insert_bulk_unchecked
// per row, then one force_flush) and journals nothing. Row counts can't tell the routes
// apart -- both end with the same rows on disk -- so this gates on the log FILE itself: it
// must grow on the hash index, never exist on the btree one, and both must hold across a
// restart.

namespace {

    std::uintmax_t size_or_zero(const std::filesystem::path& file) {
        std::error_code ec;
        const auto size = std::filesystem::file_size(file, ec);
        return ec ? 0u : size;
    }

    // Total bytes across all regular files under dir -- "this backend put bytes on disk".
    std::uintmax_t tree_bytes(const std::filesystem::path& dir) {
        std::uintmax_t total = 0;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(dir, ec), end; it != end; it.increment(ec)) {
            if (it->is_regular_file()) {
                total += it->file_size();
            }
        }
        return total;
    }

    struct index_dirs_t {
        std::filesystem::path bitcask;
        std::filesystem::path btree;
    };

    // The layout (${disk}/${table_oid}/${index_oid}/) carries no index name, so bitcask is
    // found by its CURRENT marker (written on open) and btree is taken as its only sibling.
    // The btree dir is still EMPTY here -- its metadata file appears only on first flush --
    // so it can't be recognised by content until after the INSERT.
    index_dirs_t find_index_dirs(const std::filesystem::path& disk_root) {
        index_dirs_t dirs;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(disk_root, ec), end; it != end; it.increment(ec)) {
            if (it->is_directory() && std::filesystem::exists(it->path() / "CURRENT")) {
                dirs.bitcask = it->path();
            }
        }
        if (dirs.bitcask.empty()) {
            return dirs;
        }
        for (const auto& e : std::filesystem::directory_iterator(dirs.bitcask.parent_path())) {
            if (e.is_directory() && e.path() != dirs.bitcask) {
                dirs.btree = e.path();
            }
        }
        return dirs;
    }

    // A committed DELETE is queued (commit_deletes) and only reaches the store after the
    // horizon sweep runs, since an older snapshot may still own the row. Waits on the queue
    // depth (index_deferred_deletes()) reaching zero, not on a timer, so the journal
    // assertion below observes the sweep's effect rather than racing it.
    void await_deferred_index_deletes() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (services::index::index_deferred_deletes() != 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        REQUIRE(services::index::index_deferred_deletes() == 0);
    }

} // namespace

TEST_CASE("integration::cpp::test_index_txn_log_routing::hash_journals_btree_does_not") {
    auto config = test_create_config(integration_fixture_path("test_index_txn_log_routing/routes"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    std::filesystem::path bitcask_dir;
    std::filesystem::path btree_dir;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE r;")->is_success());
        REQUIRE(exec("CREATE TABLE r.t (id bigint, k bigint, m bigint);")->is_success());
        // One table, two backends: USING hash -> bitcask (owns a txn log),
        // the plain form -> ordered B+tree (owns none).
        REQUIRE(exec("CREATE INDEX t_k ON r.t USING hash (k);")->is_success());
        REQUIRE(exec("CREATE INDEX t_m ON r.t (m);")->is_success());

        auto dirs = find_index_dirs(config.disk.path);
        INFO("a USING hash index must own a bitcask directory");
        REQUIRE_FALSE(dirs.bitcask.empty());
        INFO("a plain index must own a directory beside it");
        REQUIRE_FALSE(dirs.btree.empty());
        bitcask_dir = dirs.bitcask;
        btree_dir = dirs.btree;

        const auto txn_log = bitcask_dir / "bitcask.txn.log";
        const auto txn_applied = bitcask_dir / "bitcask.txn.applied";

        // Recorded rather than asserted-zero, so the growth checks below stay honest even
        // if some future path pre-creates the file.
        const auto log_before_insert = size_or_zero(txn_log);
        const auto btree_before_insert = tree_bytes(btree_dir);

        REQUIRE(exec("INSERT INTO r.t (id, k, m) VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300);")->is_success());

        INFO("the hash index must have journalled the INSERT through apply_txn_inserts");
        REQUIRE(std::filesystem::exists(txn_log));
        REQUIRE(size_or_zero(txn_log) > log_before_insert);
        INFO("apply_txn_inserts rewrites the applied-offset sidecar after each frame");
        REQUIRE(std::filesystem::exists(txn_applied));

        INFO("the hash index's own data must have reached disk, not only its journal");
        REQUIRE(tree_bytes(bitcask_dir) > 0);

        INFO("the btree index must NOT have taken the txn-log route");
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.applied"));
        INFO("the btree index must still have reached disk on its own route");
        REQUIRE(std::filesystem::exists(btree_dir / "metadata"));
        REQUIRE(tree_bytes(btree_dir) > btree_before_insert);

        // A bulk-path DELETE would leave the log byte-for-byte unchanged.
        const auto log_before_delete = size_or_zero(txn_log);
        REQUIRE(exec("DELETE FROM r.t WHERE k = 20;")->is_success());
        await_deferred_index_deletes();

        INFO("the hash index must have journalled the DELETE through apply_txn_deletes");
        REQUIRE(size_or_zero(txn_log) > log_before_delete);
        INFO("the btree index must still hold no journal after a DELETE");
        REQUIRE_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));

        auto by_hash = exec("SELECT id FROM r.t WHERE k = 30;");
        REQUIRE(by_hash->is_success());
        CHECK(by_hash->size() == 1);
        auto by_btree = exec("SELECT id FROM r.t WHERE m = 100;");
        REQUIRE(by_btree->is_success());
        CHECK(by_btree->size() == 1);
    }

    // The journal itself is NOT expected to survive a restart (bootstrap repopulates the
    // index and clear() unlinks it), so the check is that a post-restart statement
    // journals AGAIN, not that the old frames remain.
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        auto by_hash = exec("SELECT id FROM r.t WHERE k = 30;");
        REQUIRE(by_hash->is_success());
        CHECK(by_hash->size() == 1);
        auto by_btree = exec("SELECT id FROM r.t WHERE m = 100;");
        REQUIRE(by_btree->is_success());
        CHECK(by_btree->size() == 1);
        auto deleted = exec("SELECT id FROM r.t WHERE k = 20;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);

        const auto log_before = size_or_zero(bitcask_dir / "bitcask.txn.log");
        REQUIRE(exec("INSERT INTO r.t (id, k, m) VALUES (4, 40, 400);")->is_success());

        INFO("the txn route must still be armed after a restart");
        CHECK(size_or_zero(bitcask_dir / "bitcask.txn.log") > log_before);
        INFO("no journal may appear in the btree index's directory across a restart");
        CHECK_FALSE(std::filesystem::exists(btree_dir / "bitcask.txn.log"));

        auto reinserted = exec("SELECT id FROM r.t WHERE k = 40;");
        REQUIRE(reinserted->is_success());
        CHECK(reinserted->size() == 1);
    }
}
