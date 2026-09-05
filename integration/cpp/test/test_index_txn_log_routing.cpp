#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <services/index/manager_index.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>

// WHICH BACKEND A WRITE TRAVELS THROUGH, witnessed on disk.
//
// The index agents' insert_many / remove_many split every committed statement two ways.
// A backend that owns a durable transaction log journals the whole statement under its
// txn_id (bitcask, the index_type::hashed backend); a backend that owns none takes the
// bulk path -- insert_bulk_unchecked per row, then one force_flush (the ordered B+tree,
// every other index_type). The split is what arms the recover gate: after a crash bitcask
// replays only those journalled frames whose txn_id the WAL marked committed.
//
// "The rows are still there afterwards" does NOT witness that split. Both routes end with
// the rows on disk, so a dispatch that quietly sent EVERYTHING down the bulk path -- the
// exact shape a hook with a silent no-op default would produce -- would satisfy any
// row-count or round-trip check while the txn-log semantics disappeared unnoticed.
//
// What separates the two routes is an artefact ONLY the txn route leaves in the index
// directory:
//   bitcask.txn.log      one frame per journalled statement, appended by apply_txn_inserts
//                        / apply_txn_deletes and by nothing else;
//   bitcask.txn.applied  the applied-offset sidecar, rewritten after each frame.
// Neither is created by the bulk path, by opening an index, or by recovery running over an
// absent log (recover_txn_log_unlocked returns early when the log file does not exist).
//
// So the gate is: the hash index's directory must hold a txn log that GROWS across each
// committed statement, the btree index's directory must never hold one at all, and both
// directories must show their own data artefacts growing and surviving a restart.

namespace {

    std::uintmax_t size_or_zero(const std::filesystem::path& file) {
        std::error_code ec;
        const auto size = std::filesystem::file_size(file, ec);
        return ec ? 0u : size;
    }

    // Every regular file below dir, summed. Used as "this backend put bytes on disk".
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

    // The on-disk layout is ${disk}/${table_oid}/${index_oid}/ and carries no index NAME,
    // so the bitcask directory is found by CONTENT: it is the one holding a CURRENT
    // marker, which bitcask writes as soon as it opens. Both indexes below sit on the
    // same table, so the btree one is its only sibling. The btree directory cannot be
    // recognised by content at this point: it is still EMPTY -- a b+tree writes its
    // metadata file on its first flush, and CREATE INDEX on an empty table flushes
    // nothing. That emptiness is the reason `metadata` is asserted after the INSERT and
    // not here.
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

    // A COMMITTED DELETE NO LONGER JOURNALS INSIDE THE STATEMENT, and this waits for it.
    //
    // Since C5c the erase does not reach any store at commit time: the index would
    // otherwise stop naming a row that an older reader's snapshot still owns, and a short
    // index answer is one nothing downstream can undo (see
    // services/index/manager_index.hpp, deferred_deletes_). commit_deletes queues the
    // batch and the horizon sweep — a fire-and-forget broadcast from the dispatcher —
    // publishes it once no live snapshot can want the rows.
    //
    // WHAT IS BEING WAITED FOR IS NOT TIME. index_deferred_deletes() is the queue's own
    // depth; reaching zero means the sweep ran, which is the exact event the journal
    // assertion below depends on. Nothing here weakens that assertion: the route under
    // test — a DELETE travelling through apply_txn_deletes and not through the bulk path —
    // is unchanged, only its schedule is.
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

        // Baseline: the table is empty, no committed statement has reached either agent,
        // so nothing has been journalled yet. Recorded rather than asserted-zero so the
        // growth checks below stay honest even if some future path pre-creates the file.
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

        // A second committed statement, this time on the delete leg: apply_txn_deletes
        // appends its own frame, so the log grows again. A bulk-path DELETE would leave
        // the log byte-for-byte unchanged.
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

    // Both backends' bytes are real: a restart re-opens the same directories and both
    // indexes answer from what is on disk. The journal itself is NOT expected to survive
    // -- bootstrap repopulates the index, and clear() unlinks the log with the rest of the
    // bitcask artefacts, which is why the check below is that a post-restart statement
    // journals AGAIN, not that the old frames are still there.
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
