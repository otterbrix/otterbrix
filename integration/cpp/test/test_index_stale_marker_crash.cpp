#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

// WHAT A RESTART MUST NOT DO AFTER A COMPACTING ROUND DIED BETWEEN THE COMPACTION AND THE
// INDEX REBUILD.
//
// A CHECKPOINT round compacts each table (data_table_t::compact rebuilds it at row id 0 and
// hands every surviving row a NEW physical id, committed by the .otbx header and its
// `.wal_id` sidecar) and then rebuilds every index against the new ids. Between those two
// durable acts the device holds a POST-COMPACT TABLE UNDER PRE-COMPACT INDEXES, and that
// state SURVIVES: base_spaces rebuilds no index at startup and WAL replay maintains none, so
// whatever the interrupted round left is the answer the engine gives forever. Ordering the
// rebuild ahead of the truncation (test_checkpoint_rebuild_before_truncate) closes a
// different hole and does not shorten this window by one instruction; closing THIS one needs
// a durable fact -- "these indexes name pre-compact rows and have not been rebuilt", written
// before the compaction, cleared only after the rebuild's force_flush lands, and READ BY
// BOOTSTRAP.
//
// HOW THE WINDOW IS ENTERED WITHOUT A DEBUGGER. The rebuild's last leg is
// manager_index_t::repopulate_table -> index_agent_contract::clear, and for the bitcask
// (index_type::hashed) backend clear() begins with collect_segments(), a std::filesystem
// listing of the index directory that is also clear()'s ONE early return -- a directory it
// cannot list leaves the store exactly as it was. Stripping the read bit off that directory
// for the length of one CHECKPOINT therefore produces the exact durable state a kill -9 in
// the window leaves: compaction committed, index untouched and still naming pre-compact rows.
// The statement also fails, which is correct and beside the point; the point is the directory
// that failure leaves behind. It is copied while the engine is up (the crash mechanism of
// test_index_rebuild_crash and test_index_stale_after_compact -- the destructor's CHECKPOINT
// then mutates only the ORIGINAL) and reopened under a fresh engine.
//
// GUARDS, because a case in this family can go green for several wrong reasons:
//   * THE COMPACTION MUST REALLY HAVE HAPPENED -- checkpoint_inner writes an entry's
//     `.wal_id` sidecar only after data_table_t::compact returned true, so a sidecar that
//     moved is proof of a renumbering rather than merely of a round;
//   * THE REBUILD MUST REALLY HAVE REFUSED -- the LIVE engine's index must disagree with its
//     own full scan after the armed round; a round that compacted nothing and a rebuild that
//     quietly succeeded both leave the two agreeing;
//   * THE INJECTION MUST BE REAL -- a suite running as root would list the directory anyway;
//   * THE READ PATH MUST BE THE INDEX -- before the crash, EXPLAIN on the same query text the
//     value assertions use says Index Scan.

using namespace test_helpers;

namespace {

    // > row_group_size (1024) by a wide margin: 3000 rows span three row groups, and deleting
    // the middle third moves every surviving tail row by a full 1000 ids, so a stale index
    // cannot accidentally still name the right row.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001; // inclusive
    constexpr int64_t kDeleteTo = 2000;   // inclusive

    // Fixture roots are qualified by pid (see integration_fixture_path.hpp, and
    // services/index/tests/index_fixture_path.hpp for the storage layer's own):
    // two binaries running at once must not unlink each other's files.
    std::string fixture_root() {
        return integration_fixture_path("test_index_stale_marker_crash").string();
    }

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    void load(otterbrix::wrapper_dispatcher_t* d) {
        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO sdb.t (id, k) VALUES ";
            for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            REQUIRE(exec(d, sql)->is_success());
        }
    }

    // THE FULL SCAN IS THE TRUTH. `SELECT id, k` carries no predicate an index could serve,
    // so this is the table's own answer about which rows exist and what key each one holds.
    std::map<int64_t, int64_t> full_scan_truth(otterbrix::wrapper_dispatcher_t* d) {
        auto cur = exec(d, "SELECT id, k FROM sdb.t;");
        REQUIRE(cur->is_success());
        std::map<int64_t, int64_t> key_to_id;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            const auto id = cur->value(0, r).value<int64_t>();
            const auto k = cur->value(1, r).value<int64_t>();
            key_to_id.emplace(k, id);
        }
        return key_to_id;
    }

    std::vector<int64_t> probe_keys() {
        std::vector<int64_t> probes;
        for (int64_t id = 1; id <= kRows; id += 97) {
            probes.push_back(10 * id);
        }
        probes.push_back(10 * kDeleteFrom);
        probes.push_back(10 * kDeleteTo);
        probes.push_back(10 * kRows);
        return probes;
    }

    // How many probe keys the engine answers DIFFERENTLY from its own full scan. This is the
    // ANSWER-level question and it does not care which access path produced it: an index that
    // was refused (so the predicate falls back to a full scan) agrees, and an index still
    // naming pre-compact rows does not.
    std::size_t disagreements_with_the_full_scan(otterbrix::wrapper_dispatcher_t* d) {
        const auto truth = full_scan_truth(d);
        std::size_t disagreements = 0;
        for (const auto key : probe_keys()) {
            auto cur = exec(d, "SELECT id FROM sdb.t WHERE k = " + std::to_string(key) + ";");
            REQUIRE(cur->is_success());
            const auto expected = truth.find(key);
            if (expected == truth.end()) {
                if (cur->size() != 0) {
                    ++disagreements;
                }
                continue;
            }
            if (cur->size() != 1 || cur->value(0, 0).value<int64_t>() != expected->second) {
                ++disagreements;
            }
        }
        return disagreements;
    }

    // The bitcask index directory is found by CONTENT: it is the one holding a CURRENT
    // marker, which bitcask writes as soon as it opens. sdb.t carries the only index here.
    std::filesystem::path find_bitcask_dir(const std::filesystem::path& disk_root) {
        std::filesystem::path found;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(disk_root, ec), end; it != end; it.increment(ec)) {
            if (ec) {
                break;
            }
            if (it->is_directory() && std::filesystem::exists(it->path() / "CURRENT")) {
                found = it->path();
            }
        }
        return found;
    }

    // RAII around a DIRECTORY's permission bits; the refusal is the filesystem's own.
    struct dir_permissions_guard_t {
        std::filesystem::path directory;
        std::filesystem::perms previous;

        dir_permissions_guard_t(std::filesystem::path dir, std::filesystem::perms wanted)
            : directory(std::move(dir))
            , previous(std::filesystem::status(directory).permissions()) {
            std::error_code ec;
            std::filesystem::permissions(directory, wanted, std::filesystem::perm_options::replace, ec);
        }

        ~dir_permissions_guard_t() {
            std::error_code ec;
            std::filesystem::permissions(directory, previous, std::filesystem::perm_options::replace, ec);
        }

        dir_permissions_guard_t(const dir_permissions_guard_t&) = delete;
        dir_permissions_guard_t& operator=(const dir_permissions_guard_t&) = delete;
    };

    // CHMOD DOES NOT BIND A SUPERUSER, so a suite run as root would turn the injection into
    // one that never happened. Ask the FILESYSTEM whether the bits took, by trying the very
    // operation clear() needs -- creating an entry in the directory -- rather than asking
    // getuid(): the question is about the effect.
    bool directory_really_refuses_writes(const std::filesystem::path& directory) {
        std::error_code ec;
        const auto probe = directory / "otterbrix_write_probe";
        std::filesystem::create_directory(probe, ec);
        if (!ec) {
            std::error_code cleanup;
            std::filesystem::remove(probe, cleanup);
            return false;
        }
        return true;
    }

    // The durable half of a table's checkpoint id, written by checkpoint_inner as
    // `${db}/${namespace_oid}/${table_oid}/table.otbx.wal_id` through tmp+rename.
    uint64_t read_sidecar_wal_id(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        uint64_t value = 0;
        if (!in.is_open()) {
            return 0;
        }
        in.read(reinterpret_cast<char*>(&value), sizeof(value));
        return in ? value : 0;
    }

    // The ONE user table's sidecar. Every system table sits under the fixed system directory
    // oid, so excluding that directory leaves exactly `sdb.t` in a database with one table.
    std::filesystem::path user_table_sidecar(const std::filesystem::path& db_root) {
        const auto system_dir =
            std::to_string(static_cast<unsigned>(services::disk::manager_disk_t::system_dir_oid()));
        std::vector<std::filesystem::path> found;
        for (const auto& ns : std::filesystem::directory_iterator(db_root)) {
            if (!ns.is_directory() || ns.path().filename().string() == system_dir) {
                continue;
            }
            for (const auto& tbl : std::filesystem::directory_iterator(ns.path())) {
                if (!tbl.is_directory()) {
                    continue;
                }
                auto sidecar = tbl.path() / "table.otbx.wal_id";
                if (std::filesystem::exists(sidecar)) {
                    found.push_back(sidecar);
                }
            }
        }
        REQUIRE(found.size() == 1);
        return found.front();
    }

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

} // namespace

// Nothing durable recorded that the round had renumbered the table under its indexes, so the
// restart re-attached the bitcask store it found and wired the index as if it were current.
// Every probe key then answered with whichever row had moved into the physical id the stale
// entry holds: 34 of 34 probes disagreed with the table's own full scan, permanently.
//
// With the durable "renumbered and not rebuilt" fact in place, bootstrap declines to wire
// exactly those indexes and says so at error level; the predicate falls back to a full scan
// and the engine answers what the table holds.
TEST_CASE("integration::cpp::index_stale_marker_crash::a_restart_may_not_wire_an_index_left_naming_precompact_rows") {
    auto config = test_create_config(fixture_root() + "/orig");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Far above anything this case writes: an automatic round DOES compact and DOES rebuild,
    // and one firing mid-case would repair the very state under test.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    const std::filesystem::path crash_dir = fixture_root() + "/crashed";

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE sdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE sdb.t (id bigint, k bigint);")->is_success());
        // USING hash -> the bitcask backend, whose clear() reports a directory it cannot list
        // by value and leaves the store untouched. That is the refusal this case injects.
        REQUIRE(exec(d, "CREATE INDEX t_k ON sdb.t USING hash (k);")->is_success());
        load(d);

        // ROUND ONE, clean. It gives every entry a non-zero prev_checkpoint_wal_id_ and puts
        // the index and the table into agreement, so a disagreement later is round two's.
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        INFO("the middle third goes, so round two has 1000 ids of shift to hand out");
        REQUIRE(exec(d,
                     "DELETE FROM sdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                         " AND id <= " + std::to_string(kDeleteTo) + ";")
                    ->is_success());

        // THE ERASES MUST HAVE LANDED BEFORE THE INJECTION GOES IN. A committed DELETE does
        // not reach the index inside the statement -- commit_deletes queues it and the horizon
        // sweep publishes it once no live snapshot can want the rows -- and that publication
        // is a WRITE whose tail LISTS the index directory. Arming over an unfinished erase
        // would refuse the round's FLUSH step instead of its REBUILD step, and the round would
        // end before it ever compacted anything. The disk-round guard below fails rather than
        // passing for the wrong reason if this wait is ever too short.
        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (services::index::index_deferred_deletes() != 0 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            INFO("the deferred-erase queue has to be empty before the fault goes in");
            REQUIRE(services::index::index_deferred_deletes() == 0);

            // AND THE ERASES MUST HAVE LANDED, NOT MERELY BEEN SENT. The horizon sweep
            // subtracts from that meter where it ERASES the queue entry
            // (manager_index_t::on_horizon_advanced) and only awaits the agents'
            // commit_deletes futures further down, so a zero meter proves the messages are
            // in the agents' mailboxes and nothing more. A sleep_for(500ms) here would be a
            // clock guessing at an event.
            //
            // THE EVENT IS OBSERVABLE: an index scan is a message to the SAME agent
            // addresses commit_deletes went to, a mailbox is FIFO, and
            // bitcask_index_agent_t::commit_deletes carries NO suspension point -- it runs
            // apply_txn_deletes / publish_buckets straight through to co_return on the
            // agent's own thread -- so an answer coming back proves the erase write ahead of
            // it is finished. That is what this read is: the same probe set the pre-round
            // agreement check below uses, ordered here so the injection cannot be armed over
            // an unfinished erase.
            //
            // HONEST ABOUT THE EVIDENCE: shortening that sleep to zero did NOT make the case
            // fail in eight runs (five idle, three under a 24-way CPU load), so this is a
            // shape fix and not a reproduction. What it buys is a barrier that does not
            // depend on a duration being guessed generously enough.
            INFO("a read through the index orders the injection after the erase write");
            REQUIRE(disagreements_with_the_full_scan(d) == 0);
        }

        INFO("and the read path under test is the INDEX, on the same query text used below");
        {
            auto plan = exec(d, "EXPLAIN SELECT id FROM sdb.t WHERE k = 10;");
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("plan for the indexed predicate:\n" << text);
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        INFO("the index and the table agree BEFORE round two");
        REQUIRE(disagreements_with_the_full_scan(d) == 0);

        const auto bitcask_dir = find_bitcask_dir(config.disk.path);
        INFO("a USING hash index must own a bitcask directory: " << bitcask_dir.string());
        REQUIRE_FALSE(bitcask_dir.empty());

        const auto sidecar = user_table_sidecar(config.disk.path);
        const auto sidecar_before = read_sidecar_wal_id(sidecar);

        services::index::reset_index_repopulations();
        services::disk::reset_table_checkpoints();

        std::string round_two_reason;

        {
            // Read + execute, NO WRITE. The rebuild's first act per index is
            // index_agent_contract::clear, and for bitcask that is: list the segments (still
            // allowed), then UNLINK them, CURRENT, the txn log and the applied-offset
            // sidecar, then re-open. Every one of those is a WRITE TO THE DIRECTORY, so the
            // kernel refuses them all and the store is left holding exactly the segments it
            // held before -- i.e. the PRE-COMPACT row ids -- while the table underneath has
            // just been renumbered. That is the durable shape a kill -9 in this window
            // leaves, produced here without a debugger. An already-open descriptor is not
            // reached by chmod, so the round's earlier index FLUSH (step 1, an fsync on open
            // handles) still succeeds and the round gets as far as compacting.
            dir_permissions_guard_t no_writes(bitcask_dir,
                                              std::filesystem::perms::owner_read |
                                                  std::filesystem::perms::owner_exec);
            INFO("the injection has to be real: a suite running as root would write anyway");
            REQUIRE(directory_really_refuses_writes(bitcask_dir));

            auto round_two = exec(d, "CHECKPOINT;");
            round_two_reason = round_two->is_error() ? std::string{round_two->get_error().what.c_str()}
                                                     : std::string{"SUCCESS"};
            INFO("a rebuild that could not clear the store is a refusal, and the statement reports it: "
                 << round_two_reason);
            REQUIRE(!round_two->is_success());
        }

        INFO("NOT VACUOUS (1): a round has to have reached the disk agent at all");
        REQUIRE(services::disk::table_checkpoints() > 0);

        INFO("NOT VACUOUS (2): the sidecar only moves for an entry whose data_table_t::compact "
             "returned true, so this is the proof the rows were really renumbered");
        const auto sidecar_after = read_sidecar_wal_id(sidecar);
        REQUIRE(sidecar_after > sidecar_before);

        INFO("NOT VACUOUS (3): the round reached its rebuild step at all , repopulations = "
             << services::index::index_repopulations());
        REQUIRE(services::index::index_repopulations() > 0);

        INFO("NOT VACUOUS (4): and that rebuild REFUSED, by the reason the store itself gave: "
             << round_two_reason);
        REQUIRE(round_two_reason.find("could not be removed by clear()") != std::string::npos);

        // NOTHING IS ASSERTED ABOUT THE LIVE ENGINE'S ANSWERS HERE, and that is deliberate
        // rather than an omission: clear() ran its unlink pass over a directory it could not
        // write, so the store is left without an open active segment and refuses reads until
        // it is reopened. That is loud and correct, and it is NOT the state under test. The
        // state under test is what the DEVICE holds -- the old segments, naming pre-compact
        // rows, under a table that has just been renumbered -- and the only way to ask about
        // that is to reopen it.

        // kill -9 happens here. Nothing on disk is staged by hand: the fault above lived in
        // this process only, so the copy is simply what the device holds right now.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor's CHECKPOINT runs against the ORIGINAL directory only

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    crash_config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();

        // THE POINT. Nothing rebuilds an index at startup, so an index wired from a store
        // left naming pre-compact rows answers wrong for the life of the database. The
        // restart must decline to wire it instead.
        INFO("the reopened engine must answer what its table holds, key by key");
        CHECK(disagreements_with_the_full_scan(d) == 0);

        // AND THE REASON MUST BE THE DECLINE, not a rebuild nobody performs: the predicate
        // that was an Index Scan before the crash is served by a scan now.
        auto plan = exec(d, "EXPLAIN SELECT id FROM sdb.t WHERE k = 10;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed predicate after the restart:\n" << text);
        CHECK(text.find("Index Scan") == std::string::npos);
    }
    std::filesystem::remove_all(crash_dir);
    std::filesystem::remove_all(fixture_root());
}
