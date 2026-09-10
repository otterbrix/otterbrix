#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <services/wal/wal_page.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

// row_id is the ONLY thing an index answer carries, and the commit path makes it durable in the
// index BEFORE the commit exists at all.
//
// components/physical_plan/operators/operator_commit_transaction.cpp:64-87 awaits
// manager_index_t::commit_inserts first (the send is at :71); the btree agent's commit_inserts
// (services/index/btree_index_agent.cpp:264) publishes and force_flush()es its store on the spot
// (:261). Only THEN, at :89-112, does the WAL commit marker go down -- ":89 The single durable
// commit point and the last step that can still fail" -- and a refusal there is described as
// ":102 A refused fsync here is still a clean abort -- commit_id is stamped nowhere yet". It is
// not a clean abort: nothing walks the index commit back. A refused fsync leaves an index naming
// row ids for a transaction the journal never committed.
//
// The table then comes up short by exactly those rows (checkpoint + replay, and replay has no
// commit marker to replay), and the next INSERT is handed their row ids again:
// components/table/collection.cpp:240 -- state.row_start = total_rows_. Nothing above the index
// re-checks the predicate against the row that comes back
// (components/physical_plan_generator/impl/create_plan_match.cpp:116-146 returns the index_scan
// built at :135 bare, with no operator_match over it), and the stale-index gate cannot see it
// either: compact_epoch_ is process-local and restarts at 0 (components/table/data_table.hpp:169),
// the index re-registers with built_compact_epoch 0 (services/index/manager_index.cpp:601-607), so
// the gate at services/disk/agent_disk.cpp:933-940 compares 0 == 0 and lets the fetch through.
//
// BTREE (the DEFAULT, no USING clause) rather than USING hash. Both are durable per commit
// (bitcask: apply_txn_inserts, services/index/bitcask_index_disk.cpp:1269-1291) and MEASURED: this
// same recipe with `USING hash (k)` fails identically (k = 2010 -> id 999999). btree is the subject
// because it is what CREATE INDEX gives without being asked, and because it is the one with no
// recovery-side commit filter at all -- services/index/manager_index.cpp:663-694 hands the bitcask
// agent a commit_ids set (:669-676) and hands the btree agent nothing (:686-687) -- so
// the reproduction does not lean on how that set is computed.
//
// THE CRASH. A plain copy-the-directory crash does NOT open this window: the table's recovery
// source is checkpoint PLUS replay, and every commit fsyncs the journal
// (operator_commit_transaction.cpp:98 passes wal_sync_mode::FULL, honoured at
// services/wal/wal.cpp:332), so an un-checkpointed tail comes straight back (MEASURED: 400 of 400
// rows). The window is the one commit whose journal fsync REFUSED -- wal.cpp:332, whose own error
// text is "the commit is NOT durable". That refusal is injected here through the WAL's DEV_MODE
// file seam, the statement fails as it should, and what is then dropped from the crash copy is
// exactly the bytes that fsync refused and nothing else. This is a power loss inside that window,
// not an accusation of a lying device.

using namespace test_helpers;

namespace {

    constexpr int64_t kCheckpointed = 200;           // ids 1..200 -> row ids 0..199, durable in table.otbx
    constexpr int64_t kTailFrom = kCheckpointed + 1; // ids 201..400: indexed at commit, never committed
    constexpr int64_t kTailTo = 2 * kCheckpointed;
    constexpr int64_t kVictimKey = 10 * kTailFrom; // 2010: the key the ghost row id 200 answers to
    constexpr int64_t kSurvivorKey = 10;           // a key from the checkpointed prefix

    // Loud and outside every seeded range: a row carrying these can only be the one inserted after the crash.
    constexpr int64_t kIntruderId = 999'999;
    constexpr int64_t kIntruderKey = 777'777;

    std::string fixture_root() { return integration_fixture_path("test_row_id_reuse_crash").string(); }

    // Process-wide while it lives, and starts switched OFF: the plan is armed only after the setup
    // traffic has succeeded. Wraps WAL SEGMENTS only -- the seam is read at wal_page_writer.cpp:28
    // and wal_page_reader.cpp:16 and nowhere else, so the index's own store is never touched by it.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner != nullptr && path.filename().string().rfind("wal_", 0) == 0) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

    using wal_sizes_t = std::map<std::string, std::uintmax_t>;

    // Segment path relative to the WAL root (segments live in per-database subdirectories) -> byte
    // length, taken at a moment when every WAL fsync so far has SUCCEEDED.
    wal_sizes_t durable_wal_sizes(const std::filesystem::path& wal_dir) {
        wal_sizes_t sizes;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(wal_dir, ec), end; !ec && it != end;
             it.increment(ec)) {
            if (it->is_regular_file() && it->path().filename().string().rfind("wal_", 0) == 0) {
                sizes.emplace(std::filesystem::relative(it->path(), wal_dir).string(),
                              std::filesystem::file_size(it->path()));
            }
        }
        return sizes;
    }

    // Drops exactly the WAL bytes the engine itself refused to call durable (wal.cpp:332-341: "the
    // commit is NOT durable") -- the page reached the page cache, fsync said no, the machine died
    // there. Returns the byte count removed, so the caller can prove the fault was not a no-op.
    std::uintmax_t rewind_wal_to_last_fsync(const std::filesystem::path& wal_dir, const wal_sizes_t& durable) {
        std::vector<std::filesystem::path> segments;
        std::error_code ec;
        for (std::filesystem::recursive_directory_iterator it(wal_dir, ec), end; !ec && it != end;
             it.increment(ec)) {
            if (it->is_regular_file() && it->path().filename().string().rfind("wal_", 0) == 0) {
                segments.push_back(it->path());
            }
        }
        // Collected first: the remove() below would invalidate a live recursive iterator.
        std::uintmax_t dropped = 0;
        for (const auto& segment : segments) {
            const auto now = std::filesystem::file_size(segment);
            const auto found = durable.find(std::filesystem::relative(segment, wal_dir).string());
            if (found == durable.end()) {
                // A segment that did not exist at the last good fsync never reached the device at all.
                dropped += now;
                std::filesystem::remove(segment);
                continue;
            }
            if (now > found->second) {
                dropped += now - found->second;
                std::filesystem::resize_file(segment, found->second);
            }
        }
        return dropped;
    }

    std::string rows_sql(int64_t from, int64_t to) {
        std::string sql = "INSERT INTO rdb.t (id, k) VALUES ";
        for (int64_t i = from; i <= to; ++i) {
            if (i != from) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        return sql;
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

    // Asserted only BEFORE the crash, where it establishes that this schema and this predicate do
    // reach the index in the first place -- without it a green case could mean "the predicate went to
    // a full scan", not "there is no defect".
    void require_index_scan(otterbrix::wrapper_dispatcher_t* d, int64_t key) {
        auto plan = exec(d, "EXPLAIN SELECT id, k FROM rdb.t WHERE k = " + std::to_string(key) + ";");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the probed predicate:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    // AFTER the restart the plan shape is REPORTED, never asserted. One legitimate fix for this defect
    // is the one test_index_stale_marker_crash names: a restart that refuses to attach an index it
    // cannot trust, which sends the predicate to a full scan. Asserting "Index Scan" here would turn
    // that fix into a red test. The row this test does assert on -- a row answered for k must carry
    // that k -- is the contract under either plan shape. Today's plan prints "Index Scan on t".
    void report_plan_after_restart(otterbrix::wrapper_dispatcher_t* d, int64_t key) {
        auto plan = exec(d, "EXPLAIN SELECT id, k FROM rdb.t WHERE k = " + std::to_string(key) + ";");
        REQUIRE(plan->is_success());
        INFO("plan for the probed predicate AFTER the restart (reported, not asserted):\n" << plan_text(plan));
    }

    // `SELECT id, k` carries no predicate an index could serve: this is the TABLE's own answer.
    std::map<int64_t, int64_t> full_scan_truth(otterbrix::wrapper_dispatcher_t* d) {
        auto cur = exec(d, "SELECT id, k FROM rdb.t;");
        REQUIRE(cur->is_success());
        std::map<int64_t, int64_t> key_to_id;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            key_to_id.emplace(cur->value(1, r).value<int64_t>(), cur->value(0, r).value<int64_t>());
        }
        return key_to_id;
    }

    struct probe_t {
        // A refusal is a legal answer under the chosen policy: an index that cannot be trusted says
        // so, it does not answer. It is NOT an assertion-free pass -- every case below still pins a
        // key that must answer, so a table-wide refusal cannot make this file vacuously green.
        bool refused{false};
        std::size_t rows{0};
        int64_t id{0};
        int64_t k{0};
    };

    // Reads BOTH columns back. A count alone cannot tell "no such row any more" (correct, and the
    // honest answer here) from "here is somebody else's row" (the defect).
    probe_t probe_by_key(otterbrix::wrapper_dispatcher_t* d, int64_t key) {
        auto cur = exec(d, "SELECT id, k FROM rdb.t WHERE k = " + std::to_string(key) + ";");
        probe_t out;
        if (cur->is_error()) {
            INFO("probe refused: " << cur->get_error().what.c_str());
            out.refused = true;
            return out;
        }
        out.rows = cur->size();
        if (out.rows > 0) {
            out.id = cur->value(0, 0).value<int64_t>();
            out.k = cur->value(1, 0).value<int64_t>();
        }
        return out;
    }

    configuration::config crash_proof_config(const std::string& path) {
        auto config = test_create_config(path);
        config.log.level = log_t::level::off;
        // The tail must stay out of table.otbx until the crash; an auto round would absorb it.
        config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;
        return config;
    }

    // CREATE + the DEFAULT (btree) index + ids 1..200 + CHECKPOINT. Leaves the engine open.
    void build_durable_prefix(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE rdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE rdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX t_k ON rdb.t (k);")->is_success());
        for (int64_t start = 1; start <= kCheckpointed; start += 100) {
            REQUIRE(exec(d, rows_sql(start, std::min(start + 99, kCheckpointed)))->is_success());
        }
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        INFO("the read path under test has to be the INDEX");
        require_index_scan(d, kVictimKey);
    }

    // The whole prologue. Leaves `crash_dir` holding a crashed copy whose btree index names row
    // ids 200..399 for keys 2010..4000 and whose table holds only rows 1..200.
    void stage_a_ghost_index(const configuration::config& config, const std::filesystem::path& crash_dir) {
        wal_sizes_t durable;
        {
            wal_fault_scope_t fault;
            test_spaces space(config);
            auto* d = space.dispatcher();
            build_durable_prefix(d);

            // Everything the WAL holds up to here was fsync'd and answered SUCCESS.
            durable = durable_wal_sizes(config.wal.path);
            REQUIRE_FALSE(durable.empty());

            // ARM: the next fsync of a WAL segment is refused. Under wal_sync_mode::FULL that is
            // the commit marker's own flush_and_sync (services/wal/wal.cpp:332).
            const auto syncs_before = fault.plan.syncs_seen;
            fault.plan.fail_syncs_from = fault.plan.syncs_seen + 1;

            auto refused = exec(d, rows_sql(kTailFrom, kTailTo));

            INFO("NOT VACUOUS (1): the transaction has to be REFUSED, or there is no window at all");
            REQUIRE(refused->is_error());
            INFO("NOT VACUOUS (2): and refused by the injected fsync, not by something else: syncs_seen "
                 << fault.plan.syncs_seen);
            REQUIRE(fault.plan.syncs_seen > syncs_before);

            fault.plan.fail_syncs_from = 0; // disarm, so the shutdown below is an ordinary one

            // kill -9 HERE: the btree store already force_flush()ed the tail's keys.
            copy_dir_as_crash(config.main_path, crash_dir);
        } // the destructor's CHECKPOINT runs against the ORIGINAL directory only

        INFO("NOT VACUOUS (3): the crash has to remove something, or this is just a reopen");
        REQUIRE(rewind_wal_to_last_fsync(crash_dir / "wal", durable) > 0);
    }

    // Every crash case starts from here: the table is short, and the new row lands on row id 200.
    void reopen_and_insert_the_intruder(otterbrix::wrapper_dispatcher_t* d) {
        INFO("NOT VACUOUS (4): the TABLE must come up without the refused transaction's rows");
        const auto truth = full_scan_truth(d);
        REQUIRE(truth.size() == static_cast<std::size_t>(kCheckpointed));
        REQUIRE(truth.find(kVictimKey) == truth.end());

        REQUIRE(exec(d,
                     "INSERT INTO rdb.t (id, k) VALUES (" + std::to_string(kIntruderId) + ", " +
                         std::to_string(kIntruderKey) + ");")
                    ->is_success());
    }

    // Shared tail assertion: the durable prefix is still exact. It holds today, so a red case above
    // is about the ghost entries and not about indexes being broken across a restart.
    void require_the_durable_prefix_is_intact(otterbrix::wrapper_dispatcher_t* d) {
        const auto survivor = probe_by_key(d, kSurvivorKey);
        INFO("the checkpointed prefix must still be exactly right");
        REQUIRE_FALSE(survivor.refused);
        REQUIRE(survivor.rows == 1);
        CHECK(survivor.id == 1);
        CHECK(survivor.k == kSurvivorKey);
    }

} // namespace

// RED: k = 2010 is answered with the row inserted after the crash (id 999999, k 777777).
TEST_CASE("integration::cpp::row_id_reuse_crash::a_reused_row_id_may_not_answer_to_the_old_key") {
    auto config = crash_proof_config(fixture_root() + "/select");
    test_clear_directory(config);
    const std::filesystem::path crash_dir = fixture_root() + "/select_crashed";
    stage_a_ghost_index(config, crash_dir);

    auto crash_config = crash_proof_config(crash_dir.string());
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        reopen_and_insert_the_intruder(d);

        report_plan_after_restart(d, kVictimKey);

        // THE ASSERTION. Either that key is gone -- the honest answer, since the full scan above
        // says the row is not there -- or the row handed back is its own. A row that does not carry
        // the key that was asked for is the defect.
        const auto answer = probe_by_key(d, kVictimKey);
        INFO("k = " << kVictimKey << " answered refused=" << answer.refused << " rows=" << answer.rows
                    << " id=" << answer.id << " k=" << answer.k);
        if (!answer.refused && answer.rows != 0) {
            CHECK(answer.rows == 1);
            CHECK(answer.k == kVictimKey);
            CHECK(answer.id == kTailFrom);
        }

        require_the_durable_prefix_is_intact(d);

        const auto intruder = probe_by_key(d, kIntruderKey);
        INFO("the new row must be reachable under its OWN key -- the refusal above may not spread to a "
             "key whose row is really there");
        REQUIRE_FALSE(intruder.refused);
        REQUIRE(intruder.rows == 1);
        CHECK(intruder.id == kIntruderId);
        CHECK(intruder.k == kIntruderKey);
    }
    std::filesystem::remove_all(crash_dir);
}

// RED: the DELETE tombstones the row inserted after the crash.
TEST_CASE("integration::cpp::row_id_reuse_crash::a_delete_by_the_old_key_may_not_take_the_new_row") {
    auto config = crash_proof_config(fixture_root() + "/del");
    test_clear_directory(config);
    const std::filesystem::path crash_dir = fixture_root() + "/del_crashed";
    stage_a_ghost_index(config, crash_dir);

    auto crash_config = crash_proof_config(crash_dir.string());
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        reopen_and_insert_the_intruder(d);

        // create_plan_delete.cpp:134 lowers the DELETE over the same bare index scan, so whatever
        // row ids the index names are the rows that get tombstoned. Refusing the statement is the
        // other legal answer; what may never happen is that it takes somebody else's row.
        auto deleted = exec(d, "DELETE FROM rdb.t WHERE k = " + std::to_string(kVictimKey) + ";");
        INFO("the DELETE " << (deleted->is_error() ? deleted->get_error().what.c_str() : "was accepted"));

        const auto intruder = probe_by_key(d, kIntruderKey);
        INFO("after DELETE WHERE k = " << kVictimKey << ", the new row answered rows=" << intruder.rows
                                       << " id=" << intruder.id << " k=" << intruder.k);
        REQUIRE_FALSE(intruder.refused);
        CHECK(intruder.rows == 1);
        if (intruder.rows == 1) {
            CHECK(intruder.id == kIntruderId);
            CHECK(intruder.k == kIntruderKey);
        }

        require_the_durable_prefix_is_intact(d);
    }
    std::filesystem::remove_all(crash_dir);
}

// RED: the UPDATE rewrites the row inserted after the crash. Its own case, not a tail of the DELETE
// one -- run after a DELETE that already removed the row, this would prove nothing.
TEST_CASE("integration::cpp::row_id_reuse_crash::an_update_by_the_old_key_may_not_rewrite_the_new_row") {
    auto config = crash_proof_config(fixture_root() + "/upd");
    test_clear_directory(config);
    const std::filesystem::path crash_dir = fixture_root() + "/upd_crashed";
    stage_a_ghost_index(config, crash_dir);

    auto crash_config = crash_proof_config(crash_dir.string());
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        reopen_and_insert_the_intruder(d);

        // create_plan_update.cpp:54 shares that lowering; refusing is likewise legal, rewriting a
        // stranger's row is not.
        auto updated = exec(d, "UPDATE rdb.t SET id = -1 WHERE k = " + std::to_string(kVictimKey) + ";");
        INFO("the UPDATE " << (updated->is_error() ? updated->get_error().what.c_str() : "was accepted"));

        const auto intruder = probe_by_key(d, kIntruderKey);
        INFO("after UPDATE ... WHERE k = " << kVictimKey << ", the new row answered rows=" << intruder.rows
                                           << " id=" << intruder.id << " k=" << intruder.k);
        REQUIRE_FALSE(intruder.refused);
        CHECK(intruder.rows == 1);
        if (intruder.rows == 1) {
            CHECK(intruder.id == kIntruderId);
            CHECK(intruder.k == kIntruderKey);
        }

        require_the_durable_prefix_is_intact(d);
    }
    std::filesystem::remove_all(crash_dir);
}

// CONTROL. The same recipe end to end -- same table, same btree index, same checkpoint, same crash
// copy, same reopen, same INSERT of the same intruder, same probes, same DELETE -- with ONE
// difference: the tail transaction is allowed to commit, so the journal keeps it and replay puts
// its rows back. This must stay GREEN. If it ever goes red, the three cases above are not about a
// commit the index kept and the journal did not, and their accusation is void.
TEST_CASE("integration::cpp::row_id_reuse_crash::control_a_committed_tail_survives_the_same_crash_intact") {
    auto config = crash_proof_config(fixture_root() + "/control");
    test_clear_directory(config);
    const std::filesystem::path crash_dir = fixture_root() + "/control_crashed";

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        build_durable_prefix(d);

        // The only difference: no fault, so the commit marker lands and is fsync'd.
        REQUIRE(exec(d, rows_sql(kTailFrom, kTailTo))->is_success());

        copy_dir_as_crash(config.main_path, crash_dir);
    }
    // and no rewind: nothing here was ever declared non-durable.

    auto crash_config = crash_proof_config(crash_dir.string());
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();

        INFO("the committed tail has to come back, or this control is not the same recipe");
        REQUIRE(full_scan_truth(d).size() == static_cast<std::size_t>(kTailTo));

        REQUIRE(exec(d,
                     "INSERT INTO rdb.t (id, k) VALUES (" + std::to_string(kIntruderId) + ", " +
                         std::to_string(kIntruderKey) + ");")
                    ->is_success());
        report_plan_after_restart(d, kVictimKey);

        const auto answer = probe_by_key(d, kVictimKey);
        REQUIRE(answer.rows == 1);
        CHECK(answer.id == kTailFrom);
        CHECK(answer.k == kVictimKey);

        REQUIRE(exec(d, "DELETE FROM rdb.t WHERE k = " + std::to_string(kVictimKey) + ";")->is_success());
        const auto intruder = probe_by_key(d, kIntruderKey);
        REQUIRE(intruder.rows == 1);
        CHECK(intruder.id == kIntruderId);
        CHECK(intruder.k == kIntruderKey);

        require_the_durable_prefix_is_intact(d);
    }
    std::filesystem::remove_all(crash_dir);
    std::filesystem::remove_all(fixture_root());
}
