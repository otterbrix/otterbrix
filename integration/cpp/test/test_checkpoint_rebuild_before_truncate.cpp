#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/wal_page.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

// WHAT A CHECKPOINT MAY NOT DO BETWEEN COMPACTING A TABLE AND REBUILDING ITS INDEXES.
//
// A CHECKPOINT round is four durable acts in a fixed relationship:
//   * agent_disk_t::checkpoint_inner compacts each entry -- data_table_t::compact rebuilds
//     the table at row id 0 and hands every surviving row a NEW physical id -- and commits
//     that by writing the .otbx header and the `.wal_id` sidecar beside it;
//   * every index of that table stores the OLD physical ids and is, from that instant,
//     silently wrong (an id naming no row group is dropped by collection_t::fetch, an id that
//     now belongs to a different survivor is gathered as if it were the match);
//   * repopulate_indexes_after_compaction is what makes the indexes name the new ids again,
//     and btree_index_agent_t::publish_buckets force_flush()es the result, so the rebuild is
//     DURABLE when the driver returns;
//   * truncate_before drops the WAL segments the round made redundant.
//
// The third and fourth are the ones this file is about. THE TRUNCATION IS THE ROUND'S POINT OF
// NO RETURN: it is the only step that destroys something, and there is nothing that puts an
// index back afterwards. base_spaces does not rebuild indexes at startup -- the pass that once
// did was removed as a proven no-op and manager_index.hpp records that "nothing rewrites the
// on-disk index after a compact" -- and WAL replay maintains no index either (the bypass note
// on the replay callable says so in as many words). So a round that trims the journal while
// its indexes still name pre-compact rows has already produced the final state: permanent,
// silent, wrong answers.
//
// The WAL auto-checkpoint (manager_wal_replicate_t::run_auto_checkpoint) has the rebuild at
// step (c2), BEFORE the truncate at (d). The CHECKPOINT statement had it AFTER. The case below
// is written against the difference and nothing else.
//
// HOW THE WINDOW IS ENTERED WITHOUT A DEBUGGER. The truncate step reads each candidate segment
// through wal_page_reader_t, so the T3 WAL seam (dev_set_wal_file_interposer, the same one
// test_create_index_catchup_refusal uses) can make that read refuse. A refused truncate is a
// refused statement, and in the old order the operator returned on it -- so the rebuild never
// ran, which is EXACTLY the state a kill -9 one instruction after the truncation leaves:
// post-compact table, pre-compact index. The case then also crashes for real, by copying the
// live directory and reopening the copy, because the whole claim is that the state SURVIVES a
// restart.
//
// TWO GUARDS, both because this branch has gone green for the wrong reason before:
//   * THE COMPACTION MUST REALLY HAVE HAPPENED. checkpoint_inner writes the `.wal_id` sidecar
//     of an entry that needed a checkpoint ONLY after data_table_t::compact returned true --
//     every other outcome (`storage_degraded`, an open scan cursor, the MVCC gate, a failed
//     checkpoint) takes a `continue` that skips the sidecar. So a sidecar that moved is proof
//     of a renumbering, not merely of a round.
//   * THE LOOKUP MUST REALLY GO THROUGH THE INDEX. EXPLAIN on the SAME query text the value
//     assertions use must say Index Scan; a planner that stopped routing `WHERE k = ...` to
//     the index would satisfy every row assertion here over a full scan.

using namespace test_helpers;

namespace {

    // > row_group_size (1024) by a wide margin: 3000 rows span three row groups, and deleting
    // the middle third moves every surviving tail row by a full 1000 ids, so a stale index
    // cannot accidentally still name the right row.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001; // inclusive
    constexpr int64_t kDeleteTo = 2000;   // inclusive

    // Small enough that the load below rolls the journal over several times. truncate_before
    // never touches the segment the writer currently holds, so a single-segment journal would
    // give the fault below nothing to refuse and the case would test nothing.
    constexpr std::size_t kSegmentBytes = 64 * 1024;

    // Process-wide seam, scoped by this object, narrowed to WAL segment files by path and
    // armed only for the one statement that must meet it. Returning nullptr models a segment
    // that WILL NOT OPEN, which is what core::filesystem::open_file itself answers on failure.
    class wal_open_refusal_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_open_refusal_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_open_refusal_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_open_refusal_t(const wal_open_refusal_t&) = delete;
        wal_open_refusal_t& operator=(const wal_open_refusal_t&) = delete;

        bool armed{false};
        uint64_t refusals{0};

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (armed && path.filename().string().compare(0, 4, "wal_") == 0) {
                ++refusals;
                return nullptr;
            }
            return inner;
        }
    };

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    void load(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO " + db + ".t (id, k) VALUES ";
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

    // THE FULL SCAN IS THE TRUTH. `SELECT id, k` carries no predicate an index could serve, so
    // this is the table's own answer about which rows exist and what key each one holds.
    std::map<int64_t, int64_t> full_scan_truth(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        auto cur = exec(d, "SELECT id, k FROM " + db + ".t;");
        REQUIRE(cur->is_success());
        std::map<int64_t, int64_t> key_to_id;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            const auto id = cur->value(0, r).value<int64_t>();
            const auto k = cur->value(1, r).value<int64_t>();
            key_to_id.emplace(k, id);
        }
        return key_to_id;
    }

    // The index must answer EXACTLY what the full scan says, key by key -- the right row for a
    // key that survived, no row at all for one that did not.
    void index_must_agree_with_the_full_scan(otterbrix::wrapper_dispatcher_t* d, const std::string& db) {
        const auto truth = full_scan_truth(d, db);
        REQUIRE(truth.size() == static_cast<std::size_t>(kRows - (kDeleteTo - kDeleteFrom + 1)));

        {
            auto plan = exec(d, "EXPLAIN SELECT id FROM " + db + ".t WHERE k = 10;");
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("plan for the indexed predicate:\n" << text);
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }

        // A spread across all three row groups, including the tail rows a compaction moves by a
        // full 1000 ids and the deleted middle that must stay absent through the index just as
        // it is absent from the scan.
        std::vector<int64_t> probes;
        for (int64_t id = 1; id <= kRows; id += 97) {
            probes.push_back(10 * id);
        }
        probes.push_back(10 * kDeleteFrom);
        probes.push_back(10 * kDeleteTo);
        probes.push_back(10 * kRows);

        for (const auto key : probes) {
            auto cur = exec(d, "SELECT id FROM " + db + ".t WHERE k = " + std::to_string(key) + ";");
            REQUIRE(cur->is_success());
            const auto expected = truth.find(key);
            INFO("indexed lookup for k = " << key);
            if (expected == truth.end()) {
                CHECK(cur->size() == 0);
                continue;
            }
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == expected->second);
        }
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
    // oid, so excluding that directory leaves exactly `<db>.t` in a database with one table.
    // WAL segments are regular files in these same directories and are skipped by the
    // directories-only descent.
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

// RED before the fix, on both halves.
//
// operator_checkpoint_t ran truncate_before at step 4 and the index rebuild at step 5. The
// refused truncate below therefore returned the statement one step BEFORE the rebuild, leaving
// a table whose rows had all been renumbered and indexes that still named the old numbers:
// `WHERE k = 30000` answered with whichever row moved into the physical id the stale entry
// holds, and the crash copy answered the same after a restart, forever.
//
// With the rebuild moved ahead of the truncation the same refusal still fails the statement --
// a WAL segment that cannot be read is a real refusal and must be reported -- but it fails it
// AFTER the indexes have been made durable against the table they now describe.
TEST_CASE("integration::cpp::checkpoint_rebuild_before_truncate::a_refused_truncate_may_not_cost_the_index_rebuild") {
    auto config = test_create_config(integration_fixture_path("test_checkpoint_rebuild_before_truncate/orig"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    config.wal.max_segment_size = kSegmentBytes;
    // Far above anything this case writes: an automatic round DOES compact and DOES rebuild,
    // and one firing mid-case would repair the very state under test.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    const std::filesystem::path crash_dir =
        integration_fixture_path("test_checkpoint_rebuild_before_truncate/crashed");

    wal_open_refusal_t fault;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE tdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE tdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON tdb.t (k);")->is_success());
        load(d, "tdb");

        // ROUND ONE, clean. It is what gives every entry a non-zero prev_checkpoint_wal_id_,
        // and checkpoint_all reports min(prev) -- so without this round the second one would
        // report 0 and skip truncate_before entirely, and the fault below would meet nothing.
        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        INFO("the middle third goes, so round two has 1000 ids of shift to hand out");
        REQUIRE(exec(d,
                     "DELETE FROM tdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                         " AND id <= " + std::to_string(kDeleteTo) + ";")
                    ->is_success());

        INFO("the index and the table agree BEFORE round two, so a disagreement after it is round two's");
        index_must_agree_with_the_full_scan(d, "tdb");

        const auto sidecar = user_table_sidecar(config.disk.path);
        const auto sidecar_before = read_sidecar_wal_id(sidecar);

        services::index::reset_index_repopulations();
        services::disk::reset_table_checkpoints();

        // ROUND TWO, with the journal unreadable at the truncate step.
        fault.armed = true;
        auto round_two = exec(d, "CHECKPOINT;");
        fault.armed = false;

        INFO("a segment that will not open is a refusal, and the statement must report it");
        CHECK(!round_two->is_success());
        REQUIRE(fault.refusals > 0);

        INFO("NOT VACUOUS (1): a round has to have reached the disk agent at all");
        REQUIRE(services::disk::table_checkpoints() > 0);

        INFO("NOT VACUOUS (2): the sidecar only moves for an entry whose data_table_t::compact "
             "returned true, so this is the proof the rows were really renumbered");
        const auto sidecar_after = read_sidecar_wal_id(sidecar);
        REQUIRE(sidecar_after > sidecar_before);

        INFO("the round that renumbered owes the rebuild, and owes it BEFORE the step that can refuse");
        CHECK(services::index::index_repopulations() > 0);

        INFO("and the answer is the whole point: the index must say exactly what the full scan says");
        index_must_agree_with_the_full_scan(d, "tdb");

        // kill -9 happens here. Nothing on disk is staged by hand: the fault above lived in
        // this process only, so the copy is simply what the device holds right now.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor's CHECKPOINT runs against the ORIGINAL directory only

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    crash_config.wal.max_segment_size = kSegmentBytes;
    crash_config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();

        INFO("nothing rebuilds an index at startup -- bootstrap_index_sync re-attaches the store it "
             "finds and WAL replay maintains no index -- so whatever the round left durable is the "
             "answer this engine will give forever");
        index_must_agree_with_the_full_scan(d, "tdb");
    }
    std::filesystem::remove_all(crash_dir);
}
