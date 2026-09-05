#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_page.hpp>

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

// WHAT THE WAL AUTO-CHECKPOINT MAY NOT DO WHEN ITS INDEX REBUILD REFUSES.
//
// manager_wal_replicate_t::run_auto_checkpoint is the self-orchestrated analogue of the
// CHECKPOINT statement: (a) flush the indexes, (c) checkpoint_all the storage -- which
// COMPACTS every entry the MVCC gate lets it and hands every surviving row a NEW physical
// id -- (c2) rebuild the indexes against those new ids, (d) truncate the WAL below the
// boundary the checkpoint reported.
//
// Step (d) is the only step of the round that DESTROYS anything, and step (c2) is the
// round's last chance to fail recoverably. The ORDER was already right here -- (c2) sits
// ahead of (d) -- but the ERROR HANDLING was not: a refused rebuild was logged and the round
// fell through into the truncation. Both outcomes leave a stale index (nothing rebuilds one
// at startup: base_spaces does not, and WAL replay maintains none), but only the truncation
// ALSO unlinks journal segments. A stale index is something a later round repairs; a stale
// index whose journal has been trimmed underneath it is not.
//
// THERE IS NO STATEMENT ABOVE THIS FRAME, so "fail the query" is not on the menu -- the
// path's own comment says so. The available answer is the one step (a) already takes for a
// refused index flush: LOG IT, REBASE THE BYTE WINDOW, RELEASE THE DEDUP GUARD AND ABANDON
// THE ROUND. Loud, not fatal, and REPEATABLE -- which this case pins too, because an
// abandoned round that could never be retried would just be a slower way to lose.
//
// HOW THE REBUILD IS MADE TO REFUSE WITHOUT A DEBUGGER. The rebuild's last leg is
// manager_index_t::repopulate_table -> index_agent_contract::clear, and for the bitcask
// (index_type::hashed) backend clear() begins with collect_segments(), a plain
// std::filesystem listing of the index directory. That listing is also the ONE early return
// in clear(): a directory it cannot list leaves the store exactly as it was -- handles open,
// keydir intact, segments intact -- and reports the refusal by value. So stripping the read
// bit off the index directory for the length of one automatic round produces a genuinely
// refused rebuild AND a healthy store to retry it with, which is exactly what the
// "abandoned rounds are repeatable" half needs. chmod does not reach an already-open
// descriptor, so nothing that is merely writing or fsyncing notices it.
//
// HOW THE TRUNCATION IS DETECTED, and why not by counting files. Whether a given round
// DESTROYS a segment depends on the boundary it was handed, and that boundary is
// min(prev_checkpoint_wal_id) over every table -- so a round in which the MVCC gate deferred
// one busy table reports the PREVIOUS boundary and removes nothing, however faithfully it
// ran step (d). Measured that way the case went green over an unfixed build about half the
// time. What does not depend on the boundary is that step (d) READS: truncate_before opens
// every candidate segment through wal_page_reader_t to find the highest wal id in it, before
// it decides anything. The writer, meanwhile, only ever opens the segment it is appending to,
// and a rotation opens a HIGHER ordinal than any seen so far. So "a WAL segment below the
// high-water mark was opened" separates the truncation step from every other WAL file access
// in a running engine, and it is the same T3 seam (dev_set_wal_file_interposer) the
// neighbouring truncate cases use -- here counting rather than refusing, so the round stays
// free to do whatever it was going to do and the file-level check below stays honest.
//
// GUARDS, because a case in this family can go green for several wrong reasons:
//   * THE ROUND MUST HAVE RUN AND FINISHED -- a disk agent counted it, and the WAL manager
//     counted its exit. Reading the journal at "started", or on a timer, reads a round that
//     has not reached its destructive step yet;
//   * THE ROUND MUST HAVE REACHED ITS REBUILD -- index_repopulations();
//   * AND THAT REBUILD MUST HAVE REFUSED, over a table that really was renumbered -- the
//     index must DISAGREE with the full scan afterwards. That single fact covers both halves:
//     a round that compacted nothing, and a rebuild that quietly succeeded, both leave the
//     two agreeing;
//   * THE LOOKUP MUST GO THROUGH THE INDEX -- EXPLAIN on the same query text the value
//     assertions use must say Index Scan;
//   * THE DETECTOR MUST BE ABLE TO FIRE AT ALL -- the unfaulted round at the end must trip
//     the very counter the armed round is required to leave at zero.
//
// The WAL is churned through a SECOND, UNINDEXED table, so no churn statement opens a
// bitcask file and the only listing of the index directory inside the armed window is the
// rebuild's own.

using namespace test_helpers;

namespace {

    // > row_group_size (1024) by a wide margin: 3000 rows span three row groups.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001; // inclusive
    constexpr int64_t kDeleteTo = 2000;   // inclusive

    // Small enough that the churn below rolls the journal over many times: the truncation
    // step has to have superseded segments to read, and the writer's own segment is never
    // one of them.
    constexpr std::size_t kSegmentBytes = 16 * 1024;

    // Reached by a bounded churn loop, and far enough above the setup that the setup itself
    // does not trip it.
    constexpr std::uintmax_t kAutoCheckpointBytes = 2ull * 1024ull * 1024ull;

    // One churn statement's worth of WAL: 100 rows of a 300-character payload.
    constexpr int kChurnRowsPerStatement = 100;
    constexpr std::size_t kChurnPayloadChars = 300;

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
            std::string sql = "INSERT INTO adb.t (id, k) VALUES ";
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

    // WAL BYTES WITHOUT INDEX BYTES. adb.pad carries no index, so a statement against it
    // grows the journal and opens no bitcask file -- which is what keeps the armed window
    // free of any listing but the rebuild's.
    void churn_once(otterbrix::wrapper_dispatcher_t* d, int64_t& next_id) {
        const std::string payload(kChurnPayloadChars, 'x');
        std::string sql = "INSERT INTO adb.pad (id, payload) VALUES ";
        for (int i = 0; i < kChurnRowsPerStatement; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(next_id) + ", '" + payload + "')";
            ++next_id;
        }
        sql += ";";
        REQUIRE(exec(d, sql)->is_success());
    }

    // THE FULL SCAN IS THE TRUTH. `SELECT id, k` carries no predicate an index could serve.
    std::map<int64_t, int64_t> full_scan_truth(otterbrix::wrapper_dispatcher_t* d) {
        auto cur = exec(d, "SELECT id, k FROM adb.t;");
        REQUIRE(cur->is_success());
        std::map<int64_t, int64_t> key_to_id;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            const auto id = cur->value(0, r).value<int64_t>();
            const auto k = cur->value(1, r).value<int64_t>();
            key_to_id.emplace(k, id);
        }
        return key_to_id;
    }

    void the_lookup_must_go_through_the_index(otterbrix::wrapper_dispatcher_t* d) {
        auto plan = exec(d, "EXPLAIN SELECT id FROM adb.t WHERE k = 10;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed predicate:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    // How many probe keys the index answers DIFFERENTLY from the full scan. Zero is an index
    // that names the rows its table actually holds; anything else is one left naming
    // pre-compact ids.
    std::size_t index_disagreements_with_the_full_scan(otterbrix::wrapper_dispatcher_t* d) {
        const auto truth = full_scan_truth(d);
        the_lookup_must_go_through_the_index(d);

        std::vector<int64_t> probes;
        for (int64_t id = 1; id <= kRows; id += 97) {
            probes.push_back(10 * id);
        }
        probes.push_back(10 * kDeleteFrom);
        probes.push_back(10 * kDeleteTo);
        probes.push_back(10 * kRows);

        std::size_t disagreements = 0;
        for (const auto key : probes) {
            auto cur = exec(d, "SELECT id FROM adb.t WHERE k = " + std::to_string(key) + ";");
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

    // `wal_<database>_<ordinal>` — the ordinal is what orders the journal, and the writer only
    // ever moves forward through it. Anything that is not a segment answers nothing.
    std::optional<uint64_t> segment_ordinal(const std::filesystem::path& path) {
        const auto name = path.filename().string();
        if (name.size() < 4 || name.compare(0, 4, "wal_") != 0) {
            return std::nullopt;
        }
        const auto last_underscore = name.find_last_of('_');
        if (last_underscore == std::string::npos || last_underscore + 1 >= name.size()) {
            return std::nullopt;
        }
        uint64_t ordinal = 0;
        const char* first = name.data() + last_underscore + 1;
        const char* last = name.data() + name.size();
        if (std::from_chars(first, last, ordinal).ec != std::errc{}) {
            return std::nullopt;
        }
        return ordinal;
    }

    // WAL segments are regular files sitting DIRECTLY under `${wal}/${database}/`; the table
    // tree and the index directories share that root but keep their files at least one
    // directory deeper, so this flat descent cannot pick one up.
    std::set<std::filesystem::path> wal_segments(const std::filesystem::path& wal_root) {
        std::set<std::filesystem::path> segments;
        std::error_code ec;
        for (std::filesystem::directory_iterator db(wal_root, ec), end; db != end; db.increment(ec)) {
            if (ec) {
                break;
            }
            if (!db->is_directory(ec) || ec) {
                ec.clear();
                continue;
            }
            std::error_code inner_ec;
            for (std::filesystem::directory_iterator f(db->path(), inner_ec), fend; f != fend;
                 f.increment(inner_ec)) {
                if (inner_ec) {
                    break;
                }
                if (!f->is_regular_file(inner_ec) || inner_ec) {
                    inner_ec.clear();
                    continue;
                }
                if (segment_ordinal(f->path()).has_value()) {
                    segments.insert(f->path());
                }
            }
        }
        return segments;
    }

    uint64_t highest_segment_ordinal(const std::filesystem::path& wal_root) {
        uint64_t highest = 0;
        for (const auto& segment : wal_segments(wal_root)) {
            highest = std::max(highest, segment_ordinal(segment).value_or(0));
        }
        return highest;
    }

    std::vector<std::filesystem::path> missing_from_disk(const std::set<std::filesystem::path>& expected) {
        std::vector<std::filesystem::path> gone;
        for (const auto& path : expected) {
            if (!std::filesystem::exists(path)) {
                gone.push_back(path);
            }
        }
        return gone;
    }

    // THE TRUNCATION STEP, WITNESSED WITHOUT BEING INTERFERED WITH.
    //
    // Every handle the WAL opens comes through this seam. While armed, an open of a segment
    // BELOW the high-water ordinal is counted: the writer never goes backwards (a rotation
    // opens a higher ordinal, which raises the mark instead), so in a running engine the only
    // thing that reads a superseded segment is truncate_before deciding whether to unlink it.
    // The handle is always passed through -- refusing would stop the round from removing
    // anything, and then "the segments are still there" would be true of the unfixed build
    // too.
    class superseded_segment_watch_t final : public services::wal::wal_file_interposer_t {
    public:
        superseded_segment_watch_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~superseded_segment_watch_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        superseded_segment_watch_t(const superseded_segment_watch_t&) = delete;
        superseded_segment_watch_t& operator=(const superseded_segment_watch_t&) = delete;

        // Seeded from the directory at arm time: starting the mark at zero would let the
        // truncation's own first read raise it and go uncounted.
        void arm(uint64_t high_water_ordinal) noexcept {
            high_water_.store(high_water_ordinal, std::memory_order_relaxed);
            superseded_reads_.store(0, std::memory_order_relaxed);
            armed_.store(true, std::memory_order_release);
        }
        void disarm() noexcept { armed_.store(false, std::memory_order_release); }
        uint64_t superseded_reads() const noexcept { return superseded_reads_.load(std::memory_order_relaxed); }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (!armed_.load(std::memory_order_acquire)) {
                return inner;
            }
            const auto ordinal = segment_ordinal(path);
            if (!ordinal.has_value()) {
                return inner;
            }
            if (*ordinal >= high_water_.load(std::memory_order_relaxed)) {
                high_water_.store(*ordinal, std::memory_order_relaxed);
                return inner;
            }
            superseded_reads_.fetch_add(1, std::memory_order_relaxed);
            return inner;
        }

    private:
        std::atomic<bool> armed_{false};
        std::atomic<uint64_t> high_water_{0};
        std::atomic<uint64_t> superseded_reads_{0};
    };

    // The bitcask index directory is found by CONTENT: it is the one holding a CURRENT
    // marker, which bitcask writes as soon as it opens. adb.t carries the only index in this
    // database, so there is exactly one.
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

    // RAII around a DIRECTORY's permission bits. The refusal is the filesystem's own -- a
    // listing the kernel will not produce -- which is what collect_segments() meets and what
    // clear() turns into its one clean early return. The bits go back in the destructor so a
    // case that trips an assertion still leaves /tmp cleanable.
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
    // one that never happened. Ask the filesystem whether the bits took, rather than asking
    // getuid(): the question is about the effect.
    bool directory_really_refuses_listing(const std::filesystem::path& directory) {
        std::error_code ec;
        std::filesystem::directory_iterator it(directory, ec);
        return static_cast<bool>(ec);
    }

    // Churn until an automatic round has STARTED (a disk agent counted it), then wait until it
    // has ENDED (the WAL manager counted its exit). Both halves are needed and neither is a
    // timer: the round is fire-and-forget off commit_txn and the truncation is the LAST thing
    // it does, so a case that measured at "started" -- or after some number of milliseconds --
    // would be measuring a round that had not got there yet.
    bool churn_until_an_automatic_round_completes(otterbrix::wrapper_dispatcher_t* d,
                                                  int64_t& next_id,
                                                  int max_statements) {
        services::disk::reset_table_checkpoints();
        services::wal::reset_auto_checkpoint_rounds();
        for (int i = 0; i < max_statements && services::disk::table_checkpoints() == 0 &&
                        services::wal::auto_checkpoint_rounds() == 0;
             ++i) {
            churn_once(d, next_id);
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (services::wal::auto_checkpoint_rounds() == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        return services::wal::auto_checkpoint_rounds() > 0;
    }

} // namespace

// RED before the fix.
//
// Step (c2) called repopulate_indexes_after_compaction, logged whatever it returned and
// carried straight on into step (d). With the rebuild refused the round still went to the
// journal: the seam below counted truncate_before reading the superseded segments, and on the
// runs where the round's boundary reached them it unlinked twelve of the thirteen captured
// files as well.
//
// With the refusal ending the round -- the same shape step (a) already uses for a refused
// index flush -- step (d) is never entered, and the round that follows, with nothing in its
// way, does the rebuild and the truncation both.
TEST_CASE("integration::cpp::auto_checkpoint_rebuild_refusal::a_refused_rebuild_may_not_cost_the_journal") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_auto_checkpoint_rebuild_refusal/db");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    config.wal.max_segment_size = kSegmentBytes;
    config.wal.auto_checkpoint_threshold_bytes = kAutoCheckpointBytes;

    // Declared before the engine so the seam is installed while the WAL opens its files, and
    // still installed while it tears them down. It is inert until armed.
    superseded_segment_watch_t watch;

    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE adb;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE adb.t (id bigint, k bigint);")->is_success());
    // USING hash -> the bitcask backend, whose clear() reports a directory it cannot list by
    // value and leaves the store untouched. That is the refusal this case injects.
    REQUIRE(exec(d, "CREATE INDEX t_k ON adb.t USING hash (k);")->is_success());
    // The WAL engine: no index, so its statements never open a bitcask file.
    REQUIRE(exec(d, "CREATE TABLE adb.pad (id bigint, payload text);")->is_success());

    load(d);
    int64_t churn_id = 1;
    churn_once(d, churn_id);

    INFO("the middle third goes, so the setup round has a real compaction to perform");
    REQUIRE(exec(d,
                 "DELETE FROM adb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                     " AND id <= " + std::to_string(kDeleteTo) + ";")
                ->is_success());

    // Two clean rounds with churn between them, so the journal that goes into the armed
    // window holds a run of superseded segments for the truncation step to read.
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    for (int i = 0; i < 12; ++i) {
        churn_once(d, churn_id);
    }
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    INFO("the index and the table agree BEFORE the armed round, so a disagreement after it is that round's");
    REQUIRE(index_disagreements_with_the_full_scan(d) == 0);

    INFO("front-of-table deletes, so the armed round's compaction has a shift to hand out");
    REQUIRE(exec(d, "DELETE FROM adb.t WHERE id >= 1 AND id <= 10;")->is_success());

    // AND THOSE ERASES MUST HAVE LANDED BEFORE THE INJECTION GOES IN. A committed DELETE does
    // not reach the index inside the statement -- commit_deletes queues it and the horizon
    // sweep publishes it once no live snapshot can want the rows -- and that publication is a
    // WRITE, whose tail (bitcask_index_agent_t::pay_merge_debt) LISTS the index directory.
    // Arming over an unfinished erase therefore refused step (a)'s flush instead of step
    // (c2)'s rebuild, and the round ended before it ever reached the disk. The wait is on the
    // queue's own depth, and the short settle after it covers the store-side tail of the last
    // publication, which outlives the counter. If it ever proves too short the case says so:
    // the disk-round guard below fails rather than passing for the wrong reason.
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (services::index::index_deferred_deletes() != 0 && std::chrono::steady_clock::now() < deadline) {
            churn_once(d, churn_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        INFO("the deferred-erase queue has to be empty before the fault goes in");
        REQUIRE(services::index::index_deferred_deletes() == 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    const auto bitcask_dir = find_bitcask_dir(config.disk.path);
    INFO("a USING hash index must own a bitcask directory: " << bitcask_dir.string());
    REQUIRE_FALSE(bitcask_dir.empty());

    const auto watched_segments = wal_segments(config.wal.path);
    INFO("watched WAL segments: " << watched_segments.size());
    REQUIRE(watched_segments.size() > 1);

    services::index::reset_index_repopulations();

    bool round_completed = false;
    {
        // Write + execute, no read: the path still resolves (so already-open descriptors and
        // by-name opens are unaffected) but it cannot be LISTED, which is the door
        // collect_segments() knocks on.
        dir_permissions_guard_t no_listing(bitcask_dir,
                                           std::filesystem::perms::owner_write | std::filesystem::perms::owner_exec);
        INFO("the injection has to be real: a suite running as root would list it anyway");
        REQUIRE(directory_really_refuses_listing(bitcask_dir));

        watch.arm(highest_segment_ordinal(config.wal.path));
        round_completed = churn_until_an_automatic_round_completes(d, churn_id, 400);
        watch.disarm();
    }
    const auto reads_by_the_abandoned_round = watch.superseded_reads();

    INFO("NOT VACUOUS (1): without an automatic round -- one that RAN and one that FINISHED -- "
         "this case tests nothing");
    REQUIRE(round_completed);
    REQUIRE(services::disk::table_checkpoints() > 0);

    INFO("NOT VACUOUS (2): the round reached its rebuild step at all");
    REQUIRE(services::index::index_repopulations() > 0);

    INFO("NOT VACUOUS (3): and that rebuild REFUSED over a table that really was renumbered -- a "
         "compacted table under an index still naming pre-compact rows is what that leaves behind, "
         "and neither a round that compacted nothing nor a rebuild that quietly succeeded could");
    REQUIRE(index_disagreements_with_the_full_scan(d) > 0);

    // THE POINT. The truncation is the round's only destructive step, and the round has just
    // failed the step that was its last chance to fail recoverably, so it must not have gone
    // near the journal at all.
    INFO("superseded segments the abandoned round read: " << reads_by_the_abandoned_round);
    REQUIRE(reads_by_the_abandoned_round == 0);

    // The same claim at file level. It is a CHECK rather than the gate because it depends on
    // the boundary the round was handed -- a round that deferred one busy table reports the
    // previous boundary and removes nothing however faithfully it ran step (d) -- so it
    // corroborates the gate above without being able to stand in for it.
    const auto gone = missing_from_disk(watched_segments);
    INFO("segments the abandoned round destroyed: " << gone.size() << " of " << watched_segments.size());
    CHECK(gone.empty());

    // AND THE ROUND MUST BE REPEATABLE. An abandoned round that wedged the dedup guard, or
    // left a window nothing could trip again, would trade a recoverable failure for a
    // permanent one -- which is the whole reason abandoning is allowed to be the answer here.
    // The next automatic round runs with nothing in its way and must finish the whole job.
    services::index::reset_index_repopulations();
    watch.arm(highest_segment_ordinal(config.wal.path));
    const bool next_round_completed = churn_until_an_automatic_round_completes(d, churn_id, 400);
    watch.disarm();
    const auto reads_by_the_next_round = watch.superseded_reads();

    INFO("the next automatic round must be able to run at all");
    REQUIRE(next_round_completed);

    INFO("and it must do the rebuild the abandoned round could not");
    REQUIRE(services::index::index_repopulations() > 0);
    CHECK(index_disagreements_with_the_full_scan(d) == 0);

    // NOT VACUOUS (4), and it is what makes the zero above mean something: the very counter
    // the abandoned round had to leave at zero is one an unobstructed round in this same
    // journal trips immediately.
    INFO("superseded segments the next round read: " << reads_by_the_next_round);
    REQUIRE(reads_by_the_next_round > 0);
}
