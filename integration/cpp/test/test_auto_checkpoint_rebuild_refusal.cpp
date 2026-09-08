#include "test_config.hpp"
#include "integration_fixture_path.hpp"

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

// A refused rebuild must abandon run_auto_checkpoint's round instead of falling through to
// truncation, since nothing rebuilds an index at startup or during replay. Truncation is detected
// via WAL segment reads below the high-water ordinal, not files removed -- the naive file-count
// check still passed about half the time on an unfixed build.

using namespace test_helpers;

namespace {

    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001;
    constexpr int64_t kDeleteTo = 2000;

    constexpr std::size_t kSegmentBytes = 16 * 1024;

    constexpr std::uintmax_t kAutoCheckpointBytes = 2ull * 1024ull * 1024ull;

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
            if (cur->is_error()) {
                ++disagreements;
                continue;
            }
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

    class superseded_segment_watch_t final : public services::wal::wal_file_interposer_t {
    public:
        superseded_segment_watch_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~superseded_segment_watch_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        superseded_segment_watch_t(const superseded_segment_watch_t&) = delete;
        superseded_segment_watch_t& operator=(const superseded_segment_watch_t&) = delete;

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

    bool directory_really_refuses_listing(const std::filesystem::path& directory) {
        std::error_code ec;
        std::filesystem::directory_iterator it(directory, ec);
        return static_cast<bool>(ec);
    }

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

// Measured on the unfixed build: truncation still ran, unlinking twelve of thirteen captured segments.
TEST_CASE("integration::cpp::auto_checkpoint_rebuild_refusal::a_refused_rebuild_may_not_cost_the_journal") {
    auto config = test_create_config(integration_fixture_path("test_auto_checkpoint_rebuild_refusal/db"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    config.wal.max_segment_size = kSegmentBytes;
    config.wal.auto_checkpoint_threshold_bytes = kAutoCheckpointBytes;

    // Declared before the engine so the seam is installed for the WAL's whole lifetime; inert until armed.
    superseded_segment_watch_t watch;

    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE adb;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE adb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec(d, "CREATE INDEX t_k ON adb.t USING hash (k);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE adb.pad (id bigint, payload text);")->is_success());

    load(d);
    int64_t churn_id = 1;
    churn_once(d, churn_id);

    INFO("the middle third goes, so the setup round has a real compaction to perform");
    REQUIRE(exec(d,
                 "DELETE FROM adb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                     " AND id <= " + std::to_string(kDeleteTo) + ";")
                ->is_success());

    REQUIRE(exec(d, "CHECKPOINT;")->is_success());
    for (int i = 0; i < 12; ++i) {
        churn_once(d, churn_id);
    }
    REQUIRE(exec(d, "CHECKPOINT;")->is_success());

    INFO("the index and the table agree BEFORE the armed round, so a disagreement after it is that round's");
    REQUIRE(index_disagreements_with_the_full_scan(d) == 0);

    INFO("front-of-table deletes, so the armed round's compaction has a shift to hand out");
    REQUIRE(exec(d, "DELETE FROM adb.t WHERE id >= 1 AND id <= 10;")->is_success());

    // Must drain the deferred-delete queue before arming: its horizon sweep also lists the index
    // directory (bitcask_index_agent_t::pay_merge_debt), tripping the flush refusal instead of this rebuild.
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

    INFO("superseded segments the abandoned round read: " << reads_by_the_abandoned_round);
    REQUIRE(reads_by_the_abandoned_round == 0);

    const auto gone = missing_from_disk(watched_segments);
    INFO("segments the abandoned round destroyed: " << gone.size() << " of " << watched_segments.size());
    CHECK(gone.empty());

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

    INFO("superseded segments the next round read: " << reads_by_the_next_round);
    REQUIRE(reads_by_the_next_round > 0);
}
