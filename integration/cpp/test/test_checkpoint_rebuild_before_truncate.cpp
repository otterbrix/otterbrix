#include "integration_fixture_path.hpp"
#include "test_config.hpp"

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

// Index rebuild must run before WAL truncation, or a refused rebuild is left with no WAL records to retry from.

using namespace test_helpers;

namespace {

    // 3000 rows span three row groups (row_group_size=1024), shifting every surviving tail row by 1000 ids.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001;
    constexpr int64_t kDeleteTo = 2000;

    // Small enough that the load rolls the journal over several segments; truncate_before skips the writer's current one.
    constexpr std::size_t kSegmentBytes = 64 * 1024;

    // Returning nullptr models what core::filesystem::open_file itself answers when a segment will not open.
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

        // Spread across all three row groups, including the shifted tail and deleted middle, so no stale index gets lucky.
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

    // Written by checkpoint_inner as `${db}/${namespace_oid}/${table_oid}/table.otbx.wal_id` via tmp+rename.
    uint64_t read_sidecar_wal_id(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        uint64_t value = 0;
        if (!in.is_open()) {
            return 0;
        }
        in.read(reinterpret_cast<char*>(&value), sizeof(value));
        return in ? value : 0;
    }

    // Every system table sits under the fixed system directory oid, so excluding it leaves this db's one user table.
    std::filesystem::path user_table_sidecar(const std::filesystem::path& db_root) {
        const auto system_dir = std::to_string(static_cast<unsigned>(services::disk::manager_disk_t::system_dir_oid()));
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

TEST_CASE("integration::cpp::checkpoint_rebuild_before_truncate::a_refused_truncate_may_not_cost_the_index_rebuild") {
    auto config = test_create_config(integration_fixture_path("test_checkpoint_rebuild_before_truncate/orig"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    config.wal.max_segment_size = kSegmentBytes;
    // Far above anything this case writes, so an automatic round can't fire mid-case and repair the state under test.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    const std::filesystem::path crash_dir = integration_fixture_path("test_checkpoint_rebuild_before_truncate/crashed");

    wal_open_refusal_t fault;

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE tdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE tdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX k_idx ON tdb.t (k);")->is_success());
        load(d, "tdb");

        // Without this clean round, round two would report checkpoint 0, skip truncate_before, and meet no fault.
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

        // This copy is the kill -9: nothing staged by hand, so it's exactly what the device holds right now.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor's CHECKPOINT runs against the original directory only

    auto crash_config = test_create_config(crash_dir);
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
