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

// A crash between compacting a table and rebuilding its indexes can leave a POST-COMPACT TABLE
// UNDER PRE-COMPACT INDEXES that SURVIVES restart; closing this needs a durable fact — "these
// indexes name pre-compact rows, not yet rebuilt" — written before the compaction and cleared only
// after the rebuild's force_flush. The window is entered here by stripping the read bit off the
// index directory for one CHECKPOINT, the same state a kill -9 would leave.

using namespace test_helpers;

namespace {

    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001;
    constexpr int64_t kDeleteTo = 2000;

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

    // `SELECT id, k` carries no predicate an index could serve, so this is the table's own answer.
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

    // Found by CONTENT: the one directory holding a CURRENT marker, which bitcask writes on open.
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

    // A suite run as root ignores chmod, so this asks the filesystem by trying the write clear() needs, not getuid().
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

    uint64_t read_sidecar_wal_id(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        uint64_t value = 0;
        if (!in.is_open()) {
            return 0;
        }
        in.read(reinterpret_cast<char*>(&value), sizeof(value));
        return in ? value : 0;
    }

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

// Nothing durable recorded the renumbering, so the restart wired the stale bitcask store as
// current and 34 of 34 probes disagreed with the table's own full scan, permanently.
TEST_CASE("integration::cpp::index_stale_marker_crash::a_restart_may_not_wire_an_index_left_naming_precompact_rows") {
    auto config = test_create_config(fixture_root() + "/orig");
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    const std::filesystem::path crash_dir = fixture_root() + "/crashed";

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE sdb;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE sdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec(d, "CREATE INDEX t_k ON sdb.t USING hash (k);")->is_success());
        load(d);

        REQUIRE(exec(d, "CHECKPOINT;")->is_success());

        INFO("the middle third goes, so round two has 1000 ids of shift to hand out");
        REQUIRE(exec(d,
                     "DELETE FROM sdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                         " AND id <= " + std::to_string(kDeleteTo) + ";")
                    ->is_success());

        {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            while (services::index::index_deferred_deletes() != 0 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            INFO("the deferred-erase queue has to be empty before the fault goes in");
            REQUIRE(services::index::index_deferred_deletes() == 0);

            // AND LANDED: a zero meter only proves the erase reached the mailbox, not that it finished.
            // MEASURED: shortening this wait to zero did not fail in 8 runs (5 idle, 3 under 24-way load).
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
            // Read-only: clear() lists the segments but every unlink is refused by the kernel.
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

        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor's CHECKPOINT runs against the ORIGINAL directory only

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    crash_config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();

        // Nothing rebuilds an index at startup, so the restart must decline to wire one left naming pre-compact rows.
        INFO("the reopened engine must answer what its table holds, key by key");
        CHECK(disagreements_with_the_full_scan(d) == 0);

        // The reason must be the decline: the predicate that was an Index Scan is served by a scan now.
        auto plan = exec(d, "EXPLAIN SELECT id FROM sdb.t WHERE k = 10;");
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed predicate after the restart:\n" << text);
        CHECK(text.find("Index Scan") == std::string::npos);
    }
    std::filesystem::remove_all(crash_dir);
    std::filesystem::remove_all(fixture_root());
}
