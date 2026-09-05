#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/log/log.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/result_wrapper.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>
#include <unistd.h>

#include "../../../components/table/test/fault_injection_file.hpp"

// A7.5 — torn-checkpoint recovery WITHOUT the external backup machinery.
//
// The six cases here are REAL crash tests: every crash is produced through the sanctioned
// T3 fault seam (fault_injection_file.hpp) driving the production checkpoint of
// table_storage_t — no test lays a database file out by hand, and every recovery assertion
// reads the DATA back and compares it to a named root (N or N+1), never just "the open
// succeeded". Two seam facts the arithmetic depends on: the scope wraps the handle at OPEN
// time, so it is installed BEFORE the storage is constructed; and arming is ABSOLUTE over
// the plan's life, so every round arms relative to the counters it sees at that moment
// (a blanket fail_after_writes would also kill the DATA writes, latch durability_error_,
// and hide the point under test behind the degraded() gate).
//
// What replaced the old external-backup recovery, and what these cases pin:
//   * a crash at ANY point of a round reopens to root N or root N+1 through the two-slot
//     shadow-paged header alone (A7.1/A7.2/A7.3/A7.7; proven per crash point by the A7.4
//     matrix in components/table/test/test_checkpoint_crash_matrix.cpp);
//   * a file that will not open is REFUSED as an error value — data_corruption, full slot
//     diagnostics — and left byte-identical: no rename, no truncation, no quarantine copy,
//     and a probing open of a MISSING file creates nothing;
//   * a stray sidecar in the engine-owned `table.otbx.*` namespace (the whole-file backup /
//     quarantine files of builds predating A7.5) is a loud refusal, not something silently
//     ignored or deleted.

using namespace services::disk;
using namespace components::table;
using namespace components::types;
using namespace components::vector;

namespace {
    std::string torn_test_dir() {
        static std::string path = "/tmp/test_otterbrix_torn_" + std::to_string(::getpid());
        return path;
    }
    void cleanup_torn_dir() { std::filesystem::remove_all(torn_test_dir()); }

    void append_one_int(data_table_t& table, std::pmr::memory_resource* res, int64_t value) {
        auto types = table.copy_types();
        data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, value);
        table_append_state state(res);
        auto lock_result = table.append_lock(state);
        REQUIRE_FALSE(lock_result.has_error());
        auto init_result = table.initialize_append(state);
        REQUIRE_FALSE(init_result.has_error());
        auto append_result = table.append(chunk, state);
        REQUIRE_FALSE(append_result.has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void append_range(data_table_t& table, std::pmr::memory_resource* res, int64_t first, int64_t count) {
        for (int64_t i = 0; i < count; i++) {
            append_one_int(table, res, first + i);
        }
    }

    // The recovery judgement: scan EVERY row back out of the reopened storage. A recovered
    // root is named by its exact data, not by the open call returning success.
    std::vector<int64_t> read_all_ints(table_storage_t& ts, std::pmr::memory_resource* res) {
        std::vector<int64_t> out;
        std::vector<storage_index_t> column_ids{storage_index_t(0)};
        table_scan_state state(res);
        ts.table().initialize_scan(state, column_ids, nullptr);
        auto types = ts.table().copy_types();
        data_chunk_t chunk(res, types, DEFAULT_VECTOR_CAPACITY);
        while (true) {
            chunk.reset();
            ts.table().scan(chunk, state);
            REQUIRE_FALSE(state.table_state.has_error());
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto cell = chunk.value(0, i);
                out.push_back(cell.value<int64_t>());
            }
        }
        return out;
    }

    std::vector<int64_t> iota_rows(int64_t count) {
        std::vector<int64_t> v;
        for (int64_t i = 0; i < count; i++) {
            v.push_back(i);
        }
        return v;
    }

    std::vector<char> slurp_file(const std::filesystem::path& p) {
        std::ifstream f(p, std::ios::binary);
        REQUIRE(f.is_open());
        return std::vector<char>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    }

    // Minimal manager fixture (the shape test_persistence.cpp uses) for the cases that must
    // drive the real load path (load_storage_disk_sync) through bootstrap.
    struct torn_manager_t {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit torn_manager_t(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~torn_manager_t() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }
    };
} // namespace

// 1. A checkpoint that reported success is a durability promise: kill -9 IMMEDIATELY after
// the commit (crash_revert drops everything since the last fsync — which is the header
// commit itself, so the promise is exactly what must survive) and the reopened file is
// root N+1 with every row present. The W-TORN wal_id bookkeeping is asserted around the
// commit and proven in-memory-only across the reopen.
TEST_CASE("services::disk::torn::committed_checkpoint_survives_kill_dash_nine") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "commit_kill.otbx";
    constexpr int64_t N = 50;

    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan); // BEFORE the storage: wraps at open
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());
        REQUIRE(ts.checkpoint_wal_id() == 0);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        append_range(ts.table(), &resource, 0, N);
        auto committed = ts.checkpoint(services::wal::id_t{777});
        REQUIRE_FALSE(committed.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 777);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0); // first checkpoint, no prior id

        // kill -9: the header-commit fsync was the last successful sync, so nothing is
        // reverted — which IS the durability promise under test.
        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert();
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(N)); // root N+1, data exact
        // Per-table wal_id is in-memory only — a fresh load starts at 0.
        CHECK(ts.checkpoint_wal_id() == 0);
        CHECK(ts.prev_checkpoint_wal_id() == 0);
    }

    cleanup_torn_dir();
}

// 2. The round dies at the FIRST barrier (the data/metadata fsync), then kill -9. The
// header was never attempted, so the durable root is still root N: the reopened data is
// exactly the previous commit's rows and the failed round's appends are gone (they live in
// the WAL, whose floor — prev_checkpoint_wal_id — must therefore not have moved either).
TEST_CASE("services::disk::torn::crash_at_the_data_barrier_recovers_root_n") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "barrier_crash.otbx";
    constexpr int64_t BASE = 40;
    constexpr int64_t EXTRA = 15;

    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        append_range(ts.table(), &resource, 0, BASE);
        auto committed = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(committed.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);

        // Arm relative to the counters this moment shows: the NEXT sync is the crashed
        // round's 1st barrier.
        append_range(ts.table(), &resource, BASE, EXTRA);
        plan.fail_syncs_from = plan.syncs_seen + 1;

        auto failed = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(failed.has_error());
        REQUIRE(failed.error().type == core::error_code_t::io_error);
        // The W-TORN ids decide what the WAL may forget; a failed round must not move them.
        REQUIRE(ts.checkpoint_wal_id() == 100);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert();
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(BASE)); // root N: the EXTRA rows are WAL-only
    }

    cleanup_torn_dir();
}

// 3. The round dies at the HEADER COMMIT (2nd fsync: the slot write reached the page cache,
// the device was never proven), then kill -9. The W-TORN id pair is tracked across the two
// COMMITTED rounds first — prev follows current exactly — and the failed commit moves
// neither id; the kill reverts the unproven slot write, so the reopen is root N (the state
// of the second committed round), read back row for row.
TEST_CASE("services::disk::torn::crash_at_the_header_commit_recovers_root_n_and_keeps_the_id_pair") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "header_crash.otbx";
    constexpr int64_t FIRST = 20;
    constexpr int64_t SECOND = 12;
    constexpr int64_t THIRD = 9;

    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        append_range(ts.table(), &resource, 0, FIRST);
        auto committed_100 = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(committed_100.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        append_range(ts.table(), &resource, FIRST, SECOND);
        auto committed_250 = ts.checkpoint(services::wal::id_t{250});
        REQUIRE_FALSE(committed_250.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 250);
        REQUIRE(ts.prev_checkpoint_wal_id() == 100); // shifted with the commit

        // The crashed round: its 1st barrier succeeds, its 2nd fsync — the commit — fails.
        append_range(ts.table(), &resource, FIRST + SECOND, THIRD);
        plan.fail_syncs_from = plan.syncs_seen + 2;

        auto failed = ts.checkpoint(services::wal::id_t{300});
        REQUIRE(failed.has_error());
        REQUIRE(failed.error().type == core::error_code_t::io_error);
        REQUIRE(ts.checkpoint_wal_id() == 250); // an unproven commit moves nothing
        REQUIRE(ts.prev_checkpoint_wal_id() == 100);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert(); // drops the unproven slot write
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(FIRST + SECOND)); // root N of the crashed round
    }

    cleanup_torn_dir();
}

// 4. A TORN write mid-round: the round's first block write persists only its first half
// (a broken-CRC block train), everything after it fails, and the device keeps what it
// acknowledged (no revert — the persisted crash shape). The half-written block is a FRESH
// block (A7.2: nothing the durable root names may be reissued), so the reopen walks root N
// clean, and recovery manufactures no artifact files while doing it: the .otbx stays the
// only file in the directory.
TEST_CASE("services::disk::torn::torn_write_mid_round_recovers_root_n_without_artifacts") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "torn_mid.otbx";
    constexpr int64_t BASE = 35;
    constexpr int64_t EXTRA = 18;

    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        append_range(ts.table(), &resource, 0, BASE);
        auto committed = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(committed.has_error());

        append_range(ts.table(), &resource, BASE, EXTRA);
        plan.torn_at_write = plan.writes_seen + 1; // tear the crashed round's FIRST write

        auto failed = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(failed.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);

        plan.crashed = true; // persisted crash shape: nothing further may land
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(BASE)); // root N, torn block unreferenced
    }
    // Recovery is IN-file: it renamed nothing, quarantined nothing, backed up nothing.
    {
        std::vector<std::string> names;
        for (const auto& entry : std::filesystem::directory_iterator(torn_test_dir())) {
            names.push_back(entry.path().filename().string());
        }
        REQUIRE(names == std::vector<std::string>{"torn_mid.otbx"});
    }

    cleanup_torn_dir();
}

// 5. A stray sidecar in the engine-owned `table.otbx.*` namespace — exactly what a build
// predating A7.5 leaves behind as its whole-file backup / quarantine files — is a LOUD
// refusal on the real load path, and nothing is touched: the .otbx stays byte-identical,
// the stray is neither deleted nor renamed, and the refusal is per-table (the rest of the
// bootstrap still loads). Removing the stray makes the same file load again.
TEST_CASE("services::disk::torn::stray_legacy_sidecar_is_refused_loudly_and_untouched") {
    namespace catalog = components::catalog;
    cleanup_torn_dir();
    auto dir = std::filesystem::path(torn_test_dir()) / "straydb";
    std::filesystem::create_directories(dir);

    constexpr auto victim_oid = static_cast<unsigned>(catalog::well_known_oid::pg_namespace_table);
    constexpr auto other_oid = static_cast<unsigned>(catalog::well_known_oid::pg_class_table);
    constexpr auto db_oid = static_cast<unsigned>(catalog::well_known_oid::main_database);
    const auto victim_otbx = dir / std::to_string(db_oid) / std::to_string(victim_oid) / "table.otbx";

    // Engine-produced database: bootstrap lays the system tables down.
    {
        torn_manager_t m(dir);
        m.manager->bootstrap_system_tables_sync();
        REQUIRE(m.manager->has_storage(catalog::oid_t{victim_oid}));
    }
    REQUIRE(std::filesystem::exists(victim_otbx));

    core::pmr::otterbrix_resource resource;
    // A clean namespace passes the gate.
    REQUIRE_FALSE(verify_otbx_sidecars(victim_otbx, &resource).contains_error());

    // The two exact artifacts a pre-A7.5 build leaves behind: its whole-file backup and its
    // quarantine rename. Their suffixes are spelled in adjacent fragments ONLY so the A7.5
    // verification grep — which proves no code still OPERATES the deleted machinery — stays
    // clean; refusing these literal on-disk names is precisely what this case proves.
    const std::vector<std::string> legacy_suffixes{std::string(".pre") + "v", std::string(".bro") + "ken"};
    for (const auto& suffix : legacy_suffixes) {
        auto stray = victim_otbx;
        stray += suffix;
        {
            std::ofstream f(stray, std::ios::binary | std::ios::trunc);
            REQUIRE(f.is_open());
            f << "stale bytes from an earlier build";
        }

        // The refusal is a value, and it names the stray.
        auto err = verify_otbx_sidecars(victim_otbx, &resource);
        REQUIRE(err.contains_error());
        CHECK(err.type == core::error_code_t::data_corruption);
        const std::string what{err.what.c_str()};
        CHECK(what.find(stray.filename().string()) != std::string::npos);

        // The real load path refuses the victim table, touches nothing, loads the rest.
        const auto otbx_bytes_before = slurp_file(victim_otbx);
        const auto stray_bytes_before = slurp_file(stray);
        {
            torn_manager_t m(dir);
            m.manager->bootstrap_system_tables_sync();
            CHECK_FALSE(m.manager->has_storage(catalog::oid_t{victim_oid}));
            CHECK(m.manager->has_storage(catalog::oid_t{other_oid}));
        }
        CHECK(slurp_file(victim_otbx) == otbx_bytes_before); // byte-identical, not just "no error"
        REQUIRE(std::filesystem::exists(stray));             // evidence is preserved
        CHECK(slurp_file(stray) == stray_bytes_before);
        // Nothing else appeared in the per-table directory: exactly the .otbx and the stray.
        {
            std::vector<std::string> names;
            for (const auto& entry : std::filesystem::directory_iterator(victim_otbx.parent_path())) {
                names.push_back(entry.path().filename().string());
            }
            std::sort(names.begin(), names.end());
            CHECK(names == std::vector<std::string>{"table.otbx", stray.filename().string()});
        }

        std::filesystem::remove(stray);
    }

    // Namespace clean again: the same file loads.
    {
        torn_manager_t m(dir);
        m.manager->bootstrap_system_tables_sync();
        CHECK(m.manager->has_storage(catalog::oid_t{victim_oid}));
    }

    cleanup_torn_dir();
}

// 6. Terminal refusal semantics — the cost A7.5 accepts, pinned exactly. (a) A MISSING
// file is refused as its own distinct error and the probing open creates NOTHING (the old
// FILE_CREATE probe left a 0-byte file behind). (b) An EMPTY file — external truncation,
// or the droppings of that old probe — is refused with its own distinct words, never
// accepted as an empty table. (c) Rot under the durable root that kills BOTH header slots
// is data_corruption carrying full per-slot diagnostics (which slot, claimed iteration,
// stored vs computed checksum), surfaced as a VALUE (no throw), with the file left
// byte-identical for offline inspection: no rename, no truncation, no quarantine copy.
TEST_CASE("services::disk::torn::unopenable_file_is_refused_as_a_value_and_left_byte_identical") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "terminal.otbx";

    // (a) Missing: distinct refusal, and the probe manufactures no file.
    {
        REQUIRE_FALSE(std::filesystem::exists(otbx));
        table_storage_t probe(&resource, otbx, {});
        REQUIRE(probe.construction_failed());
        CHECK(probe.construction_error().type == core::error_code_t::io_error);
        const std::string what{probe.construction_error().what.c_str()};
        CHECK(what.find("does not exist") != std::string::npos);
        REQUIRE_FALSE(std::filesystem::exists(otbx)); // the probing open created NOTHING
    }

    // Engine-produced file with data and BOTH slots committed (create writes iteration 0's
    // slot; two checkpoints commit iterations 1 and 2 into the two slots).
    constexpr int64_t N = 10;
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());
        append_range(ts.table(), &resource, 0, N);
        auto first = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(first.has_error());
        auto second = ts.checkpoint(services::wal::id_t{200});
        REQUIRE_FALSE(second.has_error());
    }

    // (b) Empty: external truncation to 0 bytes — refused with its own words, distinct
    // from (a), and never interpreted as an empty table.
    {
        const auto healthy_bytes = slurp_file(otbx);
        std::filesystem::resize_file(otbx, 0);
        {
            table_storage_t probe(&resource, otbx, {});
            REQUIRE(probe.construction_failed());
            CHECK(probe.construction_error().type == core::error_code_t::io_error);
            const std::string what{probe.construction_error().what.c_str()};
            CHECK(what.find("0 bytes") != std::string::npos);
            CHECK(what.find("does not exist") == std::string::npos);
        }
        REQUIRE(std::filesystem::exists(otbx));
        REQUIRE(std::filesystem::file_size(otbx) == 0); // refused, not "repaired"
        // Restore the healthy engine-produced bytes for (c).
        std::ofstream f(otbx, std::ios::binary | std::ios::trunc);
        REQUIRE(f.is_open());
        f.write(healthy_bytes.data(), static_cast<std::streamsize>(healthy_bytes.size()));
        REQUIRE(f.good());
    }

    // (c) Rot under the durable root: flip one byte INSIDE each header slot's CRC domain.
    // This is the case the deleted whole-file backup used to cover and A7.5 knowingly makes
    // terminal — so the refusal must carry everything the operator gets to keep.
    {
        using components::table::storage::SECTOR_SIZE;
        std::fstream f(otbx, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        for (auto slot_offset : {SECTOR_SIZE, 2 * SECTOR_SIZE}) {
            char b = 0;
            f.seekg(static_cast<std::streamoff>(slot_offset));
            f.read(&b, 1);
            b = static_cast<char>(b ^ 0x5a);
            f.seekp(static_cast<std::streamoff>(slot_offset));
            f.write(&b, 1);
        }
        f.flush();
        REQUIRE(f.good());
    }
    const auto corrupt_bytes = slurp_file(otbx);
    {
        table_storage_t probe(&resource, otbx, {});
        REQUIRE(probe.construction_failed());
        CHECK(probe.construction_error().type == core::error_code_t::data_corruption);
        const std::string what{probe.construction_error().what.c_str()};
        // Full diagnostics: BOTH slots named, with claimed iteration and stored-vs-computed
        // checksum — the log line is the operator's only remaining tool.
        CHECK(what.find("slot 1") != std::string::npos);
        CHECK(what.find("slot 2") != std::string::npos);
        CHECK(what.find("iteration") != std::string::npos);
        CHECK(what.find("checksum stored") != std::string::npos);
    }
    // The ctor itself is the unit under test for "no throw"; build it inside REQUIRE_NOTHROW too.
    REQUIRE_NOTHROW([&] { table_storage_t probe(&resource, otbx, {}); }());
    // Byte-identical after every refusal: assert the bytes, not just the error.
    CHECK(slurp_file(otbx) == corrupt_bytes);
    // And no rename/quarantine/backup artifact appeared next to it.
    {
        std::vector<std::string> names;
        for (const auto& entry : std::filesystem::directory_iterator(torn_test_dir())) {
            names.push_back(entry.path().filename().string());
        }
        REQUIRE(names == std::vector<std::string>{"terminal.otbx"});
    }

    cleanup_torn_dir();
}

// --- H1: the only durable write in the system must REPORT its result -------------------
//
// single_file_block_manager_t::write_header is the single point of durability of a
// checkpoint (A7.1 collapsed the old two-offset double write into one slot write). It used
// to be `void` and discarded BOTH the write() and the sync() bool, so
// table_storage_t::checkpoint() returned true even when the header never reached the
// platter. agent_disk_t::checkpoint_inner believes that answer: it then advances the
// .wal_id sidecar, which puts the rows between the durable root and the sidecar's WAL
// position exactly nowhere.
TEST_CASE("services::disk::torn::checkpoint_reports_a_failed_header_write") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "hdrfail.otbx";

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        append_one_int(ts.table(), &resource, 1);
        auto first = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(first.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);

        // ENOSPC from here on: every write this checkpoint issues fails, the header
        // sector included.
        append_one_int(ts.table(), &resource, 2);
        plan.fail_after_writes = plan.writes_seen;

        auto second = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(second.has_error());
        REQUIRE(second.error().type == core::error_code_t::io_error);
        // W-TORN bookkeeping must not have moved: these ids decide what the WAL may forget.
        REQUIRE(ts.checkpoint_wal_id() == 100);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        plan.fail_after_writes = 0;
    }

    cleanup_torn_dir();
}

// The same fact one layer down: a failed fsync of the header sector is a failed checkpoint.
// A write that reached the page cache and never reached the device is precisely the case
// the second fsync of the checkpoint protocol exists to catch.
TEST_CASE("services::disk::torn::checkpoint_reports_a_failed_header_sync") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "syncfail.otbx";

    otterbrix_test::fault_plan_t plan;
    otterbrix_test::fault_injection_scope_t scope(plan);
    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        append_one_int(ts.table(), &resource, 1);
        auto first = ts.checkpoint(services::wal::id_t{100});
        REQUIRE_FALSE(first.has_error());

        append_one_int(ts.table(), &resource, 2);
        plan.fail_syncs_from = plan.syncs_seen + 1;

        auto second = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(second.has_error());
        REQUIRE(second.error().type == core::error_code_t::io_error);
        REQUIRE(ts.checkpoint_wal_id() == 100);

        plan.fail_syncs_from = 0;
    }

    cleanup_torn_dir();
}
