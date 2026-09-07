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
#include <stdexcept>
#include <iterator>
#include <string>
#include <vector>
#include <unistd.h>

#include "../../../components/table/test/fault_injection_file.hpp"

// Every crash here goes through the real T3 fault seam driving table_storage_t's production
// checkpoint, and recovery is judged by reading the data back against a named root, never just
// "the open succeeded": a crash at any point reopens to root N or N+1 via the two-slot
// shadow-paged header alone; an unopenable file is refused as an error value, left byte-identical.

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

// crash_revert() drops everything since the last fsync; here that fsync is the header commit
// itself, so nothing is reverted -- root N+1 with every row is exactly the promise under test.
TEST_CASE("services::disk::torn::committed_checkpoint_survives_kill_dash_nine") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "commit_kill.otbx";
    constexpr int64_t N = 50;

    otterbrix_test::fault_plan_t plan;
    {
        otterbrix_test::fault_injection_scope_t scope(plan);
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
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert();
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(N));
        CHECK(ts.checkpoint_wal_id() == 0);
        CHECK(ts.prev_checkpoint_wal_id() == 0);
    }

    cleanup_torn_dir();
}

// The checkpoint protocol has two fsync barriers; dying at the first (data/metadata) means the
// header was never attempted, so root N (the previous commit) is untouched.
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

        append_range(ts.table(), &resource, BASE, EXTRA);
        plan.fail_syncs_from = plan.syncs_seen + 1;

        auto failed = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(failed.has_error());
        REQUIRE(failed.error().type == core::error_code_t::io_error);
        REQUIRE(ts.checkpoint_wal_id() == 100);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert();
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(BASE));
    }

    cleanup_torn_dir();
}

// The header commit (2nd fsync) proves the write reached the device, not just the page cache.
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
        REQUIRE(ts.prev_checkpoint_wal_id() == 100);

        append_range(ts.table(), &resource, FIRST + SECOND, THIRD);
        plan.fail_syncs_from = plan.syncs_seen + 2;

        auto failed = ts.checkpoint(services::wal::id_t{300});
        REQUIRE(failed.has_error());
        REQUIRE(failed.error().type == core::error_code_t::io_error);
        REQUIRE(ts.checkpoint_wal_id() == 250);
        REQUIRE(ts.prev_checkpoint_wal_id() == 100);

        REQUIRE(scope.last() != nullptr);
        scope.last()->crash_revert();
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(FIRST + SECOND));
    }

    cleanup_torn_dir();
}

// A torn write persists only a block's first half (broken-CRC); the device keeps what it
// acknowledged, no revert. The half-written block is FRESH (unreferenced), so reopen walks root N clean.
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
        plan.torn_at_write = plan.writes_seen + 1;

        auto failed = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(failed.has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);

        plan.crashed = true;
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == iota_rows(BASE));
    }
    {
        std::vector<std::string> names;
        for (const auto& entry : std::filesystem::directory_iterator(torn_test_dir())) {
            names.push_back(entry.path().filename().string());
        }
        REQUIRE(names == std::vector<std::string>{"torn_mid.otbx"});
    }

    cleanup_torn_dir();
}

// A stray sidecar under table.otbx.* -- a pre-shadow-paging build's whole-file-backup or
// quarantine leftover -- is a loud, untouched refusal covering the whole start, not just this
// table: the .otbx stays byte-identical, the stray untouched, and removing it loads it again.
TEST_CASE("services::disk::torn::stray_legacy_sidecar_is_refused_loudly_and_untouched") {
    namespace catalog = components::catalog;
    cleanup_torn_dir();
    auto dir = std::filesystem::path(torn_test_dir()) / "straydb";
    std::filesystem::create_directories(dir);

    constexpr auto victim_oid = static_cast<unsigned>(catalog::well_known_oid::pg_namespace_table);
    constexpr auto other_oid = static_cast<unsigned>(catalog::well_known_oid::pg_class_table);
    constexpr auto db_oid = static_cast<unsigned>(catalog::well_known_oid::main_database);
    const auto victim_otbx = dir / std::to_string(db_oid) / std::to_string(victim_oid) / "table.otbx";

    {
        torn_manager_t m(dir);
        m.manager->bootstrap_system_tables_sync();
        REQUIRE(m.manager->has_storage(catalog::oid_t{victim_oid}));
    }
    REQUIRE(std::filesystem::exists(victim_otbx));

    core::pmr::otterbrix_resource resource;
    REQUIRE_FALSE(verify_otbx_sidecars(victim_otbx, &resource).contains_error());

    const std::vector<std::string> legacy_suffixes{std::string(".pre") + "v", std::string(".bro") + "ken"};
    for (const auto& suffix : legacy_suffixes) {
        auto stray = victim_otbx;
        stray += suffix;
        {
            std::ofstream f(stray, std::ios::binary | std::ios::trunc);
            REQUIRE(f.is_open());
            f << "stale bytes from an earlier build";
        }

        auto err = verify_otbx_sidecars(victim_otbx, &resource);
        REQUIRE(err.contains_error());
        CHECK(err.type == core::error_code_t::data_corruption);
        const std::string what{err.what.c_str()};
        CHECK(what.find(stray.filename().string()) != std::string::npos);

        const auto otbx_bytes_before = slurp_file(victim_otbx);
        const auto stray_bytes_before = slurp_file(stray);
        {
            torn_manager_t m(dir);
            REQUIRE_THROWS_AS(m.manager->bootstrap_system_tables_sync(), std::runtime_error);
            CHECK_FALSE(m.manager->has_storage(catalog::oid_t{victim_oid}));
            // A STOP, not a teardown: tables before the victim stay up, those after were never opened.
            CHECK(m.manager->has_storage(catalog::well_known_oid::pg_settings_table));
            CHECK_FALSE(m.manager->has_storage(catalog::oid_t{other_oid}));
        }
        CHECK(slurp_file(victim_otbx) == otbx_bytes_before);
        REQUIRE(std::filesystem::exists(stray));
        CHECK(slurp_file(stray) == stray_bytes_before);
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

    {
        torn_manager_t m(dir);
        m.manager->bootstrap_system_tables_sync();
        CHECK(m.manager->has_storage(catalog::oid_t{victim_oid}));
    }

    cleanup_torn_dir();
}

// Three distinct terminal refusals, each with its own error text: (a) missing file, (b) empty
// file (external truncation), (c) rot under both header slots -- data_corruption with full
// per-slot diagnostics, surfaced as a value, file left byte-identical.
TEST_CASE("services::disk::torn::unopenable_file_is_refused_as_a_value_and_left_byte_identical") {
    cleanup_torn_dir();
    std::filesystem::create_directories(torn_test_dir());
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(torn_test_dir()) / "terminal.otbx";

    {
        REQUIRE_FALSE(std::filesystem::exists(otbx));
        table_storage_t probe(&resource, otbx, {});
        REQUIRE(probe.construction_failed());
        CHECK(probe.construction_error().type == core::error_code_t::io_error);
        const std::string what{probe.construction_error().what.c_str()};
        CHECK(what.find("does not exist") != std::string::npos);
        REQUIRE_FALSE(std::filesystem::exists(otbx));
    }

    // create() writes iteration 0's slot; two checkpoints commit iterations 1 and 2.
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
        REQUIRE(std::filesystem::file_size(otbx) == 0);
        std::ofstream f(otbx, std::ios::binary | std::ios::trunc);
        REQUIRE(f.is_open());
        f.write(healthy_bytes.data(), static_cast<std::streamsize>(healthy_bytes.size()));
        REQUIRE(f.good());
    }

    // The rot an external backup would catch; shadow paging knowingly makes it terminal instead.
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
        CHECK(what.find("slot 1") != std::string::npos);
        CHECK(what.find("slot 2") != std::string::npos);
        CHECK(what.find("iteration") != std::string::npos);
        CHECK(what.find("checksum stored") != std::string::npos);
    }
    REQUIRE_NOTHROW([&] { table_storage_t probe(&resource, otbx, {}); }());
    CHECK(slurp_file(otbx) == corrupt_bytes);
    {
        std::vector<std::string> names;
        for (const auto& entry : std::filesystem::directory_iterator(torn_test_dir())) {
            names.push_back(entry.path().filename().string());
        }
        REQUIRE(names == std::vector<std::string>{"terminal.otbx"});
    }

    cleanup_torn_dir();
}

// write_header is the single durability point of a checkpoint; a discarded write()/sync() result
// would let checkpoint() report success while checkpoint_inner advances the .wal_id sidecar past
// a root that never reached the platter, orphaning the rows in between.
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

        append_one_int(ts.table(), &resource, 2);
        plan.fail_after_writes = plan.writes_seen;

        auto second = ts.checkpoint(services::wal::id_t{200});
        REQUIRE(second.has_error());
        REQUIRE(second.error().type == core::error_code_t::io_error);
        REQUIRE(ts.checkpoint_wal_id() == 100);
        REQUIRE(ts.prev_checkpoint_wal_id() == 0);

        plan.fail_after_writes = 0;
    }

    cleanup_torn_dir();
}

// A write that reached the page cache but never the device is what the header sector's fsync exists to catch.
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
