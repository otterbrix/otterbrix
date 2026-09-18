// Crashes are produced only via the fault-injection seam (fault_injection_file.hpp); corruption is
// produced by mutating the file's bytes directly, matching the existing checksum/bad-magic tests.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

#include "fault_injection_file.hpp"
#include "table_segment_scan.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    std::string shadow_db_path(const char* tag) {
        return "/tmp/test_otterbrix_shadow_header_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct shadow_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        shadow_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(shadow_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", logical_type::BIGINT);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "shadow_table");
    }

    void append_rows(data_table_t& table, shadow_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                chunk.set_value(0, i, static_cast<int64_t>(start + offset + i));
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    void checkpoint_production(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        REQUIRE_FALSE(table.checkpoint(writer).has_error());
        REQUIRE_FALSE(writer.flush().has_error());
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_ptr.has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
        REQUIRE_FALSE(bm.file_sync().has_error());
    }

    uint64_t scan_rows(data_table_t& table, uint64_t upper_bound) {
        uint64_t scanned = 0;
        otterbrix_test::scan_table_segment(table, 0, upper_bound, [&](data_chunk_t& chunk) {
            scanned += chunk.size();
        });
        return scanned;
    }

    uint64_t rows_at_root(shadow_env_t& env,
                          tstorage::single_file_block_manager_t& bm,
                          uint64_t meta_block,
                          uint64_t upper_bound) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::meta_block_pointer_t ptr;
        ptr.block_pointer = meta_block;
        tstorage::metadata_reader_t reader(meta_mgr, ptr);
        auto loaded = data_table_t::load_from_disk(&env.resource, bm, reader);
        REQUIRE_FALSE(loaded.has_error());
        return scan_rows(*loaded.value(), upper_bound);
    }

    // slot 0 lives at SECTOR_SIZE, slot 1 at 2 * SECTOR_SIZE (main header occupies [0, SECTOR_SIZE)).

    uint64_t slot_offset(int slot) { return slot == 0 ? tstorage::SECTOR_SIZE : 2 * tstorage::SECTOR_SIZE; }

    bool read_slot(const std::string& path, int slot, tstorage::database_header_t& out) {
        std::ifstream f(path, std::ios::binary);
        if (!f) {
            return false;
        }
        std::memset(&out, 0, sizeof(out));
        f.seekg(static_cast<std::streamoff>(slot_offset(slot)));
        f.read(reinterpret_cast<char*>(&out), sizeof(out));
        return static_cast<bool>(f);
    }

    void write_slot(const std::string& path, int slot, const tstorage::database_header_t& in) {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekp(static_cast<std::streamoff>(slot_offset(slot)));
        f.write(reinterpret_cast<const char*>(&in), sizeof(in));
        f.flush();
        REQUIRE(f.good());
    }

    tstorage::database_header_t garbage_slot(std::mt19937_64& rng, uint64_t iteration) {
        tstorage::database_header_t h{};
        auto* bytes = reinterpret_cast<uint8_t*>(&h);
        for (size_t i = 0; i < sizeof(h); i++) {
            bytes[i] = static_cast<uint8_t>(rng() & 0xFF);
        }
        h.iteration = iteration;
        if (h.checksum_ok()) {
            h.checksum ^= 1;
        }
        return h;
    }

    std::vector<char> read_whole_file(const std::string& path) {
        std::ifstream f(path, std::ios::binary);
        REQUIRE(f.is_open());
        return std::vector<char>((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    }

    // Plain bool, not REQUIRE(a == b): Catch2 would stringify multi-megabyte vectors and abort before reporting.
    bool same_bytes(const std::vector<char>& a, const std::vector<char>& b) {
        return a.size() == b.size() && std::memcmp(a.data(), b.data(), a.size()) == 0;
    }

    int valid_slot_count(const std::string& path) {
        int valid = 0;
        for (int slot = 0; slot < 2; slot++) {
            tstorage::database_header_t h{};
            if (read_slot(path, slot, h) && h.checksum_ok()) {
                valid++;
            }
        }
        return valid;
    }

} // namespace

TEST_CASE("shadow_header: a checkpoint writes ONE slot and leaves the previous root in the other") {
    const std::string path = shadow_db_path("prev_root");
    remove_file(path);
    shadow_env_t env;

    uint64_t root_a = tstorage::INVALID_INDEX;
    uint64_t root_b = tstorage::INVALID_INDEX;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);

        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
        root_a = bm.meta_block();

        append_rows(*table, env, 2000, 2000);
        checkpoint_production(bm, *table);
        root_b = bm.meta_block();
    }
    REQUIRE(root_a != tstorage::INVALID_INDEX);
    REQUIRE(root_b != tstorage::INVALID_INDEX);
    REQUIRE(root_a != root_b);

    tstorage::database_header_t s0{};
    tstorage::database_header_t s1{};
    REQUIRE(read_slot(path, 0, s0));
    REQUIRE(read_slot(path, 1, s1));

    const bool newest_in_s0 = s0.iteration > s1.iteration;
    const auto& newest = newest_in_s0 ? s0 : s1;
    const auto& previous = newest_in_s0 ? s1 : s0;

    CHECK(newest.meta_block == root_b);
    CHECK(previous.meta_block == root_a);
    CHECK(previous.iteration + 1 == newest.iteration);
    CHECK(previous.checksum_ok());
    CHECK(newest.checksum_ok());

    remove_file(path);
}

TEST_CASE("shadow_header: a crash after a checkpoint leaves the PREVIOUS root openable") {
    const std::string path = shadow_db_path("crash_prev");
    const std::string copy_path = path + ".crashcopy";
    remove_file(path);
    remove_file(copy_path);

    {
        shadow_env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);

        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
        append_rows(*table, env, 2000, 2000);
        checkpoint_production(bm, *table);

        const auto post_b = read_whole_file(path);

        append_rows(*table, env, 4000, 2000);
        {
            tstorage::metadata_manager_t meta_mgr(bm);
            tstorage::metadata_writer_t writer(meta_mgr);
            REQUIRE_FALSE(table->checkpoint(writer).has_error());
            REQUIRE_FALSE(writer.flush().has_error());
            bm.set_meta_block(writer.get_block_pointer().block_pointer);
            REQUIRE_FALSE(bm.serialize_free_list().has_error());
        }

        REQUIRE(scope.last() != nullptr);

        const auto pre_crash = read_whole_file(path);
        REQUIRE_FALSE(same_bytes(pre_crash, post_b));

        scope.last()->crash_revert();

        const auto post_crash = read_whole_file(path);
        REQUIRE(same_bytes(post_crash, post_b));

        std::filesystem::copy_file(path, copy_path, std::filesystem::copy_options::overwrite_existing);
    }

    {
        shadow_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, copy_path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());

        CHECK(rows_at_root(env, bm, bm.meta_block(), 5000) == 4000);

        tstorage::database_header_t s0{};
        tstorage::database_header_t s1{};
        REQUIRE(read_slot(copy_path, 0, s0));
        REQUIRE(read_slot(copy_path, 1, s1));
        const auto& previous = (s0.iteration > s1.iteration) ? s1 : s0;
        REQUIRE(previous.checksum_ok());
        CHECK(previous.meta_block != bm.meta_block());
        CHECK(rows_at_root(env, bm, previous.meta_block, 5000) == 2000);
    }

    remove_file(path);
    remove_file(copy_path);
}

TEST_CASE("shadow_header: the durable header carries a verifiable checksum") {
    const std::string path = shadow_db_path("checksum");
    remove_file(path);
    shadow_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
    }

    tstorage::database_header_t s0{};
    tstorage::database_header_t s1{};
    REQUIRE(read_slot(path, 0, s0));
    REQUIRE(read_slot(path, 1, s1));
    const auto& newest = (s0.iteration >= s1.iteration) ? s0 : s1;

    CHECK(newest.checksum != 0);
    CHECK(newest.checksum_ok());

    tstorage::database_header_t tampered = newest;
    tampered.meta_block += 1;
    CHECK_FALSE(tampered.checksum_ok());

    tstorage::database_header_t stray = newest;
    stray.padding[sizeof(stray.padding) - 1] ^= 0xFF;
    CHECK_FALSE(stray.checksum_ok());

    // Confirms checksum_ok's documented gap: fields+checksum fit in the first 48 bytes of the sector and
    // bytes 48.. are zero in every generation, so splicing two generations at that boundary passes -- it IS one.
    static constexpr size_t HARDWARE_SECTOR = 512;
    REQUIRE(s0.checksum_ok());
    REQUIRE(s1.checksum_ok());
    REQUIRE(s0.iteration != s1.iteration);
    tstorage::database_header_t spliced{};
    std::memcpy(&spliced, &s0, sizeof(spliced));
    std::memcpy(reinterpret_cast<char*>(&spliced) + HARDWARE_SECTOR,
                reinterpret_cast<const char*>(&s1) + HARDWARE_SECTOR,
                sizeof(spliced) - HARDWARE_SECTOR);
    CHECK(spliced.checksum_ok());
    CHECK(std::memcmp(&spliced, &s0, sizeof(spliced)) == 0);

    remove_file(path);
}

TEST_CASE("shadow_header: garbage with a huge iteration never beats a valid slot") {
    const std::string path = shadow_db_path("fuzz");
    const std::string pristine = path + ".pristine";
    remove_file(path);
    remove_file(pristine);
    shadow_env_t env;

    uint64_t root_a = tstorage::INVALID_INDEX;
    uint64_t root_b = tstorage::INVALID_INDEX;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
        root_a = bm.meta_block();
        append_rows(*table, env, 2000, 2000);
        checkpoint_production(bm, *table);
        root_b = bm.meta_block();
    }
    std::filesystem::copy_file(path, pristine, std::filesystem::copy_options::overwrite_existing);

    REQUIRE(valid_slot_count(pristine) == 2);

    tstorage::database_header_t s0{};
    tstorage::database_header_t s1{};
    REQUIRE(read_slot(pristine, 0, s0));
    REQUIRE(read_slot(pristine, 1, s1));

    std::mt19937_64 rng(0x5EEDA701ULL);
    constexpr int TRIALS = 32;

    for (int smashed = 0; smashed < 2; smashed++) {
        const auto& survivor = (smashed == 0) ? s1 : s0;
        const uint64_t expect_root = (survivor.meta_block == root_a) ? root_a : root_b;
        const uint64_t expect_rows = (expect_root == root_a) ? 2000 : 4000;

        for (int trial = 0; trial < TRIALS; trial++) {
            std::filesystem::copy_file(pristine, path, std::filesystem::copy_options::overwrite_existing);
            const uint64_t huge = (trial == 0)   ? UINT64_MAX
                                  : (trial == 1) ? (uint64_t(1) << 63)
                                                 : (rng() | (uint64_t(1) << 62));
            write_slot(path, smashed, garbage_slot(rng, huge));

            shadow_env_t reopen_env;
            tstorage::single_file_block_manager_t bm(reopen_env.buffer_manager, reopen_env.fs, path);
            auto result = bm.load_existing_database();
            REQUIRE_FALSE(result.has_error());
            REQUIRE(bm.meta_block() == expect_root);
            REQUIRE(rows_at_root(reopen_env, bm, bm.meta_block(), 5000) == expect_rows);
        }
    }

    remove_file(path);
    remove_file(pristine);
}

TEST_CASE("shadow_header: two invalid slots report data_corruption and leave the file untouched") {
    const std::string path = shadow_db_path("both_corrupt");
    remove_file(path);
    shadow_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
    }

    std::mt19937_64 rng(0xB07Bull);
    write_slot(path, 0, garbage_slot(rng, UINT64_MAX));
    write_slot(path, 1, garbage_slot(rng, UINT64_MAX - 1));
    REQUIRE(valid_slot_count(path) == 0);

    const auto before = read_whole_file(path);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        core::result_wrapper_t<bool> result = false;
        REQUIRE_NOTHROW(result = bm.load_existing_database());
        REQUIRE(result.has_error());
        REQUIRE(result.error().type == core::error_code_t::data_corruption);
    }

    const auto after = read_whole_file(path);
    CHECK(before.size() == after.size());
    CHECK(before == after);

    remove_file(path);
}

// A fresh file's never-written slot reads back as zeros -- an iteration-0 TIE pointing at meta_block 0, a
// REAL block id -- so it must be rejected on checksum, not win the tie against the real iteration-0 slot.
TEST_CASE("shadow_header: a freshly created database opens cleanly with one valid slot") {
    const std::string path = shadow_db_path("fresh");
    remove_file(path);
    shadow_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
    }

    CHECK(valid_slot_count(path) == 1);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        CHECK(bm.meta_block() == tstorage::INVALID_INDEX);
        CHECK(bm.total_blocks() == 0);
        CHECK(bm.free_blocks() == 0);
    }

    int rejected = 0;
    for (int slot = 0; slot < 2; slot++) {
        tstorage::database_header_t h{};
        REQUIRE(read_slot(path, slot, h));
        if (!h.checksum_ok()) {
            rejected++;
            CHECK(h.iteration == 0);
            CHECK(h.meta_block == 0);
        }
    }
    CHECK(rejected == 1);

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
    }
    CHECK(valid_slot_count(path) == 2);

    {
        shadow_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        CHECK(rows_at_root(env2, bm, bm.meta_block(), 3000) == 2000);
    }

    remove_file(path);
}

// Slot = f(iteration_ parity); incrementing iteration_ before the write means a FAILED write would still
// advance it, aiming a retry at the slot holding the last durable root and overwriting what it must preserve.
TEST_CASE("shadow_header: a retry after a failed header write reuses the SAME slot") {
    const std::string path = shadow_db_path("retry_slot");
    remove_file(path);

    uint64_t root_a = tstorage::INVALID_INDEX;
    uint64_t root_c = tstorage::INVALID_INDEX;
    uint64_t iteration_a = 0;
    {
        shadow_env_t env;
        otterbrix_test::fault_plan_t plan;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);

        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
        root_a = bm.meta_block();

        tstorage::database_header_t slot_a{};
        tstorage::database_header_t slot_b{};
        REQUIRE(read_slot(path, 0, slot_a));
        REQUIRE(read_slot(path, 1, slot_b));
        iteration_a = std::max(slot_a.iteration, slot_b.iteration);

        append_rows(*table, env, 2000, 2000);
        {
            tstorage::metadata_manager_t meta_mgr(bm);
            tstorage::metadata_writer_t writer(meta_mgr);
            REQUIRE_FALSE(table->checkpoint(writer).has_error());
            REQUIRE_FALSE(writer.flush().has_error());
            bm.set_meta_block(writer.get_block_pointer().block_pointer);
            auto free_ptr = bm.serialize_free_list();
            REQUIRE_FALSE(free_ptr.has_error());
            REQUIRE_FALSE(bm.file_sync().has_error());

            plan.fail_after_writes = plan.writes_seen;
            tstorage::database_header_t header{};
            header.initialize();
            header.free_list = free_ptr.value().block_pointer;
            auto failed = bm.write_header(header);
            REQUIRE(failed.has_error());
            plan.fail_after_writes = 0;
        }

        checkpoint_production(bm, *table);
        root_c = bm.meta_block();
    }

    REQUIRE(root_a != tstorage::INVALID_INDEX);
    REQUIRE(root_c != tstorage::INVALID_INDEX);
    REQUIRE(root_a != root_c);
    REQUIRE(valid_slot_count(path) == 2);

    tstorage::database_header_t s0{};
    tstorage::database_header_t s1{};
    REQUIRE(read_slot(path, 0, s0));
    REQUIRE(read_slot(path, 1, s1));
    const bool newest_in_s0 = s0.iteration > s1.iteration;
    const auto& newest = newest_in_s0 ? s0 : s1;
    const auto& previous = newest_in_s0 ? s1 : s0;

    CHECK(newest.meta_block == root_c);
    CHECK(newest.iteration == iteration_a + 1);
    REQUIRE(previous.meta_block == root_a);
    CHECK(previous.iteration == iteration_a);
    {
        shadow_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
        REQUIRE_FALSE(bm.load_existing_database().has_error());
        CHECK(bm.meta_block() == root_c);
        CHECK(rows_at_root(env2, bm, root_c, 5000) == 4000);
        CHECK(rows_at_root(env2, bm, root_a, 5000) == 2000);
    }

    remove_file(path);
}

// The writes and fsync inside create_new_database must report failure too, not just the open call:
// discarding them lets a bad write claim success, surfacing only as data_corruption on the next open.
TEST_CASE("shadow_header: create_new_database reports a write that did not land") {
    SECTION("the header slot write fails") {
        const std::string path = shadow_db_path("create_slot_fail");
        remove_file(path);
        shadow_env_t env;
        otterbrix_test::fault_plan_t plan;
        plan.fail_after_writes = 1;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        auto created = bm.create_new_database();
        REQUIRE(created.has_error());
        CHECK(created.error().type == core::error_code_t::io_error);
        CHECK(valid_slot_count(path) == 0);

        remove_file(path);
    }

    SECTION("the main header write fails") {
        const std::string path = shadow_db_path("create_main_fail");
        remove_file(path);
        shadow_env_t env;
        otterbrix_test::fault_plan_t plan;
        plan.torn_at_write = 1;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        auto created = bm.create_new_database();
        REQUIRE(created.has_error());
        CHECK(created.error().type == core::error_code_t::io_error);

        remove_file(path);
    }

    SECTION("the fsync fails") {
        const std::string path = shadow_db_path("create_sync_fail");
        remove_file(path);
        shadow_env_t env;
        otterbrix_test::fault_plan_t plan;
        plan.fail_syncs_from = 1;
        otterbrix_test::fault_injection_scope_t scope(plan);

        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        auto created = bm.create_new_database();
        REQUIRE(created.has_error());
        CHECK(created.error().type == core::error_code_t::io_error);

        remove_file(path);
    }
}

// A fresh file legitimately has meta_block == INVALID_INDEX, but a CHECKPOINTED file whose newest slot got
// corrupted falls back to the same header, silently looking empty; the tell is file size == BLOCK_START.
TEST_CASE("shadow_header: a never-checkpointed file opens as legitimately empty") {
    const std::string path = shadow_db_path("young_open");
    remove_file(path);
    shadow_env_t env;
    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
    }
    REQUIRE(std::filesystem::file_size(path) == tstorage::BLOCK_START);

    shadow_env_t env2;
    tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
    auto opened = bm.load_existing_database();
    REQUIRE_FALSE(opened.has_error());
    CHECK(bm.meta_block() == tstorage::INVALID_INDEX);

    remove_file(path);
}

TEST_CASE("shadow_header: a checkpointed file falling back to the initial empty root is refused") {
    const std::string path = shadow_db_path("stale_initial_root");
    remove_file(path);
    shadow_env_t env;

    {
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, 2000);
        checkpoint_production(bm, *table);
    }
    {
        tstorage::database_header_t s0{};
        tstorage::database_header_t s1{};
        REQUIRE(read_slot(path, 0, s0));
        REQUIRE(read_slot(path, 1, s1));
        REQUIRE(s0.checksum_ok());
        REQUIRE(s0.iteration == 1);
        REQUIRE(s1.checksum_ok());
        REQUIRE(s1.iteration == 0);
        REQUIRE(s1.meta_block == tstorage::INVALID_INDEX);
    }

    std::mt19937_64 rng(0xA76A76A7ULL);
    write_slot(path, 0, garbage_slot(rng, 1));
    REQUIRE(std::filesystem::file_size(path) > tstorage::BLOCK_START);
    const auto before = read_whole_file(path);

    {
        shadow_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
        auto opened = bm.load_existing_database();
        INFO("a checkpointed table must never silently reopen as an empty one");
        REQUIRE(opened.has_error());
        CHECK(opened.error().type == core::error_code_t::data_corruption);
    }
    CHECK(same_bytes(before, read_whole_file(path)));

    remove_file(path);
}

TEST_CASE("shadow_header: an initial root whose header contradicts the file is refused") {
    SECTION("the file grew past the header sectors") {
        const std::string path = shadow_db_path("young_grown");
        remove_file(path);
        shadow_env_t env;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE_FALSE(bm.create_new_database().has_error());
        }
        {
            std::ofstream f(path, std::ios::binary | std::ios::app);
            REQUIRE(f.is_open());
            std::vector<char> zeros(4096, 0);
            f.write(zeros.data(), static_cast<std::streamsize>(zeros.size()));
            REQUIRE(f.good());
        }
        shadow_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
        auto opened = bm.load_existing_database();
        REQUIRE(opened.has_error());
        CHECK(opened.error().type == core::error_code_t::data_corruption);
        remove_file(path);
    }

    SECTION("the initial slot itself claims blocks") {
        const std::string path = shadow_db_path("young_contradiction");
        remove_file(path);
        shadow_env_t env;
        {
            tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
            REQUIRE_FALSE(bm.create_new_database().has_error());
        }
        tstorage::database_header_t initial{};
        REQUIRE(read_slot(path, 1, initial));
        REQUIRE(initial.checksum_ok());
        REQUIRE(initial.iteration == 0);
        initial.block_count = 7;
        initial.checksum = initial.compute_checksum();
        write_slot(path, 1, initial);
        REQUIRE(std::filesystem::file_size(path) == tstorage::BLOCK_START);

        shadow_env_t env2;
        tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
        auto opened = bm.load_existing_database();
        REQUIRE(opened.has_error());
        CHECK(opened.error().type == core::error_code_t::data_corruption);
        remove_file(path);
    }
}
