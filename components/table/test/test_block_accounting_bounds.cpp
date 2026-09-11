// Corruption here is injected by forging header bytes and recomputing the checksum, so the header stays VALID.

#include <catch2/catch_test_macros.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "block_reachability_walker.hpp"

using namespace components::types;
using namespace components::vector;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    constexpr uint64_t BOUNDS_ROWS = 12000;
    constexpr uint64_t WATERMARK = std::numeric_limits<uint64_t>::max();

    std::string bounds_db_path(const char* tag) {
        return "/tmp/test_otterbrix_accounting_bounds_" + std::to_string(::getpid()) + "_" + tag + ".otbx";
    }

    void remove_file(const std::string& path) { std::remove(path.c_str()); }

    struct bounds_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;

        bounds_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };

    std::unique_ptr<data_table_t> make_table(bounds_env_t& env, tstorage::single_file_block_manager_t& bm) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", logical_type::BIGINT);
        columns.emplace_back("name", logical_type::STRING_LITERAL);
        return std::make_unique<data_table_t>(&env.resource, bm, std::move(columns), "bounds_table");
    }

    std::string row_name(uint64_t row) { return "bounds_row_payload_padding_" + std::to_string(row); }

    void append_rows(data_table_t& table, bounds_env_t& env, uint64_t start, uint64_t count) {
        auto types = table.copy_types();
        uint64_t offset = 0;
        while (offset < count) {
            uint64_t batch = std::min(count - offset, uint64_t(DEFAULT_VECTOR_CAPACITY));
            data_chunk_t chunk(&env.resource, types, batch);
            chunk.set_cardinality(batch);
            for (uint64_t i = 0; i < batch; i++) {
                uint64_t row = start + offset + i;
                chunk.set_value(0, i, static_cast<int64_t>(row));
                auto name = row_name(row);
                chunk.set_value(1, i, std::string_view{name});
            }
            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
            offset += batch;
        }
    }

    bool checkpoint_round(tstorage::single_file_block_manager_t& bm, data_table_t& table) {
        tstorage::metadata_manager_t meta_mgr(bm);
        tstorage::metadata_writer_t writer(meta_mgr);
        if (table.checkpoint(writer).has_error()) {
            return false;
        }
        if (writer.flush().has_error()) {
            return false;
        }
        bm.set_meta_block(writer.get_block_pointer().block_pointer);
        auto free_ptr = bm.serialize_free_list();
        if (free_ptr.has_error()) {
            return false;
        }
        if (bm.file_sync().has_error()) {
            return false;
        }
        tstorage::database_header_t header{};
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        return !bm.write_header(header).has_error();
    }

    // Three rounds: from the third on, an unchanged table is a closed cycle — free list stable.
    std::unique_ptr<data_table_t> reach_steady_state(bounds_env_t& env, tstorage::single_file_block_manager_t& bm) {
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, BOUNDS_ROWS);
        for (int warmup = 0; warmup < 3; ++warmup) {
            REQUIRE(table->compact(WATERMARK));
            REQUIRE(checkpoint_round(bm, *table));
        }
        return table;
    }

    uint64_t slot_offset(uint64_t iteration) {
        return (iteration % 2 == 1) ? tstorage::SECTOR_SIZE : 2 * tstorage::SECTOR_SIZE;
    }

    // Recomputes the checksum over the forged bytes, so the gate tests the field, not the checksum.
    void forge_active_header(const std::string& path, const tstorage::database_header_t& forged) {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        tstorage::database_header_t out = forged;
        out.checksum = out.compute_checksum();
        f.seekp(static_cast<std::streamoff>(slot_offset(out.iteration)));
        f.write(reinterpret_cast<const char*>(&out), sizeof(out));
        f.flush();
        REQUIRE(f.good());
    }

    bool message_names(const core::error_t& error, const std::string& needle) {
        return std::string(error.what.c_str()).find(needle) != std::string::npos;
    }

} // namespace

// GATE 1: mark_as_free rejected only the TRANSIENT domain (>= 2^62); an id between the file's
// end and 2^62 walked into the free pool and was later handed out past EOF.
TEST_CASE("accounting_bounds: releasing a block past the end of the file is refused", "[bounds]") {
    const auto path = bounds_db_path("release_past_end");
    remove_file(path);
    bounds_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = reach_steady_state(env, bm);

    const uint64_t blocks = bm.total_blocks();
    REQUIRE(blocks > 1);
    REQUIRE_FALSE(bm.degraded());

    const uint64_t inside = blocks - 1;
    bm.mark_as_free(inside);
    CHECK_FALSE(bm.degraded());
    CHECK((bm.dev_pending_free_snapshot().count(inside) != 0 || bm.dev_reusable_snapshot().count(inside) != 0));

    const uint64_t past_end = blocks + 4;
    REQUIRE(past_end < tstorage::MAXIMUM_BLOCK);
    bm.mark_as_free(past_end);
    CHECK(bm.degraded());
    CHECK(bm.dev_pending_free_snapshot().count(past_end) == 0);
    CHECK(bm.dev_reusable_snapshot().count(past_end) == 0);
    CHECK(bm.total_blocks() == blocks);

    CHECK_FALSE(checkpoint_round(bm, *table));

    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    uint64_t free_listed_past_end = 0;
    for (auto id : report.free_list_content) {
        if (id >= report.block_count) {
            ++free_listed_past_end;
        }
    }
    WARN("[bounds gate 1] walker: durable_block_count="
         << report.block_count << " chain=" << report.chain_blocks.size()
         << " durable_data=" << report.durable_data.size() << " registry=" << report.registry_live.size()
         << " freelist=" << report.free_list_content.size() << " unexplained=" << report.unexplained.size()
         << " overlap=" << report.reachable_free_overlap.size() << " free_listed_past_end=" << free_listed_past_end);
    CHECK(free_listed_past_end == 0);
    CHECK(report.unexplained.empty());
    CHECK(report.reachable_free_overlap.empty());

    remove_file(path);
}

// GATE 2: deserialize_free_list rejected only the transient domain, so a free list naming a
// block past the file's own extent opened CLEANLY and armed the next allocation to write past EOF.
TEST_CASE("accounting_bounds: a free list naming a block past the file's extent is refused at open", "[bounds]") {
    const auto path = bounds_db_path("freelist_past_end");
    remove_file(path);

    uint64_t forged_count = 0;
    uint64_t offending_id = 0;
    {
        bounds_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = reach_steady_state(env, bm);

        auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
        REQUIRE(report.ok);
        REQUIRE_FALSE(report.free_list_content.empty());
        offending_id = *report.free_list_content.rbegin();
        forged_count = offending_id;
        REQUIRE(forged_count > 0);
    }

    tstorage::database_header_t header{};
    REQUIRE(otterbrix_test::read_active_durable_header(path, header));
    REQUIRE(header.block_count > forged_count);
    header.block_count = forged_count;
    forge_active_header(path, header);

    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path);
    auto opened = bm.load_existing_database();
    REQUIRE(opened.has_error());
    CHECK(opened.error().type == core::error_code_t::data_corruption);
    CHECK(message_names(opened.error(), std::to_string(offending_id)));
    CHECK(bm.dev_reusable_snapshot().count(offending_id) == 0);
    // A refused load must leave an EMPTY pool, not a half-installed one (earlier ids already read).
    const auto leftover = bm.dev_reusable_snapshot();
    INFO("ids left in reusable_ after the refused load: " << leftover.size());
    CHECK(leftover.empty());

    remove_file(path);
}

// GATE 3: free_block_id skips a free-list candidate with a LIVE registry handle (reissuing it
// would overwrite live state); peek_free_block_id returned *reusable_.begin() regardless.
TEST_CASE("accounting_bounds: peek_free_block_id names the id free_block_id would hand out", "[bounds]") {
    const auto path = bounds_db_path("peek_mirror");
    remove_file(path);
    bounds_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = reach_steady_state(env, bm);

    auto reusable = bm.dev_reusable_snapshot();
    REQUIRE_FALSE(reusable.empty());

    const uint64_t alive_candidate = *reusable.begin();
    auto held = bm.register_block(alive_candidate);
    REQUIRE(held != nullptr);
    REQUIRE(bm.registry_alive(alive_candidate));

    const uint64_t peeked = bm.peek_free_block_id();
    const uint64_t issued = bm.free_block_id();
    INFO("reusable_.begin()=" << alive_candidate << " peeked=" << peeked << " issued=" << issued);
    CHECK(peeked == issued);
    CHECK(peeked != alive_candidate);

    remove_file(path);
}

// GATE 4: open adopted block_alloc_size only when non-zero, so a zero skipped the geometry check
// and let the engine run on whatever size the caller passed — every block_location off-stride.
TEST_CASE("accounting_bounds: a header claiming block_alloc_size 0 is refused", "[bounds]") {
    const auto path = bounds_db_path("zero_geometry");
    remove_file(path);
    {
        bounds_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
    }

    tstorage::database_header_t header{};
    REQUIRE(otterbrix_test::read_active_durable_header(path, header));
    REQUIRE(header.block_alloc_size == tstorage::DEFAULT_BLOCK_ALLOC_SIZE);
    header.block_alloc_size = 0;
    forge_active_header(path, header);

    const uint64_t other_size = 2 * tstorage::SECTOR_SIZE;
    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path, other_size);
    auto opened = bm.load_existing_database();
    REQUIRE(opened.has_error());
    CHECK(opened.error().type == core::error_code_t::data_corruption);
    CHECK(message_names(opened.error(), "block allocation size"));
    CHECK(bm.block_allocation_size() == other_size);

    remove_file(path);
}

// GATE 5: a void reserve_memory swallowed evict_blocks_or_error's OOM refusal and returned as on
// success, so the caller spent memory the pool never granted — same defect class as `void write()`.
TEST_CASE("accounting_bounds: a reservation that could not be made is reported", "[bounds]") {
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    const uint64_t pool_limit = uint64_t(1) << 20;
    tstorage::buffer_pool_t pool(&resource, pool_limit, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t manager(&resource, fs, pool);

    auto nothing = manager.reserve_memory(0);
    CHECK_FALSE(nothing.has_error());

    auto granted = manager.reserve_memory(pool_limit / 4);
    REQUIRE_FALSE(granted.has_error());
    manager.free_reserved_memory(pool_limit / 4);

    auto refused = manager.reserve_memory(pool_limit * 64);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::out_of_memory);
}

// GATE 6: serialize_free_list's registry-live term is fed disk ids with no extent check; on a
// NON-compacting round nothing frees one, publishing an id the header's own block_count disavows.
TEST_CASE("accounting_bounds: a checkpoint refuses to publish a free-list id its own header disavows", "[bounds]") {
    const auto path = bounds_db_path("writer_vs_reader");
    remove_file(path);

    {
        bounds_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = reach_steady_state(env, bm);
        REQUIRE_FALSE(bm.degraded());

        const uint64_t bogus = bm.total_blocks() + 900;
        REQUIRE(bogus < tstorage::MAXIMUM_BLOCK);
        auto held = bm.register_block(bogus);
        REQUIRE(held != nullptr);
        REQUIRE(bm.registry_alive(bogus));

        const bool committed = checkpoint_round(bm, *table);
        INFO("bogus id " << bogus << ", total_blocks " << bm.total_blocks() << ", committed " << committed
                         << ", degraded " << bm.degraded());
        REQUIRE(bogus >= bm.total_blocks());
        CHECK_FALSE(committed);
        CHECK(bm.degraded());
        REQUIRE(bm.has_allocation_error());
        CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);
        CHECK(message_names(bm.allocation_error(), std::to_string(bogus)));
    }

    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
    auto reopened = bm2.load_existing_database();
    INFO("reopen after the refused round: " << (reopened.has_error() ? reopened.error().what.c_str() : "opened"));
    CHECK_FALSE(reopened.has_error());

    remove_file(path);
}

// (7) column_segment_t must fit its block. In the reload/create constructor, the guarded
// `block` names the constructor PARAMETER already moved-from one line earlier, so `!block` was
// always TRUE and the size check never ran — hidden by the live, identical assert in the move constructors.
#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))
TEST_CASE("components::table::column_segment::a_segment_larger_than_its_block_is_refused") {
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    tstorage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);

    auto registered =
        buffer_manager.register_transient_memory(buffer_manager.block_size(), buffer_manager.block_size());
    REQUIRE_FALSE(registered.has_error());
    auto& block = registered.value();
    REQUIRE(block != nullptr);

    const uint64_t limit = block->block_size();
    REQUIRE(limit > 0);

    // BIGINT on purpose: VALIDITY/STRING_LITERAL types would overrun the block before reaching the guard.
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        ::signal(SIGABRT, SIG_DFL);
        ::signal(SIGSEGV, SIG_DFL);
        ::signal(SIGBUS, SIG_DFL);
        column_segment_t
            oversized(block, complex_logical_type{logical_type::BIGINT}, 0, 0, tstorage::INVALID_BLOCK, 0, limit + 1);
        _exit(oversized.segment_size() == limit + 1 ? 42 : 43);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    INFO("block_size " << limit << ", segment_size " << (limit + 1) << ", wait status " << status);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}
#endif
