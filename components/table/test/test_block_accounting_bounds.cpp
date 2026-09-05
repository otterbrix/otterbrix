// Block accounting: the boundary the guards MEASURE, and the failures they REPORT.
// Six gates on one contract, all in components/table/storage, numbered in TEST_CASE order —
// each gate's own comment below explains its specific bug. Corruption is injected by forging
// header bytes in place and recomputing the checksum, so the file stays a VALID header
// carrying a statement no writer of this build would make.

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

    // The checkpoint sequence table_storage_t::checkpoint runs, with no fault injection: these
    // gates need a REAL durable root and a REAL published free list, not a crash state.
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
        tstorage::database_header_t header;
        header.initialize();
        header.free_list = free_ptr.value().block_pointer;
        return !bm.write_header(header).has_error();
    }

    // Three compacting rounds: from the third on an unchanged table is a closed cycle, so the
    // free list is non-empty and the block count is stable.
    std::unique_ptr<data_table_t> reach_steady_state(bounds_env_t& env, tstorage::single_file_block_manager_t& bm) {
        auto table = make_table(env, bm);
        append_rows(*table, env, 0, BOUNDS_ROWS);
        for (int warmup = 0; warmup < 3; ++warmup) {
            REQUIRE(table->compact(WATERMARK));
            REQUIRE(checkpoint_round(bm, *table));
        }
        return table;
    }

    uint64_t slot_offset(uint64_t iteration) { return (iteration % 2 == 1) ? tstorage::SECTOR_SIZE : 2 * tstorage::SECTOR_SIZE; }

    // Rewrite the slot the ACTIVE header lives in, with the checksum recomputed over the forged
    // bytes: the file must stay a file whose header validates, or the gate would be testing the
    // checksum instead of the field.
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

// ---------------------------------------------------------------------------------------
// GATE 1 — THE RELEASE GUARD MEASURES THE FILE, NOT THE DOMAIN.
//
// mark_as_free's guard rejected only the TRANSIENT domain (>= 2^62), so an id between the
// file's end and 2^62 (the shape a corrupt free list or data_pointer_t delivers) walked into
// the free pool, was promoted by the next header, and was handed out by free_block_id — the
// write then seeks past EOF and grows the file by the gap while total_blocks() still claims
// the old extent.
// ---------------------------------------------------------------------------------------
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

    // The guard must not be TIGHTER than the file either: the last block of the file is a
    // perfectly ordinary release, and refusing it would quarantine live accounting.
    const uint64_t inside = blocks - 1;
    bm.mark_as_free(inside);
    CHECK_FALSE(bm.degraded());
    CHECK((bm.dev_pending_free_snapshot().count(inside) != 0 || bm.dev_reusable_snapshot().count(inside) != 0));

    // ...and an id the file does not contain is refused and REPORTED, exactly like a transient
    // one, because it is exactly as unaddressable.
    const uint64_t past_end = blocks + 4;
    REQUIRE(past_end < tstorage::MAXIMUM_BLOCK);
    bm.mark_as_free(past_end);
    CHECK(bm.degraded());
    CHECK(bm.dev_pending_free_snapshot().count(past_end) == 0);
    CHECK(bm.dev_reusable_snapshot().count(past_end) == 0);
    CHECK(bm.total_blocks() == blocks);

    // Taken all the way to the file: "it sat in a pool" is not yet damage, the checkpoint that
    // PUBLISHES the free list is. Either the release above is refused (and the commit with it),
    // or the file ends up with a free list naming an id past its own block_count.
    CHECK_FALSE(checkpoint_round(bm, *table));

    auto report = otterbrix_test::walk_blocks(bm, path, &env.resource);
    REQUIRE(report.ok);
    uint64_t free_listed_past_end = 0;
    for (auto id : report.free_list_content) {
        if (id >= report.block_count) {
            ++free_listed_past_end;
        }
    }
    WARN("[bounds gate 1] walker: durable_block_count=" << report.block_count << " chain="
         << report.chain_blocks.size() << " durable_data=" << report.durable_data.size() << " registry="
         << report.registry_live.size() << " freelist=" << report.free_list_content.size() << " unexplained="
         << report.unexplained.size() << " overlap=" << report.reachable_free_overlap.size()
         << " free_listed_past_end=" << free_listed_past_end);
    CHECK(free_listed_past_end == 0);
    CHECK(report.unexplained.empty());
    CHECK(report.reachable_free_overlap.empty());

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// GATE 2 — THE SAME BOUNDARY ON THE DISK PATH, WHERE THERE IS AN ERROR CHANNEL.
//
// deserialize_free_list reads ids straight into reusable_ (the pool free_block_id draws from)
// and rejected only the transient domain, so a list naming a block past the file's own
// recorded extent opened CLEANLY and armed the next allocation to write past EOF. Corruption
// is injected by forging the ACTIVE header's block_count down to the largest id its own free
// list publishes.
// ---------------------------------------------------------------------------------------
TEST_CASE("accounting_bounds: a free list naming a block past the file's extent is refused at open",
          "[bounds]") {
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
        // block_count is an EXTENT: the last legal id is block_count - 1. Claiming exactly
        // `offending_id` blocks therefore puts that one id (and nothing below it) outside.
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
    // The refused id never reached the pool the allocator draws from.
    CHECK(bm.dev_reusable_snapshot().count(offending_id) == 0);
    // ...and neither did ANY id of the refused list: a manager whose load was refused must
    // hold an EMPTY pool, not a half-installed one (ids before the offender were already read).
    const auto leftover = bm.dev_reusable_snapshot();
    INFO("ids left in reusable_ after the refused load: " << leftover.size());
    CHECK(leftover.empty());

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// GATE 3 — THE PEEK MIRRORS THE ALLOCATOR.
//
// free_block_id skips (permanently) a free-list candidate that still has a LIVE registry
// handle, since reissuing it would overwrite live state with a valid CRC. peek_free_block_id
// returned *reusable_.begin() regardless — disagreeing with the allocator exactly when the
// free list is corrupt, the only case the peek matters.
// ---------------------------------------------------------------------------------------
TEST_CASE("accounting_bounds: peek_free_block_id names the id free_block_id would hand out", "[bounds]") {
    const auto path = bounds_db_path("peek_mirror");
    remove_file(path);
    bounds_env_t env;

    tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());
    auto table = reach_steady_state(env, bm);

    auto reusable = bm.dev_reusable_snapshot();
    REQUIRE_FALSE(reusable.empty());

    // The first candidate the allocator would look at, made live: a registered handle held by
    // this test stands in for the live column_segment_t that owns it in production.
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

// ---------------------------------------------------------------------------------------
// GATE 4 — A HEADER THAT DECLARES NO GEOMETRY IS NOT A HEADER TO OPEN.
//
// The open adopted `active.block_alloc_size` only when non-zero AND different from the
// manager's current size, so a zero skipped the geometry check entirely and the engine ran on
// whatever size the CALLER passed to the constructor — legalising a header shape no writer of
// this build produces, at the price of every block_location computed with the wrong stride.
// ---------------------------------------------------------------------------------------
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

    // Constructed with a DIFFERENT, legal size: with the zero accepted, the open would keep
    // it silently and address every block in the file at the wrong stride.
    const uint64_t other_size = 2 * tstorage::SECTOR_SIZE;
    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm(env2.buffer_manager, env2.fs, path, other_size);
    auto opened = bm.load_existing_database();
    REQUIRE(opened.has_error());
    CHECK(opened.error().type == core::error_code_t::data_corruption);
    CHECK(message_names(opened.error(), "block allocation size"));
    // A refused header changes nothing: the manager keeps the size it was constructed with.
    CHECK(bm.block_allocation_size() == other_size);

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// GATE 5 — A RESERVATION THAT DID NOT HAPPEN MUST SAY SO.
//
// A void reserve_memory swallows evict_blocks_or_error's refusal on OOM and returns exactly
// as it does on success, so the caller goes on to spend memory the pool never granted -- the
// same defect class as a `void write()` one level down.
// ---------------------------------------------------------------------------------------
TEST_CASE("accounting_bounds: a reservation that could not be made is reported", "[bounds]") {
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    const uint64_t pool_limit = uint64_t(1) << 20;
    tstorage::buffer_pool_t pool(&resource, pool_limit, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t manager(&resource, fs, pool);

    // Nothing to reserve is trivially reserved.
    auto nothing = manager.reserve_memory(0);
    CHECK_FALSE(nothing.has_error());

    // Comfortably inside the limit: granted, and the grant is reported as such.
    auto granted = manager.reserve_memory(pool_limit / 4);
    REQUIRE_FALSE(granted.has_error());
    manager.free_reserved_memory(pool_limit / 4);

    // Larger than the pool can ever hold, with nothing evictable to make room: the reservation
    // CANNOT be made, and that is the answer the caller must get.
    auto refused = manager.reserve_memory(pool_limit * 64);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::out_of_memory);
}

// ---------------------------------------------------------------------------------------
// GATE 6 -- THE WRITER OBEYS THE READER'S BOUNDARY.
//
// Gate 2 made an id at or past block_count refused at open. serialize_free_list's third term
// (registry-live ids not named by the root) is fed disk ids with no extent check
// (column_data.cpp/column_state.cpp hand block ids straight to register_block), and on a
// NON-compacting checkpoint nothing calls mark_as_free on such an id, so it got published
// under a header whose own block_count disavows it — turning a recoverable leak into a file
// that refuses to ever open again. The writer must refuse the round instead.
// ---------------------------------------------------------------------------------------
TEST_CASE("accounting_bounds: a checkpoint refuses to publish a free-list id its own header disavows",
          "[bounds]") {
    const auto path = bounds_db_path("writer_vs_reader");
    remove_file(path);

    {
        bounds_env_t env;
        tstorage::single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE_FALSE(bm.create_new_database().has_error());
        auto table = reach_steady_state(env, bm);
        REQUIRE_FALSE(bm.degraded());

        // What the loader does with a corrupt pointer: an id past the file's end (far below
        // the transient domain) installed as a LIVE registry entry — register_block performs
        // no extent check, that is the production path.
        const uint64_t bogus = bm.total_blocks() + 900;
        REQUIRE(bogus < tstorage::MAXIMUM_BLOCK);
        auto held = bm.register_block(bogus);
        REQUIRE(held != nullptr);
        REQUIRE(bm.registry_alive(bogus));

        // A NON-compacting round: nothing calls mark_as_free on the bogus id, so the only
        // guard between it and the durable free list is serialize_free_list's own.
        const bool committed = checkpoint_round(bm, *table);
        INFO("bogus id " << bogus << ", total_blocks " << bm.total_blocks() << ", committed "
                         << committed << ", degraded " << bm.degraded());
        REQUIRE(bogus >= bm.total_blocks());
        CHECK_FALSE(committed);
        CHECK(bm.degraded());
        REQUIRE(bm.has_allocation_error());
        CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);
        CHECK(message_names(bm.allocation_error(), std::to_string(bogus)));
    }

    // The refused round landed NO header, so the durable root is still the last committed one
    // and the file must open (an open failing here would be a file bricked by its own writer).
    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
    auto reopened = bm2.load_existing_database();
    INFO("reopen after the refused round: "
         << (reopened.has_error() ? reopened.error().what.c_str() : "opened"));
    CHECK_FALSE(reopened.has_error());

    remove_file(path);
}

// (7) The seventh boundary, measured inside a constructor: column_segment_t must fit the
// block it's built over (assert(!block || segment_size_ <= block_manager().block_size())). In
// the reload/create constructor, `block` there named the CONSTRUCTOR PARAMETER, already moved
// into the member one line earlier — a moved-from shared_ptr is guaranteed empty, so `!block`
// was always TRUE and the size check never ran. The identical assert in the move constructors
// reads the MEMBER and is live, which is what hid the dead one.
//
// No caller can ever pass a null handle here (create_segment, checkpoint, and reload all
// forward an error-checked path ending in make_shared/register_block, never null), so the
// size guard is the only one actually protecting anything.
//
// Built in a child process: the correct answer is a deliberate abort.
#if !defined(NDEBUG) && (defined(__unix__) || defined(__APPLE__))
TEST_CASE("components::table::column_segment::a_segment_larger_than_its_block_is_refused") {
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    tstorage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);

    auto registered = buffer_manager.register_transient_memory(buffer_manager.block_size(), buffer_manager.block_size());
    REQUIRE_FALSE(registered.has_error());
    auto& block = registered.value();
    REQUIRE(block != nullptr);

    // The number the guard compares against, read from the same road the constructor takes.
    const uint64_t limit = block->block_manager.block_size();
    REQUIRE(limit > 0);

    // BIGINT on purpose: VALIDITY and STRING_LITERAL memset/format segment_size() bytes inside
    // the constructor, so an oversize segment of those types would run off the block before
    // reaching any guard, and the child would die of the overrun rather than of the check.
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        ::signal(SIGABRT, SIG_DFL);
        ::signal(SIGSEGV, SIG_DFL);
        ::signal(SIGBUS, SIG_DFL);
        column_segment_t oversized(block,
                                   complex_logical_type{logical_type::BIGINT},
                                   0,
                                   0,
                                   tstorage::INVALID_BLOCK,
                                   0,
                                   limit + 1);
        // Reached only if the guard let it through: report the size it accepted.
        _exit(oversized.segment_size() == limit + 1 ? 42 : 43);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    INFO("block_size " << limit << ", segment_size " << (limit + 1) << ", wait status " << status);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}
#endif
