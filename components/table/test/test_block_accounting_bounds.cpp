// Block accounting: the boundary the guards MEASURE, and the failures they REPORT.
//
// Six gates on one contract, all in components/table/storage, numbered in TEST_CASE order:
//
//   (1) the release guard (mark_as_free) measured the ADDRESSABLE DOMAIN (>= MAXIMUM_BLOCK)
//       instead of the FILE. An id between the end of the file and 2^62 is not addressable
//       either -- writing it seeks past EOF and grows the file by the gap -- and that is
//       precisely the shape a corrupt free list or a corrupt data_pointer_t delivers, because
//       both are read straight off the .otbx.
//   (2) the same boundary on the DISK path: deserialize_free_list HAS an error channel and did
//       not use it for this. The refusal must also be ALL-OR-NOTHING: a refused list must not
//       leave the ids that preceded the offender sitting in the allocator's pool.
//   (3) peek_free_block_id must name the id free_block_id would hand out. It did not: the real
//       allocator skips (and permanently drops) a candidate that still has a live handle in the
//       block registry, and the peek returned the first candidate regardless.
//   (4) a header claiming block_alloc_size == 0 skipped the geometry check entirely and let the
//       engine run on whatever size the CALLER happened to construct the manager with. No
//       writer in this build emits a zero (database_header_t::initialize and write_header both
//       store block_allocation_size(), which set_block_allocation_size has already proven to be
//       a non-zero sector multiple), so the branch legalises a header shape that only
//       corruption produces.
//   (5) reserve_memory had a void signature, so a reservation that could NOT be made was
//       indistinguishable from one that was, and the caller carried on spending memory the
//       pool never granted.
//   (6) the WRITER of the free list must obey the READER's boundary. serialize_free_list kept
//       the old domain filter (< MAXIMUM_BLOCK) after gate (2) moved the reader to the file's
//       extent, so one build could COMMIT a header whose own free list names an id past its
//       own recorded block_count -- and then refuse to ever open the file it just wrote.
//
// Corruption is injected the same way the neighbouring gates inject it: header bytes are
// forged in place and the checksum recomputed, so the file is a VALID header carrying a
// statement no writer of this build would make.

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
// mark_as_free is fed by disk bytes (data_table_t::compact rebuilds a segment's overflow ids
// verbatim from data_pointer_t::overflow_blocks, and reclaim_superseded_root hands it the
// durable root's own data-block list). Its guard rejected only the TRANSIENT domain
// (>= 2^62), so an id between the end of the file and 2^62 walked straight into the free
// pool, was promoted by the next committed header, and was then handed out by free_block_id:
// block_location() seeks BLOCK_START + id * alloc, past EOF, and the write grows the file by
// the whole gap while total_blocks() still claims the old extent.
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

    // THE CONSEQUENCE, taken all the way to the file, because "it sat in a pool" is not yet
    // damage. The round that follows the bad release is what would PUBLISH it: the free list
    // it serializes is reusable_ u pending_free_, and the header it commits records
    // block_count. A list naming an id past that count is a loaded gun for the NEXT process to
    // open the file -- deserialize_free_list drops it into reusable_ and the first allocation
    // writes past EOF. Either the release is refused here (and the latch refuses the commit),
    // or the file ends up carrying that statement.
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
// deserialize_free_list reads ids straight out of the .otbx and drops them into reusable_ --
// the pool free_block_id draws from RIGHT NOW. It rejected the transient domain and nothing
// else, so a list naming a block past the file's own recorded extent opened CLEANLY and armed
// the very next allocation to write past EOF.
//
// The corruption is injected by forging the ACTIVE header's block_count down to the largest
// id its own free list publishes: the header now says "the file has N blocks" while the list
// it points at says "block N is free". Nothing else in the file is touched.
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
    // ...and neither did ANY id of the refused list. The list is published sorted, so every id
    // below the offender was read BEFORE the refusal; installing them and then refusing is a
    // half-installed open -- exactly what the geometry gate above this call exists to prevent.
    // A manager whose load was refused must hold an EMPTY pool, not a partial one.
    const auto leftover = bm.dev_reusable_snapshot();
    INFO("ids left in reusable_ after the refused load: " << leftover.size());
    CHECK(leftover.empty());

    remove_file(path);
}

// ---------------------------------------------------------------------------------------
// GATE 3 — THE PEEK MIRRORS THE ALLOCATOR.
//
// free_block_id refuses a free-list candidate that still has a LIVE handle in the block
// registry (reissuing it would overwrite live table state with a valid CRC), drops it for
// good, and moves to the next candidate. peek_free_block_id returned *reusable_.begin()
// regardless -- so the two disagreed exactly in the case the free list is corrupt, which is
// the only case the peek would be consulted about. The comment above it has always required
// the mirror; the code did not implement it.
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
// The open adopted `active.block_alloc_size` only when it was non-zero AND differed from the
// manager's current size. A zero therefore skipped the geometry check entirely and the engine
// ran on whatever the CALLER passed to the constructor -- a compatibility branch for a header
// shape that no writer of this build produces (initialize() stores DEFAULT_BLOCK_ALLOC_SIZE,
// write_header stores block_allocation_size(), and set_block_allocation_size has already
// proven that to be a non-zero sector multiple). What it actually legalises is a corrupt
// header, and the price is every block_location in the file computed with the wrong stride.
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

    // A manager constructed with a DIFFERENT (perfectly legal) size. With the zero accepted,
    // the open silently keeps this size and every block in the file is then addressed at the
    // wrong stride; nothing anywhere reports that the file's own geometry was never read.
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
// Gate 2 moved the READER of the durable free list to the file's extent: an id at or past
// the header's own block_count is data_corruption and the open is refused. The WRITER of
// that very list -- serialize_free_list's third term, the registry-live ids not named by the
// root under construction -- must not stop at the domain filter (< MAXIMUM_BLOCK). Those ids
// are DISK-FED with no extent check on the way in: column_data.cpp and column_state.cpp hand
// data_pointer_t::block_pointer.block_id / overflow_blocks straight to register_block. On a
// NON-compacting checkpoint no mark_as_free ever sees such an id, so nothing latches, the
// list publishes it, and write_header stamps block_count = total_blocks() beneath it. The
// engine has now COMMITTED a file that its own next open refuses forever -- corruption
// (recoverable as a leak) has been converted into an unopenable database (not recoverable).
// The writer must refuse the round instead: no header lands, the previous root stands, and
// the file keeps opening.
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

        // The registration the loader performs, fed by a corrupt pointer: an id past the end
        // of the file (but far below the transient domain) installed as a LIVE registry entry.
        // register_block performs no extent check -- that is the production path.
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

    // The other half of the property, and the reason the refusal points the safe way: the
    // refused round landed NO header, so the durable root is still the last committed one and
    // the file must open. An open that FAILS here with data_corruption naming the bogus id is a
    // file bricked by its own writer, unrepairable from inside the engine.
    bounds_env_t env2;
    tstorage::single_file_block_manager_t bm2(env2.buffer_manager, env2.fs, path);
    auto reopened = bm2.load_existing_database();
    INFO("reopen after the refused round: "
         << (reopened.has_error() ? reopened.error().what.c_str() : "opened"));
    CHECK_FALSE(reopened.has_error());

    remove_file(path);
}

// (7) The SEVENTH boundary, and the only one in this file that is measured inside a
// constructor: a column_segment_t must fit the block it is built over. The guard reads
//
//     assert(!block || segment_size_ <= block_manager().block_size());
//
// and in the reload/create constructor `block` there named the CONSTRUCTOR PARAMETER, which
// the initializer list had already moved into the member one line earlier -- a moved-from
// std::shared_ptr is guaranteed empty, so `!block` was TRUE for every construction and the
// comparison behind it was never evaluated. The identical assert in the two move constructors
// below it reads the MEMBER and is live, which is what made the dead one invisible.
//
// Reachability of the null half, established by reading every construction site rather than
// assumed: create_segment forwards an error-checked register_transient_memory (both of its
// branches end in make_shared / pin(...).block_handle(), never null), and column_data_t's
// checkpoint (column_data.cpp:749) and reload (column_data.cpp:1041) paths forward
// block_manager_t::register_block, which returns either a locked live entry or a make_shared.
// No caller can deliver a null handle, which is why the segment's arena is taken from
// `this->block` unconditionally -- and why the surviving guard is the SIZE one.
//
// The oversize segment is built in a child process: the correct answer is a deliberate abort.
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
