#include <catch2/catch_test_macros.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pmr.hpp>
#include <core/result_wrapper.hpp>

#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <unistd.h>

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_block_manager_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    struct test_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        components::table::storage::buffer_pool_t buffer_pool;
        components::table::storage::standard_buffer_manager_t buffer_manager;

        test_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool) {}
    };
} // namespace

TEST_CASE("single_file_block_manager: write and read blocks") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    constexpr size_t NUM_BLOCKS = 5;
    std::vector<uint64_t> block_ids;
    std::vector<std::vector<std::byte>> original_data(NUM_BLOCKS);

    // write blocks
    for (size_t i = 0; i < NUM_BLOCKS; i++) {
        uint64_t id = bm.free_block_id();
        block_ids.push_back(id);

        auto blk =
            std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
        auto* data = blk->buffer();
        auto sz = blk->size();

        // fill with pattern
        for (size_t j = 0; j < sz; j++) {
            data[j] = static_cast<std::byte>((i * 37 + j * 13) & 0xFF);
        }

        // save original data for comparison
        original_data[i].assign(data, data + sz);

        REQUIRE_FALSE(bm.write(*blk, id).has_error());
    }

    REQUIRE(bm.total_blocks() == NUM_BLOCKS);

    // read blocks and compare
    for (size_t i = 0; i < NUM_BLOCKS; i++) {
        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(),
                                             block_ids[i],
                                             static_cast<uint64_t>(bm.block_size()));
        REQUIRE(!bm.read(*blk).has_error());

        auto* data = blk->buffer();
        REQUIRE(std::memcmp(data, original_data[i].data(), original_data[i].size()) == 0);
    }

    cleanup_test_file();
}

TEST_CASE("single_file_block_manager: create, close, load existing") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;

    // create and write
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        uint64_t id = bm.free_block_id();
        auto blk =
            std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
        auto* data = blk->buffer();
        for (size_t j = 0; j < blk->size(); j++) {
            data[j] = static_cast<std::byte>(42);
        }
        REQUIRE_FALSE(bm.write(*blk, id).has_error());

        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // load and read
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        REQUIRE(bm.total_blocks() == 1);

        auto blk =
            std::make_unique<block_t>(env.resource.upstream_resource(), 0, static_cast<uint64_t>(bm.block_size()));
        REQUIRE(!bm.read(*blk).has_error());

        auto* data = blk->buffer();
        for (size_t j = 0; j < blk->size(); j++) {
            REQUIRE(data[j] == static_cast<std::byte>(42));
        }
    }

    cleanup_test_file();
}

TEST_CASE("single_file_block_manager: free list reuse") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    // allocate 3 blocks
    uint64_t id0 = bm.free_block_id();
    uint64_t id1 = bm.free_block_id();
    uint64_t id2 = bm.free_block_id();

    REQUIRE(id0 == 0);
    REQUIRE(id1 == 1);
    REQUIRE(id2 == 2);
    REQUIRE(bm.total_blocks() == 3);

    // free block 1
    bm.mark_as_free(id1);
    REQUIRE(bm.free_blocks() == 1);

    // A7.2 moved the moment of reuse, and this case had to follow it. A release names a block
    // the DURABLE root may still point at (compact() releases exactly the outgoing collection,
    // which is what the durable root describes), so the block is quarantined rather than handed
    // straight back: the next allocation extends the file instead...
    uint64_t during_flight = bm.free_block_id();
    REQUIRE(during_flight != id1);
    REQUIRE(during_flight == 3);
    REQUIRE(bm.free_blocks() == 1); // withheld, NOT lost

    // ...and the durable header is what turns the release into free space. That is the whole of
    // A7.2: the promotion point is the header write's success, so an id released in a round
    // cannot be reissued inside that same round.
    auto free_ptr = bm.serialize_free_list();
    REQUIRE_FALSE(free_ptr.has_error());
    database_header_t promoting_header;
    promoting_header.initialize();
    promoting_header.free_list = free_ptr.value().block_pointer;
    REQUIRE_FALSE(bm.write_header(promoting_header).has_error());

    // NOW the freed block comes back — the original property this case exists for.
    uint64_t id3 = bm.free_block_id();
    REQUIRE(id3 == id1);
    REQUIRE(bm.free_blocks() == 0);

    cleanup_test_file();
}

TEST_CASE("single_file_block_manager: header validation") {
    using namespace components::table::storage;

    main_header_t header;
    header.initialize();
    REQUIRE(header.validate());

    header.magic = 0xDEADBEEF;
    REQUIRE_FALSE(header.validate());

    header.magic = main_header_t::MAGIC_NUMBER;
    header.version = main_header_t::CURRENT_VERSION + 1;
    REQUIRE_FALSE(header.validate());
}

TEST_CASE("single_file_block_manager: free list survives checkpoint/load") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    uint64_t free_blocks_after_serialize = 0;

    // serialize_free_list() itself allocates metadata block(s) from the free list,
    // so free_blocks() after serialize is not simply (freed count).
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        // Allocate 5 blocks (ids 0..4), write dummy data to each
        for (int i = 0; i < 5; i++) {
            uint64_t id = bm.free_block_id();
            auto blk =
                std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
            std::memset(blk->buffer(), static_cast<int>(i), blk->size());
            REQUIRE_FALSE(bm.write(*blk, id).has_error());
        }

        REQUIRE(bm.total_blocks() == 5);

        // Free blocks 1, 2, and 3
        bm.mark_as_free(1);
        bm.mark_as_free(2);
        bm.mark_as_free(3);
        REQUIRE(bm.free_blocks() == 3);

        // Serialize free list (may consume some freed blocks for metadata)
        auto free_list_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_list_ptr.has_error());
        free_blocks_after_serialize = bm.free_blocks();
        REQUIRE(free_blocks_after_serialize > 0);

        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        REQUIRE(bm.free_blocks() == free_blocks_after_serialize);

        // Allocate from free list — should reuse freed block IDs (not allocate new)
        uint64_t reused = bm.free_block_id();
        REQUIRE(reused < 5); // must be a previously freed block, not a new one
        REQUIRE(bm.free_blocks() == free_blocks_after_serialize - 1);
    }

    cleanup_test_file();
}

TEST_CASE("single_file_block_manager: empty free list persistence") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        for (int i = 0; i < 3; i++) {
            uint64_t id = bm.free_block_id();
            auto blk =
                std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
            std::memset(blk->buffer(), 0, blk->size());
            REQUIRE_FALSE(bm.write(*blk, id).has_error());
        }

        REQUIRE(bm.total_blocks() == 3);
        REQUIRE(bm.free_blocks() == 0);

        auto free_list_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_list_ptr.has_error());
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        REQUIRE(bm.free_blocks() == 0);
        // Next alloc should give block 3 (next after 0,1,2)
        uint64_t next = bm.free_block_id();
        REQUIRE(next == 3);
    }

    cleanup_test_file();
}

// ---------------------------------------------------------------------------
// Error-VALUE regression tests: the converted paths return a
// core::result_wrapper_t carrying core::error_code_t::{data_corruption,io_error}
// instead of throwing. Each test drives the error branch, asserts the error
// VALUE, and wraps the failing call in REQUIRE_NOTHROW.
// ---------------------------------------------------------------------------

namespace {
    std::string corrupt_db_path(const char* tag) {
        return "/tmp/test_otterbrix_blockmgr_err_" + std::string(tag) + "_" + std::to_string(::getpid()) + ".otbx";
    }
} // namespace

// Block checksum mismatch -> data_corruption.
// single_file_block_manager_t::read() calls verify_checksum(), which compares the
// first 8 bytes of the block (the checksum slot, written by checksum_and_write)
// against the CRC32c of the payload that follows it. We write a known block, then
// flip ONE payload byte directly in the .otbx so the stored checksum no longer
// matches the recomputed CRC -> verify_checksum() returns false -> read() returns
// error_code_t::data_corruption (NOT a throw/segfault).
TEST_CASE("single_file_block_manager: corrupt block payload -> data_corruption (error value)") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("checksum");
    std::remove(path.c_str());

    test_env_t env;
    uint64_t block_id = 0;
    uint64_t payload_disk_offset = 0;
    std::byte original_byte{};

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());

        block_id = bm.free_block_id();
        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(),
                                             block_id,
                                             static_cast<uint64_t>(bm.block_size()));
        auto* data = blk->buffer(); // payload region (internal_buffer_ + 8-byte checksum header)
        for (size_t j = 0; j < blk->size(); j++) {
            data[j] = static_cast<std::byte>((j * 7 + 1) & 0xFF);
        }
        REQUIRE_FALSE(bm.write(*blk, block_id).has_error()); // checksum_and_write stores CRC in the 8-byte header slot

        // A7.6: commit a header. Without one the reopened file is "blocks on disk, root
        // still the initial iteration-0 header" — indistinguishable from a first-checkpoint
        // crash or a corrupted-slot fallback, and load_existing_database now refuses it
        // loudly. This test measures read()'s error channel, not the open gate, so give the
        // file a legal committed root (meta_block INVALID at iteration >= 1 is the block
        // manager's committed "no metadata root" statement and stays legal).
        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());

        // On disk the block lives at BLOCK_START + block_id * block_allocation_size().
        // Bytes [0,8) of that region are the checksum slot; the payload starts at +8.
        // Corrupting a payload byte (not the slot) guarantees stored-checksum != recomputed.
        payload_disk_offset = BLOCK_START + block_id * bm.block_allocation_size() + sizeof(uint64_t);
        original_byte = data[0];
    }

    // Mutate one payload byte on disk so the persisted CRC no longer matches.
    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekg(static_cast<std::streamoff>(payload_disk_offset));
        char b = 0;
        f.read(&b, 1);
        REQUIRE(f.gcount() == 1);
        b = static_cast<char>(b ^ 0xFF); // flip every bit of this payload byte
        f.seekp(static_cast<std::streamoff>(payload_disk_offset));
        f.write(&b, 1);
        f.flush();
        REQUIRE(f.good());
        // Sanity: we really changed a byte relative to the in-memory original.
        REQUIRE(static_cast<std::byte>(b) != original_byte);
    }

    // Reopen and read the corrupted block back: read() must surface data_corruption
    // as a VALUE, not throw, and must NOT report success.
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.load_existing_database().has_error());

        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(),
                                             block_id,
                                             static_cast<uint64_t>(bm.block_size()));
        core::result_wrapper_t<bool> result = false;
        REQUIRE_NOTHROW(result = bm.read(*blk));
        REQUIRE(result.has_error()); // would be a false pass if the read succeeded
        REQUIRE(result.error().type == core::error_code_t::data_corruption);
    }

    std::remove(path.c_str());
}

// A7.5: a LOAD never creates the file. load_existing_database used to open with
// FILE_CREATE, so probing a missing .otbx silently manufactured a 0-byte file — the probe
// mutated the state it was probing. Now a missing file is its own loud refusal and the
// filesystem is left exactly as it was: nothing is created.
TEST_CASE("single_file_block_manager: load missing file -> io_error, nothing created") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("missing");
    std::remove(path.c_str());
    REQUIRE_FALSE(std::filesystem::exists(path));

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);

    core::result_wrapper_t<bool> result = false;
    REQUIRE_NOTHROW(result = bm.load_existing_database());
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::io_error);
    // "Missing" is loudly distinct from "empty": named as such, and no 0-byte file appears.
    REQUIRE(std::string(result.error().what.c_str()).find("does not exist") != std::string::npos);
    REQUIRE_FALSE(std::filesystem::exists(path));
}

// File open/header IO failure -> io_error.
// A zero-length file (external truncation, or the droppings of the pre-A7.5 FILE_CREATE
// probe) opens but is refused before the main-header read: it is not a database and is
// never silently accepted as an empty table -> io_error with its own distinct message.
TEST_CASE("single_file_block_manager: load empty file -> io_error (error value)") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("empty");
    std::remove(path.c_str());

    // Create a zero-byte file so the header read hits EOF immediately.
    {
        std::ofstream f(path, std::ios::out | std::ios::binary | std::ios::trunc);
        REQUIRE(f.is_open());
    }
    REQUIRE(std::ifstream(path, std::ios::binary).peek() == std::char_traits<char>::eof());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);

    core::result_wrapper_t<bool> result = false;
    REQUIRE_NOTHROW(result = bm.load_existing_database());
    REQUIRE(result.has_error());
    REQUIRE(result.error().type == core::error_code_t::io_error);

    std::remove(path.c_str());
}

// Bad magic/header -> data_corruption.
// Build a valid db, then overwrite the main_header magic (offset 0) with garbage.
// The header read succeeds but main_header_t::validate() fails -> data_corruption
// ("Invalid database file: bad magic or version"), surfaced as a VALUE.
TEST_CASE("single_file_block_manager: load bad-magic header -> data_corruption (error value)") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("badmagic");
    std::remove(path.c_str());

    test_env_t env;
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        database_header_t header;
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // The main_header_t::magic is the first 4 bytes of the file (offset 0).
    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        uint32_t bad_magic = 0xDEADBEEF; // != main_header_t::MAGIC_NUMBER
        REQUIRE(bad_magic != main_header_t::MAGIC_NUMBER);
        f.seekp(0);
        f.write(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic));
        f.flush();
        REQUIRE(f.good());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        core::result_wrapper_t<bool> result = false;
        REQUIRE_NOTHROW(result = bm.load_existing_database());
        REQUIRE(result.has_error());
        REQUIRE(result.error().type == core::error_code_t::data_corruption);
    }

    std::remove(path.c_str());
}

// buffer_pool set_limit / standard_buffer_manager set_memory_limit success path.
// set_limit() returns out_of_memory only when evict_blocks() cannot free enough
// memory for the new limit. With no pinned/un-evictable blocks held in the pool,
// eviction trivially succeeds (used_memory == 0), so the failure branch is not
// reachable from a fresh pool in a unit test. Asserts only the SUCCESS path:
// returns a non-error VALUE and does not throw.
TEST_CASE("buffer_pool/standard_buffer_manager: set_memory_limit success returns non-error value") {
    using namespace components::table::storage;
    test_env_t env;

    // Direct pool: lower the limit on an empty pool -> nothing to evict -> success.
    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_pool.set_limit(uint64_t(1) << 20));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == true);
    }
    // Raising the limit can never fail eviction either.
    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_pool.set_limit(uint64_t(1) << 32));
        REQUIRE_FALSE(r.has_error());
    }
    // Through the buffer manager facade (set_memory_limit delegates to set_limit).
    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_manager.set_memory_limit(uint64_t(1) << 24));
        REQUIRE_FALSE(r.has_error());
    }
}

// --- M7: the free list is DISK BYTES, so its invariants belong on the error channel ------
//
// free_block_id() guarded "the id I am about to hand out has no live registry handle" with a
// bare assert(). The free list it draws from is deserialized straight out of the .otbx, so
// that guard sits on a path fed by untrusted bytes: under NDEBUG it disappears and a corrupt
// free list quietly hands a caller an id that aliases live table state, which is then
// overwritten with a valid CRC — silent corruption in exactly the build where it matters.
// The invariant is real; the abort was the wrong way to state it, and disappearing was worse.
TEST_CASE("single_file_block_manager: a free list naming a LIVE block is refused, not asserted") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("free_list_alias");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());

    // A block with a LIVE registry handle: exactly what must never be reissued.
    uint64_t live_id = bm.free_block_id();
    auto live_handle = bm.register_block(live_id);
    REQUIRE(live_handle != nullptr);
    REQUIRE(bm.free_blocks() == 0);

    // Forge a free-list chain that names it, written by the very same metadata writer
    // serialize_free_list() uses, and feed it through the real deserializer. No file bytes
    // are laid by hand: this is the production load path fed corrupt content.
    meta_block_pointer_t poisoned;
    {
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        writer.write<uint64_t>(1);
        writer.write<uint64_t>(live_id);
        REQUIRE_FALSE(writer.flush().has_error());
        poisoned = writer.get_block_pointer();
    }
    REQUIRE(!bm.deserialize_free_list(poisoned).has_error());
    REQUIRE(bm.free_blocks() >= 1);

    // The allocator must refuse to reissue the live id...
    uint64_t issued = bm.free_block_id();
    CHECK(issued != live_id);
    // ...and must say so through the error channel, in every build, not abort in some.
    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    // A checkpoint built on a free list known to be corrupt must not become the durable root.
    database_header_t header;
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::data_corruption);

    std::remove(path.c_str());
}

// Rule 19, THIRD instance of the same class, one level further in: mark_as_free's domain guard
// was an assert() too, and the ids that reach it are not this code's own. data_table_t::compact
// collects them from the live collection, and a segment's big-string overflow ids are rebuilt
// verbatim from data_pointer_t::overflow_blocks — read straight out of the .otbx, with no domain
// check anywhere between the reader and here. Under NDEBUG the assert is gone and a
// transient-domain id (>= MAXIMUM_BLOCK) enters the free pool, gets promoted by the next durable
// header, gets handed out, and block_location wraps it: (2^62 + N) * 2^18 == N * 2^18, a REAL
// live block, rewritten with a valid CRC. The check belongs on the free-pool boundary, in every
// build, and it must not abort — mark_as_free runs on an actor thread (rule 9).
TEST_CASE("single_file_block_manager: a transient-domain id offered to mark_as_free is refused, not asserted") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("free_transient_release");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());

    const uint64_t real_id = bm.free_block_id();
    REQUIRE(real_id < MAXIMUM_BLOCK);
    REQUIRE(bm.free_blocks() == 0);

    // An id no writer of this format can emit, offered through the release path.
    REQUIRE_NOTHROW(bm.mark_as_free(MAXIMUM_BLOCK + 7));

    // It never entered the free pool — neither half of it.
    CHECK(bm.free_blocks() == 0);
    CHECK(bm.dev_reusable_snapshot().count(MAXIMUM_BLOCK + 7) == 0);
    CHECK(bm.dev_pending_free_snapshot().count(MAXIMUM_BLOCK + 7) == 0);

    // ...and it said so through the error channel, in every build.
    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    // A checkpoint standing on state known to be corrupt must not become the durable root.
    database_header_t header;
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::data_corruption);

    std::remove(path.c_str());
}

// Rule 19, same untrusted input, second reachable abort. deserialize_free_list inserts every
// id it reads without checking the ONE thing that makes an id addressable: a free list
// carrying an id from the transient domain (>= MAXIMUM_BLOCK) gets it handed out by
// free_block_id, and block_location then multiplies it out — (2^62 + N) * 2^18 wraps to
// N * 2^18, i.e. a REAL block, rewritten with a valid CRC. That is guarded by an assert too,
// so it aborts in a debug build and corrupts silently in a release one. The load path has a
// perfectly good error channel; the check belongs there, before the id is ever in the list.
TEST_CASE("single_file_block_manager: a free list naming a transient-domain id is data_corruption") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("free_list_transient");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    uint64_t real_id = bm.free_block_id();
    REQUIRE(real_id < MAXIMUM_BLOCK);

    meta_block_pointer_t poisoned;
    {
        metadata_manager_t meta_mgr(bm);
        metadata_writer_t writer(meta_mgr);
        writer.write<uint64_t>(1);
        writer.write<uint64_t>(MAXIMUM_BLOCK + 7); // an id no writer of this format can emit
        REQUIRE_FALSE(writer.flush().has_error());
        poisoned = writer.get_block_pointer();
    }

    auto loaded = bm.deserialize_free_list(poisoned);
    REQUIRE(loaded.has_error());
    CHECK(loaded.error().type == core::error_code_t::data_corruption);
    // ...and it never entered the pool it would have been issued from.
    CHECK(bm.free_blocks() == 0);

    std::remove(path.c_str());
}

// --- Rule 2 / rule 19: the block allocation size is DISK BYTES too --------------------
//
// block_manager_t::set_block_allocation_size used to be a `void` guarded by a `throw
// std::runtime_error` on a condition that could never hold (`block_alloc_size_ ==
// INVALID_INDEX`, while the constructor always sets a real size). So it validated NOTHING —
// and its one caller is load_existing_database, feeding it `active.block_alloc_size` read
// straight out of the header sector.
//
// A degenerate value there is not theoretical: block_size() is `block_alloc_size_ -
// DEFAULT_BLOCK_HEADER_SIZE`, an UNSIGNED subtraction, so any size <= 8 wraps to ~1.8e19.
// metadata_manager_t then carves 64 sub-blocks out of that, column loads compare segment
// sizes against it, and every one of those reads runs off the end of a 256 KiB buffer. The
// throw was the wrong shape as well: this runs on the open path, where an exception makes
// the database permanently unopenable (rules 2/6 — loud, but not fatal).
TEST_CASE("block_manager: a degenerate block allocation size is refused, not adopted") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("alloc_size");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());
    const uint64_t good = bm.block_allocation_size();
    REQUIRE(good == DEFAULT_BLOCK_ALLOC_SIZE);

    // The value that wraps block_size().
    auto tiny = bm.set_block_allocation_size(4);
    REQUIRE(tiny.has_error());
    CHECK(tiny.error().type == core::error_code_t::data_corruption);
    CHECK(bm.block_allocation_size() == good);
    CHECK(bm.block_size() == good - DEFAULT_BLOCK_HEADER_SIZE); // no unsigned wrap

    // Zero: the header's own "unset" value, which must not be adopted either.
    auto zero = bm.set_block_allocation_size(0);
    REQUIRE(zero.has_error());
    CHECK(bm.block_allocation_size() == good);

    // A size the file layout cannot address sector-aligned.
    auto unaligned = bm.set_block_allocation_size(DEFAULT_BLOCK_ALLOC_SIZE + 1);
    REQUIRE(unaligned.has_error());
    CHECK(bm.block_allocation_size() == good);

    // A legitimate one is still adopted — this is a guard, not a freeze.
    auto ok = bm.set_block_allocation_size(SECTOR_SIZE * 8);
    REQUIRE_FALSE(ok.has_error());
    CHECK(bm.block_allocation_size() == SECTOR_SIZE * 8);

    std::remove(path.c_str());
}

// Same guard, at the other end: a manager constructed with an unusable block size must fail
// to CREATE the file rather than lay down a header nothing can ever open.
TEST_CASE("block_manager: create_new_database refuses an unusable block allocation size") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("alloc_size_create");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path, 4);
    auto created = bm.create_new_database();
    REQUIRE(created.has_error());
    CHECK(created.error().type == core::error_code_t::data_corruption);

    std::remove(path.c_str());
}

// ---------------------------------------------------------------------------------------
// ITEM C — unregister_block(block_handle_t&) must check IDENTITY, not just the id.
//
// The registry is keyed by block id, and the handle destructor used to erase blocks_[id]
// unconditionally. That is only safe while an id is never re-registered between a stale
// handle's release and its destruction — which is exactly what A7.2/A7.3 made the NORMAL
// case: data_table_t::compact mark_as_free's + unregister_block's the outgoing collection's
// ids while its segments still own handles for them, the ids go to pending_free_, a
// committed header promotes them to reusable_, and a later round hands one back out and
// register_block()s a FRESH handle for it.
//
// If the stale handle outlives that (data_table_t::row_group() hands out shared_ptr copies
// by value, so a holder can outlive compact), its destructor erased the LIVE handle's slot.
// From that instant registry_alive(id) is false while a live segment still reads the block —
// and registry_alive is the subtraction that stops A7.3's reclaim from freeing live table
// state. The same erase also defeats register_block's dedup, so two handles with independent
// buffers back one block id and one of their writes is lost.
//
// RED on HEAD: after H1 is destroyed, registry_alive(id) is false and register_block hands
// back a THIRD handle instead of the live H2.
// ---------------------------------------------------------------------------------------
TEST_CASE("block_manager: a stale handle's destructor must not erase the live handle's slot") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    const uint64_t id = bm.free_block_id();

    // The outgoing collection's handle for `id`.
    auto stale = bm.register_block(id);
    REQUIRE(stale);
    REQUIRE(bm.registry_alive(id));

    // data_table_t::compact: release the id and drop the registry entry while `stale` — the
    // outgoing collection's segment handle — is STILL ALIVE.
    bm.mark_as_free(id);
    bm.unregister_block(id);
    CHECK_FALSE(bm.registry_alive(id));

    // A later round hands the id back out and registers a FRESH handle for it. This is the
    // live table state from here on.
    auto live = bm.register_block(id);
    REQUIRE(live);
    CHECK(live.get() != stale.get());
    CHECK(bm.registry_alive(id));

    // The stale holder finally lets go. Its destructor must not touch the live handle's slot.
    stale.reset();

    INFO("after the stale handle died, the live handle's registry entry must survive");
    CHECK(bm.registry_alive(id));
    // ...and register_block must still DEDUP onto it, rather than minting a second handle
    // with an independent buffer for the same block.
    auto again = bm.register_block(id);
    CHECK(again.get() == live.get());

    // And the live handle's own destructor still cleans up after itself: the identity check
    // must not turn the erase into a leak of expired slots.
    again.reset();
    live.reset();
    CHECK_FALSE(bm.registry_alive(id));

    cleanup_test_file();
}
