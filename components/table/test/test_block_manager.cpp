#include <catch2/catch.hpp>
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

        bm.write(*blk, id);
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
        bm.write(*blk, id);

        database_header_t header;
        header.initialize();
        bm.write_header(header);
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

    // next allocation should reuse block 1
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
            bm.write(*blk, id);
        }

        REQUIRE(bm.total_blocks() == 5);

        // Free blocks 1, 2, and 3
        bm.mark_as_free(1);
        bm.mark_as_free(2);
        bm.mark_as_free(3);
        REQUIRE(bm.free_blocks() == 3);

        // Serialize free list (may consume some freed blocks for metadata)
        auto free_list_ptr = bm.serialize_free_list();
        free_blocks_after_serialize = bm.free_blocks();
        REQUIRE(free_blocks_after_serialize > 0);

        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        bm.write_header(header);
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
            bm.write(*blk, id);
        }

        REQUIRE(bm.total_blocks() == 3);
        REQUIRE(bm.free_blocks() == 0);

        auto free_list_ptr = bm.serialize_free_list();
        database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        bm.write_header(header);
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
        bm.write(*blk, block_id); // checksum_and_write stores CRC in the 8-byte header slot

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

// File open/header IO failure -> io_error.
// load_existing_database() opens with FILE_CREATE (so a missing file is created
// empty, not an open failure). Reading the main header from a zero-length file
// makes the positional read return false -> io_error ("Failed to read main header").
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
        bm.write_header(header);
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
