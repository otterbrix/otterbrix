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
#include <components/table/storage/partial_block_manager.hpp>

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

    for (size_t i = 0; i < NUM_BLOCKS; i++) {
        uint64_t id = bm.free_block_id();
        block_ids.push_back(id);

        auto blk =
            std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
        auto* data = blk->buffer();
        auto sz = blk->size();

        for (size_t j = 0; j < sz; j++) {
            data[j] = static_cast<std::byte>((i * 37 + j * 13) & 0xFF);
        }

        original_data[i].assign(data, data + sz);

        REQUIRE_FALSE(bm.write(*blk, id).has_error());
    }

    REQUIRE(bm.total_blocks() == NUM_BLOCKS);

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

        database_header_t header{};
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

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

    uint64_t id0 = bm.free_block_id();
    uint64_t id1 = bm.free_block_id();
    uint64_t id2 = bm.free_block_id();

    REQUIRE(id0 == 0);
    REQUIRE(id1 == 1);
    REQUIRE(id2 == 2);
    REQUIRE(bm.total_blocks() == 3);

    bm.mark_as_free(id1);
    REQUIRE(bm.free_blocks() == 1);

    // Released blocks may still be named by the durable root, so they stay quarantined until a header commits.
    uint64_t during_flight = bm.free_block_id();
    REQUIRE(during_flight != id1);
    REQUIRE(during_flight == 3);
    REQUIRE(bm.free_blocks() == 1); // withheld, not lost

    auto free_ptr = bm.serialize_free_list();
    REQUIRE_FALSE(free_ptr.has_error());
    database_header_t promoting_header{};
    promoting_header.initialize();
    promoting_header.free_list = free_ptr.value().block_pointer;
    REQUIRE_FALSE(bm.write_header(promoting_header).has_error());

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

    // serialize_free_list() itself allocates metadata blocks, so free_blocks() isn't simply the freed count.
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.create_new_database().has_error());

        for (int i = 0; i < 5; i++) {
            uint64_t id = bm.free_block_id();
            auto blk =
                std::make_unique<block_t>(env.resource.upstream_resource(), id, static_cast<uint64_t>(bm.block_size()));
            std::memset(blk->buffer(), static_cast<int>(i), blk->size());
            REQUIRE_FALSE(bm.write(*blk, id).has_error());
        }

        REQUIRE(bm.total_blocks() == 5);

        bm.mark_as_free(1);
        bm.mark_as_free(2);
        bm.mark_as_free(3);
        REQUIRE(bm.free_blocks() == 3);

        auto free_list_ptr = bm.serialize_free_list();
        REQUIRE_FALSE(free_list_ptr.has_error());
        free_blocks_after_serialize = bm.free_blocks();
        REQUIRE(free_blocks_after_serialize > 0);

        database_header_t header{};
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        REQUIRE(bm.free_blocks() == free_blocks_after_serialize);

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
        database_header_t header{};
        header.initialize();
        header.free_list = free_list_ptr.value().block_pointer;
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        REQUIRE(!bm.load_existing_database().has_error());

        REQUIRE(bm.free_blocks() == 0);
        uint64_t next = bm.free_block_id();
        REQUIRE(next == 3);
    }

    cleanup_test_file();
}

// These paths return a result_wrapper_t carrying the error code instead of throwing.

namespace {
    std::string corrupt_db_path(const char* tag) {
        return "/tmp/test_otterbrix_blockmgr_err_" + std::string(tag) + "_" + std::to_string(::getpid()) + ".otbx";
    }
} // namespace

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
        auto* data = blk->buffer();
        for (size_t j = 0; j < blk->size(); j++) {
            data[j] = static_cast<std::byte>((j * 7 + 1) & 0xFF);
        }
        REQUIRE_FALSE(bm.write(*blk, block_id).has_error());

        // Without a committed header, load_existing_database refuses the reopen as an ambiguous crash state.
        database_header_t header{};
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());

        payload_disk_offset = BLOCK_START + block_id * bm.block_allocation_size() + sizeof(uint64_t);
        original_byte = data[0];
    }

    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        f.seekg(static_cast<std::streamoff>(payload_disk_offset));
        char b = 0;
        f.read(&b, 1);
        REQUIRE(f.gcount() == 1);
        b = static_cast<char>(b ^ 0xFF);
        f.seekp(static_cast<std::streamoff>(payload_disk_offset));
        f.write(&b, 1);
        f.flush();
        REQUIRE(f.good());
        REQUIRE(static_cast<std::byte>(b) != original_byte);
    }

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

TEST_CASE("single_file_block_manager: load empty file -> io_error (error value)") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("empty");
    std::remove(path.c_str());

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

TEST_CASE("single_file_block_manager: load bad-magic header -> data_corruption (error value)") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("badmagic");
    std::remove(path.c_str());

    test_env_t env;
    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
        REQUIRE(!bm.create_new_database().has_error());
        database_header_t header{};
        header.initialize();
        REQUIRE_FALSE(bm.write_header(header).has_error());
    }

    // main_header_t::magic is the first 4 bytes of the file (offset 0).
    {
        std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
        REQUIRE(f.is_open());
        uint32_t bad_magic = 0xDEADBEEF;
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

// set_limit() only fails when eviction can't free enough; an empty pool always succeeds.
TEST_CASE("buffer_pool/standard_buffer_manager: set_memory_limit success returns non-error value") {
    using namespace components::table::storage;
    test_env_t env;

    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_pool.set_limit(uint64_t(1) << 20));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == true);
    }
    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_pool.set_limit(uint64_t(1) << 32));
        REQUIRE_FALSE(r.has_error());
    }
    {
        core::result_wrapper_t<bool> r = false;
        REQUIRE_NOTHROW(r = env.buffer_manager.set_memory_limit(uint64_t(1) << 24));
        REQUIRE_FALSE(r.has_error());
    }
}

// The free list is disk bytes: a bare assert() here disappears under NDEBUG and could alias a live id.
TEST_CASE("single_file_block_manager: a free list naming a LIVE block is refused, not asserted") {
    using namespace components::table::storage;
    const std::string path = corrupt_db_path("free_list_alias");
    std::remove(path.c_str());

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, path);
    REQUIRE(!bm.create_new_database().has_error());

    uint64_t live_id = bm.free_block_id();
    auto live_handle = bm.register_block(live_id);
    REQUIRE(live_handle != nullptr);
    REQUIRE(bm.free_blocks() == 0);

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

    uint64_t issued = bm.free_block_id();
    CHECK(issued != live_id);
    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    // A checkpoint built on a free list known to be corrupt must not become the durable root.
    database_header_t header{};
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::data_corruption);

    std::remove(path.c_str());
}

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

    CHECK(bm.free_blocks() == 0);
    CHECK(bm.dev_reusable_snapshot().count(MAXIMUM_BLOCK + 7) == 0);
    CHECK(bm.dev_pending_free_snapshot().count(MAXIMUM_BLOCK + 7) == 0);

    REQUIRE(bm.has_allocation_error());
    CHECK(bm.allocation_error().type == core::error_code_t::data_corruption);

    database_header_t header{};
    header.initialize();
    auto committed = bm.write_header(header);
    REQUIRE(committed.has_error());
    CHECK(committed.error().type == core::error_code_t::data_corruption);

    std::remove(path.c_str());
}

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
        writer.write<uint64_t>(MAXIMUM_BLOCK + 7);
        REQUIRE_FALSE(writer.flush().has_error());
        poisoned = writer.get_block_pointer();
    }

    auto loaded = bm.deserialize_free_list(poisoned);
    REQUIRE(loaded.has_error());
    CHECK(loaded.error().type == core::error_code_t::data_corruption);
    CHECK(bm.free_blocks() == 0);

    std::remove(path.c_str());
}

// block_size() is unsigned subtraction; any size <= DEFAULT_BLOCK_HEADER_SIZE wraps and reads run off the buffer.
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

    auto ok = bm.set_block_allocation_size(SECTOR_SIZE * 8);
    REQUIRE_FALSE(ok.has_error());
    CHECK(bm.block_allocation_size() == SECTOR_SIZE * 8);

    std::remove(path.c_str());
}

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

// unregister_block must check identity: a freed id can get a fresh handle while a stale one for it is still alive.
TEST_CASE("block_manager: a stale handle's destructor must not erase the live handle's slot") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    const uint64_t id = bm.free_block_id();

    auto stale = bm.register_block(id);
    REQUIRE(stale);
    REQUIRE(bm.registry_alive(id));

    // Release the id and drop the registry entry while `stale` is still alive.
    bm.mark_as_free(id);
    bm.unregister_block(id);
    CHECK_FALSE(bm.registry_alive(id));

    auto live = bm.register_block(id);
    REQUIRE(live);
    CHECK(live.get() != stale.get());
    CHECK(bm.registry_alive(id));

    stale.reset();

    INFO("after the stale handle died, the live handle's registry entry must survive");
    CHECK(bm.registry_alive(id));
    // register_block must still dedup onto it, not mint a second handle for the same block.
    auto again = bm.register_block(id);
    CHECK(again.get() == live.get());

    again.reset();
    live.reset();
    CHECK_FALSE(bm.registry_alive(id));

    cleanup_test_file();
}

TEST_CASE("partial_block_manager: every packed segment offset is 8-byte aligned") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    partial_block_manager_t pbm(bm);

    // Offsets are later dereferenced as the segment's own element type, so misalignment is UB.
    auto first = pbm.get_block_allocation(4); // CONSTANT INT32 main segment
    REQUIRE(first.offset_in_block % 8 == 0);
    auto validity = pbm.get_block_allocation(128); // 1024-row validity bitmap
    REQUIRE(validity.block_id == first.block_id);
    REQUIRE(validity.offset_in_block % 8 == 0);

    // Byte-granular sizes (RLE, dictionary, big-string) must still keep every placement aligned.
    const uint64_t odd_sizes[] = {1, 3, 20, 7, 8, 9, 4096, 5, 133};
    for (auto size : odd_sizes) {
        auto alloc = pbm.get_block_allocation(size);
        REQUIRE(alloc.offset_in_block % 8 == 0);
    }

    cleanup_test_file();
}
