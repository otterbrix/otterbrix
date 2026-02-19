#include <catch2/catch.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <cstring>
#include <unistd.h>

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_block_manager_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    void cleanup_test_file() { std::remove(test_db_path().c_str()); }

    struct test_env_t {
        std::pmr::synchronized_pool_resource resource;
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
    bm.create_new_database();

    constexpr size_t NUM_BLOCKS = 5;
    std::vector<uint64_t> block_ids;
    std::vector<std::vector<std::byte>> original_data(NUM_BLOCKS);

    // write blocks
    for (size_t i = 0; i < NUM_BLOCKS; i++) {
        uint64_t id = bm.free_block_id();
        block_ids.push_back(id);

        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(), id,
                                              static_cast<uint64_t>(bm.block_size()));
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
        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(), block_ids[i],
                                              static_cast<uint64_t>(bm.block_size()));
        bm.read(*blk);

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
        bm.create_new_database();

        uint64_t id = bm.free_block_id();
        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(), id,
                                              static_cast<uint64_t>(bm.block_size()));
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
        bm.load_existing_database();

        REQUIRE(bm.total_blocks() == 1);

        auto blk = std::make_unique<block_t>(env.resource.upstream_resource(), 0,
                                              static_cast<uint64_t>(bm.block_size()));
        bm.read(*blk);

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
    bm.create_new_database();

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
