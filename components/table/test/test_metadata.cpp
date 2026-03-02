#include <catch2/catch.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstring>
#include <unistd.h>

namespace {
    std::string test_db_path() {
        static std::string path = "/tmp/test_otterbrix_metadata_" + std::to_string(::getpid()) + ".otbx";
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

TEST_CASE("metadata: write and read small data") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    // write 100 bytes
    std::vector<std::byte> test_data(100);
    for (size_t i = 0; i < test_data.size(); i++) {
        test_data[i] = static_cast<std::byte>(i);
    }

    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        writer.write_data(test_data.data(), test_data.size());
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    // read back
    {
        metadata_reader_t reader(manager, pointer);
        std::vector<std::byte> read_data(100);
        reader.read_data(read_data.data(), read_data.size());
        REQUIRE(std::memcmp(test_data.data(), read_data.data(), test_data.size()) == 0);
    }

    cleanup_test_file();
}

TEST_CASE("metadata: write and read typed data") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        writer.write<uint32_t>(12345);
        writer.write<uint64_t>(9876543210ULL);
        writer.write<uint8_t>(42);
        writer.write_string("hello world");
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        metadata_reader_t reader(manager, pointer);
        REQUIRE(reader.read<uint32_t>() == 12345);
        REQUIRE(reader.read<uint64_t>() == 9876543210ULL);
        REQUIRE(reader.read<uint8_t>() == 42);
        REQUIRE(reader.read_string() == "hello world");
    }

    cleanup_test_file();
}

TEST_CASE("metadata: multiple independent chains") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    meta_block_pointer_t ptr1, ptr2, ptr3;

    {
        metadata_writer_t writer1(manager);
        writer1.write<uint64_t>(111);
        ptr1 = writer1.get_block_pointer();

        metadata_writer_t writer2(manager);
        writer2.write<uint64_t>(222);
        ptr2 = writer2.get_block_pointer();

        metadata_writer_t writer3(manager);
        writer3.write<uint64_t>(333);
        ptr3 = writer3.get_block_pointer();

        writer1.flush();
        writer2.flush();
        writer3.flush();
    }

    // all three managed by same manager, flush once is enough
    manager.flush();

    {
        metadata_reader_t reader1(manager, ptr1);
        REQUIRE(reader1.read<uint64_t>() == 111);

        metadata_reader_t reader2(manager, ptr2);
        REQUIRE(reader2.read<uint64_t>() == 222);

        metadata_reader_t reader3(manager, ptr3);
        REQUIRE(reader3.read<uint64_t>() == 333);
    }

    cleanup_test_file();
}
