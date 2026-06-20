#include <catch2/catch.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <cstring>
#include <unistd.h>
#include <vector>

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
    REQUIRE(!bm.create_new_database().has_error());

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
    REQUIRE(!bm.create_new_database().has_error());

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
    REQUIRE(!bm.create_new_database().has_error());

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

// Metadata chain-overflow -> sticky data_corruption.
// A read past the end of the chain records a sticky core::error_t with
// error_code_t::data_corruption ("attempted to read past end of chain") and turns
// every subsequent read into a no-op, rather than throwing. We write a SMALL stream
// (one sub-block, whose header next_ptr == INVALID_INDEX marks the end of the chain)
// and then read MORE bytes than the whole chain holds, so the read provably runs off
// the end and trips the sticky error. No throw.
TEST_CASE("metadata_reader: read past end of chain -> sticky data_corruption (error value)") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    REQUIRE(!bm.create_new_database().has_error());

    metadata_manager_t manager(bm);

    // Write a small payload into a single sub-block, then flush so a reader can
    // walk the (one-sub-block) chain.
    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        writer.write<uint64_t>(0xABCDEF0123456789ULL);
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    // A single sub-block holds (sub_block_size - SUB_BLOCK_HEADER_SIZE) readable
    // bytes; ask for far more than that so the read provably runs off the end.
    const uint64_t over_read = manager.sub_block_size() * 3;
    REQUIRE(over_read > manager.sub_block_size());

    metadata_reader_t reader(manager, pointer);
    REQUIRE_FALSE(reader.has_error());

    std::vector<std::byte> sink(over_read, std::byte{0});
    REQUIRE_NOTHROW(reader.read_data(sink.data(), over_read));

    // Sticky corrupt-stream flag set; the error is data_corruption.
    REQUIRE(reader.has_error()); // would be a false pass if the read stayed in-bounds
    REQUIRE(reader.error().type == core::error_code_t::data_corruption);

    // Sticky: a further read remains a no-op and the error type is unchanged.
    uint64_t after = 0xFFFFFFFFFFFFFFFFULL;
    REQUIRE_NOTHROW(reader.read_data(reinterpret_cast<std::byte*>(&after), sizeof(after)));
    REQUIRE(reader.has_error());
    REQUIRE(reader.error().type == core::error_code_t::data_corruption);

    cleanup_test_file();
}
