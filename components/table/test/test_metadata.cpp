#include <catch2/catch.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
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

TEST_CASE("metadata: row_group_pointer reads older columnar format") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);
    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);

        data_pointer_t first;
        first.row_start = 0;
        first.tuple_count = 128;
        first.block_pointer = block_pointer_t(11, 32);
        first.compression = components::table::compression::compression_type::UNCOMPRESSED;
        first.segment_size = 1024;

        data_pointer_t second;
        second.row_start = 128;
        second.tuple_count = 128;
        second.block_pointer = block_pointer_t(12, 64);
        second.compression = components::table::compression::compression_type::RLE;
        second.segment_size = 256;

        writer.write<uint64_t>(0);
        writer.write<uint64_t>(256);
        writer.write<uint32_t>(2);
        writer.write<uint32_t>(1);
        first.serialize(writer);
        writer.write<uint32_t>(1);
        second.serialize(writer);
        writer.write<uint32_t>(0);

        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        metadata_reader_t reader(manager, pointer);
        auto row_group = row_group_pointer_t::deserialize(reader);
        REQUIRE(row_group.row_start == 0);
        REQUIRE(row_group.tuple_count == 256);
        REQUIRE(row_group.columnar_data_pointers.size() == 2);
        REQUIRE(row_group.columnar_data_pointers[0].size() == 1);
        REQUIRE(row_group.columnar_data_pointers[1].size() == 1);
        REQUIRE(row_group.columnar_data_pointers[0][0].block_pointer.block_id == 11);
        REQUIRE(row_group.columnar_data_pointers[1][0].block_pointer.block_id == 12);
        REQUIRE(row_group.layout_kind == row_group_layout_kind::COLUMNAR);
        REQUIRE_FALSE(row_group.pax_fixed_layout.has_value());
    }

    cleanup_test_file();
}

TEST_CASE("metadata: pax_fixed layout round trip") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    pax_fixed_row_group_layout_t layout;
    layout.rows_per_page = 128;

    pax_fixed_page_t first_page;
    first_page.row_offset_in_group = 0;
    first_page.tuple_count = 128;

    pax_fixed_slice_t first_slice;
    first_slice.column_index = 0;
    first_slice.column_type = pax_fixed_column_type::INT64;
    first_slice.data_pointer.row_start = 0;
    first_slice.data_pointer.tuple_count = 128;
    first_slice.data_pointer.block_pointer = block_pointer_t(101, 0);
    first_slice.data_pointer.segment_size = 1024;

    pax_fixed_slice_t second_slice;
    second_slice.column_index = 1;
    second_slice.column_type = pax_fixed_column_type::UINT32;
    second_slice.data_pointer.row_start = 0;
    second_slice.data_pointer.tuple_count = 128;
    second_slice.data_pointer.block_pointer = block_pointer_t(101, 1024);
    second_slice.data_pointer.segment_size = 512;

    first_page.slices.push_back(first_slice);
    first_page.slices.push_back(second_slice);
    layout.pages.push_back(first_page);

    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        layout.serialize(writer);
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        metadata_reader_t reader(manager, pointer);
        auto restored = pax_fixed_row_group_layout_t::deserialize(reader);
        REQUIRE(restored.version == 1);
        REQUIRE(restored.rows_per_page == 128);
        REQUIRE(restored.pages.size() == 1);
        REQUIRE(restored.pages[0].tuple_count == 128);
        REQUIRE(restored.pages[0].slices.size() == 2);
        REQUIRE(restored.pages[0].slices[0].column_index == 0);
        REQUIRE(restored.pages[0].slices[0].column_type == pax_fixed_column_type::INT64);
        REQUIRE(restored.pages[0].slices[1].column_index == 1);
        REQUIRE(restored.pages[0].slices[1].data_pointer.block_pointer.offset == 1024);
    }

    cleanup_test_file();
}
