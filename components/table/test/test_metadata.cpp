#include <catch2/catch.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/validation.hpp>
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

TEST_CASE("metadata: long chain stays within persisted block payload") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    meta_block_pointer_t pointer;
    std::vector<std::byte> test_data;

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.create_new_database();

        metadata_manager_t manager(bm);
        constexpr uint64_t sub_block_header_size = sizeof(uint64_t) + sizeof(uint32_t);
        REQUIRE(manager.sub_block_size() * META_SUB_BLOCKS_PER_BLOCK <= bm.block_size());

        const auto payload_per_sub_block = manager.sub_block_size() - sub_block_header_size;
        test_data.resize(payload_per_sub_block * (META_SUB_BLOCKS_PER_BLOCK + 6) + 31);
        for (size_t i = 0; i < test_data.size(); i++) {
            test_data[i] = static_cast<std::byte>((i * 131U + 17U) & 0xFFU);
        }

        metadata_writer_t writer(manager);
        writer.write_data(test_data.data(), test_data.size());
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
        bm.load_existing_database();

        metadata_manager_t manager(bm);
        std::vector<std::byte> read_data(test_data.size());
        metadata_reader_t reader(manager, pointer);
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
        REQUIRE_FALSE(row_group.pax_generic_layout.has_value());
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
    layout.version = 2;
    layout.rows_per_page = 256;

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
    first_slice.validity_kind = pax_fixed_validity_kind::BITMASK;
    first_slice.validity_data_pointer = data_pointer_t{};
    first_slice.validity_data_pointer->row_start = 0;
    first_slice.validity_data_pointer->tuple_count = 128;
    first_slice.validity_data_pointer->block_pointer = block_pointer_t(101, 1536);
    first_slice.validity_data_pointer->compression =
        components::table::compression::compression_type::VALIDITY_UNCOMPRESSED;
    first_slice.validity_data_pointer->segment_size = components::vector::validity_mask_t::validity_mask_size(128);

    pax_fixed_slice_t second_slice;
    second_slice.column_index = 1;
    second_slice.column_type = pax_fixed_column_type::UINT32;
    second_slice.data_pointer.row_start = 0;
    second_slice.data_pointer.tuple_count = 128;
    second_slice.data_pointer.block_pointer = block_pointer_t(101, 1024);
    second_slice.data_pointer.segment_size = 512;
    second_slice.validity_kind = pax_fixed_validity_kind::ALL_VALID;

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
        REQUIRE(restored.version == 2);
        REQUIRE(restored.rows_per_page == 256);
        REQUIRE(restored.pages.size() == 1);
        REQUIRE(restored.pages[0].tuple_count == 128);
        REQUIRE(restored.pages[0].slices.size() == 2);
        REQUIRE(restored.pages[0].slices[0].column_index == 0);
        REQUIRE(restored.pages[0].slices[0].column_type == pax_fixed_column_type::INT64);
        REQUIRE(restored.pages[0].slices[0].validity_kind == pax_fixed_validity_kind::BITMASK);
        REQUIRE(restored.pages[0].slices[0].validity_data_pointer.has_value());
        REQUIRE(restored.pages[0].slices[0].validity_data_pointer->block_pointer.offset == 1536);
        REQUIRE(restored.pages[0].slices[1].column_index == 1);
        REQUIRE(restored.pages[0].slices[1].data_pointer.block_pointer.offset == 1024);
        REQUIRE(restored.pages[0].slices[1].validity_kind == pax_fixed_validity_kind::ALL_VALID);
        REQUIRE_FALSE(restored.pages[0].slices[1].validity_data_pointer.has_value());
    }

    cleanup_test_file();
}

TEST_CASE("metadata: pax_fixed v1 layout defaults to all valid validity") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    pax_fixed_row_group_layout_t layout;
    layout.version = 1;
    layout.rows_per_page = 256;

    pax_fixed_page_t page;
    page.row_offset_in_group = 0;
    page.tuple_count = 64;

    pax_fixed_slice_t slice;
    slice.column_index = 0;
    slice.column_type = pax_fixed_column_type::INT32;
    slice.data_pointer.row_start = 0;
    slice.data_pointer.tuple_count = 64;
    slice.data_pointer.block_pointer = block_pointer_t(7, 0);
    slice.data_pointer.segment_size = 256;
    page.slices.push_back(slice);
    layout.pages.push_back(page);

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
        REQUIRE(restored.pages.size() == 1);
        REQUIRE(restored.pages[0].slices.size() == 1);
        REQUIRE(restored.pages[0].slices[0].validity_kind == pax_fixed_validity_kind::ALL_VALID);
        REQUIRE_FALSE(restored.pages[0].slices[0].validity_data_pointer.has_value());
    }

    cleanup_test_file();
}

TEST_CASE("metadata: pax_generic layout round trip") {
    using namespace components::table::storage;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    pax_generic_row_group_layout_t layout;
    layout.version = 1;
    layout.rows_per_page = 256;

    pax_generic_page_t first_page;
    first_page.row_offset_in_group = 0;
    first_page.tuple_count = 128;

    pax_generic_slice_t string_slice;
    string_slice.column_index = 0;
    string_slice.slice_kind = pax_generic_slice_kind::STRING_VALUES;
    string_slice.codec_kind = pax_generic_codec_kind::STRING_SEGMENT;
    string_slice.payload = pax_block_payload_t{};
    string_slice.payload->main_pointer.row_start = 0;
    string_slice.payload->main_pointer.tuple_count = 128;
    string_slice.payload->main_pointer.block_pointer = block_pointer_t(200, 0);
    string_slice.payload->main_pointer.segment_size = 4096;
    string_slice.payload->extra_block_ids = {300, 301};

    pax_generic_slice_t validity_slice;
    validity_slice.column_index = 0;
    validity_slice.slice_kind = pax_generic_slice_kind::VALIDITY;
    validity_slice.codec_kind = pax_generic_codec_kind::VALIDITY_BITMASK;
    validity_slice.payload = pax_block_payload_t{};
    validity_slice.payload->main_pointer.row_start = 0;
    validity_slice.payload->main_pointer.tuple_count = 128;
    validity_slice.payload->main_pointer.block_pointer = block_pointer_t(200, 4096);
    validity_slice.payload->main_pointer.compression =
        components::table::compression::compression_type::VALIDITY_UNCOMPRESSED;
    validity_slice.payload->main_pointer.segment_size = components::vector::validity_mask_t::validity_mask_size(128);

    first_page.slices.push_back(string_slice);
    first_page.slices.push_back(validity_slice);

    pax_generic_page_t second_page;
    second_page.row_offset_in_group = 128;
    second_page.tuple_count = 16;

    pax_generic_slice_t second_string_slice;
    second_string_slice.column_index = 0;
    second_string_slice.slice_kind = pax_generic_slice_kind::STRING_VALUES;
    second_string_slice.codec_kind = pax_generic_codec_kind::STRING_SEGMENT;
    second_string_slice.payload = pax_block_payload_t{};
    second_string_slice.payload->main_pointer.row_start = 128;
    second_string_slice.payload->main_pointer.tuple_count = 16;
    second_string_slice.payload->main_pointer.block_pointer = block_pointer_t(201, 0);
    second_string_slice.payload->main_pointer.segment_size = 256;

    pax_generic_slice_t second_validity_slice;
    second_validity_slice.column_index = 0;
    second_validity_slice.slice_kind = pax_generic_slice_kind::VALIDITY;
    second_validity_slice.codec_kind = pax_generic_codec_kind::VALIDITY_ALL_INVALID;

    second_page.slices.push_back(second_string_slice);
    second_page.slices.push_back(second_validity_slice);

    layout.pages.push_back(first_page);
    layout.pages.push_back(second_page);

    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        layout.serialize(writer);
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        metadata_reader_t reader(manager, pointer);
        auto restored = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(restored.version == 1);
        REQUIRE(restored.rows_per_page == 256);
        REQUIRE(restored.pages.size() == 2);
        REQUIRE(restored.pages[0].slices.size() == 2);
        REQUIRE(restored.pages[0].slices[0].slice_kind == pax_generic_slice_kind::STRING_VALUES);
        REQUIRE(restored.pages[0].slices[0].codec_kind == pax_generic_codec_kind::STRING_SEGMENT);
        REQUIRE(restored.pages[0].slices[0].payload.has_value());
        REQUIRE(restored.pages[0].slices[0].payload->main_pointer.block_pointer.block_id == 200);
        REQUIRE(restored.pages[0].slices[0].payload->extra_block_ids == std::vector<uint32_t>{300, 301});
        REQUIRE(restored.pages[0].slices[1].slice_kind == pax_generic_slice_kind::VALIDITY);
        REQUIRE(restored.pages[0].slices[1].codec_kind == pax_generic_codec_kind::VALIDITY_BITMASK);
        REQUIRE(restored.pages[0].slices[1].payload.has_value());
        REQUIRE(restored.pages[0].slices[1].payload->main_pointer.segment_size ==
                components::vector::validity_mask_t::validity_mask_size(128));
        REQUIRE(restored.pages[1].slices[1].codec_kind == pax_generic_codec_kind::VALIDITY_ALL_INVALID);
        REQUIRE_FALSE(restored.pages[1].slices[1].payload.has_value());
    }

    cleanup_test_file();
}

TEST_CASE("metadata: pax_generic v2 struct layout round trip") {
    using namespace components::table::storage;
    using namespace components::types;
    cleanup_test_file();

    test_env_t env;
    single_file_block_manager_t bm(env.buffer_manager, env.fs, test_db_path());
    bm.create_new_database();

    metadata_manager_t manager(bm);

    pax_generic_row_group_layout_t layout;
    layout.version = 2;
    layout.rows_per_page = 256;

    pax_generic_page_t page;
    page.row_offset_in_group = 0;
    page.tuple_count = 64;

    pax_generic_slice_t root_validity;
    root_validity.column_index = 2;
    root_validity.slice_kind = pax_generic_slice_kind::VALIDITY;
    root_validity.codec_kind = pax_generic_codec_kind::VALIDITY_BITMASK;
    root_validity.payload = pax_block_payload_t{};
    root_validity.payload->main_pointer.row_start = 0;
    root_validity.payload->main_pointer.tuple_count = 64;
    root_validity.payload->main_pointer.block_pointer = block_pointer_t(500, 0);
    root_validity.payload->main_pointer.compression =
        components::table::compression::compression_type::VALIDITY_UNCOMPRESSED;
    root_validity.payload->main_pointer.segment_size = components::vector::validity_mask_t::validity_mask_size(64);

    pax_generic_slice_t fixed_child;
    fixed_child.column_index = 2;
    fixed_child.slice_kind = pax_generic_slice_kind::FIXED_VALUES;
    fixed_child.codec_kind = pax_generic_codec_kind::FIXED_PLAIN;
    fixed_child.field_path = {0};
    fixed_child.fixed_logical_type = logical_type::BIGINT;
    fixed_child.payload = pax_block_payload_t{};
    fixed_child.payload->main_pointer.row_start = 0;
    fixed_child.payload->main_pointer.tuple_count = 64;
    fixed_child.payload->main_pointer.block_pointer = block_pointer_t(501, 0);
    fixed_child.payload->main_pointer.segment_size = 64 * sizeof(int64_t);

    pax_generic_slice_t string_child;
    string_child.column_index = 2;
    string_child.slice_kind = pax_generic_slice_kind::STRING_VALUES;
    string_child.codec_kind = pax_generic_codec_kind::STRING_SEGMENT;
    string_child.field_path = {1};
    string_child.payload = pax_block_payload_t{};
    string_child.payload->main_pointer.row_start = 0;
    string_child.payload->main_pointer.tuple_count = 64;
    string_child.payload->main_pointer.block_pointer = block_pointer_t(502, 0);
    string_child.payload->main_pointer.segment_size = 2048;
    string_child.payload->extra_block_ids = {700};

    pax_generic_slice_t string_validity;
    string_validity.column_index = 2;
    string_validity.slice_kind = pax_generic_slice_kind::VALIDITY;
    string_validity.codec_kind = pax_generic_codec_kind::VALIDITY_ALL_INVALID;
    string_validity.field_path = {1};

    page.slices.push_back(root_validity);
    page.slices.push_back(fixed_child);
    page.slices.push_back(string_child);
    page.slices.push_back(string_validity);
    layout.pages.push_back(page);

    meta_block_pointer_t pointer;
    {
        metadata_writer_t writer(manager);
        layout.serialize(writer);
        pointer = writer.get_block_pointer();
        writer.flush();
    }

    {
        metadata_reader_t reader(manager, pointer);
        auto restored = pax_generic_row_group_layout_t::deserialize(reader);
        REQUIRE(restored.version == 2);
        REQUIRE(restored.pages.size() == 1);
        REQUIRE(restored.pages[0].slices.size() == 4);
        REQUIRE(restored.pages[0].slices[1].slice_kind == pax_generic_slice_kind::FIXED_VALUES);
        REQUIRE(restored.pages[0].slices[1].codec_kind == pax_generic_codec_kind::FIXED_PLAIN);
        REQUIRE(restored.pages[0].slices[1].field_path == std::vector<uint16_t>{0});
        REQUIRE(restored.pages[0].slices[1].fixed_logical_type == logical_type::BIGINT);
        REQUIRE(restored.pages[0].slices[2].field_path == std::vector<uint16_t>{1});
        REQUIRE(restored.pages[0].slices[2].payload.has_value());
        REQUIRE(restored.pages[0].slices[2].payload->extra_block_ids == std::vector<uint32_t>{700});
        REQUIRE(restored.pages[0].slices[3].codec_kind == pax_generic_codec_kind::VALIDITY_ALL_INVALID);
    }

    cleanup_test_file();
}
