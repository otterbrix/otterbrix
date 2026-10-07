#include <catch2/catch_test_macros.hpp>
#include <components/table/column_data.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::table;
namespace tstorage = components::table::storage;

namespace {

    bool refused(const complex_logical_type& type, std::pmr::memory_resource* resource) {
        return column_data_t::validate_column_type(type, resource).contains_error();
    }

} // namespace

TEST_CASE("components::table::column_type_without_storage::refused_top_level_and_nested") {
    core::pmr::otterbrix_resource resource;

    CHECK(refused(complex_logical_type{logical_type::NA}, &resource));
    CHECK(refused(complex_logical_type{logical_type::UNKNOWN}, &resource));
    CHECK(refused(complex_logical_type::create_unknown("pending_t"), &resource));
    CHECK(refused(complex_logical_type{logical_type::BLOB}, &resource));
    CHECK(refused(complex_logical_type{logical_type::BIT}, &resource));
    CHECK(refused(complex_logical_type{logical_type::POINTER}, &resource));
    CHECK(refused(complex_logical_type{logical_type::VALIDITY}, &resource));

    CHECK(refused(complex_logical_type::create_list(complex_logical_type{logical_type::NA}), &resource));
    CHECK(refused(complex_logical_type::create_list(complex_logical_type{logical_type::BLOB}), &resource));
    CHECK(refused(complex_logical_type::create_array(complex_logical_type{logical_type::UNKNOWN}, 1), &resource));
    CHECK(refused(complex_logical_type::create_array(complex_logical_type{logical_type::NA}, 0), &resource));
    CHECK(refused(complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, 0), &resource));
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::NA, "n");
    CHECK(refused(complex_logical_type::create_struct("box", fields, "b"), &resource));

    CHECK_FALSE(refused(complex_logical_type{logical_type::BIGINT}, &resource));
    CHECK_FALSE(refused(complex_logical_type{logical_type::STRING_LITERAL}, &resource));
    CHECK_FALSE(refused(complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, 2), &resource));
    CHECK_FALSE(refused(complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT}), &resource));
    std::pmr::vector<complex_logical_type> stored_fields(&resource);
    stored_fields.emplace_back(logical_type::BIGINT, "x");
    CHECK_FALSE(refused(complex_logical_type::create_struct("pair", stored_fields, "p"), &resource));
}

TEST_CASE("components::table::column_type_without_storage::a_NULL_typed_column_refuses_its_first_append") {
    const std::string path =
        "/tmp/test_otterbrix_column_type_without_storage_" + std::to_string(::getpid()) + ".otbx";
    std::remove(path.c_str());
    core::pmr::otterbrix_resource resource;
    core::filesystem::local_file_system_t fs;
    tstorage::buffer_pool_t buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    tstorage::standard_buffer_manager_t buffer_manager(&resource, fs, buffer_pool);
    tstorage::single_file_block_manager_t bm(buffer_manager, fs, path);
    REQUIRE_FALSE(bm.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.emplace_back("k", logical_type::BIGINT);
    columns.emplace_back("n", complex_logical_type{logical_type::NA});
    data_table_t table(&resource, bm, std::move(columns), "na_column");

    table_append_state state(&resource);
    REQUIRE_FALSE(table.append_lock(state).has_error());
    auto opened = table.initialize_append(state);
    REQUIRE(opened.has_error());
    CHECK(opened.error().type == core::error_code_t::invalid_parameter);
    std::remove(path.c_str());
}
