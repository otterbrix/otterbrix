#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <math.h>

TEST_CASE("components::table::data_table") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    auto resource = std::pmr::synchronized_pool_resource();

    struct test_struct {
        bool flag;
        int32_t number;
        std::string name;
        std::vector<uint16_t> array;

        test_struct(bool flag, int32_t number, std::string name, std::vector<uint16_t> array)
            : flag(flag)
            , number(number)
            , name(std::move(name))
            , array(std::move(array)) {}
    };

    // set test_size to some uneven multiple of DEFAULT_VECTOR_CAPACITY
    // ideally it should be a much bigger than DEFAULT_VECTOR_CAPACITY
    constexpr size_t test_size = static_cast<size_t>(static_cast<double>(DEFAULT_VECTOR_CAPACITY) * M_PI_2);
    static_assert(test_size % 2 == 0 && "for data_table_t test it is required to have an even test size");
    constexpr size_t array_size = 128;
    constexpr size_t max_list_size = 128;
    constexpr size_t str_index_length = 10;
    auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

    auto generate_string = [](size_t i) {
        auto number = std::to_string(i);
        while (number.size() < str_index_length) {
            number.insert(number.begin(), '0');
        }
        return std::string{"long_string_with_index_" + number};
    };

    std::vector<complex_logical_type> fields;
    fields.emplace_back(logical_type::BOOLEAN, "flag");
    fields.emplace_back(logical_type::INTEGER, "number");
    fields.emplace_back(logical_type::STRING_LITERAL, "name");
    fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
    complex_logical_type struct_type = complex_logical_type::create_struct("struct", fields, "test_struct");

    std::vector<complex_logical_type> union_fields;
    union_fields.emplace_back(logical_type::BOOLEAN, "bool");
    union_fields.emplace_back(logical_type::INTEGER, "int");
    union_fields.emplace_back(logical_type::STRING_LITERAL, "string");
    complex_logical_type union_type = complex_logical_type::create_union(union_fields, "test_union");

    core::filesystem::local_file_system_t fs;
    auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
    auto block_manager = storage::in_memory_block_manager_t(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE);

    std::vector<column_definition_t> columns;
    columns.reserve(8);
    columns.emplace_back("temp_column_name0", logical_type::UBIGINT);
    columns.emplace_back("temp_column_name1", logical_type::STRING_LITERAL);
    columns.emplace_back("temp_column_name2", complex_logical_type::create_array(logical_type::UBIGINT, array_size));
    columns.emplace_back("temp_column_name3",
                         complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size));
    columns.emplace_back("temp_column_name4", complex_logical_type::create_list(logical_type::UBIGINT));
    columns.emplace_back("temp_column_name5", complex_logical_type::create_list(logical_type::STRING_LITERAL));
    columns.emplace_back("temp_column_name6", struct_type);
    columns.emplace_back("temp_column_name7", union_type);

    std::vector<test_struct> test_data;
    test_data.reserve(test_size);
    for (size_t i = 0; i < test_size; i++) {
        auto s{generate_string(i)};
        std::vector<uint16_t> arr;
        arr.reserve(i);
        for (size_t j = 0; j < i; j++) {
            arr.emplace_back(j);
        }
        test_data.emplace_back(i % 2 != 0, i, std::move(s), std::move(arr));
    }

    auto data_table = std::make_unique<data_table_t>(&resource, block_manager, std::move(columns));

    INFO("Append") {
        // set up a DataChunks
        data_chunk_t chunk(&resource, data_table->copy_types(), test_size);
        chunk.set_cardinality(test_size);
        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            { chunk.set_value(0, i, logical_value_t{&resource, uint64_t(i)}); }
            // STRING
            { chunk.set_value(1, i, logical_value_t{&resource, generate_string(i)}); }
            // ARRAY<UBIGINT>
            {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(&resource, uint64_t{i * array_size + j});
                }
                chunk.set_value(2, i, logical_value_t::create_array(&resource, logical_type::UBIGINT, arr));
            }
            // ARRAY<STRING>
            {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(&resource, generate_string(i * array_size + j));
                }
                chunk.set_value(3, i, logical_value_t::create_array(&resource, logical_type::STRING_LITERAL, arr));
            }
            // LIST<UBIGINT>
            {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(&resource, uint64_t{i * list_length(i) + j});
                }
                chunk.set_value(4, i, logical_value_t::create_list(&resource, logical_type::UBIGINT, list));
            }
            // LIST<STRING>
            {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(&resource, generate_string(i * list_length(i) + j));
                }
                chunk.set_value(5, i, logical_value_t::create_list(&resource, logical_type::STRING_LITERAL, list));
            }
            // STRUCT
            {
                std::vector<logical_value_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(&resource, test_data[i].array[j]);
                }
                std::vector<logical_value_t> value_fiels;
                value_fiels.emplace_back(&resource, test_data[i].flag);
                value_fiels.emplace_back(&resource, test_data[i].number);
                value_fiels.emplace_back(&resource, test_data[i].name);
                value_fiels.emplace_back(logical_value_t::create_list(&resource, logical_type::USMALLINT, arr));
                logical_value_t value = logical_value_t::create_struct(&resource, struct_type, value_fiels);
                chunk.set_value(6, i, value);
            }
            // UNION
            {
                switch (i % 3) {
                    case 0:
                        chunk.set_value(7,
                                        i,
                                        logical_value_t::create_union(&resource,
                                                                      union_fields,
                                                                      0,
                                                                      logical_value_t{&resource, i % 2 == 0}));
                        break;
                    case 1:
                        chunk.set_value(
                            7,
                            i,
                            logical_value_t::create_union(&resource,
                                                          union_fields,
                                                          1,
                                                          logical_value_t{&resource, static_cast<int32_t>(i)}));
                        break;
                    case 2:
                        chunk.set_value(
                            7,
                            i,
                            logical_value_t::create_union(
                                &resource,
                                union_fields,
                                2,
                                logical_value_t{&resource,
                                                std::string{"long_string_with_index_" + std::to_string(i)}}));
                        break;
                    default:
                        // unreachable
                        break;
                }
            }
        }

        table_append_state state(&resource);
        data_table->append_lock(state);
        data_table->initialize_append(state);
        data_table->append(chunk, state);
        data_table->finalize_append(state);
    }
    INFO("Fetch") {
        column_fetch_state state;
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        vector_t rows(&resource, logical_type::BIGINT, test_size);
        for (size_t i = 0; i < test_size; i++) {
            rows.set_value(i, logical_value_t(&resource, static_cast<int64_t>(i)));
        }
        data_chunk_t result(&resource, data_table->copy_types(), test_size);
        data_table->fetch(result, column_indices, rows, test_size, state);

        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string res = *value.value<std::string*>();
                REQUIRE(res == generate_string(i));
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * array_size + j));
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * list_length(i) + j));
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[i].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[i].array[j]);
                }
            }
            // UNION
            {
                logical_value_t value = result.data[7].value(i);
                REQUIRE(value.type().type() == logical_type::UNION);
                REQUIRE(value.type().alias() == "test_union");

                auto tag = value.children()[0].value<uint8_t>();
                switch (tag) {
                    case 0:
                        REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                        REQUIRE(value.type().child_types()[1].alias() == "bool");
                        REQUIRE(value.children()[1].value<bool>() == (i % 2 == 0));
                        break;
                    case 1:
                        REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                        REQUIRE(value.type().child_types()[2].alias() == "int");
                        REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(i));
                        break;
                    case 2:
                        REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                        REQUIRE(value.type().child_types()[3].alias() == "string");
                        REQUIRE(value.children()[3].value<std::string_view>() ==
                                std::string{"long_string_with_index_" + std::to_string(i)});
                        break;
                    default:
                        // unreachable
                        break;
                }
            }
        }
    }
    INFO("Scan") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        data_chunk_t result(&resource, data_table->copy_types(), test_size);
        data_table->initialize_scan(state, column_indices);
        data_table->scan(result, state);

        for (size_t i = 0; i < test_size; i++) {
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string res = *value.value<std::string*>();
                REQUIRE(res == generate_string(i));
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * array_size + j));
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * list_length(i) + j));
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[i].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[i].array[j]);
                }
            }
            // UNION
            {
                logical_value_t value = result.data[7].value(i);
                REQUIRE(value.type().type() == logical_type::UNION);
                REQUIRE(value.type().alias() == "test_union");

                auto tag = value.children()[0].value<uint8_t>();
                switch (tag) {
                    case 0:
                        REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                        REQUIRE(value.type().child_types()[1].alias() == "bool");
                        REQUIRE(value.children()[1].value<bool>() == (i % 2 == 0));
                        break;
                    case 1:
                        REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                        REQUIRE(value.type().child_types()[2].alias() == "int");
                        REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(i));
                        break;
                    case 2:
                        REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                        REQUIRE(value.type().child_types()[3].alias() == "string");
                        REQUIRE(value.children()[3].value<std::string_view>() ==
                                std::string{"long_string_with_index_" + std::to_string(i)});
                        break;
                    default:
                        // unreachable
                        break;
                }
            }
        }
    }
    INFO("Scan with predicates") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        std::pair row_range{uint64_t(test_size * 0.25f), uint64_t(test_size * 0.75f)};
        auto conj_and = std::make_unique<conjunction_and_filter_t>();
        conj_and->child_filters.emplace_back(
            std::make_unique<constant_filter_t>(components::expressions::compare_type::gte,
                                                logical_value_t{&resource, row_range.first},
                                                std::vector<uint64_t>{0}));
        conj_and->child_filters.emplace_back(
            std::make_unique<constant_filter_t>(components::expressions::compare_type::lt,
                                                logical_value_t{&resource, generate_string(row_range.second)},
                                                std::vector<uint64_t>{1}));
        data_chunk_t result(&resource, data_table->copy_types(), row_range.second - row_range.first);
        data_table->initialize_scan(state, column_indices, conj_and.get());
        data_table->scan(result, state);

        for (size_t i = row_range.first, res_index = 0; i < row_range.second; i++, res_index++) {
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(res_index);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(res_index);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string res = *value.value<std::string*>();
                REQUIRE(res == generate_string(i));
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(res_index);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(res_index);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * array_size + j));
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(res_index);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(res_index);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(i * list_length(i) + j));
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(res_index);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[i].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[i].array[j]);
                }
            }
            // UNION
            {
                logical_value_t value = result.data[7].value(res_index);
                REQUIRE(value.type().type() == logical_type::UNION);
                REQUIRE(value.type().alias() == "test_union");

                auto tag = value.children()[0].value<uint8_t>();
                switch (tag) {
                    case 0:
                        REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                        REQUIRE(value.type().child_types()[1].alias() == "bool");
                        REQUIRE(value.children()[1].value<bool>() == (i % 2 == 0));
                        break;
                    case 1:
                        REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                        REQUIRE(value.type().child_types()[2].alias() == "int");
                        REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(i));
                        break;
                    case 2:
                        REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                        REQUIRE(value.type().child_types()[3].alias() == "string");
                        REQUIRE(value.children()[3].value<std::string_view>() ==
                                std::string{"long_string_with_index_" + std::to_string(i)});
                        break;
                    default:
                        // unreachable
                        break;
                }
            }
        }
    }
    INFO("Delete") {
        vector_t v(&resource, logical_type::BIGINT, test_size / 2);
        for (size_t i = 0; i < test_size; i += 2) {
            v.set_value(i / 2, logical_value_t(&resource, int64_t(i)));
        }
        auto state = data_table->initialize_delete({});
        auto deleted_count = data_table->delete_rows(*state, v, test_size / 2);
        REQUIRE(deleted_count == test_size / 2);
    }
    INFO("Scan after delete") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        data_chunk_t result(&resource, data_table->copy_types(), test_size / 2);
        data_table->initialize_scan(state, column_indices);
        data_table->scan(result, state);

        REQUIRE(result.size() == test_size / 2);
        REQUIRE(data_table->calculate_size() == test_size / 2);
        for (size_t i = 0; i < test_size / 2; i++) {
            auto test_data_index = i * 2 + 1;
            // UBIGINT
            {
                logical_value_t value = result.data[0].value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == test_data_index);
            }
            // STRING
            {
                logical_value_t value = result.data[1].value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string res = *value.value<std::string*>();
                REQUIRE(res == generate_string(test_data_index));
            }
            // ARRAY<UBIGINT>
            {
                logical_value_t value = result.data[2].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                }
            }
            // ARRAY<STRING>
            {
                logical_value_t value = result.data[3].value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(test_data_index * array_size + j));
                }
            }
            // LIST<UBIGINT>
            {
                logical_value_t value = result.data[4].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(test_data_index); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() ==
                            test_data_index * list_length(test_data_index) + j);
                }
            }
            // LIST<STRING>
            {
                logical_value_t value = result.data[5].value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(test_data_index); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.children()[j].value<std::string*>();
                    REQUIRE(res == generate_string(test_data_index * list_length(test_data_index) + j));
                }
            }
            // STRUCT
            {
                logical_value_t value = result.data[6].value(i);
                REQUIRE(value.type().type() == logical_type::STRUCT);
                REQUIRE(value.type().alias() == "test_struct");
                REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(value.type().child_types()[0].alias() == "flag");
                REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(value.type().child_types()[1].alias() == "number");
                REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                REQUIRE(value.type().child_types()[2].alias() == "name");
                REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(value.type().child_types()[3].alias() == "array");

                REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[test_data_index].array.size());
                for (size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[test_data_index].array[j]);
                }
            }
            // UNION
            {
                logical_value_t value = result.data[7].value(i);
                REQUIRE(value.type().type() == logical_type::UNION);
                REQUIRE(value.type().alias() == "test_union");

                auto tag = value.children()[0].value<uint8_t>();
                switch (tag) {
                    case 0:
                        REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                        REQUIRE(value.type().child_types()[1].alias() == "bool");
                        REQUIRE(value.children()[1].value<bool>() == (test_data_index % 2 == 0));
                        break;
                    case 1:
                        REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                        REQUIRE(value.type().child_types()[2].alias() == "int");
                        REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(test_data_index));
                        break;
                    case 2:
                        REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                        REQUIRE(value.type().child_types()[3].alias() == "string");
                        REQUIRE(value.children()[3].value<std::string_view>() ==
                                std::string{"long_string_with_index_" + std::to_string(test_data_index)});
                        break;
                    default:
                        // unreachable
                        break;
                }
            }
        }
    }

    INFO("Extension") {
        std::unique_ptr<data_table_t> extended_table;

        {
            column_definition_t new_column{"temp_column_name7",
                                           logical_type::SMALLINT,
                                           std::make_unique<logical_value_t>(&resource, int16_t(0))};
            extended_table = std::make_unique<data_table_t>(*data_table, new_column);

            // Update values in new column
            // Since row mask is applied to every column at once, in update we have to fill whole column
            // or manually calculate ids where set is needed
            vector_t v(&resource, logical_type::BIGINT, test_size / 2);
            data_chunk_t chunk(&resource, {logical_type::SMALLINT}, test_size / 2);
            chunk.set_cardinality(test_size / 2);
            for (size_t i = 0; i < test_size / 2; i++) {
                v.set_value(i, logical_value_t(&resource, int64_t(i * 2 + 1)));
                chunk.set_value(0, i, logical_value_t{&resource, int16_t(i * 2 + 1)});
            }
            extended_table->update_column(v, {8}, chunk);
        }
        // Scan after extension
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(extended_table->column_count());
            for (size_t i = 0; i < extended_table->column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_scan_state state(&resource);
            data_chunk_t result(&resource, extended_table->copy_types());
            extended_table->initialize_scan(state, column_indices);
            extended_table->scan(result, state);

            for (size_t i = 0; i < test_size / 2; i++) {
                auto test_data_index = i * 2 + 1;
                // UBIGINT
                {
                    logical_value_t value = result.data[0].value(i);
                    REQUIRE(value.type().type() == logical_type::UBIGINT);
                    REQUIRE(value.value<uint64_t>() == test_data_index);
                }
                // STRING
                {
                    logical_value_t value = result.data[1].value(i);
                    REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.value<std::string*>();
                    REQUIRE(res == generate_string(test_data_index));
                }
                // ARRAY<UBIGINT>
                {
                    logical_value_t value = result.data[2].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                    }
                }
                // ARRAY<STRING>
                {
                    logical_value_t value = result.data[3].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string res = *value.children()[j].value<std::string*>();
                        REQUIRE(res == generate_string(test_data_index * array_size + j));
                    }
                }
                // LIST<UBIGINT>
                {
                    logical_value_t value = result.data[4].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() ==
                                test_data_index * list_length(test_data_index) + j);
                    }
                }
                // LIST<STRING>
                {
                    logical_value_t value = result.data[5].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string res = *value.children()[j].value<std::string*>();
                        REQUIRE(res == generate_string(test_data_index * list_length(test_data_index) + j));
                    }
                }
                // STRUCT
                {
                    logical_value_t value = result.data[6].value(i);
                    REQUIRE(value.type().type() == logical_type::STRUCT);
                    REQUIRE(value.type().alias() == "test_struct");
                    REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                    REQUIRE(value.type().child_types()[0].alias() == "flag");
                    REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                    REQUIRE(value.type().child_types()[1].alias() == "number");
                    REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                    REQUIRE(value.type().child_types()[2].alias() == "name");
                    REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                    REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                    REQUIRE(value.type().child_types()[3].alias() == "array");

                    REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                    REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                    REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                    std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                    REQUIRE(arr.size() == test_data[test_data_index].array.size());
                    for (size_t j = 0; j < arr.size(); j++) {
                        REQUIRE(arr[j].value<uint16_t>() == test_data[test_data_index].array[j]);
                    }
                }
                // UNION
                {
                    logical_value_t value = result.data[7].value(i);
                    REQUIRE(value.type().type() == logical_type::UNION);
                    REQUIRE(value.type().alias() == "test_union");

                    auto tag = value.children()[0].value<uint8_t>();
                    switch (tag) {
                        case 0:
                            REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                            REQUIRE(value.type().child_types()[1].alias() == "bool");
                            REQUIRE(value.children()[1].value<bool>() == (test_data_index % 2 == 0));
                            break;
                        case 1:
                            REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                            REQUIRE(value.type().child_types()[2].alias() == "int");
                            REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(test_data_index));
                            break;
                        case 2:
                            REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                            REQUIRE(value.type().child_types()[3].alias() == "string");
                            REQUIRE(value.children()[3].value<std::string_view>() ==
                                    std::string{"long_string_with_index_" + std::to_string(test_data_index)});
                            break;
                        default:
                            // unreachable
                            break;
                    }
                }
                // SMALLINT
                {
                    logical_value_t value = result.data[8].value(i);
                    REQUIRE(value.type().type() == logical_type::SMALLINT);
                    REQUIRE(value.value<int16_t>() == static_cast<int16_t>(test_data_index));
                }
            }
        }

        // Remove column
        std::unique_ptr<data_table_t> short_table = std::make_unique<data_table_t>(*extended_table, 0);

        // Scan after column removal
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(short_table->column_count());
            for (size_t i = 0; i < short_table->column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_scan_state state(&resource);
            data_chunk_t result(&resource, short_table->copy_types());
            short_table->initialize_scan(state, column_indices);
            short_table->scan(result, state);

            for (size_t i = 0; i < test_size / 2; i++) {
                auto test_data_index = i * 2 + 1;
                // STRING
                {
                    logical_value_t value = result.data[0].value(i);
                    REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                    std::string res = *value.value<std::string*>();
                    REQUIRE(res == generate_string(test_data_index));
                }
                // ARRAY<UBIGINT>
                {
                    logical_value_t value = result.data[1].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() == test_data_index * array_size + j);
                    }
                }
                // ARRAY<STRING>
                {
                    logical_value_t value = result.data[2].value(i);
                    REQUIRE(value.type().type() == logical_type::ARRAY);
                    for (size_t j = 0; j < array_size; j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string res = *value.children()[j].value<std::string*>();
                        REQUIRE(res == generate_string(test_data_index * array_size + j));
                    }
                }
                // LIST<UBIGINT>
                {
                    logical_value_t value = result.data[3].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                        REQUIRE(value.children()[j].value<uint64_t>() ==
                                test_data_index * list_length(test_data_index) + j);
                    }
                }
                // LIST<STRING>
                {
                    logical_value_t value = result.data[4].value(i);
                    REQUIRE(value.type().type() == logical_type::LIST);
                    for (size_t j = 0; j < list_length(test_data_index); j++) {
                        REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                        std::string result = *(value.children()[j].value<std::string*>());
                        REQUIRE(result == generate_string(test_data_index * list_length(test_data_index) + j));
                    }
                }
                // STRUCT
                {
                    logical_value_t value = result.data[5].value(i);
                    REQUIRE(value.type().type() == logical_type::STRUCT);
                    REQUIRE(value.type().alias() == "test_struct");
                    REQUIRE(value.type().child_types()[0].type() == logical_type::BOOLEAN);
                    REQUIRE(value.type().child_types()[0].alias() == "flag");
                    REQUIRE(value.type().child_types()[1].type() == logical_type::INTEGER);
                    REQUIRE(value.type().child_types()[1].alias() == "number");
                    REQUIRE(value.type().child_types()[2].type() == logical_type::STRING_LITERAL);
                    REQUIRE(value.type().child_types()[2].alias() == "name");
                    REQUIRE(value.type().child_types()[3].type() == logical_type::LIST);
                    REQUIRE(value.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                    REQUIRE(value.type().child_types()[3].alias() == "array");

                    REQUIRE(value.children()[0].value<bool>() == test_data[test_data_index].flag);
                    REQUIRE(value.children()[1].value<int32_t>() == test_data[test_data_index].number);
                    REQUIRE(*value.children()[2].value<std::string*>() == test_data[test_data_index].name);
                    std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                    REQUIRE(arr.size() == test_data[test_data_index].array.size());
                    for (size_t j = 0; j < arr.size(); j++) {
                        REQUIRE(arr[j].value<uint16_t>() == test_data[test_data_index].array[j]);
                    }
                }
                // UNION
                {
                    logical_value_t value = result.data[6].value(i);
                    REQUIRE(value.type().type() == logical_type::UNION);
                    REQUIRE(value.type().alias() == "test_union");

                    auto tag = value.children()[0].value<uint8_t>();
                    switch (tag) {
                        case 0:
                            REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                            REQUIRE(value.type().child_types()[1].alias() == "bool");
                            REQUIRE(value.children()[1].value<bool>() == (test_data_index % 2 == 0));
                            break;
                        case 1:
                            REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                            REQUIRE(value.type().child_types()[2].alias() == "int");
                            REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(test_data_index));
                            break;
                        case 2:
                            REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                            REQUIRE(value.type().child_types()[3].alias() == "string");
                            REQUIRE(value.children()[3].value<std::string_view>() ==
                                    std::string{"long_string_with_index_" + std::to_string(test_data_index)});
                            break;
                        default:
                            // unreachable
                            break;
                    }
                }
                // SMALLINT
                {
                    logical_value_t value = result.data[7].value(i);
                    REQUIRE(value.type().type() == logical_type::SMALLINT);
                    REQUIRE(value.value<int16_t>() == static_cast<int16_t>(test_data_index));
                }
            }
        }
    }
}