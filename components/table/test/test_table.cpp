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
    // ideally it should be a much bigger than DEFAULT_VECTOR_CAPACITY, so the table
    // spans several vectors and every append/scan/fetch crosses a chunk boundary.
    constexpr size_t test_size = static_cast<size_t>(static_cast<double>(DEFAULT_VECTOR_CAPACITY) * M_PI_2);
    static_assert(test_size % 2 == 0 && "for data_table_t test it is required to have an even test size");
    // A single data_chunk_t holds at most DEFAULT_VECTOR_CAPACITY rows, so the data is
    // moved through the table one ≤cap chunk at a time.
    constexpr size_t cap = DEFAULT_VECTOR_CAPACITY;
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

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BOOLEAN, "flag");
    fields.emplace_back(logical_type::INTEGER, "number");
    fields.emplace_back(logical_type::STRING_LITERAL, "name");
    fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
    complex_logical_type struct_type = complex_logical_type::create_struct("struct", fields, "test_struct");

    std::pmr::vector<complex_logical_type> union_fields(&resource);
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

    // Fill column `col` of `chunk` at row `row` with the value for logical index `i`.
    auto set_cell = [&](data_chunk_t& chunk, size_t col, size_t row, size_t i) {
        switch (col) {
            case 0: // UBIGINT
                chunk.set_value(col, row, uint64_t(i));
                break;
            case 1: // STRING
                chunk.set_value(col, row, std::string_view{generate_string(i)});
                break;
            case 2: { // ARRAY<UBIGINT>
                std::vector<std::optional<uint64_t>> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                chunk.set_value(col, row, arr);
                break;
            }
            case 3: { // ARRAY<STRING>
                std::vector<std::string> storage;
                storage.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    storage.push_back(generate_string(i * array_size + j));
                }
                std::vector<std::optional<std::string_view>> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(col, row, arr);
                break;
            }
            case 4: { // LIST<UBIGINT> — each list entry can be a different length
                std::vector<std::optional<uint64_t>> list;
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(uint64_t{i * list_length(i) + j});
                }
                chunk.set_value(col, row, list);
                break;
            }
            case 5: { // LIST<STRING>
                std::vector<std::string> storage;
                storage.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    storage.push_back(generate_string(i * list_length(i) + j));
                }
                std::vector<std::optional<std::string_view>> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(col, row, list);
                break;
            }
            case 6: { // STRUCT
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
                chunk.set_value(col, row, logical_value_t::create_struct(&resource, struct_type, value_fiels));
                break;
            }
            case 7: { // UNION
                switch (i % 3) {
                    case 0:
                        chunk.set_value(col,
                                        row,
                                        logical_value_t::create_union(&resource,
                                                                      union_fields,
                                                                      0,
                                                                      logical_value_t{&resource, i % 2 == 0}));
                        break;
                    case 1:
                        chunk.set_value(
                            col,
                            row,
                            logical_value_t::create_union(&resource,
                                                          union_fields,
                                                          1,
                                                          logical_value_t{&resource, static_cast<int32_t>(i)}));
                        break;
                    case 2:
                        chunk.set_value(
                            col,
                            row,
                            logical_value_t::create_union(
                                &resource,
                                union_fields,
                                2,
                                logical_value_t{&resource,
                                                std::string{"long_string_with_index_" + std::to_string(i)}}));
                        break;
                    default:
                        break;
                }
                break;
            }
            default:
                break;
        }
    };

    // Column positions of each logical column in a scanned/fetched result. `absent` marks a
    // column missing in that layout (e.g. UBIGINT once the first column is dropped).
    constexpr size_t absent = SIZE_MAX;
    struct col_layout {
        size_t ubigint, str, arr_ub, arr_str, list_ub, list_str, strct, uni, smallint;
    };

    // Verify result row `local` carries the values expected for logical index `idx`.
    auto check_cols = [&](data_chunk_t& result, size_t local, size_t idx, const col_layout& L) {
        if (L.ubigint != absent) {
            logical_value_t value = result.data[L.ubigint].value(local);
            REQUIRE(value.type().type() == logical_type::UBIGINT);
            REQUIRE(value.value<uint64_t>() == idx);
        }
        {
            logical_value_t value = result.data[L.str].value(local);
            REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
            REQUIRE(*value.value<std::string*>() == generate_string(idx));
        }
        {
            logical_value_t value = result.data[L.arr_ub].value(local);
            REQUIRE(value.type().type() == logical_type::ARRAY);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                REQUIRE(value.children()[j].value<uint64_t>() == idx * array_size + j);
            }
        }
        {
            logical_value_t value = result.data[L.arr_str].value(local);
            REQUIRE(value.type().type() == logical_type::ARRAY);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                REQUIRE(*value.children()[j].value<std::string*>() == generate_string(idx * array_size + j));
            }
        }
        {
            logical_value_t value = result.data[L.list_ub].value(local);
            REQUIRE(value.type().type() == logical_type::LIST);
            for (size_t j = 0; j < list_length(idx); j++) {
                REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                REQUIRE(value.children()[j].value<uint64_t>() == idx * list_length(idx) + j);
            }
        }
        {
            logical_value_t value = result.data[L.list_str].value(local);
            REQUIRE(value.type().type() == logical_type::LIST);
            for (size_t j = 0; j < list_length(idx); j++) {
                REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                REQUIRE(*value.children()[j].value<std::string*>() == generate_string(idx * list_length(idx) + j));
            }
        }
        {
            logical_value_t value = result.data[L.strct].value(local);
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

            REQUIRE(value.children()[0].value<bool>() == test_data[idx].flag);
            REQUIRE(value.children()[1].value<int32_t>() == test_data[idx].number);
            REQUIRE(*value.children()[2].value<std::string*>() == test_data[idx].name);
            std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
            REQUIRE(arr.size() == test_data[idx].array.size());
            for (size_t j = 0; j < arr.size(); j++) {
                REQUIRE(arr[j].value<uint16_t>() == test_data[idx].array[j]);
            }
        }
        {
            logical_value_t value = result.data[L.uni].value(local);
            REQUIRE(value.type().type() == logical_type::UNION);
            REQUIRE(value.type().alias() == "test_union");

            auto tag = value.children()[0].value<uint8_t>();
            switch (tag) {
                case 0:
                    REQUIRE(value.type().child_types()[1].type() == logical_type::BOOLEAN);
                    REQUIRE(value.type().child_types()[1].alias() == "bool");
                    REQUIRE(value.children()[1].value<bool>() == (idx % 2 == 0));
                    break;
                case 1:
                    REQUIRE(value.type().child_types()[2].type() == logical_type::INTEGER);
                    REQUIRE(value.type().child_types()[2].alias() == "int");
                    REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(idx));
                    break;
                case 2:
                    REQUIRE(value.type().child_types()[3].type() == logical_type::STRING_LITERAL);
                    REQUIRE(value.type().child_types()[3].alias() == "string");
                    REQUIRE(value.children()[3].value<std::string_view>() ==
                            std::string{"long_string_with_index_" + std::to_string(idx)});
                    break;
                default:
                    break;
            }
        }
        if (L.smallint != absent) {
            logical_value_t value = result.data[L.smallint].value(local);
            REQUIRE(value.type().type() == logical_type::SMALLINT);
            REQUIRE(value.value<int16_t>() == static_cast<int16_t>(idx));
        }
    };

    // Scan the whole table into a batch of ≤cap chunks (state carries any filter set up by
    // initialize_scan), then verify each row through `map` (result-stream position → logical index).
    auto scan_and_check =
        [&](data_table_t& table, table_scan_state& state, const col_layout& L, size_t expected_rows, auto&& map) {
            std::pmr::vector<data_chunk_t> batches(&resource);
            table.scan_batched(table.copy_types(), nullptr, batches, state, &resource);
            size_t produced = 0;
            for (auto& result : batches) {
                for (size_t local = 0; local < result.size(); ++local, ++produced) {
                    check_cols(result, local, map(produced), L);
                }
            }
            REQUIRE(produced == expected_rows);
        };

    const col_layout base_layout{0, 1, 2, 3, 4, 5, 6, 7, absent};

    auto data_table = std::make_unique<data_table_t>(&resource, block_manager, std::move(columns));

    INFO("Append") {
        table_append_state state(&resource);
        REQUIRE_FALSE(data_table->append_lock(state).has_error());
        REQUIRE_FALSE(data_table->initialize_append(state).has_error());
        for (size_t base = 0; base < test_size; base += cap) {
            const size_t count = std::min(cap, test_size - base);
            data_chunk_t chunk(&resource, data_table->copy_types(), count);
            chunk.set_cardinality(count);
            for (size_t local = 0; local < count; ++local) {
                const size_t i = base + local;
                for (size_t col = 0; col < 8; ++col) {
                    set_cell(chunk, col, local, i);
                }
            }
            REQUIRE_FALSE(data_table->append(chunk, state).has_error());
        }
        data_table->finalize_append(state, transaction_data{0, 0});
    }
    INFO("Fetch") {
        column_fetch_state state;
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        // Fetch is row-id driven and fills one ≤cap result per call, so fetch in windows.
        for (size_t base = 0; base < test_size; base += cap) {
            const size_t count = std::min(cap, test_size - base);
            vector_t rows(&resource, logical_type::BIGINT, count);
            for (size_t local = 0; local < count; local++) {
                rows.set_value(local, static_cast<int64_t>(base + local));
            }
            data_chunk_t result(&resource, data_table->copy_types(), count);
            data_table->fetch(result, column_indices, rows, count, state);
            REQUIRE(result.size() == count);
            for (size_t local = 0; local < count; local++) {
                check_cols(result, local, base + local, base_layout);
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
        data_table->initialize_scan(state, column_indices);
        scan_and_check(*data_table, state, base_layout, test_size, [](size_t produced) { return produced; });
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
                                                std::pmr::vector<uint64_t>{{uint64_t{0}}, &resource}));
        conj_and->child_filters.emplace_back(
            std::make_unique<constant_filter_t>(components::expressions::compare_type::lt,
                                                logical_value_t{&resource, generate_string(row_range.second)},
                                                std::pmr::vector<uint64_t>{{uint64_t{1}}, &resource}));
        data_table->initialize_scan(state, column_indices, conj_and.get());
        scan_and_check(*data_table, state, base_layout, row_range.second - row_range.first, [&](size_t produced) {
            return row_range.first + produced;
        });
    }
    INFO("Delete") {
        vector_t v(&resource, logical_type::BIGINT, test_size / 2);
        for (size_t i = 0; i < test_size; i += 2) {
            v.set_value(i / 2, int64_t(i));
        }
        auto state = data_table->initialize_delete({});
        auto deleted_count = data_table->delete_rows(*state, v, test_size / 2, 0);
        REQUIRE(deleted_count == test_size / 2);
    }
    INFO("Scan after delete") {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        data_table->initialize_scan(state, column_indices);
        REQUIRE(data_table->calculate_size() == test_size / 2);
        // Even rows were deleted, so surviving rows are the odd indices: 1, 3, 5, ...
        scan_and_check(*data_table, state, base_layout, test_size / 2, [](size_t produced) {
            return produced * 2 + 1;
        });
    }

    INFO("Extension") {
        std::unique_ptr<data_table_t> extended_table;

        {
            column_definition_t new_column{"temp_column_name7", logical_type::SMALLINT, int16_t(0)};
            extended_table = std::make_unique<data_table_t>(*data_table, new_column);

            // Update values in new column
            // Since row mask is applied to every column at once, in update we have to fill whole column
            // or manually calculate ids where set is needed
            vector_t v(&resource, logical_type::BIGINT, test_size / 2);
            data_chunk_t chunk(&resource, {logical_type::SMALLINT}, test_size / 2);
            chunk.set_cardinality(test_size / 2);
            for (size_t i = 0; i < test_size / 2; i++) {
                v.set_value(i, int64_t(i * 2 + 1));
                chunk.set_value(0, i, int16_t(i * 2 + 1));
            }
            auto update_result = extended_table->update_column(v, {8}, chunk);
            REQUIRE_FALSE(update_result.has_error());
        }
        // Scan after extension: base columns plus the new SMALLINT at index 8.
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(extended_table->column_count());
            for (size_t i = 0; i < extended_table->column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_scan_state state(&resource);
            extended_table->initialize_scan(state, column_indices);
            const col_layout extended_layout{0, 1, 2, 3, 4, 5, 6, 7, 8};
            scan_and_check(*extended_table, state, extended_layout, test_size / 2, [](size_t produced) {
                return produced * 2 + 1;
            });
        }

        // Remove column
        std::unique_ptr<data_table_t> short_table = std::make_unique<data_table_t>(*extended_table, 0);

        // Scan after column removal: the first column (UBIGINT) is gone, so every later
        // column shifts down by one and the SMALLINT lands at index 7.
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(short_table->column_count());
            for (size_t i = 0; i < short_table->column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_scan_state state(&resource);
            short_table->initialize_scan(state, column_indices);
            const col_layout short_layout{absent, 0, 1, 2, 3, 4, 5, 6, 7};
            scan_and_check(*short_table, state, short_layout, test_size / 2, [](size_t produced) {
                return produced * 2 + 1;
            });
        }
    }
}
