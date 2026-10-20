#include <catch2/catch_test_macros.hpp>
#include <components/expressions/execution_dag_builder.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <math.h>
#include <string>
#include <unistd.h>

namespace {
    // delete_rows answers a refusal, not just a count: result_wrapper_t::value() asserts the
    // channel was cleared first, so a test that wants the number has to clear it.
    uint64_t deleted_or_fail(core::result_wrapper_t<uint64_t> r) {
        REQUIRE_FALSE(r.has_error());
        return r.value();
    }
} // namespace

TEST_CASE("components::table::data_table") {
    using namespace components::types;
    using namespace components::vector;
    using namespace components::table;

    auto resource = core::pmr::otterbrix_resource();

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
    fields.emplace_back(logical_type::TEXT, "name");
    fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
    complex_logical_type struct_type = complex_logical_type::create_struct("struct", fields, "test_struct");

    std::pmr::vector<complex_logical_type> union_fields(&resource);
    union_fields.emplace_back(logical_type::BOOLEAN, "bool");
    union_fields.emplace_back(logical_type::INTEGER, "int");
    union_fields.emplace_back(logical_type::TEXT, "string");
    complex_logical_type union_type = complex_logical_type::create_union(union_fields, "test_union");

    // Real .otbx: test_size is more than one row group, so closing a group writes through to disk.
    const std::string db_path = "/tmp/test_otterbrix_data_table_" + std::to_string(::getpid()) + ".otbx";
    std::remove(db_path.c_str());
    core::filesystem::local_file_system_t fs;
    auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
    auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
    auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
    REQUIRE_FALSE(block_manager.create_new_database().has_error());

    std::vector<column_definition_t> columns;
    columns.reserve(8);
    columns.emplace_back("temp_column_name0", logical_type::UBIGINT);
    columns.emplace_back("temp_column_name1", logical_type::TEXT);
    columns.emplace_back("temp_column_name2", complex_logical_type::create_array(logical_type::UBIGINT, array_size));
    columns.emplace_back("temp_column_name3", complex_logical_type::create_array(logical_type::TEXT, array_size));
    columns.emplace_back("temp_column_name4", complex_logical_type::create_list(logical_type::UBIGINT));
    columns.emplace_back("temp_column_name5", complex_logical_type::create_list(logical_type::TEXT));
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
            case 1: // TEXT
                chunk.set_value(col, row, std::string_view{generate_string(i)});
                break;
            case 2: { // ARRAY<UBIGINT>
                std::vector<uint64_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                chunk.set_value(col, row, arr);
                break;
            }
            case 3: { // ARRAY<TEXT>
                std::vector<std::string> storage;
                storage.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    storage.push_back(generate_string(i * array_size + j));
                }
                std::vector<std::string_view> arr;
                arr.reserve(array_size);
                for (const auto& s : storage) {
                    arr.emplace_back(std::string_view{s});
                }
                chunk.set_value(col, row, arr);
                break;
            }
            case 4: { // LIST<UBIGINT> — each list entry can be a different length
                std::vector<uint64_t> list;
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(uint64_t{i * list_length(i) + j});
                }
                chunk.set_value(col, row, list);
                break;
            }
            case 5: { // LIST<TEXT>
                std::vector<std::string> storage;
                storage.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    storage.push_back(generate_string(i * list_length(i) + j));
                }
                std::vector<std::string_view> list;
                list.reserve(list_length(i));
                for (const auto& s : storage) {
                    list.emplace_back(std::string_view{s});
                }
                chunk.set_value(col, row, list);
                break;
            }
            case 6: { // STRUCT
                auto& fields = chunk.data[col].entries();
                fields[0]->set_value(row, test_data[i].flag);
                fields[1]->set_value(row, test_data[i].number);
                fields[2]->set_value(row, std::string_view{test_data[i].name});
                fields[3]->set_value(row, test_data[i].array);
                break;
            }
            case 7: { // UNION
                auto& members = chunk.data[col].entries();
                const auto tag = static_cast<uint8_t>(i % 3);
                members[0]->set_value(row, tag);
                for (size_t m = 1; m < members.size(); ++m) {
                    members[m]->set_null(row, m != static_cast<size_t>(tag) + 1);
                }
                switch (tag) {
                    case 0:
                        members[1]->set_value(row, i % 2 == 0);
                        break;
                    case 1:
                        members[2]->set_value(row, static_cast<int32_t>(i));
                        break;
                    default: {
                        const std::string text{"long_string_with_index_" + std::to_string(i)};
                        members[3]->set_value(row, std::string_view{text});
                        break;
                    }
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
            const auto& column = result.data[L.ubigint];
            REQUIRE(column.type().type() == logical_type::UBIGINT);
            REQUIRE(column.get_value<uint64_t>(local) == idx);
        }
        {
            const auto& column = result.data[L.str];
            REQUIRE(column.type().type() == logical_type::TEXT);
            REQUIRE(column.get_value<std::string_view>(local) == generate_string(idx));
        }
        {
            const auto& column = result.data[L.arr_ub];
            REQUIRE(column.type().type() == logical_type::ARRAY);
            const auto seq = sequence_of(column, local);
            REQUIRE(seq.length == array_size);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == idx * array_size + j);
            }
        }
        {
            const auto& column = result.data[L.arr_str];
            REQUIRE(column.type().type() == logical_type::ARRAY);
            const auto seq = sequence_of(column, local);
            REQUIRE(seq.length == array_size);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(seq.child->type().type() == logical_type::TEXT);
                REQUIRE(seq.child->get_value<std::string_view>(seq.offset + j) ==
                        generate_string(idx * array_size + j));
            }
        }
        {
            const auto& column = result.data[L.list_ub];
            REQUIRE(column.type().type() == logical_type::LIST);
            const auto seq = sequence_of(column, local);
            REQUIRE(seq.length == list_length(idx));
            for (size_t j = 0; j < list_length(idx); j++) {
                REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == idx * list_length(idx) + j);
            }
        }
        {
            const auto& column = result.data[L.list_str];
            REQUIRE(column.type().type() == logical_type::LIST);
            const auto seq = sequence_of(column, local);
            REQUIRE(seq.length == list_length(idx));
            for (size_t j = 0; j < list_length(idx); j++) {
                REQUIRE(seq.child->type().type() == logical_type::TEXT);
                REQUIRE(seq.child->get_value<std::string_view>(seq.offset + j) ==
                        generate_string(idx * list_length(idx) + j));
            }
        }
        {
            const auto& column = result.data[L.strct];
            REQUIRE(column.type().type() == logical_type::STRUCT);
            REQUIRE(column.type().alias() == "test_struct");
            REQUIRE(column.type().child_types()[0].type() == logical_type::BOOLEAN);
            REQUIRE(column.type().child_types()[0].alias() == "flag");
            REQUIRE(column.type().child_types()[1].type() == logical_type::INTEGER);
            REQUIRE(column.type().child_types()[1].alias() == "number");
            REQUIRE(column.type().child_types()[2].type() == logical_type::TEXT);
            REQUIRE(column.type().child_types()[2].alias() == "name");
            REQUIRE(column.type().child_types()[3].type() == logical_type::LIST);
            REQUIRE(column.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
            REQUIRE(column.type().child_types()[3].alias() == "array");

            const auto cell = resolve_cell(column, local);
            REQUIRE(cell.vec->entries()[0]->get_value<bool>(cell.index) == test_data[idx].flag);
            REQUIRE(cell.vec->entries()[1]->get_value<int32_t>(cell.index) == test_data[idx].number);
            REQUIRE(cell.vec->entries()[2]->get_value<std::string_view>(cell.index) == test_data[idx].name);
            REQUIRE(cell.vec->entries()[3]->get_value<std::vector<uint16_t>>(cell.index) == test_data[idx].array);
        }
        {
            const auto& column = result.data[L.uni];
            REQUIRE(column.type().type() == logical_type::UNION);
            REQUIRE(column.type().alias() == "test_union");

            const auto cell = resolve_cell(column, local);
            const auto& members = cell.vec->entries();
            auto tag = members[0]->get_value<uint8_t>(cell.index);
            switch (tag) {
                case 0:
                    REQUIRE(column.type().child_types()[1].type() == logical_type::BOOLEAN);
                    REQUIRE(column.type().child_types()[1].alias() == "bool");
                    REQUIRE(members[1]->get_value<bool>(cell.index) == (idx % 2 == 0));
                    break;
                case 1:
                    REQUIRE(column.type().child_types()[2].type() == logical_type::INTEGER);
                    REQUIRE(column.type().child_types()[2].alias() == "int");
                    REQUIRE(members[2]->get_value<int32_t>(cell.index) == static_cast<int32_t>(idx));
                    break;
                case 2:
                    REQUIRE(column.type().child_types()[3].type() == logical_type::TEXT);
                    REQUIRE(column.type().child_types()[3].alias() == "string");
                    REQUIRE(members[3]->get_value<std::string_view>(cell.index) ==
                            std::string{"long_string_with_index_" + std::to_string(idx)});
                    break;
                default:
                    break;
            }
        }
        if (L.smallint != absent) {
            const auto& column = result.data[L.smallint];
            CAPTURE(idx, local, L.smallint, result.size());
            REQUIRE(column.type().type() == logical_type::SMALLINT);
            REQUIRE(column.get_value<int16_t>(local) == static_cast<int16_t>(idx));
        }
    };

    // Scan the whole table into a batch of ≤cap chunks (state carries any filter set up by
    // initialize_scan), then verify each row through `map` (result-stream position → logical index).
    auto scan_and_check =
        [&](data_table_t& table, table_scan_state& state, const col_layout& L, size_t expected_rows, auto&& map) {
            std::pmr::vector<data_chunk_t> batches(&resource);
            table.scan_batched(table.copy_types(), batches, state, &resource);
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

    INFO("Append");
    {
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
        data_table->finalize_append(state, transaction_data::committed());
    }
    INFO("Fetch");
    {
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
            // fetch_visibility_t carries no default, so SNAPSHOT is spelled out.
            data_table->fetch(result,
                              column_indices,
                              rows,
                              count,
                              state,
                              transaction_data::committed(),
                              fetch_visibility_t::SNAPSHOT);
            REQUIRE(result.size() == count);
            for (size_t local = 0; local < count; local++) {
                check_cols(result, local, base + local, base_layout);
            }
        }
    }
    INFO("Scan");
    {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        data_table->initialize_scan(state, column_indices, transaction_data::committed());
        scan_and_check(*data_table, state, base_layout, test_size, [](size_t produced) { return produced; });
    }
    INFO("Scan with predicates");
    {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        std::pair row_range{uint64_t(test_size * 0.25f), uint64_t(test_size * 0.75f)};
        // The predicate reaches the scan as one graph, the same way a pushed WHERE does:
        // col0 >= lo AND col1 < hi.
        namespace expr = components::expressions;
        const core::parameter_id_t lo_id{0};
        const core::parameter_id_t hi_id{1};
        components::vector::storage_parameters parameters(&resource);
        REQUIRE(parameters.add(components::vector::make_scalar(&resource, row_range.first),
                               components::vector::parameter_kind::literal) == lo_id);
        REQUIRE(parameters.add(components::vector::make_scalar(&resource, generate_string(row_range.second)),
                               components::vector::parameter_kind::literal) == hi_id);

        auto column_key = [&](size_t ordinal) {
            expr::key_t key{&resource};
            key.set_path(std::pmr::vector<size_t>{{ordinal}, std::pmr::polymorphic_allocator<size_t>{&resource}});
            return key;
        };
        auto predicate = expr::make_compare_union_expression(&resource, expr::compare_type::union_and);
        predicate->append_child(
            expr::make_compare_expression(&resource, expr::compare_type::gte, column_key(0), lo_id));
        predicate->append_child(expr::make_compare_expression(&resource, expr::compare_type::lt, column_key(1), hi_id));

        auto built = expr::build_condition_graph(&resource, parameters, predicate.get(), data_table->copy_types());
        REQUIRE_FALSE(built.has_error());
        table_filter_t filter{std::move(parameters),
                              components::graph_execution_context{},
                              std::move(built.value()),
                              expr::condition_kind::computed};
        data_table->initialize_scan(state, column_indices, transaction_data::committed(), &filter);
        scan_and_check(*data_table, state, base_layout, row_range.second - row_range.first, [&](size_t produced) {
            return row_range.first + produced;
        });
    }
    INFO("Delete");
    {
        int64_t even = 0;
        auto row_ids = make_row_id_batch(&resource, test_size / 2, [&] {
            const int64_t id = even;
            even += 2;
            return id;
        });
        auto deleted_count = deleted_or_fail(data_table->delete_rows(row_ids, transaction_data::committed()));
        REQUIRE(deleted_count == test_size / 2);
    }
    INFO("Scan after delete");
    {
        std::vector<storage_index_t> column_indices;
        column_indices.reserve(data_table->column_count());
        for (size_t i = 0; i < data_table->column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table_scan_state state(&resource);
        data_table->initialize_scan(state, column_indices, transaction_data::committed());
        REQUIRE(data_table->calculate_size() == test_size / 2);
        // Even rows were deleted, so surviving rows are the odd indices: 1, 3, 5, ...
        scan_and_check(*data_table, state, base_layout, test_size / 2, [](size_t produced) {
            return produced * 2 + 1;
        });
    }

    INFO("Extension");
    {
        std::unique_ptr<data_table_t> extended_table;

        {
            column_definition_t new_column{"temp_column_name7", logical_type::SMALLINT, int16_t(0)};
            extended_table = std::make_unique<data_table_t>(*data_table, new_column);
        }
        // Scan after extension: base columns plus the new SMALLINT at index 8.
        {
            std::vector<storage_index_t> column_indices;
            column_indices.reserve(extended_table->column_count());
            for (size_t i = 0; i < extended_table->column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_scan_state state(&resource);
            extended_table->initialize_scan(state, column_indices, transaction_data::committed());
            // The added column is never written here, so only the base columns are checked:
            // populating it needed the update overlay, which no longer exists.
            const col_layout extended_layout{0, 1, 2, 3, 4, 5, 6, 7, absent};
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
            short_table->initialize_scan(state, column_indices, transaction_data::committed());
            const col_layout short_layout{absent, 0, 1, 2, 3, 4, 5, 6, absent};
            scan_and_check(*short_table, state, short_layout, test_size / 2, [](size_t produced) {
                return produced * 2 + 1;
            });
        }
    }

    std::remove(db_path.c_str());
}
