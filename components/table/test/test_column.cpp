#include <catch2/catch_test_macros.hpp>

#include <components/table/standard_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/partial_block_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/cell_equal.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <filesystem>
#include <string>
#include <unistd.h>

TEST_CASE("components::table::column") {
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

    // column_segments are supposed to be maxed at DEFAULT_VECTOR_CAPACITY; a higher amount is not guaranteed
    // to behave correctly.
    constexpr size_t test_size = DEFAULT_VECTOR_CAPACITY;
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

    // Fresh .otbx per block: child segments cap at 1024 values, so even a 128-element ARRAY
    // append writes through to disk.
    const std::string db_path = "/tmp/test_otterbrix_column_" + std::to_string(::getpid()) + ".otbx";

    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BOOLEAN, "flag");
    fields.emplace_back(logical_type::INTEGER, "number");
    fields.emplace_back(logical_type::TEXT, "name");
    fields.emplace_back(complex_logical_type::create_list(logical_type::USMALLINT, "array"));
    complex_logical_type struct_type = complex_logical_type::create_struct("struct", fields, "test_struct");
    INFO("fixed size");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, logical_type::UBIGINT);
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            for (size_t i = 0; i < test_size; i++) {
                v.set_value(i, uint64_t(i));
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::UBIGINT);
                REQUIRE(v.get_value<uint64_t>(i) == i);
            }
        }
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::UBIGINT);
                REQUIRE(v.get_value<uint64_t>(i) == i);
            }
        }
    }
    INFO("string");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, logical_type::TEXT);
        {
            vector_t v(&resource, logical_type::TEXT, test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::string value{generate_string(i)};
                v.set_value(i, std::string_view{value});
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, logical_type::TEXT, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::TEXT);
                std::string result{v.get_value<std::string_view>(i)};
                REQUIRE(result == generate_string(i));
            }
        }
        {
            vector_t v(&resource, logical_type::TEXT, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::TEXT);
                std::string result{v.get_value<std::string_view>(i)};
                REQUIRE(result == generate_string(i));
            }
        }
    }
    INFO("array of fixed size");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column =
            column_data_t::create_column(&resource,
                                         block_manager,
                                         0,
                                         0,
                                         complex_logical_type::create_array(logical_type::UBIGINT, array_size));
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<uint64_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(uint64_t{i * array_size + j});
                }
                v.set_value(i, arr);
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                    REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == i * array_size + j);
                }
            }
        }
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                    REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == i * array_size + j);
                }
            }
        }
    }
    INFO("array of string");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource,
                                                   block_manager,
                                                   0,
                                                   0,
                                                   complex_logical_type::create_array(logical_type::TEXT, array_size));
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::TEXT, array_size), test_size);
            for (size_t i = 0; i < test_size; i++) {
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
                v.set_value(i, arr);
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::TEXT, array_size), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(seq.child->type().type() == logical_type::TEXT);
                    std::string result = std::string{seq.child->get_value<std::string_view>(seq.offset + j)};
                    REQUIRE(result == generate_string(i * array_size + j));
                }
            }
        }
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::TEXT, array_size), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(seq.child->type().type() == logical_type::TEXT);
                    std::string result = std::string{seq.child->get_value<std::string_view>(seq.offset + j)};
                    REQUIRE(result == generate_string(i * array_size + j));
                }
            }
        }
    }
    INFO("list of fixed size");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource,
                                                   block_manager,
                                                   0,
                                                   0,
                                                   complex_logical_type::create_list(logical_type::UBIGINT));
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<uint64_t> list;
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(uint64_t{i * list_length(i) + j});
                }
                v.set_value(i, list);
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                    REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == i * list_length(i) + j);
                }
            }
        }
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(seq.child->type().type() == logical_type::UBIGINT);
                    REQUIRE(seq.child->get_value<uint64_t>(seq.offset + j) == i * list_length(i) + j);
                }
            }
        }
        // In place and length-preserving: each row keeps its stored list length.
    }
    INFO("list of string");
    {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource,
                                                   block_manager,
                                                   0,
                                                   0,
                                                   complex_logical_type::create_list(logical_type::TEXT));
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::TEXT), test_size);
            for (size_t i = 0; i < test_size; i++) {
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
                v.set_value(i, list);
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::TEXT), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(seq.child->type().type() == logical_type::TEXT);
                    std::string result = std::string{seq.child->get_value<std::string_view>(seq.offset + j)};
                    REQUIRE(result == generate_string(i * list_length(i) + j));
                }
            }
        }
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::TEXT), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                const auto seq = sequence_of(v, i);
                REQUIRE(v.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(seq.child->type().type() == logical_type::TEXT);
                    std::string result = std::string{seq.child->get_value<std::string_view>(seq.offset + j)};
                    REQUIRE(result == generate_string(i * list_length(i) + j));
                }
            }
        }
        // In place and length-preserving: each row keeps its stored list length. String elements exercise
        // the variable-width child path: the new strings live in the child column's update heap, the same
        // buffer that backs a plain string.
    }
    INFO("struct");
    {
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

        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        std::remove(db_path.c_str());
        auto block_manager = storage::single_file_block_manager_t(buffer_manager, fs, db_path);
        REQUIRE_FALSE(block_manager.create_new_database().has_error());
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, struct_type);

        {
            vector_t v(&resource, struct_type, test_size);

            for (size_t i = 0; i < test_size; i++) {
                const std::string name = generate_string(i);
                std::vector<uint16_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(static_cast<uint16_t>(j));
                }
                v.entries()[0]->set_value(i, i % 2 != 0);
                v.entries()[1]->set_value(i, int32_t(i));
                v.entries()[2]->set_value(i, std::string_view{name});
                v.entries()[3]->set_value(i, arr);
            }

            auto state_pbm = components::table::storage::partial_block_manager_t::for_checkpoint(block_manager);

            column_append_state state{&state_pbm};
            REQUIRE_FALSE(column->initialize_append(state).has_error());
            REQUIRE_FALSE(column->append(state, v, 0, test_size).has_error());
            REQUIRE_FALSE(state_pbm.flush_partial_blocks().has_error());
        }
        {
            vector_t v(&resource, struct_type, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::STRUCT);
                REQUIRE(v.type().alias() == "test_struct");
                REQUIRE(v.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(v.type().child_types()[0].alias() == "flag");
                REQUIRE(v.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(v.type().child_types()[1].alias() == "number");
                REQUIRE(v.type().child_types()[2].type() == logical_type::TEXT);
                REQUIRE(v.type().child_types()[2].alias() == "name");
                REQUIRE(v.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(v.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(v.type().child_types()[3].alias() == "array");

                const auto cell = resolve_cell(v, i);
                REQUIRE(cell.vec->entries()[0]->get_value<bool>(cell.index) == test_data[i].flag);
                REQUIRE(cell.vec->entries()[1]->get_value<int32_t>(cell.index) == test_data[i].number);
                REQUIRE(cell.vec->entries()[2]->get_value<std::string_view>(cell.index) == test_data[i].name);
                const auto arr = cell.vec->entries()[3]->get_value<std::vector<uint16_t>>(cell.index);
                REQUIRE(arr == test_data[i].array);
            }
        }
        {
            vector_t v(&resource, struct_type, test_size);
            column_scan_state state;
            state.child_states.resize(struct_type.child_types().size() + 1);
            state.scan_child_column.resize(struct_type.child_types().size());
            for (size_t i = 0; i < state.scan_child_column.size(); i++) {
                state.scan_child_column[i] = true;
            }
            state.child_states[1].child_states.resize(1);
            state.child_states[2].child_states.resize(1);
            state.child_states[3].child_states.resize(1);
            state.child_states[4].child_states.resize(2);
            state.child_states[4].child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                REQUIRE(v.type().type() == logical_type::STRUCT);
                REQUIRE(v.type().alias() == "test_struct");
                REQUIRE(v.type().child_types()[0].type() == logical_type::BOOLEAN);
                REQUIRE(v.type().child_types()[0].alias() == "flag");
                REQUIRE(v.type().child_types()[1].type() == logical_type::INTEGER);
                REQUIRE(v.type().child_types()[1].alias() == "number");
                REQUIRE(v.type().child_types()[2].type() == logical_type::TEXT);
                REQUIRE(v.type().child_types()[2].alias() == "name");
                REQUIRE(v.type().child_types()[3].type() == logical_type::LIST);
                REQUIRE(v.type().child_types()[3].child_type().type() == logical_type::USMALLINT);
                REQUIRE(v.type().child_types()[3].alias() == "array");

                const auto cell = resolve_cell(v, i);
                REQUIRE(cell.vec->entries()[0]->get_value<bool>(cell.index) == test_data[i].flag);
                REQUIRE(cell.vec->entries()[1]->get_value<int32_t>(cell.index) == test_data[i].number);
                REQUIRE(cell.vec->entries()[2]->get_value<std::string_view>(cell.index) == test_data[i].name);
                const auto arr = cell.vec->entries()[3]->get_value<std::vector<uint16_t>>(cell.index);
                REQUIRE(arr == test_data[i].array);
            }
        }
        /*
        // Update
        */
    }

    std::remove(db_path.c_str());
}
