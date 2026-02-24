#include <catch2/catch.hpp>

#include <components/table/standard_column_data.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <filesystem>

TEST_CASE("components::table::column") {
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

    // column_segments are supposed to be maxed at DEFAULT_VECTOR_CAPACITY
    // higher amount is not guarantied to behave correctly
    constexpr size_t test_size = DEFAULT_VECTOR_CAPACITY;
    constexpr size_t array_size = 128;
    constexpr size_t max_list_size = 128;
    constexpr size_t update_size = 32;
    constexpr size_t str_index_length = 10;
    auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };
    auto generate_update = [&](std::unique_ptr<column_data_t>& column) {
        vector_t v(&resource, column->type(), update_size);
        column_fetch_state state;
        for (size_t i = 0; i < update_size; i++) {
            column->fetch_row(state, static_cast<int64_t>(i), v, update_size - i - 1);
        }
        return v;
    };

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
    INFO("fixed size") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, logical_type::UBIGINT);
        // Append
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value{&resource, uint64_t(i)};
                v.set_value(i, value);
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
        }
        // Scan
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
        }
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for (size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, logical_type::UBIGINT, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == inverse);
            }
            for (size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::UBIGINT);
                REQUIRE(value.value<uint64_t>() == i);
            }
        }
    }
    INFO("string") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, logical_type::STRING_LITERAL);
        // Append
        {
            vector_t v(&resource, logical_type::STRING_LITERAL, test_size);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value{&resource, generate_string(i)};
                v.set_value(i, value);
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, logical_type::STRING_LITERAL, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == generate_string(i));
            }
        }
        // Scan
        {
            vector_t v(&resource, logical_type::STRING_LITERAL, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == generate_string(i));
            }
        }
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for (size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, logical_type::STRING_LITERAL, test_size);
            column_scan_state state;
            state.child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == generate_string(inverse));
            }
            for (size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::STRING_LITERAL);
                std::string result = *(value.value<std::string*>());
                REQUIRE(result == generate_string(i));
            }
        }
    }
    INFO("array of fixed size") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column =
            column_data_t::create_column(&resource,
                                         block_manager,
                                         0,
                                         0,
                                         complex_logical_type::create_array(logical_type::UBIGINT, array_size));
        // Append
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(&resource, uint64_t{i * array_size + j});
                }
                v.set_value(i, logical_value_t::create_array(v.resource(), logical_type::UBIGINT, arr));
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
        }
        // Scan
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
        }
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for (size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, complex_logical_type::create_array(logical_type::UBIGINT, array_size), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == inverse * array_size + j);
                }
            }
            for (size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
                }
            }
        }
    }
    INFO("array of string") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column =
            column_data_t::create_column(&resource,
                                         block_manager,
                                         0,
                                         0,
                                         complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size));
        // Append
        {
            vector_t v(&resource,
                       complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size),
                       test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<logical_value_t> arr;
                arr.reserve(array_size);
                for (size_t j = 0; j < array_size; j++) {
                    arr.emplace_back(v.resource(), generate_string(i * array_size + j));
                }
                v.set_value(i, logical_value_t::create_array(v.resource(), logical_type::STRING_LITERAL, arr));
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource,
                       complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size),
                       test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * array_size + j));
                }
            }
        }
        // Scan
        {
            vector_t v(&resource,
                       complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size),
                       test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * array_size + j));
                }
            }
        }
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for (size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource,
                       complex_logical_type::create_array(logical_type::STRING_LITERAL, array_size),
                       test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(inverse * array_size + j));
                }
            }
            for (size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::ARRAY);
                for (size_t j = 0; j < array_size; j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * array_size + j));
                }
            }
        }
    }
    INFO("list of fixed size") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column = column_data_t::create_column(&resource,
                                                   block_manager,
                                                   0,
                                                   0,
                                                   complex_logical_type::create_list(logical_type::UBIGINT));
        // Append
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(&resource, uint64_t{i * list_length(i) + j});
                }
                v.set_value(i, logical_value_t::create_list(v.resource(), logical_type::UBIGINT, list));
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
        }
        // Scan
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
        }
        /*
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for(size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for(size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for(size_t j = 0; j < list_length(inverse); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == inverse * list_length(inverse) + j);
                }
            }
            for(size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for(size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::UBIGINT);
                    REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
                }
            }
        }
        */
    }
    INFO("list of string") {
        core::filesystem::local_file_system_t fs;
        auto buffer_pool = storage::buffer_pool_t(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24);
        auto buffer_manager = storage::standard_buffer_manager_t(&resource, fs, buffer_pool);
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column = column_data_t::create_column(&resource,
                                                   block_manager,
                                                   0,
                                                   0,
                                                   complex_logical_type::create_list(logical_type::STRING_LITERAL));
        // Append
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::STRING_LITERAL), test_size);
            for (size_t i = 0; i < test_size; i++) {
                std::vector<logical_value_t> list;
                // test that each list entry can be a different length
                list.reserve(list_length(i));
                for (size_t j = 0; j < list_length(i); j++) {
                    list.emplace_back(v.resource(), generate_string(i * list_length(i) + j));
                }
                v.set_value(i, logical_value_t::create_list(v.resource(), logical_type::STRING_LITERAL, list));
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::STRING_LITERAL), test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * list_length(i) + j));
                }
            }
        }
        // Scan
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::STRING_LITERAL), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for (size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * list_length(i) + j));
                }
            }
        }
        /*
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for(size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, complex_logical_type::create_list(logical_type::UBIGINT), test_size);
            column_scan_state state;
            state.child_states.resize(2);
            state.child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for(size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for(size_t j = 0; j < list_length(inverse); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(inverse * list_length(inverse) + j));
                }
            }
            for(size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
                REQUIRE(value.type().type() == logical_type::LIST);
                for(size_t j = 0; j < list_length(i); j++) {
                    REQUIRE(value.children()[j].type().type() == logical_type::STRING_LITERAL);
                    std::string result = *(value.children()[j].value<std::string*>());
                    REQUIRE(result == generate_string(i * list_length(i) + j));
                }
            }
        }
        */
    }
    INFO("struct") {
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
        auto block_manager = storage::in_memory_block_manager_t(buffer_manager, uint64_t(1) << 18);
        auto column = column_data_t::create_column(&resource, block_manager, 0, 0, struct_type);

        // Append
        {
            vector_t v(&resource, struct_type, test_size);

            for (size_t i = 0; i < test_size; i++) {
                std::vector<logical_value_t> arr;
                arr.reserve(i);
                for (size_t j = 0; j < i; j++) {
                    arr.emplace_back(&resource, uint16_t(j));
                }
                std::vector<logical_value_t> value_fiels;
                value_fiels.emplace_back(&resource, i % 2 != 0);
                value_fiels.emplace_back(&resource, int32_t(i));
                value_fiels.emplace_back(v.resource(), generate_string(i));
                value_fiels.emplace_back(logical_value_t::create_list(v.resource(), logical_type::USMALLINT, arr));
                logical_value_t value = logical_value_t::create_struct(v.resource(), struct_type, value_fiels);
                v.set_value(i, value);
            }

            column_append_state state;
            column->initialize_append(state);
            column->append(state, v, test_size);
        }
        // Fetch
        {
            vector_t v(&resource, struct_type, test_size);
            column_fetch_state state;
            for (size_t i = 0; i < test_size; i++) {
                column->fetch_row(state, static_cast<int64_t>(i), v, i);
            }
            for (size_t i = 0; i < test_size; i++) {
                logical_value_t value = v.value(i);
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
        }
        // Scan
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
                logical_value_t value = v.value(i);
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
        }
        /*
        // Update
        {
            std::vector<int64_t> ids;
            ids.reserve(update_size);
            vector_t v = generate_update(column);
            for(size_t i = 0; i < update_size; i++) {
                ids.emplace_back(i);
            }
            column->update(0, v, ids.data(), update_size);
        }
        // Scan after update
        {
            vector_t v(&resource, struct_type, test_size);
            column_scan_state state;
            state.child_states.resize(struct_type.child_types().size() + 1);
            state.scan_child_column.resize(struct_type.child_types().size());
            for(size_t i = 0; i < state.scan_child_column.size(); i++) {
                state.scan_child_column[i] = true;
            }
            state.child_states[1].child_states.resize(1);
            state.child_states[2].child_states.resize(1);
            state.child_states[3].child_states.resize(1);
            state.child_states[4].child_states.resize(2);
            state.child_states[4].child_states[1].child_states.resize(1);
            column->initialize_scan(state);
            column->scan(0, state, v);
            for(size_t i = 0; i < update_size; i++) {
                size_t inverse = update_size - i - 1;
                logical_value_t value = v.value(i);
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

                REQUIRE(value.children()[0].value<bool>() == test_data[inverse].flag);
                REQUIRE(value.children()[1].value<int32_t>() == test_data[inverse].number);
                REQUIRE(*value.children()[2].value<std::string*>() == test_data[inverse].name);
                std::vector arr(*value.children()[3].value<std::vector<logical_value_t>*>());
                REQUIRE(arr.size() == test_data[inverse].array.size());
                for(size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[inverse].array[j]);
                }
            }
            for(size_t i = update_size; i < test_size; i++) {
                logical_value_t value = v.value(i);
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
                for(size_t j = 0; j < arr.size(); j++) {
                    REQUIRE(arr[j].value<uint16_t>() == test_data[i].array[j]);
                }
            }
        }
        */
    }
}