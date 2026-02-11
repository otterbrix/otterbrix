#include <catch2/catch.hpp>

#include <components/vector/vector.hpp>

TEST_CASE("components::vector::vector") {
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

    constexpr size_t test_size = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr size_t array_size = 128;
    constexpr size_t max_list_size = 128;
    auto list_length = [&](size_t i) { return i - (i / max_list_size) * max_list_size; };

    std::vector<components::types::complex_logical_type> fields;
    fields.emplace_back(components::types::logical_type::BOOLEAN, "flag");
    fields.emplace_back(components::types::logical_type::INTEGER, "number");
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
    fields.emplace_back(
        components::types::complex_logical_type::create_list(components::types::logical_type::USMALLINT, "array"));
    components::types::complex_logical_type struct_type =
        components::types::complex_logical_type::create_struct("struct", fields, "test_struct");

    INFO("sixed size") {
        components::vector::vector_t v(&resource,
                                       components::types::logical_type::UBIGINT,
                                       test_size);
        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value{&resource, uint64_t(i)};
            v.set_value(i, value);
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::UBIGINT);
            REQUIRE(value.value<uint64_t>() == i);
        }
    }
    INFO("string") {
        components::vector::vector_t v(&resource,
                                       components::types::logical_type::STRING_LITERAL,
                                       test_size);
        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value{v.resource(), std::string{"long_string_with_index_" + std::to_string(i)}};
            v.set_value(i, value);
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::STRING_LITERAL);
            std::string result = *(value.value<std::string*>());
            REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
        }
    }
    INFO("array of fixed size") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_array(components::types::logical_type::UBIGINT, array_size),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<components::types::logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(v.resource(), uint64_t{i * array_size + j});
            }
            v.set_value(
                i,
                components::types::logical_value_t::create_array(v.resource(), components::types::logical_type::UBIGINT, arr));
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::ARRAY);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(value.children()[j].type().type() == components::types::logical_type::UBIGINT);
                REQUIRE(value.children()[j].value<uint64_t>() == i * array_size + j);
            }
        }
    }
    INFO("array of string") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_array(components::types::logical_type::STRING_LITERAL,
                                                                  array_size),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<components::types::logical_value_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(v.resource(), std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
            }
            v.set_value(
                i,
                components::types::logical_value_t::create_array(v.resource(), components::types::logical_type::STRING_LITERAL, arr));
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::ARRAY);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(value.children()[j].type().type() == components::types::logical_type::STRING_LITERAL);
                std::string result = *(value.children()[j].value<std::string*>());
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
            }
        }
    }
    INFO("list of fixed size") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_list(components::types::logical_type::UBIGINT),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<components::types::logical_value_t> list;
            // test that each list entry can be a different length
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(v.resource(), uint64_t{i * list_length(i) + j});
            }
            v.set_value(
                i,
                components::types::logical_value_t::create_list(v.resource(), components::types::logical_type::UBIGINT, list));
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::LIST);
            for (size_t j = 0; j < list_length(i); j++) {
                REQUIRE(value.children()[j].type().type() == components::types::logical_type::UBIGINT);
                REQUIRE(value.children()[j].value<uint64_t>() == i * list_length(i) + j);
            }
        }
    }
    INFO("list of string") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_list(components::types::logical_type::STRING_LITERAL),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<components::types::logical_value_t> list;
            // test that each list entry can be a different length
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(v.resource(), std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
            }
            v.set_value(
                i,
                components::types::logical_value_t::create_list(v.resource(), components::types::logical_type::STRING_LITERAL, list));
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::LIST);
            for (size_t j = 0; j < list_length(i); j++) {
                REQUIRE(value.children()[j].type().type() == components::types::logical_type::STRING_LITERAL);
                std::string result = *(value.children()[j].value<std::string*>());
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
            }
        }
    }
    INFO("struct") {
        std::vector<test_struct> test_data;
        test_data.reserve(test_size);
        for (size_t i = 0; i < test_size; i++) {
            auto s{std::string{"long_string_with_index_" + std::to_string(i)}};
            std::vector<uint16_t> arr;
            arr.reserve(i);
            for (size_t j = 0; j < i; j++) {
                arr.emplace_back(j);
            }
            test_data.emplace_back(i % 2 != 0, i, std::move(s), std::move(arr));
        }

        components::vector::vector_t v(&resource, struct_type, test_size);

        for (size_t i = 0; i < test_size; i++) {
            std::vector<components::types::logical_value_t> arr;
            arr.reserve(i);
            for (size_t j = 0; j < i; j++) {
                arr.emplace_back(v.resource(), test_data[i].array[j]);
            }
            std::vector<components::types::logical_value_t> value_fiels;
            value_fiels.emplace_back(components::types::logical_value_t{v.resource(), test_data[i].flag});
            value_fiels.emplace_back(components::types::logical_value_t{v.resource(), test_data[i].number});
            value_fiels.emplace_back(components::types::logical_value_t{v.resource(), test_data[i].name});
            value_fiels.emplace_back(
                components::types::logical_value_t::create_list(v.resource(), components::types::logical_type::USMALLINT, arr));
            components::types::logical_value_t value =
                components::types::logical_value_t::create_struct(v.resource(), struct_type, value_fiels);
            v.set_value(i, value);
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = v.value(i);
            REQUIRE(value.type().type() == components::types::logical_type::STRUCT);
            REQUIRE(value.type().alias() == "test_struct");
            REQUIRE(value.type().child_types()[0].type() == components::types::logical_type::BOOLEAN);
            REQUIRE(value.type().child_types()[0].alias() == "flag");
            REQUIRE(value.type().child_types()[1].type() == components::types::logical_type::INTEGER);
            REQUIRE(value.type().child_types()[1].alias() == "number");
            REQUIRE(value.type().child_types()[2].type() == components::types::logical_type::STRING_LITERAL);
            REQUIRE(value.type().child_types()[2].alias() == "name");
            REQUIRE(value.type().child_types()[3].type() == components::types::logical_type::LIST);
            REQUIRE(value.type().child_types()[3].child_type().type() == components::types::logical_type::USMALLINT);
            REQUIRE(value.type().child_types()[3].alias() == "array");

            REQUIRE(value.children()[0].value<bool>() == test_data[i].flag);
            REQUIRE(value.children()[1].value<int32_t>() == test_data[i].number);
            REQUIRE(*value.children()[2].value<std::string*>() == test_data[i].name);
            std::vector arr(*value.children()[3].value<std::vector<components::types::logical_value_t>*>());
            REQUIRE(arr.size() == test_data[i].array.size());
            for (size_t j = 0; j < arr.size(); j++) {
                REQUIRE(arr[j].value<uint16_t>() == test_data[i].array[j]);
            }
        }
    }
    INFO("dictionary") {
        constexpr size_t string_count = 16;

        std::vector<size_t> indices;
        indices.reserve(test_size);
        for (size_t i = 0; i < test_size; i++) {
            indices.emplace_back(test_size % string_count);
        }
        std::shuffle(indices.begin(), indices.end(), std::default_random_engine{0});

        components::vector::vector_t string_array(&resource,
                                                  components::types::logical_type::STRING_LITERAL,
                                                  string_count);
        for (size_t i = 0; i < string_count; i++) {
            components::types::logical_value_t value{string_array.resource(), std::string{"long_string_with_index_" + std::to_string(i)}};
            string_array.set_value(i, value);
        }

        components::vector::indexing_vector_t indexing(&resource, test_size);
        for (size_t i = 0; i < test_size; i++) {
            indexing.set_index(i, indices[i]);
        }

        components::vector::vector_t dictionary(&resource,
                                                components::types::logical_type::STRING_LITERAL,
                                                test_size);
        dictionary.slice(string_array, indexing, test_size);
        for (size_t i = 0; i < test_size; i++) {
            indexing.set_index(i, indices[i]);
        }

        REQUIRE(dictionary.get_vector_type() == components::vector::vector_type::DICTIONARY);
        for (size_t i = 0; i < test_size; i++) {
            REQUIRE(dictionary.value(i) == string_array.value(indices[i]));
        }
    }
    INFO("union") {
        std::vector<components::types::complex_logical_type> union_fields;
        union_fields.emplace_back(components::types::logical_type::BOOLEAN, "bool");
        union_fields.emplace_back(components::types::logical_type::INTEGER, "int");
        union_fields.emplace_back(components::types::logical_type::STRING_LITERAL, "string");
        auto union_type = components::types::complex_logical_type::create_union(union_fields, "union_type");

        components::vector::vector_t union_vector(&resource, union_type, test_size);

        for (size_t i = 0; i < test_size; i++) {
            switch (i % 3) {
                case 0:
                    union_vector.set_value(i,
                                           components::types::logical_value_t::create_union(
                                               union_vector.resource(),
                                               union_fields,
                                               0,
                                               components::types::logical_value_t{union_vector.resource(), i % 2 == 0}));
                    break;
                case 1:
                    union_vector.set_value(i,
                                           components::types::logical_value_t::create_union(
                                               union_vector.resource(),
                                               union_fields,
                                               1,
                                               components::types::logical_value_t{union_vector.resource(), static_cast<int32_t>(i)}));
                    break;
                case 2:
                    union_vector.set_value(i,
                                           components::types::logical_value_t::create_union(
                                               union_vector.resource(),
                                               union_fields,
                                               2,
                                               components::types::logical_value_t{
                                                   union_vector.resource(), std::string{"long_string_with_index_" + std::to_string(i)}}));
                    break;
                default:
                    continue;
            }
        }

        for (size_t i = 0; i < test_size; i++) {
            components::types::logical_value_t value = union_vector.value(i);
            auto tag = value.children()[0].value<uint8_t>();
            switch (tag) {
                case 0:
                    REQUIRE(value.children()[1].type().type() == components::types::logical_type::BOOLEAN);
                    REQUIRE(value.children()[1].value<bool>() == (i % 2 == 0));
                    break;
                case 1:
                    REQUIRE(value.children()[2].type().type() == components::types::logical_type::INTEGER);
                    REQUIRE(value.children()[2].value<int32_t>() == static_cast<int32_t>(i));
                    break;
                case 2:
                    REQUIRE(value.children()[3].type().type() == components::types::logical_type::STRING_LITERAL);
                    REQUIRE(value.children()[3].value<std::string_view>() ==
                            std::string{"long_string_with_index_" + std::to_string(i)});
                    break;
                default:
                    continue;
            }
        }
    }
}