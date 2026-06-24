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

    std::pmr::vector<components::types::complex_logical_type> fields(&resource);
    fields.emplace_back(components::types::logical_type::BOOLEAN, "flag");
    fields.emplace_back(components::types::logical_type::INTEGER, "number");
    fields.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
    fields.emplace_back(
        components::types::complex_logical_type::create_list(components::types::logical_type::USMALLINT, "array"));
    components::types::complex_logical_type struct_type =
        components::types::complex_logical_type::create_struct("struct", fields, "test_struct");

    INFO("sixed size") {
        components::vector::vector_t v(&resource, components::types::logical_type::UBIGINT, test_size);
        for (size_t i = 0; i < test_size; i++) {
            v.set_value(i, uint64_t(i));
        }

        REQUIRE(v.type().type() == components::types::logical_type::UBIGINT);
        for (size_t i = 0; i < test_size; i++) {
            REQUIRE(v.get_value_not_null<uint64_t>(i) == i);
        }
    }
    INFO("string") {
        components::vector::vector_t v(&resource, components::types::logical_type::STRING_LITERAL, test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::string value{"long_string_with_index_" + std::to_string(i)};
            v.set_value(i, std::string_view{value});
        }

        REQUIRE(v.type().type() == components::types::logical_type::STRING_LITERAL);
        for (size_t i = 0; i < test_size; i++) {
            std::string result{v.get_value_not_null<std::string_view>(i)};
            REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
        }
    }
    INFO("array of fixed size") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_array(components::types::logical_type::UBIGINT, array_size),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<std::optional<uint64_t>> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(uint64_t{i * array_size + j});
            }
            v.set_value(i, arr);
        }

        REQUIRE(v.type().type() == components::types::logical_type::ARRAY);
        for (size_t i = 0; i < test_size; i++) {
            auto arr = v.get_value_not_null<std::vector<std::optional<uint64_t>>>(i);
            REQUIRE(arr.size() == array_size);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(arr[j] == i * array_size + j);
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
            // storage keeps the strings alive while their views are written
            std::vector<std::string> storage;
            storage.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                storage.push_back("long_string_with_index_" + std::to_string(i * array_size + j));
            }
            std::vector<std::optional<std::string_view>> arr;
            arr.reserve(array_size);
            for (const auto& s : storage) {
                arr.emplace_back(std::string_view{s});
            }
            v.set_value(i, arr);
        }

        REQUIRE(v.type().type() == components::types::logical_type::ARRAY);
        for (size_t i = 0; i < test_size; i++) {
            auto arr = v.get_value_not_null<std::vector<std::optional<std::string_view>>>(i);
            REQUIRE(arr.size() == array_size);
            for (size_t j = 0; j < array_size; j++) {
                std::string result{*arr[j]};
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
            // test that each list entry can be a different length
            std::vector<std::optional<uint64_t>> list;
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(uint64_t{i * list_length(i) + j});
            }
            v.set_value(i, list);
        }

        REQUIRE(v.type().type() == components::types::logical_type::LIST);
        for (size_t i = 0; i < test_size; i++) {
            auto list = v.get_value_not_null<std::vector<std::optional<uint64_t>>>(i);
            REQUIRE(list.size() == list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                REQUIRE(list[j] == i * list_length(i) + j);
            }
        }
    }
    INFO("list of string") {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_list(components::types::logical_type::STRING_LITERAL),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            // test that each list entry can be a different length; storage keeps the
            // strings alive while their views are written
            std::vector<std::string> storage;
            storage.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                storage.push_back("long_string_with_index_" + std::to_string(i * list_length(i) + j));
            }
            std::vector<std::optional<std::string_view>> list;
            list.reserve(list_length(i));
            for (const auto& s : storage) {
                list.emplace_back(std::string_view{s});
            }
            v.set_value(i, list);
        }

        REQUIRE(v.type().type() == components::types::logical_type::LIST);
        for (size_t i = 0; i < test_size; i++) {
            auto list = v.get_value_not_null<std::vector<std::optional<std::string_view>>>(i);
            REQUIRE(list.size() == list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                std::string result{*list[j]};
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
            std::vector<std::optional<uint16_t>> arr;
            arr.reserve(i);
            for (size_t j = 0; j < i; j++) {
                arr.emplace_back(test_data[i].array[j]);
            }
            v.set_value(i,
                        std::tuple{std::optional<bool>(test_data[i].flag),
                                   std::optional<int32_t>(test_data[i].number),
                                   std::optional<std::string_view>(test_data[i].name),
                                   std::optional<std::vector<std::optional<uint16_t>>>(std::move(arr))});
        }

        REQUIRE(v.type().type() == components::types::logical_type::STRUCT);
        REQUIRE(v.type().alias() == "test_struct");
        REQUIRE(v.type().child_types()[0].type() == components::types::logical_type::BOOLEAN);
        REQUIRE(v.type().child_types()[0].alias() == "flag");
        REQUIRE(v.type().child_types()[1].type() == components::types::logical_type::INTEGER);
        REQUIRE(v.type().child_types()[1].alias() == "number");
        REQUIRE(v.type().child_types()[2].type() == components::types::logical_type::STRING_LITERAL);
        REQUIRE(v.type().child_types()[2].alias() == "name");
        REQUIRE(v.type().child_types()[3].type() == components::types::logical_type::LIST);
        REQUIRE(v.type().child_types()[3].child_type().type() == components::types::logical_type::USMALLINT);
        REQUIRE(v.type().child_types()[3].alias() == "array");
        for (size_t i = 0; i < test_size; i++) {
            auto [flag, number, name, arr] =
                v.get_value_not_null<bool, int32_t, std::string_view, std::vector<std::optional<uint16_t>>>(i);
            REQUIRE(flag == test_data[i].flag);
            REQUIRE(number == test_data[i].number);
            REQUIRE(std::string{*name} == test_data[i].name);
            REQUIRE(arr->size() == test_data[i].array.size());
            for (size_t j = 0; j < arr->size(); j++) {
                REQUIRE((*arr)[j] == test_data[i].array[j]);
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
            std::string value{"long_string_with_index_" + std::to_string(i)};
            string_array.set_value(i, std::string_view{value});
        }

        components::vector::indexing_vector_t indexing(&resource, test_size);
        for (size_t i = 0; i < test_size; i++) {
            indexing.set_index(i, indices[i]);
        }

        components::vector::vector_t dictionary(&resource, components::types::logical_type::STRING_LITERAL, test_size);
        dictionary.slice(string_array, indexing, test_size);
        for (size_t i = 0; i < test_size; i++) {
            indexing.set_index(i, indices[i]);
        }

        REQUIRE(dictionary.get_vector_type() == components::vector::vector_type::DICTIONARY);
        for (size_t i = 0; i < test_size; i++) {
            REQUIRE(dictionary.get_value_not_null<std::string_view>(i) ==
                    string_array.get_value_not_null<std::string_view>(indices[i]));
        }
    }
    INFO("union") {
        std::pmr::vector<components::types::complex_logical_type> union_fields(&resource);
        union_fields.emplace_back(components::types::logical_type::BOOLEAN, "bool");
        union_fields.emplace_back(components::types::logical_type::INTEGER, "int");
        union_fields.emplace_back(components::types::logical_type::STRING_LITERAL, "string");
        auto union_type = components::types::complex_logical_type::create_union(union_fields, "union_type");

        components::vector::vector_t union_vector(&resource, union_type, test_size);

        for (size_t i = 0; i < test_size; i++) {
            switch (i % 3) {
                case 0:
                    union_vector.set_value(
                        i,
                        components::types::logical_value_t::create_union(
                            union_vector.resource(),
                            union_fields,
                            0,
                            components::types::logical_value_t{union_vector.resource(), i % 2 == 0}));
                    break;
                case 1:
                    union_vector.set_value(
                        i,
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
                                                   union_vector.resource(),
                                                   std::string{"long_string_with_index_" + std::to_string(i)}}));
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