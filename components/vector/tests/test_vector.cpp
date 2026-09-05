#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/vector/vector.hpp>
#include <core/resource_tracer.hpp>
#include <random>

TEST_CASE("components::vector::vector") {
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

    INFO("sixed size");
    {
        components::vector::vector_t v(&resource, components::types::logical_type::UBIGINT, test_size);
        for (size_t i = 0; i < test_size; i++) {
            v.set_value(i, uint64_t(i));
        }

        REQUIRE(v.type().type() == components::types::logical_type::UBIGINT);
        for (size_t i = 0; i < test_size; i++) {
            REQUIRE(v.get_value<uint64_t>(i) == i);
        }
    }
    INFO("string");
    {
        components::vector::vector_t v(&resource, components::types::logical_type::STRING_LITERAL, test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::string value{"long_string_with_index_" + std::to_string(i)};
            v.set_value(i, std::string_view{value});
        }

        REQUIRE(v.type().type() == components::types::logical_type::STRING_LITERAL);
        for (size_t i = 0; i < test_size; i++) {
            std::string result{v.get_value<std::string_view>(i)};
            REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i)});
        }
    }
    INFO("array of fixed size");
    {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_array(components::types::logical_type::UBIGINT, array_size),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::vector<uint64_t> arr;
            arr.reserve(array_size);
            for (size_t j = 0; j < array_size; j++) {
                arr.emplace_back(uint64_t{i * array_size + j});
            }
            v.set_value(i, arr);
        }

        REQUIRE(v.type().type() == components::types::logical_type::ARRAY);
        for (size_t i = 0; i < test_size; i++) {
            auto arr = v.get_value<std::vector<uint64_t>>(i);
            REQUIRE(arr.size() == array_size);
            for (size_t j = 0; j < array_size; j++) {
                REQUIRE(arr[j] == i * array_size + j);
            }
        }
    }
    INFO("array of string");
    {
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
            std::vector<std::string_view> arr;
            arr.reserve(array_size);
            for (const auto& s : storage) {
                arr.emplace_back(std::string_view{s});
            }
            v.set_value(i, arr);
        }

        REQUIRE(v.type().type() == components::types::logical_type::ARRAY);
        for (size_t i = 0; i < test_size; i++) {
            auto arr = v.get_value<std::vector<std::string_view>>(i);
            REQUIRE(arr.size() == array_size);
            for (size_t j = 0; j < array_size; j++) {
                std::string result{arr[j]};
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * array_size + j)});
            }
        }
    }
    INFO("list of fixed size");
    {
        components::vector::vector_t v(
            &resource,
            components::types::complex_logical_type::create_list(components::types::logical_type::UBIGINT),
            test_size);
        for (size_t i = 0; i < test_size; i++) {
            // test that each list entry can be a different length
            std::vector<uint64_t> list;
            list.reserve(list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                list.emplace_back(uint64_t{i * list_length(i) + j});
            }
            v.set_value(i, list);
        }

        REQUIRE(v.type().type() == components::types::logical_type::LIST);
        for (size_t i = 0; i < test_size; i++) {
            auto list = v.get_value<std::vector<uint64_t>>(i);
            REQUIRE(list.size() == list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                REQUIRE(list[j] == i * list_length(i) + j);
            }
        }
    }
    INFO("list of string");
    {
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
            std::vector<std::string_view> list;
            list.reserve(list_length(i));
            for (const auto& s : storage) {
                list.emplace_back(std::string_view{s});
            }
            v.set_value(i, list);
        }

        REQUIRE(v.type().type() == components::types::logical_type::LIST);
        for (size_t i = 0; i < test_size; i++) {
            auto list = v.get_value<std::vector<std::string_view>>(i);
            REQUIRE(list.size() == list_length(i));
            for (size_t j = 0; j < list_length(i); j++) {
                std::string result{list[j]};
                REQUIRE(result == std::string{"long_string_with_index_" + std::to_string(i * list_length(i) + j)});
            }
        }
    }
    INFO("struct");
    {
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
                components::types::logical_value_t::create_list(v.resource(),
                                                                components::types::logical_type::USMALLINT,
                                                                arr));
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
    INFO("dictionary");
    {
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
            REQUIRE(dictionary.get_value<std::string_view>(i) == string_array.get_value<std::string_view>(indices[i]));
        }
    }
    INFO("union");
    {
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

TEST_CASE("components::vector::nested_null_access") {
    namespace types = components::types;
    auto resource = core::pmr::otterbrix_resource();
    constexpr size_t rows = 16;
    constexpr size_t arr_len = 4;

    INFO("array element nulls");
    {
        components::vector::vector_t v(&resource,
                                       types::complex_logical_type::create_array(types::logical_type::UBIGINT, arr_len),
                                       rows);
        for (size_t i = 0; i < rows; i++) {
            std::vector<uint64_t> arr(arr_len);
            for (size_t j = 0; j < arr_len; j++) {
                arr[j] = i * arr_len + j;
            }
            v.set_value(i, arr);
        }

        // Freshly set elements are all present.
        for (size_t i = 0; i < rows; i++) {
            for (size_t j = 0; j < arr_len; j++) {
                REQUIRE_FALSE(v.is_null({i, j}));
            }
        }

        // Mark individual elements null and read them back.
        v.set_null({2, 1}, true);
        v.set_null({5, 3}, true);
        REQUIRE(v.is_null({2, 1}));
        REQUIRE(v.is_null({5, 3}));
        REQUIRE_FALSE(v.is_null({2, 0}));
        REQUIRE_FALSE(v.is_null({5, 2}));
        // The owning rows stay present.
        REQUIRE_FALSE(v.is_null(2));
        REQUIRE_FALSE(v.is_null(5));

        // Clearing an element restores it.
        v.set_null({2, 1}, false);
        REQUIRE_FALSE(v.is_null({2, 1}));

        // A null row makes every element report null.
        v.set_null(uint64_t{7}, true);
        for (uint64_t j = 0; j < arr_len; j++) {
            REQUIRE(v.is_null({7, j}));
        }
    }

    INFO("list element nulls");
    {
        components::vector::vector_t v(&resource,
                                       types::complex_logical_type::create_list(types::logical_type::UBIGINT),
                                       rows);
        auto length = [](size_t i) { return (i % 4) + 1; }; // 1..4, never empty
        for (size_t i = 0; i < rows; i++) {
            std::vector<uint64_t> list(length(i));
            for (size_t j = 0; j < length(i); j++) {
                list[j] = i * 100 + j;
            }
            v.set_value(i, list);
        }

        for (size_t i = 0; i < rows; i++) {
            for (size_t j = 0; j < length(i); j++) {
                REQUIRE_FALSE(v.is_null({i, j}));
            }
        }

        v.set_null({6, 2}, true); // row 6 has length 3
        REQUIRE(v.is_null({6, 2}));
        REQUIRE_FALSE(v.is_null({6, 0}));
        REQUIRE_FALSE(v.is_null({6, 1}));
    }
}
TEST_CASE("components::vector::vector: an NA vector allocates nothing and reads null everywhere") {
    using namespace components;

    resource_tracer_t tracer;

    SECTION("construction allocates nothing") {
        const size_t before = tracer.total_allocated();
        vector::vector_t nulls{&tracer, types::complex_logical_type{types::logical_type::NA}};
        REQUIRE(tracer.total_allocated() == before);
        REQUIRE(nulls.get_vector_type() == vector::vector_type::CONSTANT);
    }

    SECTION("every row reads as null, at any index") {
        vector::vector_t nulls{&tracer, types::complex_logical_type{types::logical_type::NA}};
        REQUIRE(nulls.is_null(0));
        REQUIRE(nulls.is_null(1));
        REQUIRE(nulls.is_null(1023));
        REQUIRE(nulls.value(7).is_null());
        REQUIRE(nulls.value(7).type().type() == types::logical_type::NA);
    }

    SECTION("writing a null into it stays allocation-free") {
        vector::vector_t nulls{&tracer, types::complex_logical_type{types::logical_type::NA}};
        const size_t before = tracer.total_allocated();
        nulls.set_null(0, true);
        nulls.set_null(500, true);
        REQUIRE(tracer.total_allocated() == before); // set_invalid must not materialize a mask
        REQUIRE(nulls.is_null(500));
    }

    SECTION("flattening it is a no-op rather than a materialization") {
        vector::vector_t nulls{&tracer, types::complex_logical_type{types::logical_type::NA}};
        const size_t before = tracer.total_allocated();
        nulls.flatten(components::vector::DEFAULT_VECTOR_CAPACITY);
        REQUIRE(tracer.total_allocated() == before);
        REQUIRE(nulls.get_vector_type() == vector::vector_type::CONSTANT);
        REQUIRE(nulls.is_null(1023));
    }

    SECTION("placing a NULL value into it allocates nothing either") {
        const size_t before = tracer.total_allocated();
        types::logical_value_t null_value{&tracer, types::complex_logical_type{types::logical_type::NA}};
        vector::vector_t placed{&tracer, null_value};
        REQUIRE(tracer.total_allocated() == before);
        REQUIRE(placed.get_vector_type() == vector::vector_type::CONSTANT);
        REQUIRE(placed.is_null(0));
    }
}

// ЗАПИСИ #349 и #350 — the assert-with-no-else surfaces of the vector layer.
//
// #350: data_chunk_t::sub_column_indices answered {size_t(-1)} behind a bare
// assert for a path it could not resolve — the same sentinel-behind-assert its
// neighbour column_index was already cured of. It answers the same
// field_not_exists error now.
//
// #349: the string legs of apply_unary_vector_op / apply_binary_vector_op were
// `assert(false)` with NO else — under NDEBUG the result vector's payload came
// back UNINITIALIZED. Both entry points refuse the type up front now,
// identically in both builds.
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>

TEST_CASE("components::vector::data_chunk::sub_column_indices answers the error channel") {
    auto resource = core::pmr::otterbrix_resource();
    using components::types::complex_logical_type;
    using components::types::logical_type;

    auto int_type = complex_logical_type{logical_type::INTEGER};
    int_type.set_alias("a");
    std::pmr::vector<complex_logical_type> field_types(&resource);
    auto leaf = complex_logical_type{logical_type::BIGINT};
    leaf.set_alias("inner");
    field_types.push_back(leaf);
    auto struct_type = complex_logical_type::create_struct("", field_types, "s");

    std::pmr::vector<complex_logical_type> chunk_types(&resource);
    chunk_types.push_back(int_type);
    chunk_types.push_back(struct_type);
    components::vector::data_chunk_t chunk(&resource, chunk_types, 4);

    auto path = [&](std::initializer_list<const char*> segments) {
        std::pmr::vector<std::pmr::string> p(&resource);
        for (const auto* s : segments) {
            p.emplace_back(s);
        }
        return p;
    };

    SECTION("a resolvable top-level column answers its index") {
        auto r = chunk.sub_column_indices(path({"a"}));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == std::pmr::vector<size_t>({0}, &resource));
    }
    SECTION("a resolvable nested field answers the index chain") {
        auto r = chunk.sub_column_indices(path({"s", "inner"}));
        REQUIRE_FALSE(r.has_error());
        REQUIRE(r.value() == std::pmr::vector<size_t>({1, 0}, &resource));
    }
    SECTION("a missing top-level column is a field_not_exists error, not a sentinel") {
        auto r = chunk.sub_column_indices(path({"nope"}));
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::field_not_exists);
    }
    SECTION("a missing nested segment is a field_not_exists error too") {
        auto r = chunk.sub_column_indices(path({"s", "nope"}));
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::field_not_exists);
    }
    SECTION("an empty path is a refusal") {
        auto r = chunk.sub_column_indices(path({}));
        REQUIRE(r.has_error());
        REQUIRE(r.error().type == core::error_code_t::field_not_exists);
    }
}

TEST_CASE("components::vector::apply vector ops refuse string operands loudly") {
    auto resource = core::pmr::otterbrix_resource();
    using components::types::complex_logical_type;
    using components::types::logical_type;
    using components::vector::vector_ops::apply_binary_vector_op;
    using components::vector::vector_ops::apply_unary_vector_op;
    using components::vector::vector_ops::binary_vector_op;
    using components::vector::vector_ops::unary_vector_op;
    using components::vector::vector_t;

    constexpr uint64_t count = 2;
    vector_t strings(&resource, complex_logical_type{logical_type::STRING_LITERAL}, count);
    strings.set_value(0, components::types::logical_value_t{&resource, std::string{"a"}});
    strings.set_value(1, components::types::logical_value_t{&resource, std::string{"b"}});
    vector_t ints(&resource, complex_logical_type{logical_type::INTEGER}, count);
    ints.set_value(0, components::types::logical_value_t{&resource, int32_t{1}});
    ints.set_value(1, components::types::logical_value_t{&resource, int32_t{2}});

    SECTION("unary op over strings refuses (used to answer uninitialized memory under NDEBUG)") {
        auto r = apply_unary_vector_op(&resource, unary_vector_op::abs, strings, count);
        REQUIRE(r.has_error());
    }
    SECTION("unary DOUBLE-producing op over strings refuses too") {
        auto r = apply_unary_vector_op(&resource, unary_vector_op::sqr_root, strings, count);
        REQUIRE(r.has_error());
    }
    SECTION("bitwise op over strings refuses") {
        auto r = apply_binary_vector_op(&resource, binary_vector_op::bit_and, strings, strings, count);
        REQUIRE(r.has_error());
    }
    SECTION("bitwise op over floats refuses (the other assert leg)") {
        vector_t doubles(&resource, complex_logical_type{logical_type::DOUBLE}, count);
        doubles.set_value(0, components::types::logical_value_t{&resource, 1.5});
        doubles.set_value(1, components::types::logical_value_t{&resource, 2.5});
        auto r = apply_binary_vector_op(&resource, binary_vector_op::bit_or, doubles, doubles, count);
        REQUIRE(r.has_error());
    }
    SECTION("integer legs still answer") {
        auto u = apply_unary_vector_op(&resource, unary_vector_op::abs, ints, count);
        REQUIRE_FALSE(u.has_error());
        auto b = apply_binary_vector_op(&resource, binary_vector_op::bit_and, ints, ints, count);
        REQUIRE_FALSE(b.has_error());
    }
}
