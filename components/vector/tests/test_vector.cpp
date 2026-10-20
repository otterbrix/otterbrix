#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <components/vector/cell_equal.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/counting_resource.hpp>
#include <core/resource_tracer.hpp>
#include <cstring>
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
    fields.emplace_back(components::types::logical_type::TEXT, "name");
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
        components::vector::vector_t v(&resource, components::types::logical_type::TEXT, test_size);
        for (size_t i = 0; i < test_size; i++) {
            std::string value{"long_string_with_index_" + std::to_string(i)};
            v.set_value(i, std::string_view{value});
        }

        REQUIRE(v.type().type() == components::types::logical_type::TEXT);
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
            components::types::complex_logical_type::create_array(components::types::logical_type::TEXT, array_size),
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
            components::types::complex_logical_type::create_list(components::types::logical_type::TEXT),
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
            v.entries()[0]->set_value(i, test_data[i].flag);
            v.entries()[1]->set_value(i, test_data[i].number);
            v.entries()[2]->set_value(i, std::string_view{test_data[i].name});
            v.entries()[3]->set_value(i, test_data[i].array);
        }

        REQUIRE(v.type().type() == components::types::logical_type::STRUCT);
        REQUIRE(v.type().alias() == "test_struct");
        REQUIRE(v.type().child_types()[0].type() == components::types::logical_type::BOOLEAN);
        REQUIRE(v.type().child_types()[0].alias() == "flag");
        REQUIRE(v.type().child_types()[1].type() == components::types::logical_type::INTEGER);
        REQUIRE(v.type().child_types()[1].alias() == "number");
        REQUIRE(v.type().child_types()[2].type() == components::types::logical_type::TEXT);
        REQUIRE(v.type().child_types()[2].alias() == "name");
        REQUIRE(v.type().child_types()[3].type() == components::types::logical_type::LIST);
        REQUIRE(v.type().child_types()[3].child_type().type() == components::types::logical_type::USMALLINT);
        REQUIRE(v.type().child_types()[3].alias() == "array");

        for (size_t i = 0; i < test_size; i++) {
            REQUIRE(v.entries()[0]->get_value<bool>(i) == test_data[i].flag);
            REQUIRE(v.entries()[1]->get_value<int32_t>(i) == test_data[i].number);
            REQUIRE(v.entries()[2]->get_value<std::string_view>(i) == test_data[i].name);
            REQUIRE(v.entries()[3]->get_value<std::vector<uint16_t>>(i) == test_data[i].array);
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

        components::vector::vector_t string_array(&resource, components::types::logical_type::TEXT, string_count);
        for (size_t i = 0; i < string_count; i++) {
            std::string value{"long_string_with_index_" + std::to_string(i)};
            string_array.set_value(i, std::string_view{value});
        }

        components::vector::indexing_vector_t indexing(&resource, test_size);
        for (size_t i = 0; i < test_size; i++) {
            indexing.set_index(i, indices[i]);
        }

        components::vector::vector_t dictionary(&resource, components::types::logical_type::TEXT, test_size);
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
        union_fields.emplace_back(components::types::logical_type::TEXT, "string");
        auto union_type = components::types::complex_logical_type::create_union(union_fields, "union_type");

        components::vector::vector_t union_vector(&resource, union_type, test_size);

        for (size_t i = 0; i < test_size; i++) {
            const auto tag = static_cast<uint8_t>(i % 3);
            union_vector.entries()[0]->set_value(i, tag);
            switch (tag) {
                case 0:
                    union_vector.entries()[1]->set_value(i, i % 2 == 0);
                    break;
                case 1:
                    union_vector.entries()[2]->set_value(i, static_cast<int32_t>(i));
                    break;
                default: {
                    const std::string text{"long_string_with_index_" + std::to_string(i)};
                    union_vector.entries()[3]->set_value(i, std::string_view{text});
                    break;
                }
            }
        }

        for (size_t i = 0; i < test_size; i++) {
            const auto tag = union_vector.entries()[0]->get_value<uint8_t>(i);
            switch (tag) {
                case 0:
                    REQUIRE(union_vector.entries()[1]->type().type() == components::types::logical_type::BOOLEAN);
                    REQUIRE(union_vector.entries()[1]->get_value<bool>(i) == (i % 2 == 0));
                    break;
                case 1:
                    REQUIRE(union_vector.entries()[2]->type().type() == components::types::logical_type::INTEGER);
                    REQUIRE(union_vector.entries()[2]->get_value<int32_t>(i) == static_cast<int32_t>(i));
                    break;
                default:
                    REQUIRE(union_vector.entries()[3]->type().type() == components::types::logical_type::TEXT);
                    REQUIRE(union_vector.entries()[3]->get_value<std::string_view>(i) ==
                            std::string{"long_string_with_index_" + std::to_string(i)});
                    break;
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
        REQUIRE(nulls.is_null(7));
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

    SECTION("making a NULL one allocates nothing either") {
        const size_t before = tracer.total_allocated();
        auto placed = vector::make_null(&tracer, types::complex_logical_type{types::logical_type::NA});
        REQUIRE(tracer.total_allocated() == before);
        REQUIRE(placed.get_vector_type() == vector::vector_type::CONSTANT);
        REQUIRE(placed.is_null(0));
    }
}

// sub_column_indices answers field_not_exists for an unresolvable path, not a
// {size_t(-1)} sentinel (same channel as column_index). apply_unary/binary_vector_op's
// string legs refuse the type up front instead of `assert(false)` with no else, which
// under NDEBUG would return the result vector's payload uninitialized.
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
        REQUIRE(r.value() == std::pmr::vector<size_t>(std::initializer_list<size_t>{0}, &resource));
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
    using components::vector::vector_t;
    using components::vector::vector_ops::apply_binary_vector_op;
    using components::vector::vector_ops::apply_unary_vector_op;
    using components::vector::vector_ops::binary_vector_op;
    using components::vector::vector_ops::unary_vector_op;

    constexpr uint64_t count = 2;
    vector_t strings(&resource, complex_logical_type{logical_type::TEXT}, count);
    strings.set_value(0, std::string_view{"a"});
    strings.set_value(1, std::string_view{"b"});
    vector_t ints(&resource, complex_logical_type{logical_type::INTEGER}, count);
    ints.set_value(0, int32_t{1});
    ints.set_value(1, int32_t{2});

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
        doubles.set_value(0, 1.5);
        doubles.set_value(1, 2.5);
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

// PostgreSQL's hashfloat8 canonicalises NaN: one value, one hash, whatever sign and payload the bits carry.
TEST_CASE("components::vector::hash gives every NaN of a float type the same hash, and keeps +0 and -0 together") {
    auto resource = core::pmr::otterbrix_resource();
    auto hashes_of = [&](components::types::logical_type type, auto... values) {
        using T = std::common_type_t<decltype(values)...>;
        components::vector::vector_t input(&resource, components::types::complex_logical_type{type}, sizeof...(values));
        uint64_t row = 0;
        ((input.set_value(row++, static_cast<T>(values))), ...);
        components::vector::vector_t hashes(
            &resource,
            components::types::complex_logical_type{components::types::logical_type::UBIGINT},
            sizeof...(values));
        components::vector::vector_ops::hash(input, hashes, sizeof...(values));
        std::vector<uint64_t> result;
        for (uint64_t i = 0; i < sizeof...(values); i++) {
            result.push_back(hashes.data<uint64_t>()[i]);
        }
        return result;
    };
    auto from_bits = [](uint64_t bits) {
        double value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };
    const auto doubles = hashes_of(components::types::logical_type::DOUBLE,
                                   from_bits(0x7ff8000000000000ULL),
                                   from_bits(0xfff8000000000000ULL),
                                   from_bits(0x7ff8000000000001ULL),
                                   from_bits(0x7ff0000000000001ULL),
                                   1.5);
    CHECK(doubles[0] == doubles[1]);
    CHECK(doubles[0] == doubles[2]);
    CHECK(doubles[0] == doubles[3]);
    CHECK(doubles[0] != doubles[4]);
    const auto zeros = hashes_of(components::types::logical_type::DOUBLE, 0.0, -0.0);
    CHECK(zeros[0] == zeros[1]);

    auto float_from_bits = [](uint32_t bits) {
        float value;
        std::memcpy(&value, &bits, sizeof(value));
        return value;
    };
    const auto floats = hashes_of(components::types::logical_type::FLOAT,
                                  float_from_bits(0x7fc00000U),
                                  float_from_bits(0xffc00000U),
                                  float_from_bits(0x7fc00001U),
                                  1.5f);
    CHECK(floats[0] == floats[1]);
    CHECK(floats[0] == floats[2]);
    CHECK(floats[0] != floats[3]);
}

TEST_CASE("components::vector::cells_equal: UNION compares the tag and the active member only") {
    using namespace components;

    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<types::complex_logical_type> members(&resource);
    members.emplace_back(types::logical_type::INTEGER, "i");
    members.emplace_back(types::logical_type::TEXT, "s");
    vector::vector_t u(&resource, types::complex_logical_type::create_union(members), 5);

    auto put = [&](uint64_t row, uint8_t tag, int32_t i, std::string_view s) {
        u.entries()[0]->set_value(row, tag);
        u.entries()[1]->set_value(row, i);
        u.entries()[2]->set_value(row, s);
    };
    put(0, 0, 5, "x");
    put(1, 0, 5, "y");
    put(2, 1, 5, "x");
    put(3, 0, 6, "x");
    put(4, 1, 0, "x");

    REQUIRE(vector::cells_equal(u, 0, u, 1));
    REQUIRE_FALSE(vector::cells_equal(u, 0, u, 2));
    REQUIRE_FALSE(vector::cells_equal(u, 0, u, 3));
    REQUIRE(vector::cells_equal(u, 2, u, 4));

    u.entries()[0]->set_null(1, true);
    REQUIRE_FALSE(vector::cells_equal(u, 0, u, 1));
    u.entries()[0]->set_null(0, true);
    REQUIRE(vector::cells_equal(u, 0, u, 1));
}

TEST_CASE("components::vector::vector: a UNION vector is well formed, nested UNION members included") {
    using namespace components;
    auto resource = core::pmr::otterbrix_resource();

    SECTION("UNION whose member type is itself a UNION") {
        std::pmr::vector<types::complex_logical_type> inner_types(&resource);
        inner_types.emplace_back(types::logical_type::BIGINT);
        auto inner = types::complex_logical_type::create_union(inner_types);

        std::pmr::vector<types::complex_logical_type> member_types(&resource);
        member_types.emplace_back(types::logical_type::BIGINT);
        member_types.emplace_back(inner);

        vector::vector_t u(&resource, types::complex_logical_type::create_union(member_types), 2);
        REQUIRE(u.type().type() == types::logical_type::UNION);
        REQUIRE(u.entries().size() == 3); // the tag slot plus one per member type
        u.entries()[0]->set_value(0, uint8_t{0});
        u.entries()[1]->set_value(0, int64_t{7});
        REQUIRE_FALSE(u.is_null(0));
        REQUIRE(u.entries()[1]->get_value<int64_t>(0) == 7);
        REQUIRE(u.entries()[2]->type().type() == types::logical_type::UNION);
        REQUIRE(u.entries()[2]->entries().size() == 2);
    }
}

TEST_CASE("components::vector::make_null: an ARRAY NULL row, whatever its element type or size") {
    using namespace components;
    auto resource = core::pmr::otterbrix_resource();

    SECTION("ARRAY(NA, 0)") {
        const auto type =
            types::complex_logical_type::create_array(types::complex_logical_type{types::logical_type::NA}, 0);
        auto null_array = vector::make_null(&resource, type);
        REQUIRE(null_array.is_null(0));
        auto copy = vector::make_constant_copy(&resource, null_array, 0);
        REQUIRE(copy.type() == type);
        REQUIRE(copy.is_null(0));
        copy.flatten(2);
        REQUIRE(copy.is_null(1));
    }

    SECTION("ARRAY(INTEGER, 3)") {
        const auto type =
            types::complex_logical_type::create_array(types::complex_logical_type{types::logical_type::INTEGER}, 3);
        auto null_array = vector::make_null(&resource, type);
        REQUIRE(null_array.is_null(0));
        auto copy = vector::make_constant_copy(&resource, null_array, 0);
        REQUIRE(copy.type() == type);
        REQUIRE(copy.is_null(0));
        copy.flatten(2);
        REQUIRE(copy.is_null(1));
    }
}

TEST_CASE("components::vector::vector: get_value on a dictionary over a sequence reads the sequence") {
    using namespace components;
    auto resource = core::pmr::otterbrix_resource();

    vector::vector_t source(&resource, types::complex_logical_type(types::logical_type::BIGINT), 4);
    source.sequence(100, 10, 4);

    vector::indexing_vector_t selection(&resource, 2);
    selection.set_index(0, 2);
    selection.set_index(1, 0);
    vector::vector_t sliced(&resource, types::complex_logical_type(types::logical_type::BIGINT), 4);
    sliced.slice(source, selection, 2);

    REQUIRE(sliced.get_value<int64_t>(0) == 120);
    REQUIRE(sliced.get_value<int64_t>(1) == 100);
}

TEST_CASE("components::vector::vector: slicing a small vector allocates no payload for the dictionary") {
    using namespace components;
    core::pmr::otterbrix_resource upstream;
    core::pmr::counting_resource_t arena{&upstream};

    vector::vector_t source(&arena, types::complex_logical_type(types::logical_type::BIGINT), 3);
    vector::indexing_vector_t selection(&arena, 3);
    for (uint64_t i = 0; i < 3; ++i) {
        selection.set_index(i, 2 - i);
    }
    vector::vector_t sliced(&arena, types::complex_logical_type(types::logical_type::BIGINT), false, false, 0);

    arena.reset();
    sliced.slice(source, selection, 3);
    INFO("slice allocated " << arena.allocated_bytes() << " bytes in " << arena.allocations() << " blocks");
    CHECK(arena.allocated_bytes() < 512);
}
