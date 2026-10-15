#include <catch2/catch_test_macros.hpp>
#include <components/cursor/cursor.hpp>
#include <components/tests/generaty.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/pmr.hpp>

#include <array>
#include <memory>

using namespace core::pmr;

TEST_CASE("components::cursor::construction") {
    auto resource = core::pmr::otterbrix_resource();
    INFO("empty cursor");
    {
        auto cursor = components::cursor::make_cursor(&resource);
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("successful operation cursor");
    {
        auto cursor = components::cursor::make_cursor(&resource, core::error_t::no_error());
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
    }
    INFO("error cursor");
    {
        std::pmr::string description = {"error description", &resource};
        auto cursor =
            components::cursor::make_cursor(&resource, core::error_t(core::error_code_t::other_error, description));
        REQUIRE_FALSE(cursor->is_success());
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == core::error_code_t::other_error);
        REQUIRE(cursor->get_error().what == description);
    }
}

TEST_CASE("components::cursor::data_chunk") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("cursor with data_chunk") {
        auto chunk = gen_data_chunk(100, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE_FALSE(cursor->is_error());
        REQUIRE(cursor->size() == 100);
    }

    SECTION("cursor with empty data_chunk") {
        auto chunk = gen_data_chunk(0, &resource);
        auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 0);
    }
}

namespace {
    using components::cursor::cursor_t_ptr;
    using components::types::complex_logical_type;
    using components::types::int128_t;
    using components::types::logical_type;
    using components::types::uint128_t;
    using components::vector::data_chunk_t;
    using components::vector::vector_t;

    template<typename T>
    T ok(core::result_wrapper_t<T> result) {
        REQUIRE_FALSE(result.has_error());
        return result.value();
    }

    template<typename T>
    void fails(core::result_wrapper_t<T> result, core::error_code_t code) {
        REQUIRE(result.has_error());
        REQUIRE(result.error().type == code);
    }

    data_chunk_t bigint_chunk(std::pmr::memory_resource* resource, int64_t first, uint64_t count) {
        std::pmr::vector<complex_logical_type> types{resource};
        types.emplace_back(logical_type::BIGINT, std::string("v"));
        data_chunk_t chunk{resource, types};
        for (uint64_t i = 0; i < count; ++i) {
            chunk.data[0].data<int64_t>()[i] = first + static_cast<int64_t>(i);
        }
        chunk.set_cardinality(count);
        return chunk;
    }

    cursor_t_ptr one_row(std::pmr::memory_resource* resource, std::vector<vector_t> columns) {
        std::pmr::vector<complex_logical_type> types{resource};
        for (const auto& column : columns) {
            types.push_back(column.type());
        }
        data_chunk_t chunk{resource, types, 1};
        for (std::size_t i = 0; i < columns.size(); ++i) {
            chunk.data[i] = std::move(columns[i]);
        }
        chunk.set_cardinality(1);
        return components::cursor::make_cursor(resource, std::move(chunk));
    }

    complex_logical_type bigint() { return complex_logical_type{logical_type::BIGINT}; }

    complex_logical_type point_type(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT, "a");
        fields.emplace_back(logical_type::STRING_LITERAL, "b");
        return complex_logical_type::create_struct("point", fields);
    }
} // namespace

TEST_CASE("components::cursor::locate and get across chunks") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::vector<data_chunk_t> chunks{&resource};
    chunks.emplace_back(bigint_chunk(&resource, 0, 3));
    chunks.emplace_back(bigint_chunk(&resource, 3, 0));
    chunks.emplace_back(bigint_chunk(&resource, 3, 1024));
    chunks.emplace_back(bigint_chunk(&resource, 1027, 2));
    auto cursor = components::cursor::make_cursor(&resource, std::move(chunks));
    REQUIRE(cursor->size() == 1029);

    struct expectation_t {
        uint64_t row;
        std::size_t chunk;
        uint64_t in_chunk;
    };
    for (const auto& e : {expectation_t{0, 0, 0},
                          expectation_t{2, 0, 2},
                          expectation_t{3, 2, 0},
                          expectation_t{1026, 2, 1023},
                          expectation_t{1027, 3, 0},
                          expectation_t{1028, 3, 1}}) {
        const auto location = ok(cursor->locate(e.row));
        CHECK(location.chunk == e.chunk);
        CHECK(location.row == e.in_chunk);
    }
    fails(cursor->locate(1029), core::error_code_t::invalid_parameter);

    for (uint64_t row = 0; row < cursor->size(); ++row) {
        REQUIRE(ok(cursor->get<int64_t>(0, row)) == static_cast<int64_t>(row));
        REQUIRE_FALSE(ok(cursor->is_null(0, row)));
    }
}

TEST_CASE("components::cursor::locate on a cursor without rows") {
    auto resource = core::pmr::otterbrix_resource();
    fails(components::cursor::make_cursor(&resource)->locate(0), core::error_code_t::invalid_parameter);
    auto empty = components::cursor::make_cursor(&resource, std::pmr::vector<data_chunk_t>{&resource});
    fails(empty->locate(0), core::error_code_t::invalid_parameter);
    auto typed = components::cursor::make_cursor(
        &resource,
        std::pmr::vector<complex_logical_type>{{complex_logical_type{logical_type::BIGINT}}, &resource});
    fails(typed->locate(0), core::error_code_t::invalid_parameter);
}

TEST_CASE("components::cursor::is_null and get on a NULL cell") {
    auto resource = core::pmr::otterbrix_resource();
    auto chunk = bigint_chunk(&resource, 10, 5);
    chunk.data[0].validity().set_invalid(2);
    auto cursor = components::cursor::make_cursor(&resource, std::move(chunk));

    CHECK(ok(cursor->is_null(0, 2)));
    CHECK_FALSE(ok(cursor->is_null(0, 1)));
    CHECK(ok(cursor->get<int64_t>(0, 1)) == 11);
    fails(cursor->get<int64_t>(0, 2), core::error_code_t::conversion_failure);
}

TEST_CASE("components::cursor::get casts by physical type") {
    auto resource = core::pmr::otterbrix_resource();
    using components::vector::make_scalar;
    std::vector<vector_t> columns;
    columns.push_back(make_scalar<int64_t>(&resource, int64_t{4294967297}));
    columns.push_back(make_scalar<uint64_t>(&resource, uint64_t{7}));
    columns.push_back(make_scalar<double>(&resource, 2.5));
    columns.push_back(make_scalar<int32_t>(&resource, 3));
    columns.push_back(make_scalar<bool>(&resource, true));
    columns.push_back(make_scalar<float>(&resource, 1.5f));
    columns.push_back(make_scalar<int32_t>(&resource, complex_logical_type{logical_type::DATE}, 777));
    auto cursor = one_row(&resource, std::move(columns));

    CHECK(ok(cursor->get<int32_t>(0, 0)) == 1);
    CHECK(ok(cursor->get<int64_t>(0, 0)) == int64_t{4294967297});
    CHECK(ok(cursor->get<int64_t>(1, 0)) == 7);
    CHECK(ok(cursor->get<double>(2, 0)) == 2.5);
    CHECK(ok(cursor->get<double>(3, 0)) == 3.0);
    CHECK(ok(cursor->get<int64_t>(4, 0)) == 1);
    CHECK(ok(cursor->get<double>(5, 0)) == 1.5);
    CHECK(ok(cursor->get<int32_t>(6, 0)) == 777);
    CHECK_FALSE(ok(cursor->is_null(6, 0)));
    CHECK(ok(cursor->get<core::date::date_t>(6, 0)).value == core::date::days{777});
}

TEST_CASE("components::cursor::get on DECIMAL reads the unscaled integer") {
    auto resource = core::pmr::otterbrix_resource();
    const int128_t big = int128_t{1} << 100;
    auto small = components::vector::make_decimal(&resource, 1234, 4, 1);
    auto mid = components::vector::make_decimal(&resource, int128_t{123456789012345}, 18, 3);
    auto wide = components::vector::make_decimal(&resource, big, 38, 0);
    REQUIRE_FALSE(small.has_error());
    REQUIRE_FALSE(mid.has_error());
    REQUIRE_FALSE(wide.has_error());
    std::vector<vector_t> columns;
    columns.push_back(std::move(small.value()));
    columns.push_back(std::move(mid.value()));
    columns.push_back(std::move(wide.value()));
    auto cursor = one_row(&resource, std::move(columns));

    CHECK(ok(cursor->get<int64_t>(0, 0)) == 1234);
    CHECK(ok(cursor->get<int64_t>(1, 0)) == 123456789012345);
    CHECK(ok(cursor->get<int128_t>(2, 0)) == big);
}

TEST_CASE("components::cursor::get on UUID keeps all 128 bits") {
    auto resource = core::pmr::otterbrix_resource();
    const auto id = absl::MakeInt128(0x0123456789abcdefULL, 0xfedcba9876543210ULL);
    std::vector<vector_t> columns;
    columns.push_back(
        components::vector::make_scalar<int128_t>(&resource, complex_logical_type{logical_type::UUID}, id));
    auto cursor = one_row(&resource, std::move(columns));

    const auto got = ok(cursor->get<uint128_t>(0, 0));
    CHECK(absl::Uint128High64(got) == 0x0123456789abcdefULL);
    CHECK(absl::Uint128Low64(got) == 0xfedcba9876543210ULL);
}

TEST_CASE("components::cursor::get string_view points into the cursor") {
    auto resource = core::pmr::otterbrix_resource();
    std::vector<vector_t> columns;
    columns.push_back(components::vector::make_scalar<std::string_view>(&resource, "a string past the small buffer"));
    auto cursor = one_row(&resource, std::move(columns));

    CHECK(ok(cursor->get<std::string_view>(0, 0)) == "a string past the small buffer");
}

TEST_CASE("components::cursor::get and is_null errors") {
    auto resource = core::pmr::otterbrix_resource();
    using components::vector::make_scalar;
    std::vector<vector_t> columns;
    columns.push_back(make_scalar<int64_t>(&resource, 5));
    columns.push_back(make_scalar<std::string_view>(&resource, "text"));
    auto cursor = one_row(&resource, std::move(columns));

    fails(cursor->get<std::string_view>(0, 0), core::error_code_t::conversion_failure);
    fails(cursor->get<core::date::date_t>(0, 0), core::error_code_t::conversion_failure);
    fails(cursor->get<core::date::interval_t>(0, 0), core::error_code_t::conversion_failure);
    fails(cursor->get<int64_t>(1, 0), core::error_code_t::conversion_failure);
    fails(cursor->get<int64_t>(2, 0), core::error_code_t::field_not_exists);
    fails(cursor->is_null(2, 0), core::error_code_t::field_not_exists);
    fails(cursor->get<int64_t>(0, 1), core::error_code_t::invalid_parameter);
    fails(cursor->is_null(0, 1), core::error_code_t::invalid_parameter);
    const std::array<std::size_t, 1> step{0};
    fails(cursor->get<int64_t>(0, 0, step), core::error_code_t::invalid_parameter);
    fails(cursor->is_null(0, 0, step), core::error_code_t::invalid_parameter);
    fails(cursor->list_size(0, 0), core::error_code_t::invalid_parameter);
    fails(cursor->list_size(2, 0), core::error_code_t::field_not_exists);
    fails(cursor->list_size(0, 1), core::error_code_t::invalid_parameter);
}

TEST_CASE("components::cursor::nested get, is_null and list_size") {
    auto resource = core::pmr::otterbrix_resource();
    using components::vector::make_array;
    using components::vector::make_list;
    using components::vector::make_null;
    using components::vector::make_scalar;
    using components::vector::make_struct;
    const auto point = point_type(&resource);

    std::vector<vector_t> columns;
    {
        std::array<vector_t, 2> fields{make_scalar<int64_t>(&resource, 1),
                                       make_scalar<std::string_view>(&resource, "x")};
        columns.push_back(make_struct(&resource, point, fields));
    }
    {
        std::array<vector_t, 2> fields{make_null(&resource, bigint()), make_scalar<std::string_view>(&resource, "y")};
        columns.push_back(make_struct(&resource, point, fields));
    }
    {
        std::array<vector_t, 3> elements{make_scalar<int64_t>(&resource, 10),
                                         make_null(&resource, bigint()),
                                         make_scalar<int64_t>(&resource, 30)};
        columns.push_back(make_list(&resource, bigint(), elements));
    }
    {
        std::array<vector_t, 2> elements{make_scalar<int64_t>(&resource, 7), make_scalar<int64_t>(&resource, 8)};
        columns.push_back(make_array(&resource, bigint(), elements));
    }
    auto cursor = one_row(&resource, std::move(columns));

    const std::array<std::size_t, 1> first{0};
    const std::array<std::size_t, 1> second{1};
    const std::array<std::size_t, 1> third{2};
    const std::array<std::size_t, 1> far{5};
    const std::array<std::size_t, 1> bad_field{9};
    const std::array<std::size_t, 2> too_deep{0, 0};

    SECTION("struct field") {
        CHECK(ok(cursor->get<int64_t>(0, 0, first)) == 1);
        CHECK(ok(cursor->get<std::string_view>(0, 0, second)) == "x");
        CHECK_FALSE(ok(cursor->is_null(0, 0, first)));
        CHECK(ok(cursor->is_null(1, 0, first)));
        fails(cursor->get<int64_t>(1, 0, first), core::error_code_t::conversion_failure);
        fails(cursor->get<int64_t>(0, 0, bad_field), core::error_code_t::invalid_parameter);
        fails(cursor->get<int64_t>(0, 0, too_deep), core::error_code_t::invalid_parameter);
    }
    SECTION("list element") {
        CHECK(ok(cursor->get<int64_t>(2, 0, first)) == 10);
        CHECK(ok(cursor->is_null(2, 0, second)));
        fails(cursor->get<int64_t>(2, 0, second), core::error_code_t::conversion_failure);
        CHECK(ok(cursor->get<int64_t>(2, 0, third)) == 30);
        CHECK(ok(cursor->is_null(2, 0, far)));
    }
    SECTION("array element") {
        CHECK(ok(cursor->get<int64_t>(3, 0, second)) == 8);
        fails(cursor->get<int64_t>(3, 0, third), core::error_code_t::invalid_parameter);
    }
    SECTION("braced path") {
        CHECK(ok(cursor->get<int64_t>(0, 0, {0})) == 1);
        CHECK(ok(cursor->get<int64_t>(2, 0, {2})) == 30);
        CHECK(ok(cursor->is_null(2, 0, {1})));
        CHECK_FALSE(ok(cursor->is_null(0, 0, {})));
        fails(cursor->get<int64_t>(2, 0, {}), core::error_code_t::conversion_failure);
        fails(cursor->get<int64_t>(0, 0, {0, 0}), core::error_code_t::invalid_parameter);
        fails(cursor->is_null(0, 0, {9}), core::error_code_t::invalid_parameter);
    }
    SECTION("list_size") {
        CHECK(ok(cursor->list_size(2, 0)) == 3);
        CHECK(ok(cursor->list_size(3, 0)) == 2);
        fails(cursor->list_size(0, 0), core::error_code_t::invalid_parameter);
    }
}

TEST_CASE("components::cursor::NA-typed nested leaf is NULL") {
    auto resource = core::pmr::otterbrix_resource();
    using components::vector::make_scalar;
    std::pmr::vector<complex_logical_type> fields(&resource);
    fields.emplace_back(logical_type::BIGINT, "a");
    fields.emplace_back(logical_type::NA, "n");
    const auto type = complex_logical_type::create_struct("with_na", fields);
    std::array<vector_t, 2> members{make_scalar<int64_t>(&resource, 1),
                                    components::vector::make_null(&resource, complex_logical_type{logical_type::NA})};
    std::vector<vector_t> columns;
    columns.push_back(components::vector::make_struct(&resource, type, members));
    auto cursor = one_row(&resource, std::move(columns));

    CHECK_FALSE(ok(cursor->is_null(0, 0, {0})));
    CHECK(ok(cursor->is_null(0, 0, {1})));
    fails(cursor->get<int64_t>(0, 0, {1}), core::error_code_t::conversion_failure);
}

TEST_CASE("components::cursor::list_size on a MAP and on a NULL list") {
    auto resource = core::pmr::otterbrix_resource();
    using components::vector::make_scalar;
    const complex_logical_type varchar{logical_type::STRING_LITERAL};
    std::array<vector_t, 2> keys{make_scalar<std::string_view>(&resource, "a"),
                                 make_scalar<std::string_view>(&resource, "b")};
    std::array<vector_t, 2> values{make_scalar<int64_t>(&resource, 1), make_scalar<int64_t>(&resource, 2)};
    std::vector<vector_t> columns;
    columns.push_back(components::vector::make_map(&resource, varchar, bigint(), keys, values));
    columns.push_back(components::vector::make_null(&resource, complex_logical_type::create_list(bigint())));
    auto cursor = one_row(&resource, std::move(columns));

    CHECK(ok(cursor->list_size(0, 0)) == 2);
    CHECK(ok(cursor->is_null(1, 0)));
    fails(cursor->list_size(1, 0), core::error_code_t::conversion_failure);
}
