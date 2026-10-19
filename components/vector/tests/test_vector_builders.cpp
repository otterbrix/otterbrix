#include <catch2/catch_test_macros.hpp>
#include <core/pmr.hpp>

#include <array>
#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/operations_helper.hpp>

// Every make_* builder is checked against direct expectations: the declared type, the vector_type,
// the validity and the cell payload read back through typed accessors.

using namespace components::vector;
using components::types::complex_logical_type;
using components::types::int128_t;
using components::types::list_entry_t;
using components::types::logical_type;
using components::types::physical_type;
using components::types::uint128_t;

namespace {

    template<typename T>
    T element(const vector_t& v, uint64_t row, uint64_t k) {
        const auto seq = sequence_of(v, row);
        REQUIRE(k < seq.length);
        return seq.child->get_value<T>(seq.offset + k);
    }

    bool element_null(const vector_t& v, uint64_t row, uint64_t k) {
        const auto seq = sequence_of(v, row);
        REQUIRE(k < seq.length);
        return seq.child->is_null(seq.offset + k);
    }

    uint64_t length(const vector_t& v, uint64_t row) { return sequence_of(v, row).length; }

    template<typename T>
    T field(const vector_t& v, uint64_t row, uint64_t k) {
        const auto cell = resolve_cell(v, row);
        return cell.vec->entries()[k]->template get_value<T>(cell.index);
    }

    bool field_null(const vector_t& v, uint64_t row, uint64_t k) {
        const auto cell = resolve_cell(v, row);
        return cell.vec->entries()[k]->is_null(cell.index);
    }

    template<typename... Vs>
    std::array<vector_t, sizeof...(Vs)> pack(Vs&&... vs) {
        return {std::forward<Vs>(vs)...};
    }

    template<typename T>
    void check_scalar(T value) {
        auto resource = core::pmr::otterbrix_resource();
        constexpr bool is_text = detail::is_string_like_v<T>;
        using read_t = std::conditional_t<is_text, std::string_view, T>;
        const auto expected = [&]() {
            if constexpr (is_text) {
                return read_t{value};
            } else {
                return value;
            }
        }();
        const auto expected_type = is_text ? logical_type::STRING_LITERAL : components::types::to_logical_type<T>();

        auto built = make_scalar<T>(&resource, value);
        REQUIRE(built.type().type() == expected_type);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(core::is_equals(built.template get_value<read_t>(0), expected));

        // After flatten, every row reads the same cell.
        built.flatten(3);
        REQUIRE(built.get_vector_type() == vector_type::FLAT);
        for (uint64_t row = 0; row < 3; row++) {
            REQUIRE_FALSE(built.is_null(row));
            REQUIRE(core::is_equals(built.template get_value<read_t>(row), expected));
        }
    }

    complex_logical_type bigint() { return complex_logical_type{logical_type::BIGINT}; }
    complex_logical_type varchar() { return complex_logical_type{logical_type::STRING_LITERAL}; }

    complex_logical_type point_type(std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> fields(resource);
        fields.emplace_back(logical_type::BIGINT, "a");
        fields.emplace_back(logical_type::STRING_LITERAL, "b");
        return complex_logical_type::create_struct("point", fields);
    }

    complex_logical_type decimal(std::pmr::memory_resource* resource, uint8_t width, uint8_t scale) {
        auto type = complex_logical_type::create_decimal(resource, width, scale);
        REQUIRE_FALSE(type.has_error());
        return type.value();
    }

    int128_t read_decimal(const vector_t& v, uint64_t row) {
        switch (v.type().to_physical_type()) {
            case physical_type::INT16:
                return v.get_value<int16_t>(row);
            case physical_type::INT32:
                return v.get_value<int32_t>(row);
            case physical_type::INT64:
                return v.get_value<int64_t>(row);
            default:
                return v.get_value<int128_t>(row);
        }
    }

} // namespace

TEST_CASE("make_scalar: integer, boolean and floating point") {
    check_scalar<bool>(true);
    check_scalar<bool>(false);
    check_scalar<int8_t>(-5);
    check_scalar<int16_t>(-30000);
    check_scalar<int32_t>(-2000000000);
    check_scalar<int64_t>(-9000000000000000000LL);
    check_scalar<uint8_t>(250);
    check_scalar<uint16_t>(65000);
    check_scalar<uint32_t>(4000000000U);
    check_scalar<uint64_t>(18000000000000000000ULL);
    check_scalar<int128_t>((int128_t{1} << 100) + 7);
    check_scalar<int128_t>(-((int128_t{1} << 100) + 7));
    check_scalar<uint128_t>((uint128_t{1} << 120) + 9);
    check_scalar<float>(1.5f);
    check_scalar<double>(-2.25);
}

TEST_CASE("make_scalar: string") {
    check_scalar<std::string_view>("hello");
    check_scalar<std::string_view>("");
    check_scalar<std::string>(std::string(100, 'x'));
    check_scalar<const char*>("literal");
}

TEST_CASE("make_scalar: date and time families") {
    using namespace core::date;
    check_scalar<date_t>(date_t{days{12345}});
    check_scalar<core::date::time_t>(core::date::time_t{microseconds{86399999999LL}});
    check_scalar<timestamp_t>(timestamp_t{microseconds{-1234567890123LL}});
    check_scalar<timestamptz_t>(timestamptz_t{microseconds{1234567890123LL}});
    check_scalar<timetz_t>(timetz_t{microseconds{3600000000LL}, seconds_i32{-18000}});
    check_scalar<interval_t>(interval_t{microseconds{1500000}, days{3}, months{14}});
}

TEST_CASE("make_scalar: explicit type keeps alias and uses the type's storage") {
    auto resource = core::pmr::otterbrix_resource();

    auto aliased = complex_logical_type{logical_type::BIGINT};
    aliased.set_alias("col");
    auto built = make_scalar<int64_t>(&resource, aliased, 42);
    REQUIRE(built.type().type() == logical_type::BIGINT);
    REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
    REQUIRE(built.get_value<int64_t>(0) == 42);
    REQUIRE(built.type().alias() == "col");

    // DATE stored as its int32 payload
    auto date = make_scalar<int32_t>(&resource, complex_logical_type{logical_type::DATE}, 777);
    REQUIRE(date.type().type() == logical_type::DATE);
    REQUIRE(date.get_vector_type() == vector_type::CONSTANT);
    REQUIRE(date.get_value<core::date::date_t>(0).value.count() == 777);

    // TIMESTAMP stored as its int64 payload
    auto ts = make_scalar<int64_t>(&resource, complex_logical_type{logical_type::TIMESTAMP}, -99);
    REQUIRE(ts.type().type() == logical_type::TIMESTAMP);
    REQUIRE(ts.get_value<core::date::timestamp_t>(0).value.count() == -99);

    // VARCHAR with an alias
    auto text = complex_logical_type{logical_type::STRING_LITERAL};
    text.set_alias("name");
    auto str = make_scalar<std::string_view>(&resource, text, "abc");
    REQUIRE(str.type().alias() == "name");
    REQUIRE(str.get_value<std::string_view>(0) == "abc");
}

TEST_CASE("make_scalar: UUID is a full 128-bit payload") {
    auto resource = core::pmr::otterbrix_resource();
    const int128_t id = (int128_t{0x0123456789abcdefLL} << 64) | int128_t{0x1122334455667788ULL};
    auto built = make_scalar<int128_t>(&resource, complex_logical_type{logical_type::UUID}, id);
    REQUIRE(built.type().type() == logical_type::UUID);
    REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
    REQUIRE_FALSE(built.is_null(0));
    REQUIRE(built.get_value<int128_t>(0) == id);
}

TEST_CASE("make_null: typed NULL of every type family") {
    auto resource = core::pmr::otterbrix_resource();
    const std::array<complex_logical_type, 13> types = {
        complex_logical_type{logical_type::BOOLEAN},
        complex_logical_type{logical_type::TINYINT},
        complex_logical_type{logical_type::BIGINT},
        complex_logical_type{logical_type::UBIGINT},
        complex_logical_type{logical_type::HUGEINT},
        complex_logical_type{logical_type::UHUGEINT},
        complex_logical_type{logical_type::FLOAT},
        complex_logical_type{logical_type::DOUBLE},
        complex_logical_type{logical_type::STRING_LITERAL},
        complex_logical_type{logical_type::DATE},
        complex_logical_type{logical_type::TIMESTAMP},
        complex_logical_type{logical_type::INTERVAL},
        complex_logical_type{logical_type::UUID},
    };
    for (const auto& type : types) {
        auto built = make_null(&resource, type);
        REQUIRE(built.type() == type);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE(built.is_null(0));
        // same validity word as a typed vector with its row 0 nulled
        vector_t reference(&resource, type, 1);
        reference.set_null(0, true);
        REQUIRE(built.validity().get_validity_entry(0) == reference.validity().get_validity_entry(0));
    }

    const std::array<complex_logical_type, 7> nested = {
        decimal(&resource, 4, 2),
        decimal(&resource, 38, 10),
        complex_logical_type::create_list(bigint()),
        complex_logical_type::create_array(bigint(), 3),
        point_type(&resource),
        complex_logical_type::create_map(&resource, varchar(), bigint()),
        complex_logical_type::create_list(point_type(&resource)),
    };
    for (const auto& type : nested) {
        auto built = make_null(&resource, type);
        REQUIRE(built.type() == type);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE(built.is_null(0));
    }

    SECTION("NULL STRUCT nulls its fields too") {
        auto built = make_null(&resource, point_type(&resource));
        for (const auto& field : built.entries()) {
            REQUIRE(field->is_null(0));
        }
    }

    SECTION("NA type is a CONSTANT NULL") {
        auto built = make_null(&resource, complex_logical_type{logical_type::NA});
        REQUIRE(built.type().type() == logical_type::NA);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE(built.is_null(0));
    }

    SECTION("flatten of a NULL marks every row NULL") {
        auto built = make_null(&resource, bigint());
        built.flatten(5);
        for (uint64_t row = 0; row < 5; row++) {
            REQUIRE(built.is_null(row));
        }
    }
}

TEST_CASE("make_decimal: storage width follows the type") {
    auto resource = core::pmr::otterbrix_resource();
    struct spec_t {
        uint8_t width;
        uint8_t scale;
        physical_type storage;
        int64_t value;
    };
    const std::array<spec_t, 4> specs = {{{4, 2, physical_type::INT16, -1234},
                                          {9, 3, physical_type::INT32, 123456789},
                                          {18, 5, physical_type::INT64, -123456789012345678LL},
                                          {38, 10, physical_type::INT128, 5}}};
    for (const auto& spec : specs) {
        const auto type = decimal(&resource, spec.width, spec.scale);
        REQUIRE(type.to_physical_type() == spec.storage);
        auto made = make_decimal(&resource, spec.value, spec.width, spec.scale);
        REQUIRE_FALSE(made.has_error());
        auto& built = made.value();
        REQUIRE(built.type() == type);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(read_decimal(built, 0) == int128_t{spec.value});
        auto direct = make_decimal(&resource, type, spec.value);
        REQUIRE(direct.type() == type);
        REQUIRE(read_decimal(direct, 0) == int128_t{spec.value});
    }

    SECTION("128-bit payload") {
        const int128_t big = (int128_t{1} << 100) + 3;
        const auto type = decimal(&resource, 38, 0);
        auto made = make_decimal(&resource, big, 38, 0);
        REQUIRE_FALSE(made.has_error());
        auto& built = made.value();
        REQUIRE(built.type() == type);
        REQUIRE(built.get_value<int128_t>(0) == big);
    }
    SECTION("invalid (width, scale) is an error") {
        REQUIRE(make_decimal(&resource, 1, 0, 0).has_error());
        REQUIRE(make_decimal(&resource, 1, 39, 0).has_error());
        REQUIRE(make_decimal(&resource, 1, 5, 6).has_error());
    }
    SECTION("flatten") {
        auto made = make_decimal(&resource, -77, 9, 2);
        REQUIRE_FALSE(made.has_error());
        auto& built = made.value();
        built.flatten(3);
        for (uint64_t row = 0; row < 3; row++) {
            REQUIRE(built.get_value<int32_t>(row) == -77);
        }
    }
}

TEST_CASE("make_list") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("integers") {
        auto built = make_list(&resource,
                               bigint(),
                               pack(make_scalar<int64_t>(&resource, 1),
                                    make_scalar<int64_t>(&resource, 2),
                                    make_scalar<int64_t>(&resource, 3)));
        REQUIRE(built.type() == complex_logical_type::create_list(bigint()));
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(length(built, 0) == 3);
        REQUIRE(element<int64_t>(built, 0, 0) == 1);
        REQUIRE(element<int64_t>(built, 0, 1) == 2);
        REQUIRE(element<int64_t>(built, 0, 2) == 3);
    }
    SECTION("NULL element") {
        auto built = make_list(&resource,
                               bigint(),
                               pack(make_scalar<int64_t>(&resource, 1),
                                    make_null(&resource, bigint()),
                                    make_scalar<int64_t>(&resource, 3)));
        REQUIRE(length(built, 0) == 3);
        REQUIRE(element<int64_t>(built, 0, 0) == 1);
        REQUIRE(element_null(built, 0, 1));
        REQUIRE(element<int64_t>(built, 0, 2) == 3);
        REQUIRE(built.entry().is_null(1));
    }
    SECTION("empty") {
        auto built = make_list(&resource, bigint(), std::span<const vector_t>{});
        REQUIRE(built.type() == complex_logical_type::create_list(bigint()));
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(built.data<list_entry_t>()[0].length == 0);
    }
    SECTION("strings") {
        auto built = make_list(&resource,
                               varchar(),
                               pack(make_scalar<std::string_view>(&resource, "a"),
                                    make_scalar<std::string_view>(&resource, "longer than sso string")));
        REQUIRE(length(built, 0) == 2);
        REQUIRE(element<std::string_view>(built, 0, 0) == "a");
        REQUIRE(element<std::string_view>(built, 0, 1) == "longer than sso string");
    }
    SECTION("more elements than one growth step") {
        std::vector<vector_t> elements;
        for (int64_t i = 0; i < 5000; i++) {
            elements.push_back(make_scalar<int64_t>(&resource, i));
        }
        auto built = make_list(&resource, bigint(), elements);
        REQUIRE(length(built, 0) == 5000);
        for (int64_t i = 0; i < 5000; i++) {
            REQUIRE(element<int64_t>(built, 0, static_cast<uint64_t>(i)) == i);
        }
    }
    SECTION("list of list") {
        auto inner_a = make_list(&resource, bigint(), pack(make_scalar<int64_t>(&resource, 1)));
        auto inner_b = make_list(&resource, bigint(), std::span<const vector_t>{});
        const auto inner_type = complex_logical_type::create_list(bigint());
        auto built = make_list(&resource, inner_type, pack(std::move(inner_a), std::move(inner_b)));
        REQUIRE(built.type() == complex_logical_type::create_list(inner_type));
        REQUIRE(length(built, 0) == 2);
        const auto seq = sequence_of(built, 0);
        REQUIRE(length(*seq.child, seq.offset) == 1);
        REQUIRE(element<int64_t>(*seq.child, seq.offset, 0) == 1);
        REQUIRE(length(*seq.child, seq.offset + 1) == 0);
    }
    SECTION("list of struct") {
        const auto point = point_type(&resource);
        auto built = make_list(
            &resource,
            point,
            pack(make_struct(&resource,
                             point,
                             pack(make_scalar<int64_t>(&resource, 1), make_scalar<std::string_view>(&resource, "x"))),
                 make_struct(&resource,
                             point,
                             pack(make_null(&resource, bigint()), make_scalar<std::string_view>(&resource, "y")))));
        REQUIRE(built.type() == complex_logical_type::create_list(point));
        const auto seq = sequence_of(built, 0);
        REQUIRE(seq.length == 2);
        REQUIRE(field<int64_t>(*seq.child, seq.offset, 0) == 1);
        REQUIRE(field<std::string_view>(*seq.child, seq.offset, 1) == "x");
        REQUIRE(field_null(*seq.child, seq.offset + 1, 0));
        REQUIRE(field<std::string_view>(*seq.child, seq.offset + 1, 1) == "y");
    }
}

TEST_CASE("make_array") {
    auto resource = core::pmr::otterbrix_resource();

    SECTION("integers with a NULL element") {
        auto built = make_array(&resource,
                                bigint(),
                                pack(make_scalar<int64_t>(&resource, 7),
                                     make_null(&resource, bigint()),
                                     make_scalar<int64_t>(&resource, 9)));
        REQUIRE(built.type() == complex_logical_type::create_array(bigint(), 3));
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE(length(built, 0) == 3);
        REQUIRE(element<int64_t>(built, 0, 0) == 7);
        REQUIRE(element_null(built, 0, 1));
        REQUIRE(element<int64_t>(built, 0, 2) == 9);
        REQUIRE(built.entry().is_null(1));
    }
    SECTION("strings") {
        auto built = make_array(
            &resource,
            varchar(),
            pack(make_scalar<std::string_view>(&resource, "p"), make_scalar<std::string_view>(&resource, "q")));
        REQUIRE(length(built, 0) == 2);
        REQUIRE(element<std::string_view>(built, 0, 0) == "p");
        REQUIRE(element<std::string_view>(built, 0, 1) == "q");
    }
    SECTION("empty") {
        auto built = make_array(&resource, bigint(), std::span<const vector_t>{});
        REQUIRE(built.type() == complex_logical_type::create_array(bigint(), 0));
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(length(built, 0) == 0);
    }
    SECTION("flatten") {
        auto built = make_array(&resource,
                                bigint(),
                                pack(make_scalar<int64_t>(&resource, 1), make_scalar<int64_t>(&resource, 2)));
        built.flatten(3);
        for (uint64_t row = 0; row < 3; row++) {
            REQUIRE(length(built, row) == 2);
            REQUIRE(element<int64_t>(built, row, 0) == 1);
            REQUIRE(element<int64_t>(built, row, 1) == 2);
        }
    }
}

TEST_CASE("make_struct") {
    auto resource = core::pmr::otterbrix_resource();
    const auto point = point_type(&resource);

    SECTION("fields") {
        auto built =
            make_struct(&resource,
                        point,
                        pack(make_scalar<int64_t>(&resource, 5), make_scalar<std::string_view>(&resource, "five")));
        REQUIRE(built.type() == point);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(field<int64_t>(built, 0, 0) == 5);
        REQUIRE(field<std::string_view>(built, 0, 1) == "five");
    }
    SECTION("NULL field") {
        auto built =
            make_struct(&resource,
                        point,
                        pack(make_null(&resource, bigint()), make_scalar<std::string_view>(&resource, "five")));
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(built.entries()[0]->is_null(0));
        REQUIRE(field<std::string_view>(built, 0, 1) == "five");
    }
    SECTION("list field") {
        std::pmr::vector<complex_logical_type> fields(&resource);
        fields.emplace_back(logical_type::BIGINT, "id");
        fields.emplace_back(complex_logical_type::create_list(bigint()));
        fields.back().set_alias("tags");
        const auto type = complex_logical_type::create_struct("rec", fields);
        auto built = make_struct(
            &resource,
            type,
            pack(make_scalar<int64_t>(&resource, 1),
                 make_list(&resource,
                           bigint(),
                           pack(make_scalar<int64_t>(&resource, 10), make_scalar<int64_t>(&resource, 20)))));
        REQUIRE(built.type() == type);
        REQUIRE(field<int64_t>(built, 0, 0) == 1);
        const auto& tags = *resolve_cell(built, 0).vec->entries()[1];
        REQUIRE(length(tags, 0) == 2);
        REQUIRE(element<int64_t>(tags, 0, 0) == 10);
        REQUIRE(element<int64_t>(tags, 0, 1) == 20);
    }
    SECTION("flatten") {
        auto built =
            make_struct(&resource,
                        point,
                        pack(make_scalar<int64_t>(&resource, 5), make_scalar<std::string_view>(&resource, "five")));
        built.flatten(3);
        for (uint64_t row = 0; row < 3; row++) {
            REQUIRE(field<int64_t>(built, row, 0) == 5);
            REQUIRE(field<std::string_view>(built, row, 1) == "five");
        }
    }
}

TEST_CASE("make_map") {
    auto resource = core::pmr::otterbrix_resource();
    const auto map_type = complex_logical_type::create_map(&resource, varchar(), bigint());

    SECTION("entries") {
        auto built =
            make_map(&resource,
                     varchar(),
                     bigint(),
                     pack(make_scalar<std::string_view>(&resource, "a"), make_scalar<std::string_view>(&resource, "b")),
                     pack(make_scalar<int64_t>(&resource, 1), make_scalar<int64_t>(&resource, 2)));
        REQUIRE(built.type().type() == logical_type::MAP);
        REQUIRE(built.type() == map_type);
        REQUIRE(built.get_vector_type() == vector_type::CONSTANT);
        const auto seq = sequence_of(built, 0);
        REQUIRE(seq.length == 2);
        REQUIRE(field<std::string_view>(*seq.child, seq.offset, 0) == "a");
        REQUIRE(field<int64_t>(*seq.child, seq.offset, 1) == 1);
        REQUIRE(field<std::string_view>(*seq.child, seq.offset + 1, 0) == "b");
        REQUIRE(field<int64_t>(*seq.child, seq.offset + 1, 1) == 2);
    }
    SECTION("NULL value") {
        auto built = make_map(&resource,
                              varchar(),
                              bigint(),
                              pack(make_scalar<std::string_view>(&resource, "a")),
                              pack(make_null(&resource, bigint())));
        const auto seq = sequence_of(built, 0);
        REQUIRE(seq.length == 1);
        REQUIRE(field<std::string_view>(*seq.child, seq.offset, 0) == "a");
        REQUIRE(field_null(*seq.child, seq.offset, 1));
    }
    SECTION("empty") {
        auto built = make_map(&resource, varchar(), bigint(), std::span<const vector_t>{}, std::span<const vector_t>{});
        REQUIRE(built.type() == map_type);
        REQUIRE_FALSE(built.is_null(0));
        REQUIRE(built.data<list_entry_t>()[0].length == 0);
    }
}

TEST_CASE("list of NA elements grows its child buffer") {
    auto resource = core::pmr::otterbrix_resource();
    const complex_logical_type na{logical_type::NA};

    SECTION("make_list of two NA elements") {
        auto built = make_list(&resource, na, pack(make_null(&resource, na), make_null(&resource, na)));
        REQUIRE(built.data<list_entry_t>()[0].length == 2);
        REQUIRE(element_null(built, 0, 0));
        REQUIRE(element_null(built, 0, 1));
    }
    SECTION("make_list of 3000 NA elements") {
        std::vector<vector_t> nulls;
        for (int i = 0; i < 3000; i++) {
            nulls.push_back(make_null(&resource, na));
        }
        auto built = make_list(&resource, na, nulls);
        REQUIRE(built.data<list_entry_t>()[0].length == 3000);
    }
}

TEST_CASE("flatten of a CONSTANT past the default capacity keeps every row addressable") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr uint64_t rows = 3000;

    SECTION("NULL") {
        auto built = make_null(&resource, bigint());
        built.flatten(rows);
        REQUIRE(built.validity().count() >= rows);
        for (uint64_t row = 0; row < rows; row++) {
            REQUIRE(built.is_null(row));
        }
        built.set_null(rows - 1, false);
        REQUIRE_FALSE(built.is_null(rows - 1));
        REQUIRE(built.is_null(rows - 2));
    }
    SECTION("non-NULL") {
        auto built = make_scalar<int64_t>(&resource, 7);
        built.flatten(rows);
        REQUIRE(built.validity().count() >= rows);
        built.set_null(rows - 1, true);
        REQUIRE(built.is_null(rows - 1));
        REQUIRE_FALSE(built.is_null(rows - 2));
        REQUIRE(built.get_value<int64_t>(rows - 2) == 7);
    }
}

TEST_CASE("make_null of LIST and ARRAY flattens to NULL rows") {
    auto resource = core::pmr::otterbrix_resource();
    for (const auto& type :
         {complex_logical_type::create_list(bigint()), complex_logical_type::create_array(bigint(), 3)}) {
        auto built = make_null(&resource, type);
        built.flatten(5);
        for (uint64_t row = 0; row < 5; row++) {
            REQUIRE(built.is_null(row));
        }
    }
}

TEST_CASE("indexed flatten of a CONSTANT past the default capacity keeps every row addressable") {
    auto resource = core::pmr::otterbrix_resource();
    constexpr uint64_t rows = 3000;
    indexing_vector_t zeros(&resource, rows);
    for (uint64_t row = 0; row < rows; row++) {
        zeros.set_index(row, 0);
    }

    SECTION("NULL") {
        auto built = make_null(&resource, bigint());
        built.flatten(zeros, rows);
        REQUIRE(built.validity().count() >= rows);
        for (uint64_t row = 0; row < rows; row++) {
            REQUIRE(built.is_null(row));
        }
        built.set_null(rows - 1, false);
        REQUIRE_FALSE(built.is_null(rows - 1));
        REQUIRE(built.is_null(rows - 2));
    }
    SECTION("non-NULL") {
        auto built = make_scalar<int64_t>(&resource, 7);
        built.flatten(zeros, rows);
        REQUIRE(built.validity().count() >= rows);
        built.set_null(rows - 1, true);
        REQUIRE(built.is_null(rows - 1));
        REQUIRE_FALSE(built.is_null(rows - 2));
        REQUIRE(built.get_value<int64_t>(rows - 2) == 7);
    }
}
