#include "operations_helper.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>
#include <core/operations_helper.hpp>
#include <core/pmr.hpp>
#include <array>
#include <cstddef>
#include <memory_resource>
#include <random>
#include <string_view>

using namespace components::types;

namespace {
    // Not the process-global resource: create_decimal's refusal message needs a
    // caller-owned arena.
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // Split from make_decimal() below so a test can drive this one call to refuse and still
    // observe the arena it was handed, not merely one the file happens to name elsewhere.
    core::result_wrapper_t<components::types::complex_logical_type>
    try_make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        return components::types::complex_logical_type::create_decimal(decimal_resource(),
                                                                       width,
                                                                       scale,
                                                                       std::move(alias));
    }

    // Every literal these tests use is inside the window, so this asserts rather than propagates.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = try_make_decimal(width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

TEST_CASE("components::types::physical_value") {
    std::vector<physical_value> values;
    std::string_view str1 = "test string";
    std::string_view str2 = "bigger test string but shouldn't be; b < t";

    INFO("initialization");
    {
        values.emplace_back();
        values.emplace_back(false);
        values.emplace_back(true);
        values.emplace_back(uint8_t(53));
        values.emplace_back(uint16_t(643));
        values.emplace_back(uint32_t(3167));
        values.emplace_back(uint64_t(47853));
        values.emplace_back(int8_t(-57));
        values.emplace_back(int16_t(-731));
        values.emplace_back(int32_t(-9691));
        values.emplace_back(int64_t(-478346));
        values.emplace_back(float(-63.239f));
        values.emplace_back(double(577.3910246));
        values.emplace_back(str1);
        values.emplace_back(str2);
    }

    INFO("value getters");
    {
        REQUIRE(values[0].value<physical_type::NA>() == nullptr);
        REQUIRE(values[1].value<physical_type::BOOL>() == false);
        REQUIRE(values[2].value<physical_type::BOOL>() == true);
        REQUIRE(values[3].value<physical_type::UINT8>() == uint8_t(53));
        REQUIRE(values[4].value<physical_type::UINT16>() == uint16_t(643));
        REQUIRE(values[5].value<physical_type::UINT32>() == uint32_t(3167));
        REQUIRE(values[6].value<physical_type::UINT64>() == uint64_t(47853));
        REQUIRE(values[7].value<physical_type::INT8>() == int8_t(-57));
        REQUIRE(values[8].value<physical_type::INT16>() == int16_t(-731));
        REQUIRE(values[9].value<physical_type::INT32>() == int32_t(-9691));
        REQUIRE(values[10].value<physical_type::INT64>() == int64_t(-478346));
        REQUIRE(core::is_equals(values[11].value<physical_type::FLOAT>(), -63.239f));
        REQUIRE(core::is_equals(values[12].value<physical_type::DOUBLE>(), 577.3910246));
        REQUIRE(values[13].value<physical_type::STRING>() == str1);
        REQUIRE(values[14].value<physical_type::STRING>() == str2);
    }

    INFO("sort");
    {
        std::shuffle(values.begin(), values.end(), std::default_random_engine{0});
        std::sort(values.begin(), values.end());

        REQUIRE(values[0].type() == physical_type::BOOL);
        REQUIRE(values[1].type() == physical_type::BOOL);
        REQUIRE(values[2].type() == physical_type::INT64);
        REQUIRE(values[3].type() == physical_type::INT32);
        REQUIRE(values[4].type() == physical_type::INT16);
        REQUIRE(values[5].type() == physical_type::FLOAT);
        REQUIRE(values[6].type() == physical_type::INT8);
        REQUIRE(values[7].type() == physical_type::UINT8);
        REQUIRE(values[8].type() == physical_type::DOUBLE);
        REQUIRE(values[9].type() == physical_type::UINT16);
        REQUIRE(values[10].type() == physical_type::UINT32);
        REQUIRE(values[11].type() == physical_type::UINT64);
        REQUIRE(values[12].type() == physical_type::STRING);
        REQUIRE(values[12].value<physical_type::STRING>() == str2);
        REQUIRE(values[13].type() == physical_type::STRING);
        REQUIRE(values[13].value<physical_type::STRING>() == str1);
        REQUIRE(values[14].type() == physical_type::NA);
    }
}

TEST_CASE("components::types::operations_helper::powers_of_ten") {
    for (size_t i = 1; i < sizeof(POWERS_OF_TEN) / sizeof(int128_t); i++) {
        REQUIRE(POWERS_OF_TEN[i - 1] * 10 == POWERS_OF_TEN[i]);
    }
}

TEST_CASE("components::types::decimal") {
    auto check_conversion =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };
    auto check_arithmetics =
        []<typename Storage, typename Source>(Source val, uint8_t width, uint8_t scale, const std::string& result) {
            Storage decimal_value = components::types::to_decimal<Storage, Source>(val, width, scale);
            decimal_value /= 10;
            REQUIRE(decimal_to_string(decimal_value, width, scale) == result);
        };

    SECTION("int16_t") {
        static constexpr uint8_t width = 3;
        static constexpr uint8_t scale = 1;
        // verify storage size
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT16);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int16_t, double>(1.27, width, scale, "1.3");
            // round down
            check_conversion.operator()<int16_t, double>(1.21, width, scale, "1.2");
            // from int
            check_conversion.operator()<int16_t, int64_t>(1, width, scale, "1.0");
            // round up
            check_conversion.operator()<int16_t, double>(-1.27, width, scale, "-1.3");
            // round down
            check_conversion.operator()<int16_t, double>(-1.21, width, scale, "-1.2");
            // from int
            check_conversion.operator()<int16_t, int64_t>(-1, width, scale, "-1.0");
            // special_values
            check_conversion.operator()<int16_t, double>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, double>(-10000000, width, scale, "-Infinity");
            check_conversion.operator()<int16_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int16_t, int64_t>(10000000, width, scale, "Infinity");
            check_conversion.operator()<int16_t, int64_t>(-10000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            // round up
            check_arithmetics.operator()<int16_t, double>(1.27, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(1.21, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, int64_t>(1, width, scale, "0.1");
            check_arithmetics.operator()<int16_t, double>(-1.27, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, double>(-1.21, width, scale, "-0.1");
            check_arithmetics.operator()<int16_t, int64_t>(-1, width, scale, "-0.1");
        }
    }

    SECTION("int32_t") {
        static constexpr uint8_t width = 8;
        static constexpr uint8_t scale = 2;
        // verify storage size
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT32);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int32_t, double>(502.215, width, scale, "502.22");
            // round down
            check_conversion.operator()<int32_t, double>(502.214, width, scale, "502.21");
            // from int
            check_conversion.operator()<int32_t, int64_t>(502, width, scale, "502.00");
            // round up
            check_conversion.operator()<int32_t, double>(-502.215, width, scale, "-502.22");
            // round down
            check_conversion.operator()<int32_t, double>(-502.214, width, scale, "-502.21");
            // from int
            check_conversion.operator()<int32_t, int64_t>(-502, width, scale, "-502.00");
            // special_values
            check_conversion.operator()<int32_t, double>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, double>(-10000000000, width, scale, "-Infinity");
            check_conversion.operator()<int32_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int32_t, int64_t>(10000000000, width, scale, "Infinity");
            check_conversion.operator()<int32_t, int64_t>(-10000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int32_t, double>(502.215, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, double>(502.214, width, scale, "50.22");
            check_arithmetics.operator()<int32_t, int64_t>(502, width, scale, "50.20");
            check_arithmetics.operator()<int32_t, double>(-502.215, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, double>(-502.214, width, scale, "-50.22");
            check_arithmetics.operator()<int32_t, int64_t>(-502, width, scale, "-50.20");
        }
    }

    SECTION("int64_t") {
        static constexpr uint8_t width = 12;
        static constexpr uint8_t scale = 3;
        // verify storage size
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT64);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int64_t, double>(502.2157, width, scale, "502.216");
            // round down
            check_conversion.operator()<int64_t, double>(502.2151, width, scale, "502.215");
            // from int
            check_conversion.operator()<int64_t, int64_t>(502, width, scale, "502.000");
            // round up
            check_conversion.operator()<int64_t, double>(-502.2157, width, scale, "-502.216");
            // round down
            check_conversion.operator()<int64_t, double>(-502.2151, width, scale, "-502.215");
            // from int
            check_conversion.operator()<int64_t, int64_t>(-502, width, scale, "-502.000");
            // special_values
            check_conversion.operator()<int64_t, double>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, double>(-10000000000000, width, scale, "-Infinity");
            check_conversion.operator()<int64_t, double>(std::numeric_limits<double>::quiet_NaN(), width, scale, "NaN");
            check_conversion.operator()<int64_t, int64_t>(10000000000000, width, scale, "Infinity");
            check_conversion.operator()<int64_t, int64_t>(-10000000000000, width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int64_t, double>(502.2157, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, double>(502.2151, width, scale, "50.221");
            check_arithmetics.operator()<int64_t, int64_t>(502, width, scale, "50.200");
            check_arithmetics.operator()<int64_t, double>(-502.2157, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, double>(-502.2151, width, scale, "-50.221");
            check_arithmetics.operator()<int64_t, int64_t>(-502, width, scale, "-50.200");
        }
    }

    INFO("int128_t");
    {
        static constexpr uint8_t width = 20;
        static constexpr uint8_t scale = 4;
        // verify storage size
        REQUIRE(make_decimal(width, scale).to_physical_type() == physical_type::INT128);

        SECTION("convert") {
            // round up
            check_conversion.operator()<int128_t, double>(502.21575, width, scale, "502.2158");
            // round down
            check_conversion.operator()<int128_t, double>(502.21572, width, scale, "502.2157");
            // from int
            check_conversion.operator()<int128_t, int64_t>(502, width, scale, "502.0000");
            // round up
            check_conversion.operator()<int128_t, double>(-502.21575, width, scale, "-502.2158");
            // round down
            check_conversion.operator()<int128_t, double>(-502.21572, width, scale, "-502.2157");
            // from int
            check_conversion.operator()<int128_t, int64_t>(-502, width, scale, "-502.0000");
            // special_values
            check_conversion.operator()<int128_t, double>(1e30, width, scale, "Infinity");
            check_conversion.operator()<int128_t, double>(-1e30, width, scale, "-Infinity");
            check_conversion.operator()<int128_t, double>(std::numeric_limits<double>::quiet_NaN(),
                                                          width,
                                                          scale,
                                                          "NaN");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(10000000, 0), width, scale, "Infinity");
            check_conversion.operator()<int128_t, int128_t>(absl::MakeInt128(-10000000, 0), width, scale, "-Infinity");
        }
        SECTION("convert an divide by 10") {
            check_arithmetics.operator()<int128_t, double>(502.21575, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, double>(502.21572, width, scale, "50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(502, width, scale, "50.2000");
            check_arithmetics.operator()<int128_t, double>(-502.21575, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, double>(-502.21572, width, scale, "-50.2215");
            check_arithmetics.operator()<int128_t, int64_t>(-502, width, scale, "-50.2000");
        }
    }
}
TEST_CASE("components::types::logical_value::null_children_safe") {
    // children() must not dereference the null payload of a NULL (NA-typed) value.
    auto* resource = std::pmr::get_default_resource();
    logical_value_t null_value(resource, complex_logical_type{logical_type::NA});
    REQUIRE(null_value.is_null());
    CHECK(null_value.children().empty());
    // A non-null nested value keeps returning its real elements.
    auto list =
        logical_value_t::create_list(resource,
                                     complex_logical_type{logical_type::BIGINT},
                                     {logical_value_t(resource, int64_t{1}), logical_value_t(resource, int64_t{2})});
    REQUIRE_FALSE(list.is_null());
    CHECK(list.children().size() == 2);
}

TEST_CASE("components::types::logical_value::cast_as_null_returns_error") {
    // Must surface conversion_failure, not dispatch into the scalar switch's throwing
    // `default:` arm -- under the executor's -fno-exceptions coroutine that is a SIGABRT.
    std::pmr::monotonic_buffer_resource resource;

    logical_value_t null_value(&resource, complex_logical_type{logical_type::NA});
    REQUIRE(null_value.is_null());

    auto casted = null_value.cast_as(complex_logical_type{logical_type::BIGINT}, {});
    REQUIRE(casted.has_error());
    CHECK(casted.error().type == core::error_code_t::conversion_failure);

    // A well-typed numeric cast still succeeds and yields the converted value.
    logical_value_t int_value(&resource, int32_t{7});
    auto ok = int_value.cast_as(complex_logical_type{logical_type::BIGINT}, {});
    REQUIRE_FALSE(ok.has_error());
    CHECK(ok.value().value<int64_t>() == 7);
}

// A bare assert(false) here SIGABRTs in Debug and silently returns NA under NDEBUG; these
// cases pin the correct VALUE, not the crash.
TEST_CASE("components::types::logical_value::cast_to_decimal_answers_every_numeric_width") {
    std::pmr::monotonic_buffer_resource resource;
    const auto decimal_type = make_decimal(10, 2);

    SECTION("the 8-bit widths are the ones the switch forgot") {
        // TINYINT/UTINYINT are is_numeric() too but had no arms of their own.
        logical_value_t tiny(&resource, int8_t{7});
        auto casted = tiny.cast_as(decimal_type, {});
        REQUIRE_FALSE(casted.has_error());
        CHECK(casted.value().type().type() == logical_type::DECIMAL);
        CHECK(casted.value().value<int64_t>() == 700);

        logical_value_t utiny(&resource, uint8_t{7});
        auto ucasted = utiny.cast_as(decimal_type, {});
        REQUIRE_FALSE(ucasted.has_error());
        CHECK(ucasted.value().value<int64_t>() == 700);

        // 255 read as int8_t is -1, and -128 has no unsigned reading -- a single arm covering
        // both would pass the case above and fail here.
        for (const auto [source, scaled] : std::initializer_list<std::pair<int8_t, int64_t>>{{-128, -12800},
                                                                                            {127, 12700}}) {
            auto edge = logical_value_t(&resource, source).cast_as(decimal_type, {});
            REQUIRE_FALSE(edge.has_error());
            CHECK(edge.value().value<int64_t>() == scaled);
        }
        auto full_byte = logical_value_t(&resource, uint8_t{255}).cast_as(decimal_type, {});
        REQUIRE_FALSE(full_byte.has_error());
        CHECK(full_byte.value().value<int64_t>() == 25500);
    }

    SECTION("BOOLEAN reaches the branch and has no decimal reading") {
        // is_numeric(BOOLEAN) is true but there is no meaningful scaled payload for it.
        logical_value_t flag(&resource, true);
        auto casted = flag.cast_as(decimal_type, {});
        REQUIRE(casted.has_error());
        CHECK(casted.error().type == core::error_code_t::conversion_failure);
    }
}

TEST_CASE("components::types::logical_value::cast_struct_keeps_null_fields_and_refuses_a_shape_change") {
    std::pmr::monotonic_buffer_resource resource;

    SECTION("a NULL field stays a NULL slot, as it already does inside ARRAY and LIST") {
        // STRUCT needs the same NA-child guard ARRAY/LIST already have: otherwise one NULL
        // field refuses the whole row cast.
        std::vector<logical_value_t> fields;
        fields.emplace_back(&resource, int32_t{1});
        fields.emplace_back(&resource, complex_logical_type{logical_type::NA});
        auto source = logical_value_t::create_struct(&resource, "src", fields);

        std::pmr::vector<complex_logical_type> target_fields(&resource);
        target_fields.emplace_back(logical_type::BIGINT);
        target_fields.emplace_back(logical_type::BIGINT);
        auto target = complex_logical_type::create_struct("dst", target_fields);

        auto casted = source.cast_as(target, {});
        REQUIRE_FALSE(casted.has_error());
        REQUIRE(casted.value().children().size() == 2);
        CHECK(casted.value().children()[0].value<int64_t>() == 1);
        CHECK(casted.value().children()[1].is_null());
    }

    SECTION("a different field count refuses instead of asserting") {
        // A field-count mismatch is a conversion failure, not a broken invariant.
        std::vector<logical_value_t> fields;
        fields.emplace_back(&resource, int32_t{1});
        fields.emplace_back(&resource, int32_t{2});
        auto source = logical_value_t::create_struct(&resource, "src", fields);

        std::pmr::vector<complex_logical_type> target_fields(&resource);
        target_fields.emplace_back(logical_type::BIGINT);
        auto target = complex_logical_type::create_struct("dst", target_fields);

        auto casted = source.cast_as(target, {});
        REQUIRE(casted.has_error());
        CHECK(casted.error().type == core::error_code_t::conversion_failure);
    }
}

// UNION/VARIANT must be vector-backed like every other nested type here (TIME_TZ, INTERVAL,
// LIST, ARRAY, MAP, STRUCT): create_union builds each member slot through this same
// constructor, so a member that is itself UNION/VARIANT passes through here too.
TEST_CASE("logical_value: a UNION built through the plain constructor is well formed") {
    std::pmr::monotonic_buffer_resource resource;

    SECTION("UNION") {
        logical_value_t value(&resource, complex_logical_type{logical_type::UNION});
        CHECK(value.type().type() == logical_type::UNION);
        CHECK_FALSE(value.is_null());
        // The read that would follow a null pointer on a half-built value.
        CHECK(value.children().empty());
    }

    SECTION("VARIANT") {
        logical_value_t value(&resource, complex_logical_type{logical_type::VARIANT});
        CHECK(value.type().type() == logical_type::VARIANT);
        CHECK(value.children().empty());
    }

    SECTION("a union whose member type is itself a union -- the factory's own path") {
        // create_union fills every slot via logical_value_t(r, types[i]), the constructor above.
        std::pmr::vector<complex_logical_type> inner_types(&resource);
        inner_types.emplace_back(logical_type::BIGINT);
        auto inner = complex_logical_type::create_union(inner_types);

        std::pmr::vector<complex_logical_type> types(&resource);
        types.emplace_back(logical_type::BIGINT);
        types.emplace_back(inner);

        auto value = logical_value_t::create_union(&resource, types, 0, logical_value_t(&resource, int64_t{7}));
        CHECK(value.type().type() == logical_type::UNION);
        REQUIRE(value.children().size() == 3); // the tag slot plus one per member type
        CHECK(value.children()[1].value<int64_t>() == 7);
        // The nested-union slot is present and readable rather than a null payload.
        CHECK(value.children()[2].type().type() == logical_type::UNION);
        CHECK(value.children()[2].children().empty());
    }
}

// Arithmetic/bit entry points run in a build with exceptions off, so `2.0 ^ 3.0` etc. must
// answer a core::error_t, not throw (see evaluate_const_a_expr in sql/transformer/utils.cpp).
TEST_CASE("logical_value: an unsupported operand type is a refusal, not a throw") {
    std::pmr::monotonic_buffer_resource resource;

    const logical_value_t two_point_oh(&resource, double{2.0});
    const logical_value_t three(&resource, int64_t{3});

    SECTION("modulus over a floating operand") {
        auto result = logical_value_t::modulus(two_point_oh, two_point_oh);
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::arithmetics_failure);
    }

    SECTION("exponent has no floating arm at all") {
        auto result = logical_value_t::exponent(two_point_oh, two_point_oh);
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::arithmetics_failure);
    }

    SECTION("bit_and over a floating operand") {
        auto result = logical_value_t::bit_and(two_point_oh, two_point_oh);
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::arithmetics_failure);
    }

    SECTION("a supported pair still answers with the value") {
        auto sum = logical_value_t::sum(three, three);
        REQUIRE_FALSE(sum.has_error());
        CHECK(sum.value().value<int64_t>() == 6);

        auto product = logical_value_t::mult(two_point_oh, two_point_oh);
        REQUIRE_FALSE(product.has_error());
        CHECK(product.value().value<double>() == 4.0);
    }
}

// A miss in the entry table must refuse, not answer NA (PostgreSQL: `invalid input value
// for enum`), since NA travels on as an ordinary NULL value.
TEST_CASE("logical_value: cast of a string that is not an enum entry is a refusal") {
    std::pmr::monotonic_buffer_resource resource;

    std::vector<logical_value_t> entries;
    {
        logical_value_t happy(&resource, int32_t{0});
        happy.set_alias("happy");
        entries.push_back(std::move(happy));
        logical_value_t sad(&resource, int32_t{7});
        sad.set_alias("sad");
        entries.push_back(std::move(sad));
    }
    auto mood = complex_logical_type::create_enum("mood", std::move(entries));

    const logical_value_t absent(&resource, std::string("angry"));
    auto result = absent.cast_as(mood, {});
    REQUIRE(result.has_error());
    CHECK(result.error().type == core::error_code_t::conversion_failure);

    // A string that IS an entry still casts.
    const logical_value_t present(&resource, std::string("sad"));
    auto ok = present.cast_as(mood, {});
    REQUIRE_FALSE(ok.has_error());
    CHECK(ok.value().value<int32_t>() == 7);

    // The numeric leg has the same contract: an ordinal that names no entry is a refusal.
    const logical_value_t bad_ordinal(&resource, int32_t{99});
    auto ordinal_result = bad_ordinal.cast_as(mood, {});
    REQUIRE(ordinal_result.has_error());
    CHECK(ordinal_result.error().type == core::error_code_t::conversion_failure);

    const logical_value_t good_ordinal(&resource, int32_t{7});
    auto good = good_ordinal.cast_as(mood, {});
    REQUIRE_FALSE(good.has_error());
    CHECK(good.value().value<int32_t>() == 7);
}

// int_to_decimal signals width overflow via the Int128Max/Min sentinels; passing one on as a
// payload instead of refusing would silently store a wrong value (PostgreSQL: `numeric field overflow`).
TEST_CASE("logical_value: numeric overflow into DECIMAL is a refusal") {
    std::pmr::monotonic_buffer_resource resource;

    const auto decimal_3_1 = make_decimal(3, 1);

    SECTION("positive overflow") {
        const logical_value_t big(&resource, int64_t{10000});
        auto result = big.cast_as(decimal_3_1, {});
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("negative overflow") {
        const logical_value_t big(&resource, int64_t{-10000});
        auto result = big.cast_as(decimal_3_1, {});
        REQUIRE(result.has_error());
        CHECK(result.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("floating NaN and overflow") {
        const logical_value_t nan_val(&resource, std::numeric_limits<double>::quiet_NaN());
        auto nan_result = nan_val.cast_as(decimal_3_1, {});
        REQUIRE(nan_result.has_error());

        const logical_value_t huge(&resource, double{1e30});
        auto huge_result = huge.cast_as(decimal_3_1, {});
        REQUIRE(huge_result.has_error());
    }

    SECTION("a fitting value still casts") {
        const logical_value_t fits(&resource, int64_t{99});
        auto result = fits.cast_as(decimal_3_1, {});
        REQUIRE_FALSE(result.has_error());
    }
}

// Reverse of the int->DECIMAL overflow refusal: DECIMAL back into an integer must refuse,
// not answer a silent NA, when decimal_to_numeric says "does not fit".
TEST_CASE("components::types::logical_value::decimal_to_integer_overflow_is_a_refusal") {
    std::pmr::monotonic_buffer_resource resource;

    // NUMERIC(10,0) holding 1000: descale to TINYINT (max 127) cannot represent it.
    auto dec = logical_value_t(&resource, int64_t{1000}).cast_as(make_decimal(10, 0), {});
    REQUIRE_FALSE(dec.has_error());
    REQUIRE(dec.value().type().type() == logical_type::DECIMAL);

    SECTION("descale overflow refuses instead of answering NA") {
        auto back = dec.value().cast_as(complex_logical_type{logical_type::TINYINT}, {});
        REQUIRE(back.has_error());
        CHECK(back.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("a negative value cannot descale into an unsigned width") {
        auto neg = logical_value_t(&resource, int64_t{-5}).cast_as(make_decimal(10, 0), {});
        REQUIRE_FALSE(neg.has_error());
        auto back = neg.value().cast_as(complex_logical_type{logical_type::UTINYINT}, {});
        REQUIRE(back.has_error());
        CHECK(back.error().type == core::error_code_t::conversion_failure);
    }

    SECTION("an in-range descale still answers the value") {
        auto small = logical_value_t(&resource, int64_t{42}).cast_as(make_decimal(10, 0), {});
        REQUIRE_FALSE(small.has_error());
        auto back = small.value().cast_as(complex_logical_type{logical_type::TINYINT}, {});
        REQUIRE_FALSE(back.has_error());
        CHECK(back.value().value<int8_t>() == 42);
    }
}

// Unguarded, BIGINT+STRING would dispatch on the left type and read the string's heap
// pointer as an int64 payload; every mixed pair outside numeric promotion must refuse.
TEST_CASE("components::types::logical_value::mixed_operand_arithmetic_refuses") {
    std::pmr::monotonic_buffer_resource resource;
    const logical_value_t str(&resource, std::string{"a"});
    const logical_value_t num(&resource, int64_t{1});

    SECTION("string + number refuses (used to throw)") {
        auto r = logical_value_t::sum(str, num);
        REQUIRE(r.has_error());
    }
    SECTION("number + string refuses (used to answer pointer bits)") {
        auto r = logical_value_t::sum(num, str);
        REQUIRE(r.has_error());
    }
    SECTION("string - number refuses") {
        auto r = logical_value_t::subtract(str, num);
        REQUIRE(r.has_error());
    }
    SECTION("number * string refuses") {
        auto r = logical_value_t::mult(num, str);
        REQUIRE(r.has_error());
    }
    SECTION("number % string refuses") {
        auto r = logical_value_t::modulus(num, str);
        REQUIRE(r.has_error());
    }
    SECTION("number ^ string refuses") {
        auto r = logical_value_t::exponent(num, str);
        REQUIRE(r.has_error());
    }
    SECTION("number & string refuses") {
        auto r = logical_value_t::bit_and(num, str);
        REQUIRE(r.has_error());
    }
    SECTION("mixed NUMERIC pairs still promote and answer") {
        auto r = logical_value_t::sum(logical_value_t(&resource, int32_t{2}), num);
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value().value<int64_t>() == 3);
    }
}

namespace {
    // Counts what is asked of it and forwards the rest. `allocations` is the number that tells
    // an arena that was USED apart from one that was merely named.
    struct counting_resource_t final : std::pmr::memory_resource {
        explicit counting_resource_t(std::pmr::memory_resource* upstream) noexcept
            : upstream_(upstream) {}

        size_t allocations = 0;
        size_t bytes = 0;

    private:
        void* do_allocate(size_t b, size_t a) override {
            ++allocations;
            bytes += b;
            return upstream_->allocate(b, a);
        }
        void do_deallocate(void* p, size_t b, size_t a) override { upstream_->deallocate(p, b, a); }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::pmr::memory_resource* upstream_;
    };
} // namespace

TEST_CASE("components::types::complex_logical_type::create_decimal_reports_on_the_caller_arena") {
    // Create_decimal's only allocation is its refusal message, so this pins that the
    // message is both BUILT on the caller's arena and still LIVE there after the
    // result_wrapper_t is returned and moved -- error_t's copy assignment re-anchors onto the
    // default resource, so a correct build can still fail the second check.
    std::array<std::byte, 4096> storage{};
    std::pmr::monotonic_buffer_resource stack_arena{storage.data(),
                                                   storage.size(),
                                                   std::pmr::null_memory_resource()};
    counting_resource_t arena{&stack_arena};

    INFO("an out-of-window DECIMAL reports on the arena it was handed");
    auto refused = complex_logical_type::create_decimal(&arena, 39, 0);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(arena.allocations >= 1);
    CHECK(refused.error().what.get_allocator().resource() == &arena);
    CHECK(std::string_view{refused.error().what}.find("DECIMAL(39,0)") != std::string_view::npos);

    INFO("a second refusal on a second arena does not drift back to the first");
    std::array<std::byte, 4096> other_storage{};
    std::pmr::monotonic_buffer_resource other_stack{other_storage.data(),
                                                    other_storage.size(),
                                                    std::pmr::null_memory_resource()};
    counting_resource_t other{&other_stack};
    const size_t first_arena_allocations = arena.allocations;
    auto refused_elsewhere = complex_logical_type::create_decimal(&other, 5, 7);
    REQUIRE(refused_elsewhere.has_error());
    CHECK(other.allocations >= 1);
    CHECK(refused_elsewhere.error().what.get_allocator().resource() == &other);
    CHECK(arena.allocations == first_arena_allocations);

    INFO("an in-window DECIMAL costs the arena nothing");
    const size_t before = arena.allocations;
    auto built = complex_logical_type::create_decimal(&arena, 18, 4);
    REQUIRE_FALSE(built.has_error());
    CHECK(built.value().type() == logical_type::DECIMAL);
    CHECK(arena.allocations == before);
}

TEST_CASE("components::types::complex_logical_type::decimal_helpers_name_an_arena_of_their_own") {
    // Unlike the case above (the FACTORY reports on whatever arena it's handed), this pins
    // that the test HELPERS route through decimal_resource() and not the forbidden
    // process-global arena -- naming only one arena couldn't catch that drift, so this names two.
    auto* helper_arena = decimal_resource();

    INFO("the helper's arena is not the process-global one");
    // A default-constructed std::pmr::string is anchored on the process default resource.
    const std::pmr::string process_anchored;
    CHECK(helper_arena != process_anchored.get_allocator().resource());

    INFO("and a refusal routed through the HELPER's own call lands there and stays there");
    // try_make_decimal, not create_decimal: the exact call make_decimal makes.
    auto refused = try_make_decimal(39, 0);
    REQUIRE(refused.has_error());
    CHECK(refused.error().type == core::error_code_t::invalid_parameter);
    CHECK(refused.error().what.get_allocator().resource() == helper_arena);
    CHECK(std::string_view{refused.error().what}.find("DECIMAL(39,0)") != std::string_view::npos);

    INFO("the same arena is the one the in-window helper path actually uses");
    const auto built = make_decimal(38, 20, "d");
    REQUIRE(built.type() == logical_type::DECIMAL);
    const auto* ext = built.extension_as<decimal_logical_type_extension>();
    REQUIRE(ext != nullptr);
    CHECK(ext->width() == 38);
    CHECK(ext->scale() == 20);
    CHECK(decimal_resource() == helper_arena);
}
