#include "operations_helper.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>
#include <core/operations_helper.hpp>
#include <memory_resource>
#include <random>

using namespace components::types;

namespace {
    // create_decimal reports an out-of-window (width, scale) through core::error_t. Every
    // literal these tests use is inside the window, so the helper checks the result and
    // hands back the type.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(width, scale, std::move(alias));
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
    // children() on a NULL (NA-typed) value must not dereference the null payload
    // pointer: NULL nested values are ordinary result-set data, so reading them
    // through the children() idiom has to be safe.
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
    // cast_as() on a NULL/NA-typed value must surface a conversion_failure through
    // result_wrapper_t. Dispatching it into the scalar physical-type switch lands on a `default:`
    // arm that throws std::logic_error, and under the executor's -fno-exceptions coroutine that
    // becomes unhandled_exception() -> assert(false) -> SIGABRT.
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

// -----------------------------------------------------------------------------
// cast_as: A SWITCH ARM THAT ONLY ASSERTS ANSWERS DIFFERENTLY IN THE TWO BUILDS.
//
// A bare assert(false) with no return on the `default:` arm of cast_as's two DECIMAL
// branches is SIGABRT in a Debug build; under NDEBUG the assert is compiled out and control
// walks off the end of the switch, out of the else-if chain and into the function's trailing
// `return NA`, so the SAME call answers a silent NULL. Neither answer is one a caller can
// act on, and the two builds disagreeing is worse than either. These cases pin the VALUE,
// not the crash.
TEST_CASE("components::types::logical_value::cast_to_decimal_answers_every_numeric_width") {
    std::pmr::monotonic_buffer_resource resource;
    const auto decimal_type = make_decimal(10, 2);

    SECTION("the 8-bit widths are the ones the switch forgot") {
        // TINYINT and UTINYINT are is_numeric(), so they enter the DECIMAL branch and need
        // arms of their own. CAST(<tinyint> AS NUMERIC(10,2)) is ordinary SQL; it must produce
        // 7.00, scaled, like every other integer width.
        logical_value_t tiny(&resource, int8_t{7});
        auto casted = tiny.cast_as(decimal_type, {});
        REQUIRE_FALSE(casted.has_error());
        CHECK(casted.value().type().type() == logical_type::DECIMAL);
        CHECK(casted.value().value<int64_t>() == 700);

        logical_value_t utiny(&resource, uint8_t{7});
        auto ucasted = utiny.cast_as(decimal_type, {});
        REQUIRE_FALSE(ucasted.has_error());
        CHECK(ucasted.value().value<int64_t>() == 700);

        // THE ENDS OF BOTH WIDTHS, which is what separates the two arms from each other: 255
        // read through int8_t is -1, and -128 has no unsigned reading at all. A single arm
        // covering both would pass the 7 above and fail here.
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
        // is_numeric(BOOLEAN) is true, so a boolean walks into the DECIMAL branch too -- but
        // unlike the integer widths there is no meaningful scaled payload for it. It must
        // REFUSE, in both builds, with the same conversion_failure the scalar guard uses.
        logical_value_t flag(&resource, true);
        auto casted = flag.cast_as(decimal_type, {});
        REQUIRE(casted.has_error());
        CHECK(casted.error().type == core::error_code_t::conversion_failure);
    }
}

TEST_CASE("components::types::logical_value::cast_struct_keeps_null_fields_and_refuses_a_shape_change") {
    std::pmr::monotonic_buffer_resource resource;

    SECTION("a NULL field stays a NULL slot, as it already does inside ARRAY and LIST") {
        // The ARRAY and LIST arms skip a NA child ("a NULL element stays a NULL slot"), and
        // STRUCT needs the same guard: without it one NULL field makes the scalar guard refuse
        // the WHOLE row value -- and a NULL is not a failed cast.
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
        // Two fields cast to a one-field struct: a shape the caller got wrong, which is a
        // conversion failure, not a broken invariant of this class.
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

// A UNION/VARIANT VALUE FROM THE PLAIN CONSTRUCTOR MUST BE FULLY BUILT.
//
// logical_value_t(resource, complex_logical_type) allocates the backing vector for every
// vector-backed type -- TIME_TZ, INTERVAL, LIST, ARRAY, MAP, STRUCT -- and UNION/VARIANT
// must not be left out of that list with an assert. Debug dies, Release walks on: under
// NDEBUG the assert is not compiled and the constructor RETURNS a value whose type is UNION
// and whose data_ is 0 -- and children() guards only is_null() (type == NA), so it
// dereferences a null pointer on the next read.
//
// Such an assert is also false about its own file: create_union builds each member slot with
// exactly this constructor (`union_values->emplace_back(r, types[i])`), so a union with a
// UNION or VARIANT member reaches it THROUGH the factory it names.
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
        // create_union fills every slot except `tag` with logical_value_t(r, types[i]), so a
        // nested union type walks into the constructor above.
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

// THE SIXTEEN ARITHMETIC AND BIT ENTRY POINTS ANSWER WITH A VALUE, NOT AN EXCEPTION.
//
// They run in a build that turns exceptions off, and every one of them is reachable from
// ordinary typing: `2.0 ^ 3.0`, `5.5 % 2` and bit_and over a DOUBLE are not exotic inputs,
// they are the arms nobody wrote. The refusal is a core::error_t on the channel the caller
// already has (components/sql/transformer/utils.cpp, evaluate_const_a_expr returns
// result_wrapper_t).
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

// CASTING A STRING THAT NAMES NO ENUM ENTRY IS A REFUSAL, NOT A NULL.
//
// A miss in the entry table must not answer logical_type::NA -- the tree's spelling of NULL.
// NA flows on as a normal value: bound into a parameter it compares as UNKNOWN, and on the
// INSERT coercion path it stores a silent NULL in place of the misspelled label. PostgreSQL
// refuses: `invalid input value for enum`. The refusal travels the cast_as error channel.
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

// A NUMERIC THAT DOES NOT FIT THE DECIMAL WIDTH IS A REFUSAL, NOT A SENTINEL.
//
// int_to_decimal answers width overflow with decimal_limits::pos_inf/neg_inf -- Int128Max /
// Int128Min for the int128 storage cast_as uses. Wrapping that sentinel into a DECIMAL
// logical_value and answering it as a normal value turns CAST(10000 AS NUMERIC(3,1)) into a
// "decimal" whose payload is 170141183460469231731687303715884105727. PostgreSQL refuses:
// `numeric field overflow`.
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

// THE REVERSE OF THE int->DECIMAL OVERFLOW REFUSAL. The forward direction (numeric into
// DECIMAL) refuses an out-of-range value with conversion_failure. The descale direction —
// DECIMAL back into an integer — must refuse identically when decimal_to_numeric says "does
// not fit"; a SILENT NA there is a success-shaped result carrying NULL for a value that
// exists.
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

// MIXED OPERANDS OUTSIDE NUMERIC PROMOTION DISPATCH BY THE LEFT TYPE AND READ THE RIGHT
// OPERAND WITH THE LEFT'S GETTER. needs_numeric_promotion requires BOTH operands numeric,
// so unguarded STRING+BIGINT enters the STRING arm and `sum('a', 1)` throws std::logic_error
// ("value<T>() is not implemented") out of an error-channel function — a rule-2 violation
// reachable from every predicate evaluator. Worse, BIGINT+STRING reads the string's HEAP
// POINTER as an int64 payload and answers garbage. Every mixed pair outside promotion (and
// outside the explicit temporal combinations) must come back as an error.
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
