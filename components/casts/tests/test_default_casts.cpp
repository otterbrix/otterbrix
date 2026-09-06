#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/casts/composite_cast.hpp>
#include <components/casts/default_casts.hpp>

#include <core/date/date_parse.hpp>
#include <core/date/date_to_string.hpp>
#include <core/pmr.hpp>

#include <cmath>
#include <limits>
#include <optional>
#include <vector>

using namespace components;
using namespace components::casts;
using types::complex_logical_type;
using types::logical_type;

namespace {
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
}

namespace {
    // Search operates on complex_logical_type, since a bare logical_type can't carry decimal params.
    const complex_logical_type integer_type{logical_type::INTEGER};
    const complex_logical_type bigint_type{logical_type::BIGINT};
    const complex_logical_type hugeint_type{logical_type::HUGEINT};
    const complex_logical_type ubigint_type{logical_type::UBIGINT};
    const complex_logical_type float_type{logical_type::FLOAT};
    const complex_logical_type double_type{logical_type::DOUBLE};
    const complex_logical_type string_type{logical_type::STRING_LITERAL};

    uint32_t precision_loss(const cast_registry_t& registry,
                            const complex_logical_type& source,
                            const complex_logical_type& target) {
        const cast_entry* entry = registry.find(source, target);
        REQUIRE(entry != nullptr);
        REQUIRE(entry->has_fixed_cost);
        return entry->fixed_cost.precision_loss;
    }

    std::optional<cast_registry_t::common_type>
    common(const cast_registry_t& registry, const complex_logical_type& left, const complex_logical_type& right) {
        return registry.find_best_common_type(left, right);
    }
}

TEST_CASE("default casts: registered numeric-tower entries") {
    cast_registry_t registry{std::pmr::get_default_resource()};
    register_default_casts(registry);

    const cast_entry* widen = registry.find(integer_type, bigint_type);
    REQUIRE(widen != nullptr);
    REQUIRE(widen->promotes());

    // Exact conversions cost nothing, lossy ones cost something; we assert only zero-vs-nonzero.

    REQUIRE(precision_loss(registry, integer_type, bigint_type) == 0);
    REQUIRE(precision_loss(registry, float_type, double_type) == 0);
    REQUIRE(precision_loss(registry, integer_type, double_type) == 0);

    REQUIRE(precision_loss(registry, integer_type, float_type) > 0);
    REQUIRE(precision_loss(registry, bigint_type, double_type) > 0);

    REQUIRE(precision_loss(registry, bigint_type, float_type) > precision_loss(registry, integer_type, float_type));
    REQUIRE(precision_loss(registry, bigint_type, float_type) > precision_loss(registry, bigint_type, double_type));

    // BIGINT -> INTEGER is a narrowing at ASSIGNMENT level, so it never enters common-type search.
    const cast_entry* narrowing = registry.find(bigint_type, integer_type);
    REQUIRE(narrowing != nullptr);
    REQUIRE(narrowing->fn.has_try_cast());
    REQUIRE(narrowing->level == cast_type::assignment);
    REQUIRE_FALSE(registry.cost_of(bigint_type, integer_type).has_value());
}

TEST_CASE("default casts: find_best_common_type over the numeric tower") {
    cast_registry_t registry{std::pmr::get_default_resource()};
    register_default_casts(registry);

    auto same = common(registry, integer_type, integer_type);
    REQUIRE(same.has_value());
    REQUIRE(same->type.type() == logical_type::INTEGER);
    REQUIRE_FALSE(same->left_cast);
    REQUIRE_FALSE(same->right_cast);

    auto widen = common(registry, integer_type, bigint_type);
    REQUIRE(widen.has_value());
    REQUIRE(widen->type.type() == logical_type::BIGINT);
    REQUIRE(widen->left_cast);
    REQUIRE_FALSE(widen->right_cast);

    auto widen_swapped = common(registry, bigint_type, integer_type);
    REQUIRE(widen_swapped.has_value());
    REQUIRE(widen_swapped->type.type() == logical_type::BIGINT);
    REQUIRE_FALSE(widen_swapped->left_cast);
    REQUIRE(widen_swapped->right_cast);

    auto mixed = common(registry, integer_type, float_type);
    REQUIRE(mixed.has_value());
    REQUIRE(mixed->type.type() == logical_type::DOUBLE);
    REQUIRE(mixed->left_cast);
    REQUIRE(mixed->right_cast);

    auto cross = common(registry, ubigint_type, integer_type);
    REQUIRE(cross.has_value());
    REQUIRE(cross->type.type() == logical_type::HUGEINT);
    REQUIRE(cross->left_cast);
    REQUIRE(cross->right_cast);
}

TEST_CASE("default casts: the integer ladder meets at the narrowest type that holds both") {
    cast_registry_t registry{std::pmr::get_default_resource()};
    register_default_casts(registry);

    const complex_logical_type tinyint_type{logical_type::TINYINT};
    const complex_logical_type smallint_type{logical_type::SMALLINT};
    const complex_logical_type usmallint_type{logical_type::USMALLINT};
    const complex_logical_type uinteger_type{logical_type::UINTEGER};
    const complex_logical_type uhugeint_type{logical_type::UHUGEINT};

    auto meets_at = [&](const complex_logical_type& left, const complex_logical_type& right, logical_type expected) {
        auto found = common(registry, left, right);
        INFO("left " << static_cast<int>(left.type()) << " right " << static_cast<int>(right.type()));
        REQUIRE(found.has_value());
        CHECK(found->type.type() == expected);
        auto swapped = common(registry, right, left);
        REQUIRE(swapped.has_value());
        CHECK(swapped->type.type() == expected);
    };

    meets_at(tinyint_type, smallint_type, logical_type::SMALLINT);

    // Mixed signedness picks the narrowest SIGNED type that holds every value of the unsigned side.
    meets_at(usmallint_type, integer_type, logical_type::INTEGER);
    meets_at(uinteger_type, integer_type, logical_type::BIGINT);
    meets_at(ubigint_type, bigint_type, logical_type::HUGEINT);

    meets_at(uhugeint_type, hugeint_type, logical_type::HUGEINT);

    CHECK(precision_loss(registry, uhugeint_type, hugeint_type) <
          precision_loss(registry, hugeint_type, uhugeint_type));
    CHECK(precision_loss(registry, uhugeint_type, hugeint_type) > 0);
}

TEST_CASE("default casts: DOUBLE -> INTEGER is fallible and assignment, so it never wins a search") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const cast_entry* entry = registry.find(double_type, integer_type);
    REQUIRE(entry != nullptr);
    // An ASSIGNMENT cast (e.g. float into an int column) never promotes, so it carries no cost.
    REQUIRE(entry->level == cast_type::assignment);
    REQUIRE(registry.resolve(double_type, integer_type, cast_type::assignment).has_value());
    REQUIRE_FALSE(entry->promotes());
    REQUIRE_FALSE(registry.resolve(double_type, integer_type, cast_type::implicit).has_value());
    REQUIRE_FALSE(registry.cost_of(double_type, integer_type).has_value());
    auto promoted = common(registry, integer_type, double_type);
    REQUIRE(promoted.has_value());
    REQUIRE(promoted->type.type() == logical_type::DOUBLE);
    REQUIRE(entry->fn.has_try_cast());

    graph_execution_context params{};

    // In-range values round to nearest, ties to even (like PostgreSQL).
    {
        constexpr uint64_t count = 4;
        const double values[count] = {3.9, 2.5, 3.5, -2.5};
        vector::vector_t source{resource, double_type};
        for (uint64_t row = 0; row < count; ++row) {
            source.set_value(row, static_cast<double>(values[row]));
        }
        vector::vector_t result{resource, integer_type};
        core::error_t error = entry->fn.invoke(cast_kind::cast, source, &result, params, count);
        REQUIRE_FALSE(error.contains_error());
        REQUIRE(result.get_value<int32_t>(0) == 4);
        REQUIRE(result.get_value<int32_t>(1) == 2);
        REQUIRE(result.get_value<int32_t>(2) == 4);
        REQUIRE(result.get_value<int32_t>(3) == -2);
    }

    // Overflow and inf/nan all fail (PostgreSQL: overflow errors rather than saturates).
    const double invalid[] = {1e30,
                              -1e30,
                              std::numeric_limits<double>::infinity(),
                              -std::numeric_limits<double>::infinity(),
                              std::numeric_limits<double>::quiet_NaN()};
    for (double bad : invalid) {
        vector::vector_t source{resource, double_type};
        source.set_value(0, static_cast<double>(bad));

        vector::vector_t cast_result{resource, integer_type};
        core::error_t error = entry->fn.invoke(cast_kind::cast, source, &cast_result, params, 1);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, integer_type};
        core::error_t try_error = entry->fn.invoke(cast_kind::try_cast, source, &try_result, params, 1);
        REQUIRE_FALSE(try_error.contains_error());
        REQUIRE(try_result.is_null(0));
    }
}

TEST_CASE("default casts: integer narrowing errors on overflow (PostgreSQL rule)") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type smallint_type{logical_type::SMALLINT};
    const complex_logical_type utinyint_type{logical_type::UTINYINT};
    const complex_logical_type uinteger_type{logical_type::UINTEGER};

    REQUIRE(registry.find(smallint_type, integer_type) != nullptr);
    const cast_entry* narrow = registry.find(integer_type, smallint_type);
    REQUIRE(narrow != nullptr);
    REQUIRE(narrow->level == cast_type::assignment);
    REQUIRE(narrow->fn.has_try_cast());
    REQUIRE_FALSE(narrow->promotes());

    auto promoted = common(registry, smallint_type, integer_type);
    REQUIRE(promoted.has_value());
    REQUIRE(promoted->type.type() == logical_type::INTEGER);

    graph_execution_context params{};

    {
        const cast_entry* to_uint8 = registry.find(integer_type, utinyint_type);
        REQUIRE(to_uint8 != nullptr);

        vector::vector_t source{resource, integer_type};
        source.set_value(0, static_cast<int32_t>(200));
        source.set_value(1, static_cast<int32_t>(-1));
        source.set_value(2, static_cast<int32_t>(256));

        vector::vector_t cast_result{resource, utinyint_type};
        core::error_t error = to_uint8->fn.invoke(cast_kind::cast, source, &cast_result, params, 3);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, utinyint_type};
        REQUIRE_FALSE(to_uint8->fn.invoke(cast_kind::try_cast, source, &try_result, params, 3).contains_error());
        REQUIRE(try_result.get_value<uint8_t>(0) == 200);
        REQUIRE(try_result.is_null(1));
        REQUIRE(try_result.is_null(2));
    }

    {
        const cast_entry* uint32_to_int32 = registry.find(uinteger_type, integer_type);
        REQUIRE(uint32_to_int32 != nullptr);

        vector::vector_t source{resource, uinteger_type};
        source.set_value(0, static_cast<uint32_t>(2000000000));
        source.set_value(1, static_cast<uint32_t>(4000000000));

        vector::vector_t try_result{resource, integer_type};
        REQUIRE_FALSE(uint32_to_int32->fn.invoke(cast_kind::try_cast, source, &try_result, params, 2).contains_error());
        REQUIRE(try_result.get_value<int32_t>(0) == 2000000000);
        REQUIRE(try_result.is_null(1));
    }
}

TEST_CASE("default casts: double -> float narrows at assignment level") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const cast_entry* narrow = registry.find(double_type, float_type);
    REQUIRE(narrow != nullptr);
    REQUIRE(narrow->level == cast_type::assignment);
    REQUIRE(narrow->fn.has_try_cast());
    REQUIRE_FALSE(narrow->promotes());

    auto promoted = common(registry, float_type, double_type);
    REQUIRE(promoted.has_value());
    REQUIRE(promoted->type.type() == logical_type::DOUBLE);

    graph_execution_context params{};

    {
        vector::vector_t source{resource, double_type};
        source.set_value(0, 0.5);
        source.set_value(1, -1.0e30);
        source.set_value(2, std::numeric_limits<double>::infinity());

        vector::vector_t result{resource, float_type};
        REQUIRE_FALSE(narrow->fn.invoke(cast_kind::cast, source, &result, params, 3).contains_error());
        REQUIRE(result.get_value<float>(0) == Catch::Approx(0.5f));
        REQUIRE(result.get_value<float>(1) == Catch::Approx(-1.0e30f));
        REQUIRE(std::isinf(result.get_value<float>(2)));
    }

    {
        vector::vector_t source{resource, double_type};
        source.set_value(0, 1.0e300);
        source.set_value(1, 1.0e-300);

        vector::vector_t cast_result{resource, float_type};
        core::error_t error = narrow->fn.invoke(cast_kind::cast, source, &cast_result, params, 2);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, float_type};
        REQUIRE_FALSE(narrow->fn.invoke(cast_kind::try_cast, source, &try_result, params, 2).contains_error());
        REQUIRE(try_result.is_null(0));
        REQUIRE(try_result.is_null(1));
    }
}

TEST_CASE("default casts: DECIMAL <-> double round-trips, passes inf/nan, errors on overflow") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type decimal_type = make_decimal(10, 2);
    const cast_entry* to_double = registry.find(decimal_type, double_type);
    const cast_entry* from_double = registry.find(double_type, decimal_type);
    REQUIRE(to_double != nullptr);
    REQUIRE(from_double != nullptr);
    REQUIRE_FALSE(to_double->fn.has_try_cast());
    REQUIRE(from_double->fn.has_try_cast());

    // decimal->double promotes but double->decimal is only assignment, so a mixed pair meets at double.
    REQUIRE(to_double->promotes());
    REQUIRE(from_double->level == cast_type::assignment);
    REQUIRE(registry.cost_of(decimal_type, double_type).has_value());
    REQUIRE_FALSE(registry.cost_of(double_type, decimal_type).has_value());
    {
        auto meet = common(registry, decimal_type, double_type);
        REQUIRE(meet.has_value());
        REQUIRE(meet->type.type() == logical_type::DOUBLE);
        auto swapped = common(registry, double_type, decimal_type);
        REQUIRE(swapped.has_value());
        REQUIRE(swapped->type.type() == logical_type::DOUBLE);
    }

    graph_execution_context context{};

    {
        vector::vector_t source{resource, double_type};
        source.set_value(0, 123.456);
        source.set_value(1, -0.005);
        source.set_value(2, 1e9);
        source.set_value(3, std::numeric_limits<double>::infinity());

        vector::vector_t cast_result{resource, decimal_type};
        core::error_t error = from_double->fn.invoke(cast_kind::cast, source, &cast_result, context, 4);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, decimal_type};
        REQUIRE_FALSE(from_double->fn.invoke(cast_kind::try_cast, source, &try_result, context, 4).contains_error());
        vector::vector_t back{resource, double_type};
        REQUIRE_FALSE(to_double->fn.invoke(cast_kind::cast, try_result, &back, context, 4).contains_error());
        REQUIRE(back.get_value<double>(0) == Catch::Approx(123.46));
        REQUIRE(back.get_value<double>(1) == Catch::Approx(-0.01));
        REQUIRE(try_result.is_null(2));
        REQUIRE(std::isinf(back.get_value<double>(3)));
    }
}

TEST_CASE("default casts: DECIMAL <-> integer rounds, fails on overflow and specials") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type decimal_type = make_decimal(10, 2);
    const cast_entry* to_bigint = registry.find(decimal_type, bigint_type);
    const cast_entry* from_bigint = registry.find(bigint_type, decimal_type);
    REQUIRE(to_bigint != nullptr);
    REQUIRE(from_bigint != nullptr);
    REQUIRE(from_bigint->promotes());
    REQUIRE(to_bigint->level == cast_type::assignment);
    REQUIRE(to_bigint->fn.has_try_cast());
    REQUIRE(from_bigint->fn.has_try_cast());
    REQUIRE_FALSE(from_bigint->has_fixed_cost);

    // An integer and a decimal meet at DECIMAL: the demoting direction is assignment-only.
    {
        auto forward = common(registry, integer_type, decimal_type);
        REQUIRE(forward.has_value());
        REQUIRE(forward->type.type() == logical_type::DECIMAL);
        auto swapped = common(registry, decimal_type, integer_type);
        REQUIRE(swapped.has_value());
        REQUIRE(swapped->type.type() == logical_type::DECIMAL);
    }
    // A wider integer still wins over a DECIMAL key: the collapsed key is never a third-type candidate.
    {
        auto integers = common(registry, ubigint_type, integer_type);
        REQUIRE(integers.has_value());
        REQUIRE(integers->type.type() == logical_type::HUGEINT);
    }

    graph_execution_context context{};

    {
        vector::vector_t source{resource, bigint_type};
        source.set_value(0, static_cast<int64_t>(42));
        source.set_value(1, static_cast<int64_t>(-7));
        source.set_value(2, static_cast<int64_t>(100000000));

        vector::vector_t cast_result{resource, decimal_type};
        core::error_t error = from_bigint->fn.invoke(cast_kind::cast, source, &cast_result, context, 3);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, decimal_type};
        REQUIRE_FALSE(from_bigint->fn.invoke(cast_kind::try_cast, source, &try_result, context, 3).contains_error());
        REQUIRE(try_result.is_null(2));

        vector::vector_t back{resource, bigint_type};
        REQUIRE_FALSE(to_bigint->fn.invoke(cast_kind::cast, try_result, &back, context, 2).contains_error());
        REQUIRE(back.get_value<int64_t>(0) == 42);
        REQUIRE(back.get_value<int64_t>(1) == -7);
    }

    {
        const complex_logical_type physical_decimal = decimal_type;
        vector::vector_t source{resource, physical_decimal};
        const auto infinity =
            static_cast<int64_t>(types::decimal_special::positive_infinity(types::physical_type::INT64));
        source.set_value(0, static_cast<int64_t>(250));
        source.set_value(1, static_cast<int64_t>(-250));
        source.set_value(2, static_cast<int64_t>(149));
        source.set_value(3, infinity);

        vector::vector_t cast_result{resource, bigint_type};
        core::error_t error = to_bigint->fn.invoke(cast_kind::cast, source, &cast_result, context, 4);
        REQUIRE(error.contains_error());

        vector::vector_t try_result{resource, bigint_type};
        REQUIRE_FALSE(to_bigint->fn.invoke(cast_kind::try_cast, source, &try_result, context, 4).contains_error());
        REQUIRE(try_result.get_value<int64_t>(0) == 3);
        REQUIRE(try_result.get_value<int64_t>(1) == -3);
        REQUIRE(try_result.get_value<int64_t>(2) == 1);
        REQUIRE(try_result.is_null(3));
    }
}

TEST_CASE("default casts: DECIMAL -> DECIMAL rescales, rounds half away, overflows, passes specials") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type narrow = make_decimal(10, 2);
    const complex_logical_type wider = make_decimal(12, 4);
    const cast_entry* up = registry.find(narrow, wider);
    const cast_entry* down = registry.find(wider, narrow);
    REQUIRE(up != nullptr);
    REQUIRE(down != nullptr);
    REQUIRE(up->promotes());
    REQUIRE(up->fn.has_try_cast());
    REQUIRE_FALSE(up->has_fixed_cost);

    // reach() must use exact equality: two decimals can share a cast identity yet still need a rescale.
    {
        auto meet = common(registry, narrow, wider);
        REQUIRE(meet.has_value());
        const auto* extension = meet->type.extension_as<types::decimal_logical_type_extension>();
        REQUIRE(extension != nullptr);
        REQUIRE(extension->width() == 12);
        REQUIRE(extension->scale() == 4);
        auto swapped = common(registry, wider, narrow);
        REQUIRE(swapped.has_value());
        REQUIRE(swapped->type.extension_as<types::decimal_logical_type_extension>()->width() == 12);
    }

    graph_execution_context context{};

    {
        vector::vector_t source{resource, narrow};
        source.set_value(0, static_cast<int64_t>(12345));
        source.set_value(1, static_cast<int64_t>(-100));
        vector::vector_t result{resource, wider};
        REQUIRE_FALSE(up->fn.invoke(cast_kind::cast, source, &result, context, 2).contains_error());
        REQUIRE(result.get_value<int64_t>(0) == 1234500);
        REQUIRE(result.get_value<int64_t>(1) == -10000);
    }

    {
        vector::vector_t source{resource, wider};
        source.set_value(0, static_cast<int64_t>(123450));
        source.set_value(1, static_cast<int64_t>(-123450));
        source.set_value(2, static_cast<int64_t>(123440));
        vector::vector_t result{resource, narrow};
        REQUIRE_FALSE(down->fn.invoke(cast_kind::cast, source, &result, context, 3).contains_error());
        REQUIRE(result.get_value<int64_t>(0) == 1235);
        REQUIRE(result.get_value<int64_t>(1) == -1235);
        REQUIRE(result.get_value<int64_t>(2) == 1234);
    }

    {
        const complex_logical_type tiny = make_decimal(6, 2);
        const cast_entry* to_tiny = registry.find(narrow, tiny);
        REQUIRE(to_tiny != nullptr);

        vector::vector_t source{resource, narrow};
        source.set_value(0, static_cast<int64_t>(123));
        source.set_value(1, static_cast<int64_t>(10000000));
        const auto infinity =
            static_cast<int64_t>(types::decimal_special::positive_infinity(types::physical_type::INT64));
        source.set_value(2, infinity);

        vector::vector_t cast_result{resource, tiny};
        REQUIRE(to_tiny->fn.invoke(cast_kind::cast, source, &cast_result, context, 3).contains_error());

        vector::vector_t try_result{resource, tiny};
        REQUIRE_FALSE(to_tiny->fn.invoke(cast_kind::try_cast, source, &try_result, context, 3).contains_error());
        REQUIRE(try_result.get_value<int32_t>(0) == 123);
        REQUIRE(try_result.is_null(1));
        REQUIRE(try_result.get_value<int32_t>(2) == static_cast<int32_t>(types::decimal_special::positive_infinity(
                                                        types::physical_type::INT32)));
    }
}

TEST_CASE("default casts: two DECIMALs promote to their deduced supertype") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    auto decimal_common = [&](uint8_t lw, uint8_t ls, uint8_t rw, uint8_t rs) {
        return common(registry,
                      make_decimal(lw, ls),
                      make_decimal(rw, rs));
    };

    // Two decimals promote to integer-digits=max, scale=max: dec(10,4)+dec(12,2) -> dec(14,4).
    {
        auto meet = decimal_common(10, 4, 12, 2);
        REQUIRE(meet.has_value());
        const auto* extension = meet->type.extension_as<types::decimal_logical_type_extension>();
        REQUIRE(extension != nullptr);
        REQUIRE(extension->width() == 14);
        REQUIRE(extension->scale() == 4);
        auto swapped = decimal_common(12, 2, 10, 4);
        REQUIRE(swapped.has_value());
        REQUIRE(swapped->type.extension_as<types::decimal_logical_type_extension>()->width() == 14);
        REQUIRE(swapped->type.extension_as<types::decimal_logical_type_extension>()->scale() == 4);
    }

    {
        auto meet = decimal_common(10, 2, 12, 4);
        REQUIRE(meet.has_value());
        REQUIRE(meet->type.extension_as<types::decimal_logical_type_extension>()->width() == 12);
        REQUIRE(meet->type.extension_as<types::decimal_logical_type_extension>()->scale() == 4);
    }

    // A deduced decimal width exceeding 38 (the int128 limit) falls back to DOUBLE.
    {
        auto meet = decimal_common(38, 0, 20, 20);
        REQUIRE(meet.has_value());
        REQUIRE(meet->type.type() == logical_type::DOUBLE);
    }
}

TEST_CASE("default casts: DECIMAL <-> string round-trips, rounds, handles specials (explicit-only)") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type decimal_type = make_decimal(10, 2);
    const cast_entry* to_string = registry.find(decimal_type, string_type);
    const cast_entry* from_string = registry.find(string_type, decimal_type);
    REQUIRE(to_string != nullptr);
    REQUIRE(from_string != nullptr);

    REQUIRE_FALSE(to_string->promotes());
    REQUIRE_FALSE(from_string->promotes());
    REQUIRE_FALSE(common(registry, decimal_type, string_type).has_value());

    REQUIRE_FALSE(to_string->fn.has_try_cast());
    REQUIRE(from_string->fn.has_try_cast());

    graph_execution_context context{};

    {
        vector::vector_t source{resource, decimal_type};
        source.set_value(0, static_cast<int64_t>(12345));
        source.set_value(1, static_cast<int64_t>(-100));
        source.set_value(2, static_cast<int64_t>(5));
        const auto infinity =
            static_cast<int64_t>(types::decimal_special::positive_infinity(types::physical_type::INT64));
        source.set_value(3, infinity);
        vector::vector_t result{resource, string_type};
        REQUIRE_FALSE(to_string->fn.invoke(cast_kind::cast, source, &result, context, 4).contains_error());
        REQUIRE(result.get_value<std::string_view>(0) == "123.45");
        REQUIRE(result.get_value<std::string_view>(1) == "-1.00");
        REQUIRE(result.get_value<std::string_view>(2) == "0.05");
        REQUIRE(result.get_value<std::string_view>(3) == "Infinity");
    }

    {
        vector::vector_t source{resource, string_type};
        source.set_value(0, std::string_view{" 123.45 "});
        source.set_value(1, std::string_view{"1.235"});
        source.set_value(2, std::string_view{"-0.5"});
        source.set_value(3, std::string_view{"abc"});
        source.set_value(4, std::string_view{"100000000"});
        source.set_value(5, std::string_view{"NaN"});

        vector::vector_t cast_result{resource, decimal_type};
        REQUIRE(from_string->fn.invoke(cast_kind::cast, source, &cast_result, context, 6).contains_error());

        vector::vector_t try_result{resource, decimal_type};
        REQUIRE_FALSE(from_string->fn.invoke(cast_kind::try_cast, source, &try_result, context, 6).contains_error());
        REQUIRE(try_result.get_value<int64_t>(0) == 12345);
        REQUIRE(try_result.get_value<int64_t>(1) == 124);
        REQUIRE(try_result.get_value<int64_t>(2) == -50);
        REQUIRE(try_result.is_null(3));
        REQUIRE(try_result.is_null(4));
        REQUIRE(try_result.get_value<int64_t>(5) == static_cast<int64_t>(types::decimal_special::not_a_number(
                                                        types::physical_type::INT64)));
    }

    {
        vector::vector_t source{resource, decimal_type};
        source.set_value(0, static_cast<int64_t>(-987654));
        vector::vector_t text{resource, string_type};
        REQUIRE_FALSE(to_string->fn.invoke(cast_kind::cast, source, &text, context, 1).contains_error());
        REQUIRE(text.get_value<std::string_view>(0) == "-9876.54");
        vector::vector_t back{resource, decimal_type};
        REQUIRE_FALSE(from_string->fn.invoke(cast_kind::cast, text, &back, context, 1).contains_error());
        REQUIRE(back.get_value<int64_t>(0) == -987654);
    }
}

TEST_CASE("default casts: string conversions are explicit-only and non-throwing") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const cast_entry* to_string = registry.find(integer_type, string_type);
    const cast_entry* from_string = registry.find(string_type, integer_type);
    REQUIRE(to_string != nullptr);
    REQUIRE(from_string != nullptr);

    REQUIRE_FALSE(to_string->promotes());
    REQUIRE_FALSE(from_string->promotes());
    REQUIRE_FALSE(registry.find_best_common_type(integer_type, string_type).has_value());

    REQUIRE_FALSE(to_string->fn.has_try_cast());
    REQUIRE(from_string->fn.has_try_cast());

    graph_execution_context params{};

    {
        vector::vector_t source{resource, integer_type};
        source.set_value(0, static_cast<int32_t>(-42));
        source.set_value(1, static_cast<int32_t>(0));
        vector::vector_t result{resource, string_type};
        REQUIRE_FALSE(to_string->fn.invoke(cast_kind::cast, source, &result, params, 2).contains_error());
        REQUIRE(result.get_value<std::string_view>(0) == "-42");
        REQUIRE(result.get_value<std::string_view>(1) == "0");
    }

    {
        vector::vector_t source{resource, string_type};
        source.set_value(0, std::string_view{"  +42 "});
        source.set_value(1, std::string_view{"abc"});
        source.set_value(2, std::string_view{"99999999999"});

        vector::vector_t cast_result{resource, integer_type};
        core::error_t error = from_string->fn.invoke(cast_kind::cast, source, &cast_result, params, 3);
        REQUIRE(error.contains_error());
        REQUIRE(error.type == core::error_code_t::conversion_failure);

        vector::vector_t try_result{resource, integer_type};
        REQUIRE_FALSE(from_string->fn.invoke(cast_kind::try_cast, source, &try_result, params, 3).contains_error());
        REQUIRE(try_result.get_value<int32_t>(0) == 42);
        REQUIRE(try_result.is_null(1));
        REQUIRE(try_result.is_null(2));
    }

    {
        const complex_logical_type hugeint{logical_type::HUGEINT};
        const types::int128_t big = types::int128_t{1} << 100;
        const std::string_view big_text{"1267650600228229401496703205376"};

        REQUIRE(registry.find(hugeint, string_type) != nullptr);
        const cast_entry* parse_huge = registry.find(string_type, hugeint);
        REQUIRE(parse_huge != nullptr);

        vector::vector_t number_to_text_source{resource, hugeint};
        number_to_text_source.set_value(0, big);
        vector::vector_t text_result{resource, string_type};
        REQUIRE_FALSE(registry.find(hugeint, string_type)
                          ->fn.invoke(cast_kind::cast, number_to_text_source, &text_result, params, 1)
                          .contains_error());
        REQUIRE(text_result.get_value<std::string_view>(0) == big_text);

        vector::vector_t text_source{resource, string_type};
        text_source.set_value(0, big_text);
        vector::vector_t number_result{resource, hugeint};
        REQUIRE_FALSE(parse_huge->fn.invoke(cast_kind::cast, text_source, &number_result, params, 1).contains_error());
        REQUIRE(number_result.get_value<types::int128_t>(0) == big);
    }
}

TEST_CASE("default casts: date/time conversions, string parse/format, and to_string") {
    namespace cd = core::date;
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type date_type{logical_type::DATE};
    const complex_logical_type time_type{logical_type::TIME};
    const complex_logical_type timestamp_type{logical_type::TIMESTAMP};
    const complex_logical_type timestamptz_type{logical_type::TIMESTAMP_TZ};

    // type<->type is implicit, string<->datetime is explicit-only; DATE widens to TIMESTAMP as their common type.
    {
        const cast_entry* date_to_ts = registry.find(date_type, timestamp_type);
        const cast_entry* str_to_date = registry.find(string_type, date_type);
        REQUIRE(date_to_ts != nullptr);
        REQUIRE(str_to_date != nullptr);
        REQUIRE(date_to_ts->promotes());
        REQUIRE_FALSE(str_to_date->promotes());
        auto meet = common(registry, date_type, timestamp_type);
        REQUIRE(meet.has_value());
        REQUIRE(meet->type.type() == logical_type::TIMESTAMP);
    }

    graph_execution_context context{};

    {
        const cast_entry* parse = registry.find(string_type, timestamp_type);
        const cast_entry* format = registry.find(timestamp_type, string_type);
        REQUIRE(parse->fn.has_try_cast());
        REQUIRE_FALSE(format->fn.has_try_cast());

        vector::vector_t source{resource, string_type};
        source.set_value(0, std::string_view{"2024-03-15 12:30:00"});
        source.set_value(1, std::string_view{"not a date"});
        vector::vector_t parsed{resource, timestamp_type};
        REQUIRE(parse->fn.invoke(cast_kind::cast, source, &parsed, context, 2).contains_error());
        vector::vector_t parsed_try{resource, timestamp_type};
        REQUIRE_FALSE(parse->fn.invoke(cast_kind::try_cast, source, &parsed_try, context, 2).contains_error());
        REQUIRE(parsed_try.is_null(1));

        vector::vector_t text{resource, string_type};
        REQUIRE_FALSE(format->fn.invoke(cast_kind::cast, parsed_try, &text, context, 1).contains_error());
        REQUIRE(text.get_value<std::string_view>(0) == "2024-03-15 12:30:00");
    }

    {
        vector::vector_t dates{resource, date_type};
        dates.set_value(0, *cd::parse_date("2024-03-15"));
        vector::vector_t as_ts{resource, timestamp_type};
        REQUIRE_FALSE(registry.find(date_type, timestamp_type)
                          ->fn.invoke(cast_kind::cast, dates, &as_ts, context, 1)
                          .contains_error());
        vector::vector_t back{resource, string_type};
        REQUIRE_FALSE(registry.find(timestamp_type, string_type)
                          ->fn.invoke(cast_kind::cast, as_ts, &back, context, 1)
                          .contains_error());
        REQUIRE(back.get_value<std::string_view>(0) == "2024-03-15 00:00:00");

        vector::vector_t stamps{resource, timestamp_type};
        stamps.set_value(0, *cd::parse_timestamp("2024-03-15 12:30:45"));
        vector::vector_t only_date{resource, date_type};
        REQUIRE_FALSE(registry.find(timestamp_type, date_type)
                          ->fn.invoke(cast_kind::cast, stamps, &only_date, context, 1)
                          .contains_error());
        REQUIRE(only_date.get_value<cd::date_t>(0) == *cd::parse_date("2024-03-15"));
    }

    {
        graph_execution_context offset_context{cd::timezone_offset_t{3600}};
        vector::vector_t local{resource, timestamp_type};
        local.set_value(0, *cd::parse_timestamp("2024-03-15 12:30:00"));
        vector::vector_t utc{resource, timestamptz_type};
        REQUIRE_FALSE(registry.find(timestamp_type, timestamptz_type)
                          ->fn.invoke(cast_kind::cast, local, &utc, offset_context, 1)
                          .contains_error());
        vector::vector_t text{resource, string_type};
        REQUIRE_FALSE(registry.find(timestamptz_type, string_type)
                          ->fn.invoke(cast_kind::cast, utc, &text, context, 1)
                          .contains_error());
        REQUIRE(text.get_value<std::string_view>(0) == "2024-03-15 11:30:00+00");
    }

    // TIME_TZ and INTERVAL are STRUCT-physical; set_value writes their child fields directly.
    const complex_logical_type timetz_type{logical_type::TIME_TZ};
    const complex_logical_type interval_type{logical_type::INTERVAL};
    {
        vector::vector_t text{resource, string_type};
        text.set_value(0, std::string_view{"13:45:06+05:30"});
        vector::vector_t tz{resource, timetz_type};
        REQUIRE_FALSE(registry.find(string_type, timetz_type)
                          ->fn.invoke(cast_kind::cast, text, &tz, context, 1)
                          .contains_error());
        REQUIRE(tz.get_value<cd::timetz_t>(0) == *cd::parse_timetz("13:45:06+05:30"));
        vector::vector_t back{resource, string_type};
        REQUIRE_FALSE(registry.find(timetz_type, string_type)
                          ->fn.invoke(cast_kind::cast, tz, &back, context, 1)
                          .contains_error());
        REQUIRE(back.get_value<std::string_view>(0) == "13:45:06+05:30");
    }
    {
        vector::vector_t text{resource, string_type};
        text.set_value(0, std::string_view{"2 years 3 months 10 days"});
        vector::vector_t interval{resource, interval_type};
        REQUIRE_FALSE(registry.find(string_type, interval_type)
                          ->fn.invoke(cast_kind::cast, text, &interval, context, 1)
                          .contains_error());
        vector::vector_t back{resource, string_type};
        REQUIRE_FALSE(registry.find(interval_type, string_type)
                          ->fn.invoke(cast_kind::cast, interval, &back, context, 1)
                          .contains_error());
        REQUIRE(back.get_value<std::string_view>(0) == "2 years 3 mons 10 days");
    }
    {
        graph_execution_context zone_context{cd::timezone_offset_t{19800}};
        vector::vector_t times{resource, time_type};
        times.set_value(0, *cd::parse_time("13:45:06"));
        vector::vector_t tz{resource, timetz_type};
        REQUIRE_FALSE(registry.find(time_type, timetz_type)
                          ->fn.invoke(cast_kind::cast, times, &tz, zone_context, 1)
                          .contains_error());
        REQUIRE(tz.get_value<cd::timetz_t>(0) == *cd::parse_timetz("13:45:06+05:30"));
    }
    REQUIRE(cd::to_string(*cd::parse_time("13:45:06.5")) == "13:45:06.5");
    REQUIRE(cd::to_string(*cd::parse_interval("04:30:00")) == "04:30:00");
}

TEST_CASE("composite_cast: build_cast composes STRUCT and ARRAY towers over registered leaf casts") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };
    graph_execution_context context{};

    // build_cast auto-derives an anonymous struct cast; a NAMED struct is indivisible and must be declared.
    {
        std::pmr::vector<complex_logical_type> float_fields{
            {field(logical_type::FLOAT, "x"), field(logical_type::FLOAT, "y")},
            resource};
        std::pmr::vector<complex_logical_type> int_fields{
            {field(logical_type::INTEGER, "x"), field(logical_type::INTEGER, "y")},
            resource};
        const complex_logical_type vec2f = complex_logical_type::create_struct("", float_fields);
        const complex_logical_type vec2i = complex_logical_type::create_struct("", int_fields);

        auto composite = registry.resolve(vec2f, vec2i, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, vec2f};
        source.entries()[0]->set_value(0, 1.9f);
        source.entries()[1]->set_value(0, 3.5f);
        vector::vector_t result{resource, vec2i};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.entries()[0]->get_value<int32_t>(0) == 2);
        REQUIRE(result.entries()[1]->get_value<int32_t>(0) == 4);
    }

    // build_cast wraps a leaf cast in array levels; execution casts the flat leaf buffer once.
    {
        const complex_logical_type inner_float =
            complex_logical_type::create_array(complex_logical_type{logical_type::FLOAT}, 2);
        const complex_logical_type tower_float = complex_logical_type::create_array(inner_float, 2);
        const complex_logical_type inner_int =
            complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 2);
        const complex_logical_type tower_int = complex_logical_type::create_array(inner_int, 2);

        auto composite = registry.resolve(tower_float, tower_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, tower_float};
        vector::vector_t& leaf_source = source.entry().entry();
        const float inputs[4] = {1.4f, 2.5f, -1.5f, 10.9f};
        for (uint64_t index = 0; index < 4; ++index) {
            leaf_source.set_value(index, inputs[index]);
        }
        vector::vector_t result{resource, tower_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        const vector::vector_t& leaf_result = result.entry().entry();
        REQUIRE(leaf_result.get_value<int32_t>(0) == 1);
        REQUIRE(leaf_result.get_value<int32_t>(1) == 2);
        REQUIRE(leaf_result.get_value<int32_t>(2) == -2);
        REQUIRE(leaf_result.get_value<int32_t>(3) == 11);
    }

    {
        std::pmr::vector<complex_logical_type> float_fields{
            {field(logical_type::FLOAT, "x"), field(logical_type::FLOAT, "y")},
            resource};
        std::pmr::vector<complex_logical_type> int_fields{
            {field(logical_type::INTEGER, "x"), field(logical_type::INTEGER, "y")},
            resource};
        const complex_logical_type array_struct_float =
            complex_logical_type::create_array(complex_logical_type::create_struct("", float_fields), 2);
        const complex_logical_type array_struct_int =
            complex_logical_type::create_array(complex_logical_type::create_struct("", int_fields), 2);

        auto composite = registry.resolve(array_struct_float, array_struct_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, array_struct_float};
        vector::vector_t& struct_source = source.entry();
        struct_source.entries()[0]->set_value(0, 0.4f);
        struct_source.entries()[0]->set_value(1, 1.6f);
        struct_source.entries()[1]->set_value(0, 2.5f);
        struct_source.entries()[1]->set_value(1, 9.9f);
        vector::vector_t result{resource, array_struct_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        const vector::vector_t& struct_result = result.entry();
        REQUIRE(struct_result.entries()[0]->get_value<int32_t>(0) == 0);
        REQUIRE(struct_result.entries()[0]->get_value<int32_t>(1) == 2);
        REQUIRE(struct_result.entries()[1]->get_value<int32_t>(0) == 2);
        REQUIRE(struct_result.entries()[1]->get_value<int32_t>(1) == 10);
    }

    // A LIST's per-row (offset, length) structure is copied verbatim; only the child block is cast.
    {
        const complex_logical_type list_float =
            complex_logical_type::create_list(complex_logical_type{logical_type::FLOAT});
        const complex_logical_type list_int =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});

        auto composite = registry.resolve(list_float, list_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, list_float};
        source.set_value(0, std::pmr::vector<float>{{1.4f, 2.5f, -1.5f}, resource});
        source.set_value(1, std::pmr::vector<float>{{10.9f}, resource});
        vector::vector_t result{resource, list_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const std::pmr::vector<int32_t> row0 = result.get_value<std::pmr::vector<int32_t>>(0);
        REQUIRE(row0.size() == 3);
        REQUIRE(row0[0] == 1);
        REQUIRE(row0[1] == 2);
        REQUIRE(row0[2] == -2);
        const std::pmr::vector<int32_t> row1 = result.get_value<std::pmr::vector<int32_t>>(1);
        REQUIRE(row1.size() == 1);
        REQUIRE(row1[0] == 11);
    }

    {
        const complex_logical_type list_list_float = complex_logical_type::create_list(
            complex_logical_type::create_list(complex_logical_type{logical_type::FLOAT}));
        const complex_logical_type list_list_int = complex_logical_type::create_list(
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}));

        auto composite = registry.resolve(list_list_float, list_list_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, list_list_float};
        std::pmr::vector<std::pmr::vector<float>> row0{resource};
        row0.push_back(std::pmr::vector<float>{{1.4f, 2.5f}, resource});
        row0.push_back(std::pmr::vector<float>{{-1.5f}, resource});
        source.set_value(0, std::move(row0));
        std::pmr::vector<std::pmr::vector<float>> row1{resource};
        row1.push_back(std::pmr::vector<float>{{10.9f}, resource});
        source.set_value(1, std::move(row1));

        vector::vector_t result{resource, list_list_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const auto out0 = result.get_value<std::pmr::vector<std::pmr::vector<int32_t>>>(0);
        REQUIRE(out0.size() == 2);
        REQUIRE(out0[0].size() == 2);
        REQUIRE(out0[0][0] == 1);
        REQUIRE(out0[0][1] == 2);
        REQUIRE(out0[1].size() == 1);
        REQUIRE(out0[1][0] == -2);
        const auto out1 = result.get_value<std::pmr::vector<std::pmr::vector<int32_t>>>(1);
        REQUIRE(out1.size() == 1);
        REQUIRE(out1[0].size() == 1);
        REQUIRE(out1[0][0] == 11);
    }

    {
        const complex_logical_type list_i32 =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
        const complex_logical_type list_i64 =
            complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT});

        auto composite = registry.resolve(list_i32, list_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, list_i32};
        source.set_value(0,
                         std::pmr::vector<int32_t>{{1, -2, 2147483647}, resource});
        source.set_value(1, std::pmr::vector<int32_t>{{-2147483648}, resource});
        vector::vector_t result{resource, list_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const std::pmr::vector<int64_t> row0 = result.get_value<std::pmr::vector<int64_t>>(0);
        REQUIRE(row0.size() == 3);
        REQUIRE(row0[0] == 1);
        REQUIRE(row0[1] == -2);
        REQUIRE(row0[2] == 2147483647LL);
        const std::pmr::vector<int64_t> row1 = result.get_value<std::pmr::vector<int64_t>>(1);
        REQUIRE(row1.size() == 1);
        REQUIRE(row1[0] == -2147483648LL);
    }

    {
        const complex_logical_type list_array_i32 = complex_logical_type::create_list(
            complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 2));
        const complex_logical_type list_array_i64 = complex_logical_type::create_list(
            complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, 2));

        auto composite = registry.resolve(list_array_i32, list_array_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, list_array_i32};
        std::pmr::vector<std::pmr::vector<int32_t>> row0{resource};
        row0.push_back(std::pmr::vector<int32_t>{{1, 2}, resource});
        row0.push_back(std::pmr::vector<int32_t>{{3, 4}, resource});
        source.set_value(0, std::move(row0));
        std::pmr::vector<std::pmr::vector<int32_t>> row1{resource};
        row1.push_back(std::pmr::vector<int32_t>{{2147483647, -2147483648}, resource});
        source.set_value(1, std::move(row1));

        vector::vector_t result{resource, list_array_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const auto out0 = result.get_value<std::pmr::vector<std::pmr::vector<int64_t>>>(0);
        REQUIRE(out0.size() == 2);
        REQUIRE(out0[0] == std::pmr::vector<int64_t>{{1, 2}, resource});
        REQUIRE(out0[1] == std::pmr::vector<int64_t>{{3, 4}, resource});
        const auto out1 = result.get_value<std::pmr::vector<std::pmr::vector<int64_t>>>(1);
        REQUIRE(out1.size() == 1);
        REQUIRE(out1[0] == std::pmr::vector<int64_t>{{2147483647LL, -2147483648LL}, resource});
    }

    {
        const complex_logical_type array_list_i32 = complex_logical_type::create_array(
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER}),
            2);
        const complex_logical_type array_list_i64 = complex_logical_type::create_array(
            complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT}),
            2);

        auto composite = registry.resolve(array_list_i32, array_list_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, array_list_i32};
        vector::vector_t& list_source = source.entry();
        list_source.set_value(0,
                              std::pmr::vector<int32_t>{{10, 20, 30}, resource});
        list_source.set_value(1, std::pmr::vector<int32_t>{{40}, resource});

        vector::vector_t result{resource, array_list_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());

        const auto out = result.get_value<std::pmr::vector<std::pmr::vector<int64_t>>>(0);
        REQUIRE(out.size() == 2);
        REQUIRE(out[0] == std::pmr::vector<int64_t>{{10, 20, 30}, resource});
        REQUIRE(out[1] == std::pmr::vector<int64_t>{{40}, resource});
    }

    // MAP is physically a LIST of struct<key,value>, so build_cast composes list_cast over it.
    {
        const complex_logical_type map_i32_f32 =
            complex_logical_type::create_map(resource,
                                             complex_logical_type{logical_type::INTEGER},
                                             complex_logical_type{logical_type::FLOAT});
        const complex_logical_type map_i64_f64 =
            complex_logical_type::create_map(resource,
                                             complex_logical_type{logical_type::BIGINT},
                                             complex_logical_type{logical_type::DOUBLE});

        auto composite = registry.resolve(map_i32_f32, map_i64_f64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, map_i32_f32};
        source.reserve(3);
        vector::vector_t& source_struct = source.entry();
        vector::vector_t& source_keys = *source_struct.entries()[0];
        vector::vector_t& source_values = *source_struct.entries()[1];
        const int32_t keys[3] = {10, 20, 30};
        const float values[3] = {1.5f, 2.5f, 3.5f};
        for (uint64_t pair = 0; pair < 3; ++pair) {
            source_keys.set_value(pair, keys[pair]);
            source_values.set_value(pair, values[pair]);
        }
        source.set_list_size(3);
        source.data<types::list_entry_t>()[0] = types::list_entry_t{0, 2};
        source.data<types::list_entry_t>()[1] = types::list_entry_t{2, 1};

        vector::vector_t result{resource, map_i64_f64};
        result.reserve(3);
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const types::list_entry_t* result_entries = result.data<types::list_entry_t>();
        REQUIRE(result_entries[0].offset == 0);
        REQUIRE(result_entries[0].length == 2);
        REQUIRE(result_entries[1].offset == 2);
        REQUIRE(result_entries[1].length == 1);
        const vector::vector_t& result_struct = result.entry();
        const vector::vector_t& result_keys = *result_struct.entries()[0];
        const vector::vector_t& result_values = *result_struct.entries()[1];
        for (uint64_t pair = 0; pair < 3; ++pair) {
            REQUIRE(result_keys.get_value<int64_t>(pair) == static_cast<int64_t>(keys[pair]));
            REQUIRE(result_values.get_value<double>(pair) == Catch::Approx(static_cast<double>(values[pair])));
        }
    }
}

// A null composite ROW is nullity of the whole container, separate from element-level nulls.
TEST_CASE("composite_cast: null container rows propagate validity") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };

    {
        std::pmr::vector<complex_logical_type> float_fields{
            {field(logical_type::FLOAT, "x"), field(logical_type::FLOAT, "y")},
            resource};
        std::pmr::vector<complex_logical_type> int_fields{
            {field(logical_type::INTEGER, "x"), field(logical_type::INTEGER, "y")},
            resource};
        const complex_logical_type vec2f = complex_logical_type::create_struct("", float_fields);
        const complex_logical_type vec2i = complex_logical_type::create_struct("", int_fields);

        auto composite = registry.resolve(vec2f, vec2i, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, vec2f};
        source.entries()[0]->set_value(0, 1.9f);
        source.entries()[1]->set_value(0, 3.5f);
        source.set_null(1, true);
        vector::vector_t result{resource, vec2i};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        REQUIRE_FALSE(result.is_null(0));
        REQUIRE(result.entries()[0]->get_value<int32_t>(0) == 2);
        REQUIRE(result.is_null(1));
    }

    {
        const complex_logical_type list_float =
            complex_logical_type::create_list(complex_logical_type{logical_type::FLOAT});
        const complex_logical_type list_int =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});

        auto composite = registry.resolve(list_float, list_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, list_float};
        source.set_value(0, std::pmr::vector<float>{{1.4f, 2.5f}, resource});
        source.set_value(1, std::pmr::vector<float>{{9.9f}, resource});
        source.set_null(1, true);
        vector::vector_t result{resource, list_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        REQUIRE_FALSE(result.is_null(0));
        REQUIRE(result.get_value<std::pmr::vector<int32_t>>(0) == std::pmr::vector<int32_t>{{1, 2}, resource});
        REQUIRE(result.is_null(1));
    }

    {
        const complex_logical_type array_float =
            complex_logical_type::create_array(complex_logical_type{logical_type::FLOAT}, 2);
        const complex_logical_type array_int =
            complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 2);

        auto composite = registry.resolve(array_float, array_int, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, array_float};
        vector::vector_t& leaf = source.entry();
        const float inputs[4] = {1.4f, 2.5f, 3.6f, 4.4f};
        for (uint64_t index = 0; index < 4; ++index) {
            leaf.set_value(index, inputs[index]);
        }
        source.set_null(1, true);
        vector::vector_t result{resource, array_int};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        REQUIRE_FALSE(result.is_null(0));
        REQUIRE(result.entry().get_value<int32_t>(0) == 1);
        REQUIRE(result.is_null(1));
    }

    {
        const complex_logical_type map_i32_f32 =
            complex_logical_type::create_map(resource,
                                             complex_logical_type{logical_type::INTEGER},
                                             complex_logical_type{logical_type::FLOAT});
        const complex_logical_type map_i64_f64 =
            complex_logical_type::create_map(resource,
                                             complex_logical_type{logical_type::BIGINT},
                                             complex_logical_type{logical_type::DOUBLE});

        auto composite = registry.resolve(map_i32_f32, map_i64_f64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, map_i32_f32};
        source.reserve(2);
        vector::vector_t& source_struct = source.entry();
        source_struct.entries()[0]->set_value(0, int32_t{10});
        source_struct.entries()[1]->set_value(0, 1.5f);
        source.set_list_size(1);
        source.data<types::list_entry_t>()[0] = types::list_entry_t{0, 1};
        source.data<types::list_entry_t>()[1] = types::list_entry_t{1, 0};
        source.set_null(1, true);
        vector::vector_t result{resource, map_i64_f64};
        result.reserve(2);
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        REQUIRE_FALSE(result.is_null(0));
        REQUIRE(result.entry().entries()[0]->get_value<int64_t>(0) == 10);
        REQUIRE(result.is_null(1));
    }
}

// LIST and ARRAY are interchangeable containers, so build_cast composes any combination, nested arbitrarily deep.
TEST_CASE("composite_cast: cross-kind LIST/ARRAY towers deeper than two levels") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    using ints3 = std::pmr::vector<std::pmr::vector<std::pmr::vector<int32_t>>>;
    using longs3 = std::pmr::vector<std::pmr::vector<std::pmr::vector<int64_t>>>;

    const int64_t values[8] = {1, -2, 2147483647, -2147483648, 100, 200, 300, 400};
    auto tower32 = [&]() {
        ints3 outer{resource};
        for (int i = 0; i < 2; ++i) {
            std::pmr::vector<std::pmr::vector<int32_t>> mid{resource};
            for (int j = 0; j < 2; ++j) {
                std::pmr::vector<int32_t> inner{resource};
                for (int k = 0; k < 2; ++k) {
                    inner.push_back(static_cast<int32_t>(values[(i * 2 + j) * 2 + k]));
                }
                mid.push_back(std::move(inner));
            }
            outer.push_back(std::move(mid));
        }
        return outer;
    };
    auto tower64 = [&]() {
        longs3 outer{resource};
        for (int i = 0; i < 2; ++i) {
            std::pmr::vector<std::pmr::vector<int64_t>> mid{resource};
            for (int j = 0; j < 2; ++j) {
                std::pmr::vector<int64_t> inner{resource};
                for (int k = 0; k < 2; ++k) {
                    inner.push_back(values[(i * 2 + j) * 2 + k]);
                }
                mid.push_back(std::move(inner));
            }
            outer.push_back(std::move(mid));
        }
        return outer;
    };

    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};
    const complex_logical_type array3_i32 = complex_logical_type::create_array(
        complex_logical_type::create_array(complex_logical_type::create_array(i32, 2), 2),
        2);
    const complex_logical_type array3_i64 = complex_logical_type::create_array(
        complex_logical_type::create_array(complex_logical_type::create_array(i64, 2), 2),
        2);
    const complex_logical_type list3_i32 =
        complex_logical_type::create_list(complex_logical_type::create_list(complex_logical_type::create_list(i32)));
    const complex_logical_type list3_i64 =
        complex_logical_type::create_list(complex_logical_type::create_list(complex_logical_type::create_list(i64)));

    {
        auto composite = registry.resolve(array3_i32, list3_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());
        vector::vector_t source{resource, array3_i32};
        source.set_value(0, tower32());
        vector::vector_t result{resource, list3_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.get_value<longs3>(0) == tower64());
    }

    {
        auto composite = registry.resolve(list3_i32, array3_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());
        vector::vector_t source{resource, list3_i32};
        source.set_value(0, tower32());
        vector::vector_t result{resource, array3_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.get_value<longs3>(0) == tower64());
    }

    {
        const complex_logical_type array_list_array_i32 = complex_logical_type::create_array(
            complex_logical_type::create_list(complex_logical_type::create_array(i32, 2)),
            2);
        const complex_logical_type list_array_list_i64 = complex_logical_type::create_list(
            complex_logical_type::create_array(complex_logical_type::create_list(i64), 2));

        auto composite = registry.resolve(array_list_array_i32, list_array_list_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());
        vector::vector_t source{resource, array_list_array_i32};
        source.set_value(0, tower32());
        vector::vector_t result{resource, list_array_list_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.get_value<longs3>(0) == tower64());
    }

    {
        auto composite = registry.resolve(list3_i32, array3_i64, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        std::pmr::vector<std::pmr::vector<int32_t>> mid0{resource};
        mid0.push_back(std::pmr::vector<int32_t>{{1, 2}, resource});
        mid0.push_back(std::pmr::vector<int32_t>{{3, 4}, resource});
        std::pmr::vector<std::pmr::vector<int32_t>> mid1{resource};
        mid1.push_back(std::pmr::vector<int32_t>{{5, 6, 7}, resource});
        mid1.push_back(std::pmr::vector<int32_t>{{8, 9}, resource});
        ints3 row0{resource};
        row0.push_back(std::move(mid0));
        row0.push_back(std::move(mid1));

        vector::vector_t source{resource, list3_i32};
        source.set_value(0, row0);
        vector::vector_t result_cast{resource, array3_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result_cast, context, 1).contains_error());

        vector::vector_t result_try{resource, array3_i64};
        REQUIRE_FALSE((*composite)(cast_kind::try_cast, source, &result_try, context, 1).contains_error());
    }
}

// registry.resolve() is the single entry point: one uniform cast_t for a leaf or a composite tower.
TEST_CASE("cast_registry: resolve() is one uniform entry point for leaf and composite casts") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    {
        const complex_logical_type i32{logical_type::INTEGER};
        const complex_logical_type i64{logical_type::BIGINT};
        auto cast = registry.resolve(i32, i64, cast_type::explicit_only);
        REQUIRE(cast.has_value());
        vector::vector_t source{resource, i32};
        source.set_value(0, int32_t{-5});
        source.set_value(1, int32_t{2147483647});
        vector::vector_t result{resource, i64};
        REQUIRE_FALSE((*cast)(cast_kind::cast, source, &result, context, 2).contains_error());
        REQUIRE(result.get_value<int64_t>(0) == -5);
        REQUIRE(result.get_value<int64_t>(1) == 2147483647LL);
    }

    {
        const complex_logical_type list_i32 =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
        const complex_logical_type list_i64 =
            complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT});
        auto cast = registry.resolve(list_i32, list_i64, cast_type::explicit_only);
        REQUIRE(cast.has_value());
        vector::vector_t source{resource, list_i32};
        source.set_value(0, std::pmr::vector<int32_t>{{1, 2, 3}, resource});
        source.set_value(1, std::pmr::vector<int32_t>{{4}, resource});
        vector::vector_t result{resource, list_i64};
        REQUIRE_FALSE((*cast)(cast_kind::cast, source, &result, context, 2).contains_error());
        REQUIRE(result.get_value<std::pmr::vector<int64_t>>(0) == std::pmr::vector<int64_t>{{1, 2, 3}, resource});
        REQUIRE(result.get_value<std::pmr::vector<int64_t>>(1) == std::pmr::vector<int64_t>{{4}, resource});
    }

    {
        const complex_logical_type list_bool =
            complex_logical_type::create_list(complex_logical_type{logical_type::BOOLEAN});
        const complex_logical_type list_date =
            complex_logical_type::create_list(complex_logical_type{logical_type::DATE});
        REQUIRE_FALSE(registry.resolve(list_bool, list_date, cast_type::explicit_only).has_value());
    }
}

// A registered composite cast overrides automatic structural composition, even nested as a sub-cast.
TEST_CASE("cast_registry: a composite cast can be registered and resolve() returns it") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };
    std::pmr::vector<complex_logical_type> fields{
        {field(logical_type::INTEGER, "a"), field(logical_type::INTEGER, "b")},
        resource};
    const complex_logical_type pair_type = complex_logical_type::create_struct("pair", fields);

    cast_t custom = [](cast_kind,
                       const vector::vector_t& source,
                       vector::vector_t* result,
                       const graph_execution_context&,
                       uint64_t count) -> core::error_t {
        for (uint64_t row = 0; row < count; ++row) {
            result->entries()[0]->set_value(row, source.entries()[0]->get_value<int32_t>(row));
            result->entries()[1]->set_value(row, int32_t{999});
        }
        return core::error_t::no_error();
    };
    REQUIRE_FALSE(
        registry.add(pair_type, pair_type, complex_cast_entry{custom, cast_type::assignment}).contains_error());
    REQUIRE(registry.add(pair_type, pair_type, complex_cast_entry{custom, cast_type::assignment}).contains_error());

    {
        auto resolved = registry.resolve(pair_type, pair_type, cast_type::explicit_only);
        REQUIRE(resolved.has_value());
        vector::vector_t source{resource, pair_type};
        source.entries()[0]->set_value(0, int32_t{7});
        source.entries()[1]->set_value(0, int32_t{7});
        vector::vector_t result{resource, pair_type};
        REQUIRE_FALSE((*resolved)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.entries()[0]->get_value<int32_t>(0) == 7);
        REQUIRE(result.entries()[1]->get_value<int32_t>(0) == 999);
    }

    {
        const complex_logical_type list_pair = complex_logical_type::create_list(pair_type);
        auto nested = registry.resolve(list_pair, list_pair, cast_type::explicit_only);
        REQUIRE(nested.has_value());

        vector::vector_t source{resource, list_pair};
        source.reserve(2);
        vector::vector_t& source_pairs = source.entry();
        source_pairs.entries()[0]->set_value(0, int32_t{10});
        source_pairs.entries()[0]->set_value(1, int32_t{20});
        source_pairs.entries()[1]->set_value(0, int32_t{10});
        source_pairs.entries()[1]->set_value(1, int32_t{20});
        source.set_list_size(2);
        source.data<types::list_entry_t>()[0] = types::list_entry_t{0, 2};

        vector::vector_t result{resource, list_pair};
        result.reserve(2);
        REQUIRE_FALSE((*nested)(cast_kind::cast, source, &result, context, 1).contains_error());
        const vector::vector_t& result_pairs = result.entry();
        REQUIRE(result_pairs.entries()[0]->get_value<int32_t>(0) == 10);
        REQUIRE(result_pairs.entries()[0]->get_value<int32_t>(1) == 20);
        REQUIRE(result_pairs.entries()[1]->get_value<int32_t>(0) == 999);
        REQUIRE(result_pairs.entries()[1]->get_value<int32_t>(1) == 999);
    }
}

// add(complex_cast_entry) declares a cast's coercion level explicitly.
TEST_CASE("cast_registry: add(complex_cast_entry) stores a struct cast at its declared level") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    std::pmr::vector<complex_logical_type> fields{{complex_logical_type{logical_type::INTEGER}}, resource};
    const complex_logical_type pair_type = complex_logical_type::create_struct("pair", fields);

    cast_t custom =
        [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
            return core::error_t::no_error();
        };

    REQUIRE_FALSE(
        registry.add(pair_type, pair_type, complex_cast_entry{custom, cast_type::assignment}).contains_error());

    REQUIRE(registry.level_of(pair_type, pair_type) == std::optional<cast_type>{cast_type::assignment});
    REQUIRE(registry.resolve(pair_type, pair_type, cast_type::explicit_only).has_value());
    REQUIRE(registry.resolve(pair_type, pair_type, cast_type::assignment).has_value());
    REQUIRE_FALSE(registry.resolve(pair_type, pair_type, cast_type::implicit).has_value());

    REQUIRE_FALSE(registry.cost_of(pair_type, pair_type).has_value());

    REQUIRE(registry.add(pair_type, pair_type, complex_cast_entry{custom, cast_type::assignment}).contains_error());
}

// cost_of answers only for IMPLICIT casts; a container takes its element's cost verbatim.
TEST_CASE("cast_registry: cost_of reports the element cost for containers, nothing for non-promoting casts") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};
    const complex_logical_type f32{logical_type::FLOAT};

    const auto widen = registry.cost_of(i32, i64);
    REQUIRE(widen.has_value());
    REQUIRE(widen->precision_loss == 0);

    const auto lossy = registry.cost_of(i32, f32);
    REQUIRE(lossy.has_value());
    REQUIRE(lossy->precision_loss > widen->precision_loss);

    REQUIRE(registry.level_of(i64, i32) == std::optional<cast_type>{cast_type::assignment});
    REQUIRE_FALSE(registry.cost_of(i64, i32).has_value());

    {
        const auto list_cost =
            registry.cost_of(complex_logical_type::create_list(i32), complex_logical_type::create_list(i64));
        REQUIRE(list_cost.has_value());
        REQUIRE(list_cost->precision_loss == widen->precision_loss);
        REQUIRE(list_cost->footprint == widen->footprint);

        const auto array_cost =
            registry.cost_of(complex_logical_type::create_array(i32, 2), complex_logical_type::create_array(i64, 2));
        REQUIRE(array_cost.has_value());
        REQUIRE(array_cost->precision_loss == widen->precision_loss);
        REQUIRE(array_cost->footprint == widen->footprint);

        REQUIRE_FALSE(registry.cost_of(complex_logical_type::create_list(i64), complex_logical_type::create_list(i32))
                          .has_value());
    }

    {
        const auto identity = registry.cost_of(i32, i32);
        REQUIRE(identity.has_value());
        REQUIRE(identity->precision_loss == 0);
        REQUIRE(identity->footprint == static_cast<uint32_t>(i32.size()));
        const auto list_identity =
            registry.cost_of(complex_logical_type::create_list(i32), complex_logical_type::create_list(i32));
        REQUIRE(list_identity.has_value());
        REQUIRE(list_identity->footprint == identity->footprint);
        REQUIRE(list_identity->precision_loss == 0);
    }

    {
        auto field = [](logical_type type, const char* name) {
            complex_logical_type result{type};
            result.set_alias(name);
            return result;
        };
        std::pmr::vector<complex_logical_type> src_fields{{field(logical_type::INTEGER, "a")}, resource};
        std::pmr::vector<complex_logical_type> tgt_fields{{field(logical_type::BIGINT, "a")}, resource};
        const complex_logical_type anonymous = complex_logical_type::create_struct("", src_fields);
        const complex_logical_type named = complex_logical_type::create_struct("s", tgt_fields);
        REQUIRE(registry.level_of(anonymous, named) == std::optional<cast_type>{cast_type::assignment});
        REQUIRE_FALSE(registry.cost_of(anonymous, named).has_value());
    }

    {
        std::pmr::vector<complex_logical_type> fields{{complex_logical_type{logical_type::INTEGER}}, resource};
        const complex_logical_type pair_type = complex_logical_type::create_struct("pair", fields);
        const complex_logical_type other_type = complex_logical_type::create_struct("other", fields);
        cast_t custom =
            [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
                return core::error_t::no_error();
            };
        const cast_cost declared_cost{.precision_loss = 3, .footprint = 42};
        REQUIRE_FALSE(registry.add(pair_type, other_type, complex_cast_entry{custom, declared_cost}).contains_error());

        const auto pair_cost = registry.cost_of(pair_type, other_type);
        REQUIRE(pair_cost.has_value());
        REQUIRE(pair_cost->precision_loss == 3);
        REQUIRE(pair_cost->footprint == 42);
        REQUIRE(registry.level_of(pair_type, other_type) == std::optional<cast_type>{cast_type::implicit});

        const auto list_pair_cost = registry.cost_of(complex_logical_type::create_list(pair_type),
                                                     complex_logical_type::create_list(other_type));
        REQUIRE(list_pair_cost.has_value());
        REQUIRE(list_pair_cost->footprint == pair_cost->footprint);
    }
}

// level_of: a container passes its element's level through verbatim; a STRUCT's level is declared.
TEST_CASE("cast_registry: level_of passes containers through and takes structs as declared") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    using level = cast_type;
    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};
    const complex_logical_type str{logical_type::STRING_LITERAL};

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };
    auto make_struct = [&](const char* name, logical_type a) {
        std::pmr::vector<complex_logical_type> fields{{field(a, "x")}, resource};
        return complex_logical_type::create_struct(name, fields);
    };

    REQUIRE(registry.level_of(i32, i64) == std::optional<level>{level::implicit});
    REQUIRE(registry.level_of(i64, i32) == std::optional<level>{level::assignment});
    REQUIRE(registry.level_of(i32, str) == std::optional<level>{level::assignment});
    REQUIRE(registry.level_of(str, i32) == std::optional<level>{level::explicit_only});

    REQUIRE(registry.level_of(complex_logical_type::create_list(i32), complex_logical_type::create_list(i64)) ==
            std::optional<level>{level::implicit});
    REQUIRE(registry.level_of(complex_logical_type::create_list(i32), complex_logical_type::create_list(str)) ==
            std::optional<level>{level::assignment});
    REQUIRE(registry.level_of(complex_logical_type::create_array(i32, 2), complex_logical_type::create_array(i64, 2)) ==
            std::optional<level>{level::implicit});

    {
        const complex_logical_type map_i32_i32 = complex_logical_type::create_map(resource, i32, i32);
        const complex_logical_type map_i64_i64 = complex_logical_type::create_map(resource, i64, i64);
        const complex_logical_type map_str_i64 = complex_logical_type::create_map(resource, str, i64);
        REQUIRE(registry.level_of(map_i32_i32, map_i64_i64) == std::optional<level>{level::implicit});
        REQUIRE(registry.level_of(map_i32_i32, map_str_i64) ==
                std::optional<level>{level::assignment});
    }

    {
        const complex_logical_type struct_i32 = make_struct("s", logical_type::INTEGER);
        const complex_logical_type struct_i64 = make_struct("s", logical_type::BIGINT);
        REQUIRE_FALSE(registry.level_of(struct_i32, struct_i64).has_value());

        cast_t noop =
            [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
                return core::error_t::no_error();
            };
        REQUIRE_FALSE(
            registry
                .add(struct_i32, struct_i64, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 8}})
                .contains_error());
        REQUIRE(registry.level_of(struct_i32, struct_i64) == std::optional<level>{level::implicit});
        REQUIRE(registry.level_of(complex_logical_type::create_list(struct_i32),
                                  complex_logical_type::create_list(struct_i64)) ==
                std::optional<level>{level::implicit});
    }

    {
        const complex_logical_type struct_a = make_struct("t", logical_type::INTEGER);
        const complex_logical_type struct_b = make_struct("t", logical_type::BIGINT);
        cast_t noop =
            [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
                return core::error_t::no_error();
            };
        REQUIRE_FALSE(
            registry.add(struct_a, struct_b, complex_cast_entry{noop, level::explicit_only}).contains_error());
        REQUIRE(registry.level_of(struct_a, struct_b) == std::optional<level>{level::explicit_only});
    }

    {
        std::pmr::vector<complex_logical_type> row{{field(logical_type::INTEGER, "x")}, resource};
        const complex_logical_type anonymous = complex_logical_type::create_struct("", row);
        REQUIRE(registry.level_of(anonymous, make_struct("u", logical_type::BIGINT)) ==
                std::optional<level>{level::assignment});
    }

    REQUIRE(registry.level_of(i32, i32) == std::optional<level>{level::implicit});
    REQUIRE_FALSE(registry
                      .level_of(complex_logical_type::create_list(complex_logical_type{logical_type::BOOLEAN}),
                                complex_logical_type::create_list(complex_logical_type{logical_type::DATE}))
                      .has_value());
}

// Reconciling LIST -> fixed ARRAY pads a short row and truncates a long one; CAST errors, try_cast salvages.
TEST_CASE("composite_cast: ragged LIST -> fixed ARRAY reconciles per row") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    const complex_logical_type list_i32 =
        complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
    const complex_logical_type array2_i64 =
        complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, 2);

    auto composite = registry.resolve(list_i32, array2_i64, cast_type::explicit_only);
    REQUIRE(composite.has_value());

    auto build_source = [&]() {
        vector::vector_t source{resource, list_i32};
        source.set_value(0, std::pmr::vector<int32_t>{{10, 20}, resource});
        source.set_value(1, std::pmr::vector<int32_t>{{30}, resource});
        source.set_value(2, std::pmr::vector<int32_t>{{40, 50, 60}, resource});
        return source;
    };

    auto reconciles_per_row = [&](cast_kind kind) {
        vector::vector_t source = build_source();
        vector::vector_t result{resource, array2_i64};
        REQUIRE_FALSE((*composite)(kind, source, &result, context, 3).contains_error());

        const vector::vector_t& child = result.entry();
        REQUIRE_FALSE(result.is_null(0));
        REQUIRE(child.get_value<int64_t>(0) == 10);
        REQUIRE(child.get_value<int64_t>(1) == 20);
        REQUIRE_FALSE(result.is_null(1));
        REQUIRE(child.get_value<int64_t>(2) == 30);
        REQUIRE(child.is_null(3));
        REQUIRE_FALSE(result.is_null(2));
        REQUIRE(child.get_value<int64_t>(4) == 40);
        REQUIRE(child.get_value<int64_t>(5) == 50);
    };

    reconciles_per_row(cast_kind::cast);
    reconciles_per_row(cast_kind::try_cast);
}

TEST_CASE("composite_cast: fixed ARRAY -> a different fixed ARRAY reconciles the length") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    const complex_logical_type array2_i32 =
        complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 2);
    const complex_logical_type array3_i64 =
        complex_logical_type::create_array(complex_logical_type{logical_type::BIGINT}, 3);

    {
        auto composite = registry.resolve(array2_i32, array3_i64, cast_type::assignment);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, array2_i32};
        vector::vector_t& source_child = source.entry();
        const int32_t inputs[4] = {10, 20, 30, 40};
        for (uint64_t index = 0; index < 4; ++index) {
            source_child.set_value(index, inputs[index]);
        }

        vector::vector_t result{resource, array3_i64};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const vector::vector_t& child = result.entry();
        REQUIRE(child.get_value<int64_t>(0) == 10);
        REQUIRE(child.get_value<int64_t>(1) == 20);
        REQUIRE(child.is_null(2));
        REQUIRE(child.get_value<int64_t>(3) == 30);
        REQUIRE(child.get_value<int64_t>(4) == 40);
        REQUIRE(child.is_null(5));
    }

    {
        auto composite = registry.resolve(array3_i64, array2_i32, cast_type::assignment);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, array3_i64};
        vector::vector_t& source_child = source.entry();
        const int64_t inputs[6] = {1, 2, 3, 4, 5, 6};
        for (uint64_t index = 0; index < 6; ++index) {
            source_child.set_value(index, inputs[index]);
        }

        vector::vector_t result{resource, array2_i32};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        const vector::vector_t& child = result.entry();
        REQUIRE(child.get_value<int32_t>(0) == 1);
        REQUIRE(child.get_value<int32_t>(1) == 2);
        REQUIRE(child.get_value<int32_t>(2) == 4);
        REQUIRE(child.get_value<int32_t>(3) == 5);
    }
}

// An empty ARRAY literal has no element type (NA), so it null-fills the target's declared length.
TEST_CASE("composite_cast: an empty array literal fills a fixed ARRAY with nulls") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    const complex_logical_type empty_literal =
        complex_logical_type::create_array(complex_logical_type{logical_type::NA}, 0);
    const complex_logical_type array3_i32 =
        complex_logical_type::create_array(complex_logical_type{logical_type::INTEGER}, 3);

    auto composite = registry.resolve(empty_literal, array3_i32, cast_type::assignment);
    REQUIRE(composite.has_value());

    vector::vector_t source{resource, empty_literal};
    vector::vector_t result{resource, array3_i32};
    REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());

    REQUIRE_FALSE(result.is_null(0));
    const vector::vector_t& child = result.entry();
    REQUIRE(child.is_null(0));
    REQUIRE(child.is_null(1));
    REQUIRE(child.is_null(2));
}

// An identical source/target leaf has no registered cast, so build_cast copies it verbatim.
TEST_CASE("composite_cast: identity/copy leaf builds partially-changed composites") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };

    {
        std::pmr::vector<complex_logical_type> src_fields{
            {field(logical_type::INTEGER, "a"), field(logical_type::FLOAT, "b")},
            resource};
        std::pmr::vector<complex_logical_type> tgt_fields{
            {field(logical_type::INTEGER, "a"), field(logical_type::DOUBLE, "b")},
            resource};
        const complex_logical_type src = complex_logical_type::create_struct("", src_fields);
        const complex_logical_type tgt = complex_logical_type::create_struct("", tgt_fields);

        auto composite = registry.resolve(src, tgt, cast_type::explicit_only);
        REQUIRE(composite.has_value());

        vector::vector_t source{resource, src};
        source.entries()[0]->set_value(0, int32_t{-77});
        source.entries()[1]->set_value(0, 2.5f);
        source.entries()[0]->set_value(1, int32_t{123});
        source.entries()[1]->set_value(1, 9.5f);
        source.set_null(1, true);
        vector::vector_t result{resource, tgt};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());

        REQUIRE(result.entries()[0]->get_value<int32_t>(0) == -77);
        REQUIRE(result.entries()[1]->get_value<double>(0) == Catch::Approx(2.5));
        REQUIRE(result.is_null(1));
    }

    {
        std::pmr::vector<complex_logical_type> src_fields{{field(logical_type::INTEGER, "a")}, resource};
        std::pmr::vector<complex_logical_type> tgt_fields{{field(logical_type::INTEGER, "renamed")}, resource};
        const complex_logical_type src = complex_logical_type::create_struct("", src_fields);
        const complex_logical_type tgt = complex_logical_type::create_struct("", tgt_fields);

        auto composite = registry.resolve(src, tgt, cast_type::explicit_only);
        REQUIRE(composite.has_value());
        vector::vector_t source{resource, src};
        source.entries()[0]->set_value(0, int32_t{42});
        vector::vector_t result{resource, tgt};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 1).contains_error());
        REQUIRE(result.entries()[0]->get_value<int32_t>(0) == 42);
    }

    {
        const complex_logical_type list_i32 =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
        auto composite = registry.resolve(list_i32, list_i32, cast_type::explicit_only);
        REQUIRE(composite.has_value());
        vector::vector_t source{resource, list_i32};
        source.set_value(0, std::pmr::vector<int32_t>{{5, -6, 7}, resource});
        source.set_value(1, std::pmr::vector<int32_t>{{8}, resource});
        vector::vector_t result{resource, list_i32};
        REQUIRE_FALSE((*composite)(cast_kind::cast, source, &result, context, 2).contains_error());
        REQUIRE(result.get_value<std::pmr::vector<int32_t>>(0) == std::pmr::vector<int32_t>{{5, -6, 7}, resource});
        REQUIRE(result.get_value<std::pmr::vector<int32_t>>(1) == std::pmr::vector<int32_t>{{8}, resource});
    }

    // find() runs before same_cast_type: same_cast_type collapses DECIMAL params and would skip a real scale change.
    {
        const complex_logical_type dec_10_2 = make_decimal(10, 2);
        const complex_logical_type dec_12_4 = make_decimal(12, 4);

        auto identity = registry.resolve(dec_10_2, dec_10_2, cast_type::explicit_only);
        REQUIRE(identity.has_value());
        vector::vector_t dsource{resource, dec_10_2};
        dsource.set_value(0, static_cast<int64_t>(12345));
        vector::vector_t dresult{resource, dec_10_2};
        REQUIRE_FALSE((*identity)(cast_kind::cast, dsource, &dresult, context, 1).contains_error());
        REQUIRE(dresult.get_value<int64_t>(0) == 12345);

        auto rescale = registry.resolve(dec_10_2, dec_12_4, cast_type::explicit_only);
        REQUIRE(rescale.has_value());
        vector::vector_t rresult{resource, dec_12_4};
        REQUIRE_FALSE((*rescale)(cast_kind::cast, dsource, &rresult, context, 1).contains_error());
        REQUIRE(rresult.get_value<int64_t>(0) == 1234500);
    }
}

TEST_CASE("default casts: BOOLEAN <-> numeric and string") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type bool_type{logical_type::BOOLEAN};

    // bool<->numeric is explicit-only both ways, as in PostgreSQL: bool converts to a numeric but isn't one.
    const cast_entry* bool_to_int = registry.find(bool_type, integer_type);
    const cast_entry* int_to_bool = registry.find(integer_type, bool_type);
    REQUIRE(bool_to_int != nullptr);
    REQUIRE(int_to_bool != nullptr);
    REQUIRE(bool_to_int->level == cast_type::explicit_only);
    REQUIRE(int_to_bool->level == cast_type::explicit_only);
    REQUIRE_FALSE(common(registry, bool_type, integer_type).has_value());
    REQUIRE_FALSE(registry.resolve(bool_type, integer_type, cast_type::assignment).has_value());

    graph_execution_context context{};

    {
        vector::vector_t flags{resource, bool_type};
        flags.set_value(0, true);
        flags.set_value(1, false);
        vector::vector_t ints{resource, integer_type};
        REQUIRE_FALSE(bool_to_int->fn.invoke(cast_kind::cast, flags, &ints, context, 2).contains_error());
        REQUIRE(ints.get_value<int32_t>(0) == 1);
        REQUIRE(ints.get_value<int32_t>(1) == 0);

        vector::vector_t source{resource, integer_type};
        source.set_value(0, static_cast<int32_t>(5));
        source.set_value(1, static_cast<int32_t>(0));
        vector::vector_t bools{resource, bool_type};
        REQUIRE_FALSE(int_to_bool->fn.invoke(cast_kind::cast, source, &bools, context, 2).contains_error());
        REQUIRE(bools.get_value<bool>(0) == true);
        REQUIRE(bools.get_value<bool>(1) == false);
    }

    {
        const cast_entry* to_string = registry.find(bool_type, string_type);
        const cast_entry* from_string = registry.find(string_type, bool_type);
        REQUIRE(to_string != nullptr);
        REQUIRE(from_string != nullptr);
        REQUIRE_FALSE(to_string->promotes());
        REQUIRE(from_string->fn.has_try_cast());

        vector::vector_t flags{resource, bool_type};
        flags.set_value(0, true);
        flags.set_value(1, false);
        vector::vector_t text{resource, string_type};
        REQUIRE_FALSE(to_string->fn.invoke(cast_kind::cast, flags, &text, context, 2).contains_error());
        REQUIRE(text.get_value<std::string_view>(0) == "true");
        REQUIRE(text.get_value<std::string_view>(1) == "false");

        vector::vector_t words{resource, string_type};
        words.set_value(0, std::string_view{" TRUE "});
        words.set_value(1, std::string_view{"f"});
        words.set_value(2, std::string_view{"yes"});
        words.set_value(3, std::string_view{"maybe"});
        vector::vector_t cast_result{resource, bool_type};
        REQUIRE(from_string->fn.invoke(cast_kind::cast, words, &cast_result, context, 4).contains_error());
        vector::vector_t try_result{resource, bool_type};
        REQUIRE_FALSE(from_string->fn.invoke(cast_kind::try_cast, words, &try_result, context, 4).contains_error());
        REQUIRE(try_result.get_value<bool>(0) == true);
        REQUIRE(try_result.get_value<bool>(1) == false);
        REQUIRE(try_result.get_value<bool>(2) == true);
        REQUIRE(try_result.is_null(3));
    }
}

TEST_CASE("default casts: INTEGER -> BIGINT executes over a vector, preserving nulls") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const cast_entry* entry = registry.find(integer_type, bigint_type);
    REQUIRE(entry != nullptr);

    vector::vector_t source{resource, integer_type};
    vector::vector_t result{resource, bigint_type};

    constexpr uint64_t count = 4;
    const int32_t inputs[count] = {-2000000000, -1, 0, 2000000000};
    for (uint64_t row = 0; row < count; ++row) {
        source.set_value(row, static_cast<int32_t>(inputs[row]));
    }
    source.set_null(2, true);

    graph_execution_context params{};
    core::error_t error = entry->fn.invoke(cast_kind::cast, source, &result, params, count);
    REQUIRE_FALSE(error.contains_error());

    for (uint64_t row = 0; row < count; ++row) {
        if (row == 2) {
            REQUIRE(result.is_null(row));
        } else {
            REQUIRE(result.get_value<int64_t>(row) == static_cast<int64_t>(inputs[row]));
        }
    }
}

// A container contributes nothing to promotion: its common type is its elements' common type, rebuilt around it.
TEST_CASE("cast_registry: containers promote through their element") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};

    {
        auto promoted =
            common(registry, complex_logical_type::create_list(i32), complex_logical_type::create_list(i64));
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::LIST);
        REQUIRE(promoted->type.child_type().type() == logical_type::BIGINT);
        REQUIRE(promoted->left_cast);
        REQUIRE_FALSE(promoted->right_cast);
    }

    {
        auto promoted =
            common(registry, complex_logical_type::create_array(i32, 2), complex_logical_type::create_array(i64, 2));
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::ARRAY);
        REQUIRE(promoted->type.child_type().type() == logical_type::BIGINT);
    }

    // Two arrays of different fixed lengths have no common ARRAY, so they widen to a LIST instead.
    {
        auto promoted =
            common(registry, complex_logical_type::create_array(i32, 2), complex_logical_type::create_array(i64, 3));
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::LIST);
        REQUIRE(promoted->type.child_type().type() == logical_type::BIGINT);
    }

    {
        auto promoted = common(registry,
                               complex_logical_type::create_map(resource, i32, i32),
                               complex_logical_type::create_map(resource, i64, i64));
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::MAP);
    }

    REQUIRE_FALSE(common(registry,
                         complex_logical_type::create_list(complex_logical_type{logical_type::BOOLEAN}),
                         complex_logical_type::create_list(complex_logical_type{logical_type::DATE}))
                      .has_value());
}

// Container transparency stops at a shape change: filling a fixed array may fail per row, so it's capped at assignment.
TEST_CASE("cast_registry: a shape-changing container cast is capped at assignment") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    using level = cast_type;
    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};
    const complex_logical_type list_i32 = complex_logical_type::create_list(i32);
    const complex_logical_type array2_i32 = complex_logical_type::create_array(i32, 2);
    const complex_logical_type array3_i64 = complex_logical_type::create_array(i64, 3);

    REQUIRE(registry.level_of(array2_i32, list_i32) == std::optional<level>{level::implicit});

    REQUIRE(registry.level_of(list_i32, array2_i32) == std::optional<level>{level::assignment});
    REQUIRE(registry.level_of(array2_i32, array3_i64) == std::optional<level>{level::assignment});

    const complex_logical_type list_str =
        complex_logical_type::create_list(complex_logical_type{logical_type::STRING_LITERAL});
    const complex_logical_type array2_str =
        complex_logical_type::create_array(complex_logical_type{logical_type::STRING_LITERAL}, 2);
    REQUIRE(registry.level_of(list_str, complex_logical_type::create_array(i32, 2)) ==
            std::optional<level>{level::explicit_only});
    REQUIRE(registry.level_of(list_i32, array2_str) == std::optional<level>{level::assignment});

    REQUIRE_FALSE(registry.cost_of(list_i32, array2_i32).has_value());
    REQUIRE_FALSE(registry.resolve(list_i32, array2_i32, level::implicit).has_value());

    // List and array meet at LIST, since dropping a fixed length always works but promotion never fills one.
    {
        auto promoted = common(registry, list_i32, array2_i32);
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::LIST);
        REQUIRE_FALSE(promoted->left_cast);
        REQUIRE(promoted->right_cast);
    }
}

// resolve() is gated by the level the call site accepts (PostgreSQL's ccontext >= castcontext).
TEST_CASE("cast_registry: the requested coercion level gates what resolve() returns") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    using level = cast_type;
    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i64{logical_type::BIGINT};
    const complex_logical_type str{logical_type::STRING_LITERAL};

    REQUIRE(registry.resolve(i32, i64, level::implicit).has_value());
    REQUIRE(registry.resolve(i32, i64, level::assignment).has_value());
    REQUIRE(registry.resolve(i32, i64, level::explicit_only).has_value());

    REQUIRE_FALSE(registry.resolve(i64, i32, level::implicit).has_value());
    REQUIRE(registry.resolve(i64, i32, level::assignment).has_value());
    REQUIRE_FALSE(registry.resolve(i32, str, level::implicit).has_value());
    REQUIRE(registry.resolve(i32, str, level::assignment).has_value());

    REQUIRE_FALSE(registry.resolve(str, i32, level::implicit).has_value());
    REQUIRE_FALSE(registry.resolve(str, i32, level::assignment).has_value());
    REQUIRE(registry.resolve(str, i32, level::explicit_only).has_value());

    const complex_logical_type list_str = complex_logical_type::create_list(str);
    const complex_logical_type list_i32 = complex_logical_type::create_list(i32);
    REQUIRE_FALSE(registry.resolve(list_str, list_i32, level::assignment).has_value());
    REQUIRE(registry.resolve(list_str, list_i32, level::explicit_only).has_value());
}

// Entries are stored in registration order with no cost-based sorting; search scores every candidate.
TEST_CASE("cast_registry: registration order does not affect which common type wins") {
    auto* resource = std::pmr::get_default_resource();

    struct helper {
        static core::error_t
        noop(const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) noexcept {
            return core::error_t::no_error();
        }
    };

    const complex_logical_type left{logical_type::INTEGER};
    const complex_logical_type right{logical_type::SMALLINT};
    const complex_logical_type expensive{logical_type::FLOAT};
    const complex_logical_type cheap{logical_type::BIGINT};

    auto entry = [](uint32_t precision_loss, uint32_t footprint) {
        return cast_entry{cast_function_t{&helper::noop, nullptr},
                          cast_cost{.precision_loss = precision_loss, .footprint = footprint},
                          /*convertable_inplace*/ false};
    };

    {
        cast_registry_t registry{resource};
        REQUIRE_FALSE(registry.add(left, expensive, entry(9, 4)).contains_error());
        REQUIRE_FALSE(registry.add(right, expensive, entry(9, 4)).contains_error());
        REQUIRE_FALSE(registry.add(left, cheap, entry(0, 8)).contains_error());
        REQUIRE_FALSE(registry.add(right, cheap, entry(0, 8)).contains_error());

        auto promoted = registry.find_best_common_type(left, right);
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::BIGINT);
    }

    {
        cast_registry_t registry{resource};
        REQUIRE_FALSE(registry.add(left, cheap, entry(0, 8)).contains_error());
        REQUIRE_FALSE(registry.add(right, cheap, entry(0, 8)).contains_error());
        REQUIRE_FALSE(registry.add(left, expensive, entry(9, 4)).contains_error());
        REQUIRE_FALSE(registry.add(right, expensive, entry(9, 4)).contains_error());

        auto promoted = registry.find_best_common_type(left, right);
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::BIGINT);
    }
}

// A struct cast is DECLARED and may be declared implicit, so it can promote as a third type neither side started at.
TEST_CASE("cast_registry: a declared implicit struct cast takes part in promotion") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };
    auto make_struct = [&](const char* name, logical_type type) {
        std::pmr::vector<complex_logical_type> fields{{field(type, "x")}, resource};
        return complex_logical_type::create_struct(name, fields);
    };
    cast_t noop = [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
        return core::error_t::no_error();
    };

    const complex_logical_type narrow = make_struct("narrow", logical_type::INTEGER);
    const complex_logical_type wide = make_struct("wide", logical_type::BIGINT);
    const complex_logical_type widest = make_struct("widest", logical_type::HUGEINT);

    REQUIRE_FALSE(registry.add(narrow, wide, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 8}})
                      .contains_error());
    {
        auto promoted = registry.find_best_common_type(narrow, wide);
        REQUIRE(promoted.has_value());
        REQUIRE(same_cast_type(promoted->type, wide));
        REQUIRE(promoted->left_cast);
        REQUIRE_FALSE(promoted->right_cast);
    }

    {
        cast_registry_t third{resource};
        REQUIRE_FALSE(
            third.add(narrow, widest, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 16}})
                .contains_error());
        REQUIRE_FALSE(third.add(wide, widest, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 16}})
                          .contains_error());

        auto promoted = third.find_best_common_type(wide, narrow);
        REQUIRE(promoted.has_value());
        REQUIRE(same_cast_type(promoted->type, widest));
        REQUIRE(promoted->left_cast);
        REQUIRE(promoted->right_cast);
    }

    {
        cast_registry_t other{resource};
        const complex_logical_type from = make_struct("from", logical_type::INTEGER);
        const complex_logical_type to = make_struct("to", logical_type::BIGINT);
        REQUIRE_FALSE(other.add(from, to, complex_cast_entry{noop, cast_type::assignment}).contains_error());
        REQUIRE(other.resolve(from, to, cast_type::assignment).has_value());
        REQUIRE_FALSE(other.find_best_common_type(from, to).has_value());
    }
}

// A declared cast isn't restricted to struct->struct: a UDT converting to a built-in type lives in the same table.
TEST_CASE("cast_registry: declared UDT <-> built-in casts take part in promotion") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type i32{logical_type::INTEGER};
    const complex_logical_type i16{logical_type::SMALLINT};

    auto field = [](logical_type type, const char* name) {
        complex_logical_type result{type};
        result.set_alias(name);
        return result;
    };
    std::pmr::vector<complex_logical_type> fields{{field(logical_type::INTEGER, "v")}, resource};
    const complex_logical_type udt = complex_logical_type::create_struct("money", fields);

    cast_t noop = [](cast_kind, const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) {
        return core::error_t::no_error();
    };

    REQUIRE_FALSE(registry.add(i32, udt, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 8}})
                      .contains_error());
    REQUIRE_FALSE(registry.add(udt, i32, complex_cast_entry{noop, cast_type::assignment}).contains_error());

    REQUIRE(registry.level_of(i32, udt) == std::optional<cast_type>{cast_type::implicit});
    REQUIRE(registry.level_of(udt, i32) == std::optional<cast_type>{cast_type::assignment});
    REQUIRE(registry.resolve(i32, udt, cast_type::implicit).has_value());
    REQUIRE_FALSE(registry.resolve(udt, i32, cast_type::implicit).has_value());
    REQUIRE(registry.resolve(udt, i32, cast_type::assignment).has_value());

    {
        auto promoted = registry.find_best_common_type(i32, udt);
        REQUIRE(promoted.has_value());
        REQUIRE(same_cast_type(promoted->type, udt));
        REQUIRE(promoted->left_cast);
        REQUIRE_FALSE(promoted->right_cast);
    }
    {
        auto promoted = registry.find_best_common_type(udt, i32);
        REQUIRE(promoted.has_value());
        REQUIRE(same_cast_type(promoted->type, udt));
        REQUIRE_FALSE(promoted->left_cast);
        REQUIRE(promoted->right_cast);
    }

    // Casts are not chained: a two-step path (smallint->int->UDT) is not a common type.
    REQUIRE_FALSE(registry.level_of(i16, udt).has_value());
    REQUIRE_FALSE(registry.find_best_common_type(i16, udt).has_value());
    {
        cast_registry_t direct{resource};
        register_default_casts(direct);
        REQUIRE_FALSE(direct.add(i16, udt, complex_cast_entry{noop, cast_cost{.precision_loss = 0, .footprint = 8}})
                          .contains_error());
        auto promoted = direct.find_best_common_type(i16, udt);
        REQUIRE(promoted.has_value());
        REQUIRE(same_cast_type(promoted->type, udt));
        REQUIRE(promoted->left_cast);
        REQUIRE_FALSE(promoted->right_cast);
    }

    {
        auto promoted = registry.find_best_common_type(complex_logical_type::create_list(i32),
                                                       complex_logical_type::create_list(udt));
        REQUIRE(promoted.has_value());
        REQUIRE(promoted->type.type() == logical_type::LIST);
        REQUIRE(same_cast_type(promoted->type.child_type(), udt));
    }

    {
        cast_registry_t other{resource};
        register_default_casts(other);
        REQUIRE_FALSE(other.add(i32, udt, complex_cast_entry{noop, cast_type::assignment}).contains_error());
        REQUIRE(other.resolve(i32, udt, cast_type::assignment).has_value());
        REQUIRE_FALSE(other.find_best_common_type(i32, udt).has_value());
    }
}

TEST_CASE("default casts: common(DECIMAL, floating) depends on the decimal width") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type narrow = make_decimal(6, 2);
    const complex_logical_type wide = make_decimal(10, 2);

    REQUIRE(registry.cost_of(narrow, float_type)->precision_loss == 0);
    REQUIRE(registry.cost_of(wide, float_type)->precision_loss > 0);
    REQUIRE(registry.cost_of(wide, double_type)->precision_loss == 0);

    REQUIRE(common(registry, wide, float_type)->type.type() == logical_type::DOUBLE);
    REQUIRE(common(registry, float_type, wide)->type.type() == logical_type::DOUBLE);

    REQUIRE(common(registry, narrow, float_type)->type.type() == logical_type::FLOAT);
    REQUIRE(common(registry, float_type, narrow)->type.type() == logical_type::FLOAT);
}

TEST_CASE("default casts: BOOLEAN <-> DECIMAL is explicit-only, like BOOLEAN <-> the other numerics") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type boolean{logical_type::BOOLEAN};
    const complex_logical_type decimal = make_decimal(10, 2);

    REQUIRE(registry.level_of(boolean, decimal) == std::optional<cast_type>{cast_type::explicit_only});
    REQUIRE(registry.resolve(boolean, decimal, cast_type::explicit_only).has_value());
    REQUIRE_FALSE(registry.resolve(boolean, decimal, cast_type::assignment).has_value());

    REQUIRE_FALSE(common(registry, decimal, boolean).has_value());
    REQUIRE_FALSE(common(registry, boolean, decimal).has_value());
}

TEST_CASE("default casts: DECIMAL -> BOOLEAN is (x != 0), not a truncation") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);
    graph_execution_context context{};

    const complex_logical_type boolean{logical_type::BOOLEAN};
    const complex_logical_type decimal = make_decimal(10, 2);

    REQUIRE(registry.level_of(decimal, boolean) == std::optional<cast_type>{cast_type::explicit_only});
    REQUIRE_FALSE(registry.resolve(decimal, boolean, cast_type::assignment).has_value());

    auto cast = registry.resolve(decimal, boolean, cast_type::explicit_only);
    REQUIRE(cast.has_value());

    vector::vector_t doubles{resource, double_type};
    doubles.set_value(0, 0.0);
    doubles.set_value(1, 0.5);
    doubles.set_value(2, -0.5);
    doubles.set_value(3, 5.0);
    vector::vector_t source{resource, decimal};
    auto to_decimal = registry.resolve(double_type, decimal, cast_type::assignment);
    REQUIRE(to_decimal.has_value());
    REQUIRE_FALSE((*to_decimal)(cast_kind::cast, doubles, &source, context, 4).contains_error());

    vector::vector_t result{resource, boolean};
    REQUIRE_FALSE((*cast)(cast_kind::cast, source, &result, context, 4).contains_error());
    REQUIRE_FALSE(result.get_value<bool>(0));
    REQUIRE(result.get_value<bool>(1));
    REQUIRE(result.get_value<bool>(2));
    REQUIRE(result.get_value<bool>(3));
}

TEST_CASE("default casts: find_best_common_type over N inputs") {
    cast_registry_t registry{std::pmr::get_default_resource()};
    register_default_casts(registry);

    auto common_of = [&](std::vector<complex_logical_type> types) {
        return registry.find_best_common_type(std::span<const complex_logical_type>{types});
    };

    SECTION("one input is its own common type") {
        auto single = common_of({integer_type});
        REQUIRE(single.has_value());
        REQUIRE(single->type.type() == logical_type::INTEGER);
        REQUIRE(single->casts.size() == 1);
        REQUIRE_FALSE(single->casts[0]);
    }

    SECTION("the widest of the tower wins and only the narrower sides cast") {
        auto widened = common_of({integer_type, bigint_type, hugeint_type});
        REQUIRE(widened.has_value());
        REQUIRE(widened->type.type() == logical_type::HUGEINT);
        REQUIRE(widened->casts.size() == 3);
        REQUIRE(widened->casts[0]);
        REQUIRE(widened->casts[1]);
        REQUIRE_FALSE(widened->casts[2]);
    }

    SECTION("the answer does not depend on argument order") {
        auto forward = common_of({integer_type, bigint_type, double_type});
        auto shuffled = common_of({double_type, integer_type, bigint_type});
        auto reversed = common_of({double_type, bigint_type, integer_type});
        REQUIRE(forward.has_value());
        REQUIRE(shuffled.has_value());
        REQUIRE(reversed.has_value());
        REQUIRE(forward->type.type() == logical_type::DOUBLE);
        REQUIRE(shuffled->type.type() == forward->type.type());
        REQUIRE(reversed->type.type() == forward->type.type());
    }

    SECTION("one unreachable input makes the whole thing an error") {
        REQUIRE_FALSE(common_of({integer_type, bigint_type, string_type}).has_value());
    }

    SECTION("no inputs has no common type") { REQUIRE_FALSE(common_of({}).has_value()); }

    SECTION("decimals widen to a constructed supertype that is no input") {
        const auto narrow = make_decimal(6, 2);
        const auto scaled = make_decimal(6, 4);
        auto widened = common_of({narrow, scaled});
        REQUIRE(widened.has_value());
        REQUIRE(widened->type.type() == logical_type::DECIMAL);
        const auto* extension = widened->type.extension_as<components::types::decimal_logical_type_extension>();
        REQUIRE(extension->scale() == 4);
        REQUIRE(extension->width() >= 8);
        REQUIRE(widened->casts.size() == 2);
    }
}

// NULL is not a type that converts into others; one cast body serves every target, implicit and lossless.
TEST_CASE("default casts: NULL reaches every type by one lossless implicit cast") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type null_type{logical_type::NA};

    SECTION("it reaches concrete, parameterized and constructed targets alike") {
        const auto decimal = make_decimal(9, 3);
        std::pmr::vector<complex_logical_type> fields{resource};
        fields.emplace_back(logical_type::INTEGER);
        fields.back().set_alias("f");
        const auto structure = complex_logical_type::create_struct("s", fields, "s");
        for (const auto& target :
             {integer_type, double_type, string_type, decimal, structure, complex_logical_type{logical_type::DATE}}) {
            REQUIRE(registry.level_of(null_type, target) == cast_type::implicit);
            const auto cost = registry.cost_of(null_type, target);
            REQUIRE(cost.has_value());
            REQUIRE(cost->precision_loss == 0);
            REQUIRE(registry.resolve(null_type, target, cast_type::implicit).has_value());
            REQUIRE(registry.resolve(null_type, target, cast_type::assignment).has_value());
        }
    }

    SECTION("the reverse edge does not exist") {
        // A concrete type already represents its own nulls; a T -> NULL cast would let NA collapse an expression.
        REQUIRE_FALSE(registry.level_of(integer_type, null_type).has_value());
        REQUIRE_FALSE(registry.resolve(integer_type, null_type, cast_type::explicit_only).has_value());
    }

    SECTION("executing it invalidates every row and reads nothing from the source") {
        vector::vector_t source{resource, complex_logical_type{logical_type::NA}};
        vector::vector_t result{resource, bigint_type};
        constexpr uint64_t count = 4;
        for (uint64_t row = 0; row < count; ++row) {
            result.set_value(row, static_cast<int64_t>(row + 1));
        }

        auto cast = registry.resolve(null_type, bigint_type, cast_type::implicit);
        REQUIRE(cast.has_value());
        graph_execution_context params{};
        REQUIRE_FALSE((*cast)(cast_kind::cast, source, &result, params, count).contains_error());

        for (uint64_t row = 0; row < count; ++row) {
            REQUIRE(result.is_null(row));
        }
    }
}

TEST_CASE("default casts: NULL is transparent to common-type resolution") {
    auto* resource = std::pmr::get_default_resource();
    cast_registry_t registry{resource};
    register_default_casts(registry);

    const complex_logical_type null_type{logical_type::NA};

    SECTION("pairwise: the other side wins, and the null side gets a real cast") {
        auto with_null = common(registry, null_type, integer_type);
        REQUIRE(with_null.has_value());
        REQUIRE(with_null->type.type() == logical_type::INTEGER);
        REQUIRE(with_null->left_cast);
        REQUIRE_FALSE(with_null->right_cast);

        auto mirrored = common(registry, integer_type, null_type);
        REQUIRE(mirrored.has_value());
        REQUIRE(mirrored->type.type() == logical_type::INTEGER);
        REQUIRE(mirrored->right_cast);
        REQUIRE_FALSE(mirrored->left_cast);
    }

    SECTION("pairwise: two nulls stay NULL") {
        auto both = common(registry, null_type, null_type);
        REQUIRE(both.has_value());
        REQUIRE(both->type.type() == logical_type::NA);
    }

    SECTION("pairwise: a null does not rescue an otherwise unreachable pair") {
        REQUIRE_FALSE(common(registry, integer_type, string_type).has_value());
    }

    auto common_of = [&](std::vector<complex_logical_type> types) {
        return registry.find_best_common_type(std::span<const complex_logical_type>{types});
    };

    SECTION("n-ary: the concrete inputs decide and the null follows") {
        auto widened = common_of({null_type, integer_type, bigint_type});
        REQUIRE(widened.has_value());
        REQUIRE(widened->type.type() == logical_type::BIGINT);
        REQUIRE(widened->casts.size() == 3);
        REQUIRE(widened->casts[0]);
        REQUIRE(widened->casts[1]);
        REQUIRE_FALSE(widened->casts[2]);
    }

    SECTION("n-ary: a null does not knock decimals off their own supertype rule") {
        // The parameterized families settle by folding width/scale, which a null can't take part in.
        const auto narrow = make_decimal(6, 2);
        const auto scaled = make_decimal(6, 4);
        auto without_null = common_of({narrow, scaled});
        auto with_null = common_of({null_type, narrow, scaled});
        REQUIRE(without_null.has_value());
        REQUIRE(with_null.has_value());
        REQUIRE(with_null->type.type() == logical_type::DECIMAL);
        const auto* expected = without_null->type.extension_as<types::decimal_logical_type_extension>();
        const auto* actual = with_null->type.extension_as<types::decimal_logical_type_extension>();
        REQUIRE(actual->width() == expected->width());
        REQUIRE(actual->scale() == expected->scale());
        REQUIRE(with_null->casts.size() == 3);
    }

    SECTION("n-ary: all-null inputs stay NULL") {
        auto all_null = common_of({null_type, null_type});
        REQUIRE(all_null.has_value());
        REQUIRE(all_null->type.type() == logical_type::NA);
    }

    SECTION("n-ary: a null does not rescue an otherwise unreachable list") {
        REQUIRE_FALSE(common_of({null_type, integer_type, string_type}).has_value());
    }
}
