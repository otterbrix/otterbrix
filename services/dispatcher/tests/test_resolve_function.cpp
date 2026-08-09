#include <catch2/catch_test_macros.hpp>

#include <components/casts/default_casts.hpp>
#include <services/dispatcher/resolve_function.hpp>

using namespace services::dispatcher;
using components::types::complex_logical_type;
using components::types::logical_type;

namespace {

    std::pmr::memory_resource* resource() { return std::pmr::get_default_resource(); }

    const components::casts::cast_registry_t& casts() {
        static components::casts::cast_registry_t registry{std::pmr::new_delete_resource()};
        static const bool loaded = [] {
            components::casts::register_default_casts(registry);
            return true;
        }();
        (void) loaded;
        return registry;
    }

    const components::compute::function_registry_t& functions() {
        return *components::compute::function_registry_t::get_default();
    }

    std::pmr::vector<complex_logical_type> args(std::initializer_list<logical_type> types) {
        std::pmr::vector<complex_logical_type> out(resource());
        for (auto type : types) {
            out.emplace_back(type);
        }
        return out;
    }

    components::compute::function_types_mask any_kind() {
        return components::compute::create_mask(components::compute::function_type_t::row,
                                               components::compute::function_type_t::vector,
                                               components::compute::function_type_t::aggregate,
                                               components::compute::function_type_t::expand);
    }

    core::result_wrapper_t<resolved_function_t> resolve(std::string_view name,
                                                        std::initializer_list<logical_type> types) {
        auto arguments = args(types);
        return resolve_function(resource(),
                                casts(),
                                components::graph_execution_context{},
                                functions(),
                                name,
                                arguments,
                                any_kind());
    }

} // namespace

TEST_CASE("dispatcher::resolve_function: an exact fit needs no casts") {
    auto resolved = resolve("substring", {logical_type::STRING_LITERAL, logical_type::BIGINT});
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE(resolved.value().arguments.size() == 2);
    REQUIRE_FALSE(resolved.value().arguments[0].cast);
    REQUIRE_FALSE(resolved.value().arguments[1].cast);
    REQUIRE(resolved.value().result.type() == logical_type::STRING_LITERAL);
}

// The parameter is BIGINT, so an INTEGER argument has to be converted -- under the old
// matcher model it merely "matched" and the kernel widened it per row instead.
TEST_CASE("dispatcher::resolve_function: a narrower argument gets a cast to the parameter") {
    auto resolved = resolve("substring", {logical_type::STRING_LITERAL, logical_type::INTEGER});
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE_FALSE(resolved.value().arguments[0].cast);
    REQUIRE(resolved.value().arguments[1].cast);
    REQUIRE(resolved.value().arguments[1].target.type() == logical_type::BIGINT);
}

TEST_CASE("dispatcher::resolve_function: an untyped NULL fits any parameter without a cast") {
    auto resolved = resolve("substring", {logical_type::NA, logical_type::BIGINT});
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE_FALSE(resolved.value().arguments[0].cast);
    REQUIRE(resolved.value().arguments[0].target.type() == logical_type::STRING_LITERAL);
}

TEST_CASE("dispatcher::resolve_function: arity picks between overloads of one name") {
    auto two = resolve("substring", {logical_type::STRING_LITERAL, logical_type::BIGINT});
    auto three = resolve("substring", {logical_type::STRING_LITERAL, logical_type::BIGINT, logical_type::BIGINT});
    REQUIRE_FALSE(two.has_error());
    REQUIRE_FALSE(three.has_error());
    REQUIRE(three.value().arguments.size() == 3);

    auto four =
        resolve("substring",
                {logical_type::STRING_LITERAL, logical_type::BIGINT, logical_type::BIGINT, logical_type::BIGINT});
    REQUIRE(four.has_error());
}

// A variable with no admissible list is anyelement: it takes the argument type as-is and
// the return type follows it.
TEST_CASE("dispatcher::resolve_function: an unconstrained variable adopts the argument type") {
    auto integer = resolve("min", {logical_type::INTEGER});
    REQUIRE_FALSE(integer.has_error());
    REQUIRE_FALSE(integer.value().arguments[0].cast);
    REQUIRE(integer.value().result.type() == logical_type::INTEGER);

    auto text = resolve("max", {logical_type::STRING_LITERAL});
    REQUIRE_FALSE(text.has_error());
    REQUIRE_FALSE(text.value().arguments[0].cast);
    REQUIRE(text.value().result.type() == logical_type::STRING_LITERAL);

    // Even a type no cast reaches works, because nothing has to be converted.
    auto date = resolve("min", {logical_type::DATE});
    REQUIRE_FALSE(date.has_error());
    REQUIRE(date.value().result.type() == logical_type::DATE);
}

TEST_CASE("dispatcher::resolve_function: a restricted variable admits its domain and rejects outside it") {
    auto integer = resolve("sum", {logical_type::INTEGER});
    REQUIRE_FALSE(integer.has_error());
    REQUIRE_FALSE(integer.value().arguments[0].cast);
    REQUIRE(integer.value().result.type() == logical_type::INTEGER);

    // DATE is not summable, and BOOLEAN was removed from the domain because
    // sum_operator_t accumulates into a bool, making SUM(bool) a logical OR.
    REQUIRE(resolve("sum", {logical_type::DATE}).has_error());
    REQUIRE(resolve("sum", {logical_type::BOOLEAN}).has_error());
}

TEST_CASE("dispatcher::resolve_function: the function decides its own return type") {
    // length returns BIGINT regardless of its argument.
    auto resolved = resolve("length", {logical_type::STRING_LITERAL});
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE(resolved.value().result.type() == logical_type::BIGINT);

    // count returns UBIGINT, and has a zero-argument overload for COUNT(*).
    auto counted = resolve("count", {logical_type::STRING_LITERAL});
    REQUIRE_FALSE(counted.has_error());
    REQUIRE(counted.value().result.type() == logical_type::UBIGINT);

    std::pmr::vector<complex_logical_type> none(resource());
    auto star =
        resolve_function(resource(), casts(), components::graph_execution_context{}, functions(), "count", none, any_kind());
    REQUIRE_FALSE(star.has_error());
    REQUIRE(star.value().result.type() == logical_type::UBIGINT);
}

TEST_CASE("dispatcher::resolve_function: an unknown name and a non-viable call report differently") {
    auto unknown = resolve("no_such_function", {logical_type::BIGINT});
    REQUIRE(unknown.has_error());
    REQUIRE(unknown.error().type == core::error_code_t::unrecognized_function);

    auto wrong = resolve("substring", {logical_type::BIGINT, logical_type::BIGINT});
    REQUIRE(wrong.has_error());
    REQUIRE(wrong.error().type == core::error_code_t::incorrect_function_argument);
}

TEST_CASE("dispatcher::resolve_function: mergeable rides along from the matched function") {
    auto resolved = resolve("sum", {logical_type::BIGINT});
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE(resolved.value().mergeable);
}

// A DECIMAL entry in the domain carries no width/scale, so it stands for the whole family:
// the argument keeps its own parameters and nothing is converted. The runtime agrees --
// operator_switch sums the raw integer and rebuilds a decimal from the input's type.
TEST_CASE("dispatcher::resolve_function: a family entry keeps the argument's parameters") {
    auto decimal = complex_logical_type::create_decimal(10, 2);
    std::pmr::vector<complex_logical_type> arguments(resource());
    arguments.emplace_back(decimal);

    auto resolved = resolve_function(resource(),
                                     casts(),
                                     components::graph_execution_context{},
                                     functions(),
                                     "sum",
                                     arguments,
                                     any_kind());
    REQUIRE_FALSE(resolved.has_error());
    REQUIRE_FALSE(resolved.value().arguments[0].cast);
    REQUIRE(resolved.value().arguments[0].target == decimal);
    REQUIRE(resolved.value().result == decimal);
}

// Which kinds of function a clause accepts is decided by the clause. An aggregate cannot be
// evaluated before grouping, so WHERE takes only scalar functions; the aggregate slot of a
// GROUP BY is the mirror image and takes nothing else.
TEST_CASE("dispatcher::resolve_function: the clause decides which function kinds are allowed") {
    const auto scalar_only = components::compute::create_mask(components::compute::function_type_t::row,
                                                             components::compute::function_type_t::vector);
    const auto aggregate_only = components::compute::create_mask(components::compute::function_type_t::aggregate);

    auto integer = args({logical_type::BIGINT});
    auto text = args({logical_type::STRING_LITERAL});

    auto sum_in_where =
        resolve_function(resource(), casts(), components::graph_execution_context{}, functions(), "sum", integer, scalar_only);
    REQUIRE(sum_in_where.has_error());
    REQUIRE(sum_in_where.error().type == core::error_code_t::incorrect_function_argument);

    auto length_in_aggregate = resolve_function(resource(),
                                                casts(),
                                                components::graph_execution_context{},
                                                functions(),
                                                "length",
                                                text,
                                                aggregate_only);
    REQUIRE(length_in_aggregate.has_error());

    // Each is fine in the clause that accepts it.
    REQUIRE_FALSE(
        resolve_function(resource(), casts(), components::graph_execution_context{}, functions(), "sum", integer, aggregate_only)
            .has_error());
    REQUIRE_FALSE(
        resolve_function(resource(), casts(), components::graph_execution_context{}, functions(), "length", text, scalar_only)
            .has_error());
}

// abs keeps the argument's own type, including a decimal's (width, scale) and an unsigned's.
// DECIMAL must be in the domain, because decimal->double is implicit and would otherwise silently
// re-target to DOUBLE while the kernel returns a decimal; the unsigned types must be in it for the
// same class of reason -- absent, they are reached by WIDENING into a signed type, which converts
// a value that was already its own magnitude and returns a wider column than was asked for.
TEST_CASE("dispatcher::resolve_function: abs resolves per argument type") {
    auto integer = resolve("abs", {logical_type::INTEGER});
    REQUIRE_FALSE(integer.has_error());
    REQUIRE_FALSE(integer.value().arguments[0].cast);
    REQUIRE(integer.value().result.type() == logical_type::INTEGER);

    auto real = resolve("abs", {logical_type::DOUBLE});
    REQUIRE_FALSE(real.has_error());
    REQUIRE(real.value().result.type() == logical_type::DOUBLE);

    auto decimal = complex_logical_type::create_decimal(12, 4);
    std::pmr::vector<complex_logical_type> decimal_arg(resource());
    decimal_arg.emplace_back(decimal);
    auto scaled = resolve_function(resource(),
                                   casts(),
                                   components::graph_execution_context{},
                                   functions(),
                                   "abs",
                                   decimal_arg,
                                   any_kind());
    REQUIRE_FALSE(scaled.has_error());
    REQUIRE_FALSE(scaled.value().arguments[0].cast);
    REQUIRE(scaled.value().result == decimal);

    // An unsigned argument is IN the domain: abs over it is the identity, so it is resolved with
    // no cast at all and keeps its own type. Widening here would have cost a conversion and a
    // wider result column to express a negation that cannot occur.
    for (auto type : {logical_type::UTINYINT,
                      logical_type::USMALLINT,
                      logical_type::UINTEGER,
                      logical_type::UBIGINT,
                      logical_type::UHUGEINT}) {
        INFO("unsigned type " << static_cast<int>(type));
        auto unsigned_arg = resolve("abs", {type});
        REQUIRE_FALSE(unsigned_arg.has_error());
        REQUIRE_FALSE(unsigned_arg.value().arguments[0].cast);
        REQUIRE(unsigned_arg.value().result.type() == type);
    }

    REQUIRE(resolve("abs", {logical_type::STRING_LITERAL}).has_error());
}
