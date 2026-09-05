#include <catch2/catch_test_macros.hpp>

#include <components/casts/cast_registry.hpp>
#include <core/pmr.hpp>

#include <algorithm>

using namespace components;
using namespace components::casts;
using types::complex_logical_type;
using types::logical_type;

namespace {
    // The ONE arena this file builds DECIMALs on. create_decimal allocates only on its refusal
    // path, and that message belongs to the caller, so the caller has to name an arena it owns
    // rather than reach for the process-global one (rule 14).
    std::pmr::memory_resource* decimal_resource() {
        static core::pmr::otterbrix_resource arena;
        return &arena;
    }

    // create_decimal reports an out-of-window (width, scale) through core::error_t. Every
    // literal these tests use is inside the window, so the helper checks the result and
    // hands back the type.
    components::types::complex_logical_type
    make_decimal(uint8_t width, uint8_t scale, std::string alias = "") {
        auto created = components::types::complex_logical_type::create_decimal(decimal_resource(), width, scale, std::move(alias));
        REQUIRE_FALSE(created.has_error());
        return std::move(created.value());
    }
} // namespace

namespace {

    core::error_t
    noop_cast(const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) noexcept {
        return core::error_t::no_error();
    }

    // An implicit entry must carry a cost; an assignment/explicit one must not, so the two
    // take different constructors.
    cast_entry make_entry(cast_type level = cast_type::implicit) {
        if (level == cast_type::implicit) {
            return cast_entry{cast_function_t{noop_cast, nullptr},
                              cast_cost{.precision_loss = 0, .footprint = 8},
                              /*convertable_inplace*/ false};
        }
        return cast_entry{cast_function_t{noop_cast, nullptr}, level, /*convertable_inplace*/ false};
    }

} // namespace

TEST_CASE("cast_registry: add / find / remove round trip") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type source{logical_type::INTEGER};
    const complex_logical_type target{logical_type::BIGINT};

    REQUIRE(registry.find(source, target) == nullptr);

    REQUIRE_FALSE(registry.add(source, target, make_entry()).contains_error());

    const cast_entry* found = registry.find(source, target);
    REQUIRE(found != nullptr);
    REQUIRE(found->promotes());
    REQUIRE(found->has_fixed_cost);
    REQUIRE_FALSE(found->fn.has_try_cast());

    REQUIRE(registry.remove(source, target));
    REQUIRE(registry.find(source, target) == nullptr);
    REQUIRE_FALSE(registry.remove(source, target));
}

TEST_CASE("cast_registry: duplicate registration is rejected, no override") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type source{logical_type::INTEGER};
    const complex_logical_type target{logical_type::DOUBLE};

    REQUIRE_FALSE(registry.add(source, target, make_entry()).contains_error());

    core::error_t duplicate = registry.add(source, target, make_entry(cast_type::explicit_only));
    REQUIRE(duplicate.contains_error());
    REQUIRE(duplicate.type == core::error_code_t::already_exists);

    // First entry is untouched (no override).
    const cast_entry* found = registry.find(source, target);
    REQUIRE(found != nullptr);
    REQUIRE(found->promotes());
}

TEST_CASE("cast_registry: an entry survives sibling inserts to the same source") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type source{logical_type::INTEGER};
    const complex_logical_type target{logical_type::BIGINT};
    REQUIRE_FALSE(registry.add(source, target, make_entry()).contains_error());

    // The per-source targets are contiguous, so adding siblings may relocate them -- the registry
    // does NOT promise a stable entry address (resolve() returns a self-contained cast_t). What
    // must hold is that find() still returns the right entry with its properties intact.
    for (auto other : {logical_type::DOUBLE, logical_type::FLOAT, logical_type::SMALLINT}) {
        REQUIRE_FALSE(registry.add(source, complex_logical_type{other}, make_entry()).contains_error());
    }

    const cast_entry* found = registry.find(source, target);
    REQUIRE(found != nullptr);
    REQUIRE(found->promotes()); // the entry's properties are intact after relocation
    REQUIRE_FALSE(found->fn.has_try_cast());
}

TEST_CASE("cast_registry: targets_from enumerates every target of a source") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type source{logical_type::INTEGER};

    // No casts leave the source yet.
    REQUIRE(registry.targets_from(source) == nullptr);

    const logical_type target_ids[] = {logical_type::BIGINT, logical_type::DOUBLE, logical_type::HUGEINT};
    for (auto target_id : target_ids) {
        REQUIRE_FALSE(registry.add(source, complex_logical_type{target_id}, make_entry()).contains_error());
    }
    // A cast from an unrelated source must not show up here.
    REQUIRE_FALSE(
        registry
            .add(complex_logical_type{logical_type::FLOAT}, complex_logical_type{logical_type::DOUBLE}, make_entry())
            .contains_error());

    const cast_registry_t::target_entries_t* targets = registry.targets_from(source);
    REQUIRE(targets != nullptr);
    REQUIRE(targets->size() == 3);

    // Every registered target is present exactly once, and each list entry points
    // at the same object find() returns.
    for (auto target_id : target_ids) {
        const complex_logical_type target{target_id};
        auto match = std::find_if(targets->begin(), targets->end(), [&](const auto& pair) {
            return same_cast_type(pair.first, target);
        });
        REQUIRE(match != targets->end());
        REQUIRE(&match->second == registry.find(source, target));
    }
}

TEST_CASE("cast_registry: DECIMAL identity collapses width/scale") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type decimal_a = make_decimal(6, 2);
    const complex_logical_type decimal_b = make_decimal(10, 4);
    const complex_logical_type double_type{logical_type::DOUBLE};

    // A single (DECIMAL, DOUBLE) entry answers for every DECIMAL(p,s): the
    // width/scale are runtime data the one decimal kernel reads, not identity.
    REQUIRE_FALSE(registry.add(decimal_a, double_type, make_entry()).contains_error());
    REQUIRE(registry.find(decimal_b, double_type) != nullptr);
    REQUIRE(registry.find(decimal_a, double_type) == registry.find(decimal_b, double_type));

    // ...and registering DECIMAL(10,4) again collides with DECIMAL(6,2).
    REQUIRE(registry.add(decimal_b, double_type, make_entry()).contains_error());
}

TEST_CASE("cast_registry: LIST identity recurses into element, ignores length") {
    cast_registry_t registry{std::pmr::get_default_resource()};

    const complex_logical_type list_of_int =
        complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
    const complex_logical_type list_of_int_again =
        complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
    const complex_logical_type list_of_double =
        complex_logical_type::create_list(complex_logical_type{logical_type::DOUBLE});
    const complex_logical_type target{logical_type::STRING_LITERAL};

    REQUIRE_FALSE(registry.add(list_of_int, target, make_entry()).contains_error());

    // Same element type -> same identity.
    REQUIRE(registry.find(list_of_int_again, target) != nullptr);
    // Different element type -> distinct entry.
    REQUIRE(registry.find(list_of_double, target) == nullptr);
}