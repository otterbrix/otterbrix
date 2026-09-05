// output_type's resolver channel, pinned on two axes at once.
//
// SHAPE (rule 14): the resolver a kernel signature carries must not be a type-erasing
// std::function. The three assertions below say that in terms nothing can satisfy by
// accident -- an erasing wrapper is neither trivially copyable nor trivially
// destructible, and it does not fit in three pointers.
//
// ANSWERS: the same file pins what each of the three output kinds actually resolves to,
// so a change of the storage shape cannot quietly change the answers with it.

#include <catch2/catch_test_macros.hpp>
#include <components/compute/kernel_signature.hpp>
#include <core/pmr.hpp>

#include <type_traits>

using components::compute::fixed_t;
using components::compute::output_type;
using components::compute::same_type_resolver;
using components::compute::type_resolver_fn;
using components::types::logical_type;

TEST_CASE("components::compute::output_type::the_resolver_carries_no_erased_state") {
    CAPTURE(sizeof(type_resolver_fn));
    CHECK(std::is_trivially_copyable_v<type_resolver_fn>);
    CHECK(std::is_trivially_destructible_v<type_resolver_fn>);
    CHECK(std::is_nothrow_move_constructible_v<type_resolver_fn>);
    CHECK(sizeof(type_resolver_fn) <= 3 * sizeof(void*));
}

namespace {

    std::pmr::vector<fixed_t> two_inputs(std::pmr::memory_resource* resource) {
        std::pmr::vector<fixed_t> inputs{resource};
        inputs.emplace_back(logical_type::INTEGER);
        inputs.emplace_back(logical_type::DOUBLE);
        return inputs;
    }

} // namespace

TEST_CASE("components::compute::output_type::every_kind_resolves_to_its_own_answer") {
    core::pmr::otterbrix_resource resource;
    const auto inputs = two_inputs(&resource);

    auto fixed = output_type::fixed(fixed_t{logical_type::BIGINT});
    auto fixed_resolved = fixed.resolve(&resource, inputs);
    REQUIRE_FALSE(fixed_resolved.has_error());
    CHECK(fixed_resolved.value().type() == logical_type::BIGINT);
    CHECK(fixed.kind() == output_type::kind_t::fixed_value);

    // The declared-identity form: introspectable, so pg_proc.prorettype can persist it.
    auto same = output_type::same_type_at(1);
    auto same_resolved = same.resolve(&resource, inputs);
    REQUIRE_FALSE(same_resolved.has_error());
    CHECK(same_resolved.value().type() == logical_type::DOUBLE);
    CHECK(same.kind() == output_type::kind_t::same_type_at_index);
    CHECK(same.input_index() == 1u);

    // The SAME runtime answer through the opaque form, which must stay a distinct kind:
    // test_prorettype_channel pins that this one encodes as "c" and not as "s:0".
    auto computed = output_type::computed(same_type_resolver(0));
    auto computed_resolved = computed.resolve(&resource, inputs);
    REQUIRE_FALSE(computed_resolved.has_error());
    CHECK(computed_resolved.value().type() == logical_type::INTEGER);
    CHECK(computed.kind() == output_type::kind_t::custom);
}

TEST_CASE("components::compute::output_type::an_index_past_the_inputs_refuses") {
    core::pmr::otterbrix_resource resource;
    const auto inputs = two_inputs(&resource);

    auto same = output_type::same_type_at(2);
    auto same_resolved = same.resolve(&resource, inputs);
    REQUIRE(same_resolved.has_error());
    CHECK(same_resolved.error().type == core::error_code_t::incorrect_function_argument);

    auto computed = output_type::computed(same_type_resolver(5));
    auto computed_resolved = computed.resolve(&resource, inputs);
    REQUIRE(computed_resolved.has_error());
    CHECK(computed_resolved.error().type == core::error_code_t::incorrect_function_argument);
}

TEST_CASE("components::compute::output_type::a_resolver_that_was_never_set_refuses_loudly") {
    core::pmr::otterbrix_resource resource;
    const auto inputs = two_inputs(&resource);

    // Rule 6: an empty resolver is a caller mistake, and it has to arrive as an error on the
    // channel. std::function answered this with std::bad_function_call.
    auto computed = output_type::computed(type_resolver_fn{});
    auto resolved = computed.resolve(&resource, inputs);
    REQUIRE(resolved.has_error());
    CHECK(resolved.error().type == core::error_code_t::kernel_error);
}
