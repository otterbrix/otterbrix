#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/param_storage.hpp>
#include <core/pmr.hpp>

using namespace components::logical_plan;

// "Not bound" is not a value, so it must not be spelled as one. The lookup used to return
// `const expr_value_t&` and therefore had to invent an in-domain sentinel
// (types::NULL_LOGICAL_VALUE) built on std::pmr::null_memory_resource(): a logical_value_t
// whose every allocating operation aborts -- including the ERROR path of cast_as, which
// builds its own diagnostic std::pmr::string on that resource. A pointer return puts absence
// back outside the value domain, where the caller must handle it before it can read anything.
TEST_CASE("logical_plan::get_parameter returns nullptr for an unbound id") {
    auto resource = core::pmr::otterbrix_resource();
    storage_parameters storage(&resource);
    add_parameter(storage, core::parameter_id_t(uint16_t(0)), int64_t(7));

    const expr_value_t* bound = get_parameter(&storage, core::parameter_id_t(uint16_t(0)));
    REQUIRE(bound != nullptr);
    CHECK(bound->value<int64_t>() == 7);
    // A bound value always carries a real allocator, never the null resource.
    CHECK(bound->resource() == &resource);

    CHECK(get_parameter(&storage, core::parameter_id_t(uint16_t(1))) == nullptr);
}

TEST_CASE("logical_plan::parameter_node_t::parameter returns nullptr for an unbound id") {
    auto resource = core::pmr::otterbrix_resource();
    auto params = make_parameter_node(&resource);
    const auto id = params->add_parameter(int64_t(42));

    const expr_value_t* bound = params->parameter(id);
    REQUIRE(bound != nullptr);
    CHECK(bound->value<int64_t>() == 42);

    const auto unbound_id = core::parameter_id_t(static_cast<uint16_t>(uint16_t(id) + 1));
    CHECK(params->parameter(unbound_id) == nullptr);
}
