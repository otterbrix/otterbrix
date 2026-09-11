// regression test: an uninitialised pmr member binds to get_default_resource(), not the
// caller's arena. slot_sizes_ is private, so it is measured via a counting resource installed
// as the process default rather than read back directly.

#include <catch2/catch_test_macros.hpp>

#include <components/execution_dag/execution_dag.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <memory_resource>
#include <optional>

using namespace components::execution_dag;

using components::operators::operator_code;
using components::types::logical_type;

namespace {

    using core::pmr::default_resource_window_t;
    using core::pmr::process_default_probe;

} // namespace

TEST_CASE("components::execution_dag::every internal vector lives on the arena the graph named") {
    core::pmr::otterbrix_resource arena;

    auto& probe = process_default_probe();
    probe.reset();

    // constructed inside the window: pmr members bind to the default resource at construction time
    std::optional<execution_dag_t> graph;
    {
        default_resource_window_t window{&probe};
        graph.emplace(&arena);
    }
    const auto after_construction = probe.allocations();
    INFO("allocations taken from the process-global default resource while constructing the graph: "
         << after_construction << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(after_construction == 0);

    auto left = graph->declare_slot();
    graph->bind_input(left, 0, logical_type::BIGINT);
    auto right = graph->declare_slot();
    graph->bind_input(right, 1, logical_type::BIGINT);

    auto node = graph->add_operator(operator_code::add, left, right);
    graph->set_slot_type(graph->output_slot(node), logical_type::BIGINT);
    graph->add_key_slot(left);
    graph->set_output(slot_list_t({graph->output_slot(node)}, &arena));

    REQUIRE(graph->slot_count() == 3);
    CHECK(graph->key_slots().get_allocator().resource() == &arena);
    REQUIRE(graph->key_slots().size() == 1);
    CHECK(graph->key_slots().front() == left);

    // prepare() is where slot_sizes_ takes its one block: assign(slots_.size(), 0) = 3 * 8 bytes.
    probe.reset();
    {
        default_resource_window_t window{&probe};
        REQUIRE_FALSE(graph->prepare().contains_error());
    }

    INFO("allocations taken from the process-global default resource during prepare(): "
         << probe.allocations() << " (" << probe.allocated_bytes() << " bytes)");
    CHECK(probe.allocations() == 0);
}
