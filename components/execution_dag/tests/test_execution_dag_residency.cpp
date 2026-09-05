// regression test: an uninitialised pmr member binds to get_default_resource(), not the
// caller's arena. slot_sizes_ is private, so it is measured via a counting resource installed
// as the process default rather than read back directly.

#include <catch2/catch_test_macros.hpp>

#include <components/execution_dag/execution_dag.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <cstdint>
#include <memory_resource>
#include <optional>

using namespace components::execution_dag;

using components::operators::operator_code;
using components::types::logical_type;

namespace {

    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        std::atomic<uint64_t> allocations{0};
        std::atomic<uint64_t> bytes{0};

        void reset() noexcept {
            allocations.store(0, std::memory_order_relaxed);
            bytes.store(0, std::memory_order_relaxed);
        }

    private:
        void* do_allocate(size_t size, size_t align) override {
            allocations.fetch_add(1, std::memory_order_relaxed);
            bytes.fetch_add(size, std::memory_order_relaxed);
            return std::pmr::new_delete_resource()->allocate(size, align);
        }
        void do_deallocate(void* p, size_t size, size_t align) override {
            std::pmr::new_delete_resource()->deallocate(p, size, align);
        }
        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    };

    // leaked on purpose: memory it hands out may be freed long after this window closes
    counting_resource_t& process_default_probe() {
        static counting_resource_t* probe = new counting_resource_t();
        return *probe;
    }

    struct default_resource_window_t final {
        std::pmr::memory_resource* previous;

        explicit default_resource_window_t(std::pmr::memory_resource* probe)
            : previous(std::pmr::set_default_resource(probe)) {}

        ~default_resource_window_t() { std::pmr::set_default_resource(previous); }
    };

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
    const uint64_t after_construction = probe.allocations.load();
    INFO("allocations taken from the process-global default resource while constructing the graph: "
         << after_construction << " (" << probe.bytes.load() << " bytes)");
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
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    CHECK(probe.allocations.load() == 0);
}
