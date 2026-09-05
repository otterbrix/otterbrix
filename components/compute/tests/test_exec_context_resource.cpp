// compute must allocate from the resource its CALLER named, and from nothing else. The
// process-global std::pmr default resource is banned outright (rule 14 lists
// std::pmr::get_default_resource among the forbidden constructs), and it used to be where
// every defaulted `exec_context_t& ctx = default_exec_context()` argument pointed: a
// function-local static exec_context_t built on get_default_resource(), i.e. one process-wide
// arena shared by every caller that did not spell a context out.
//
// The measurement below does not read the code: it installs a counting resource AS the process
// default and counts what compute takes from it while running a function. The only way that
// count is zero is if nothing in the path consults the global default.
//
// The counting resource is immortal on purpose. It is installed as the process default for the
// duration of one call, and anything allocated through it in that window may be freed long
// after the window closes.

#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>

#include <atomic>
#include <memory_resource>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

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

    counting_resource_t& process_default_probe() {
        static counting_resource_t* probe = new counting_resource_t();
        return *probe;
    }

    core::error_t double_it(kernel_context&, const data_chunk_t& in, vector_t& out) {
        for (uint64_t row = 0; row < in.size(); row++) {
            out.data<int>()[row] = in.data[0].data<int>()[row] * 2;
        }
        return core::error_t::no_error();
    }

} // anonymous namespace

TEST_CASE("components::compute::exec_context::executing_a_function_never_touches_the_process_default_resource") {
    // Constructed BEFORE the probe is installed, so its own upstream is new_delete and the
    // count below measures compute's behaviour, not this fixture's.
    core::pmr::otterbrix_resource resource;

    auto fn = std::make_unique<vector_function>("ctx_probe", arity::unary(), function_doc{}, 1);
    kernel_signature_t sig(function_type_t::vector,
                           {parameter_type::exact(logical_type::INTEGER)},
                           {output_type::fixed(logical_type::INTEGER)});
    vector_kernel k(std::move(sig), double_it);
    REQUIRE_FALSE(fn->add_kernel(&resource, std::move(k)).contains_error());

    data_chunk_t chunk(&resource, {logical_type::INTEGER});
    chunk.set_value(0, 0, 21);
    chunk.set_cardinality(1);

    auto& probe = process_default_probe();
    probe.reset();

    exec_context_t ctx(&resource);
    std::pmr::set_default_resource(&probe);
    auto res = fn->execute(chunk, nullptr, ctx);
    std::pmr::set_default_resource(std::pmr::new_delete_resource());

    REQUIRE_FALSE(res.has_error());
    REQUIRE(std::get<data_chunk_t>(res.value()).data[0].data<int>()[0] == 42);

    INFO("allocations taken from the process-global default resource: "
         << probe.allocations.load() << " (" << probe.bytes.load() << " bytes)");
    REQUIRE(probe.allocations.load() == 0);
}
