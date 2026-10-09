// Guards against a regression to a process-global default resource (get_default_resource() is
// banned; a removed default_exec_context() used to fall back to it). Measured,
// not read from the code: a counting resource is installed as the process default and must see
// zero allocations while compute runs. It is deliberately leaked -- it stays installed only for
// one call, but frees from that window can land after the window closes.

#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/counting_resource.hpp>
#include <core/pmr.hpp>

#include <memory_resource>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    using core::pmr::default_resource_window_t;
    using core::pmr::process_default_probe;

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

    auto fn = core::pmr::make_polymorphic_unique<vector_function>(&resource,
                                                                  "ctx_probe",
                                                                  arity::unary(),
                                                                  function_doc{&resource},
                                                                  size_t{1});
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
    auto res = [&] {
        default_resource_window_t window{&probe};
        return fn->execute(chunk, nullptr, ctx);
    }();

    REQUIRE_FALSE(res.has_error());
    REQUIRE(res.value().data[0].data<int>()[0] == 42);

    INFO("allocations taken from the process-global default resource: " << probe.allocations() << " ("
                                                                        << probe.allocated_bytes() << " bytes)");
    REQUIRE(probe.allocations() == 0);
}

TEST_CASE("components::compute::function::a_copy_takes_and_gives_back_only_through_the_resource_it_is_given") {
    core::pmr::otterbrix_resource home;
    function_registry_t registry(&home);
    register_default_functions(registry);
    REQUIRE_FALSE(registry.builtin_registration_error().contains_error());

    core::pmr::counting_resource_t target{std::pmr::new_delete_resource()};
    auto& probe = process_default_probe();
    probe.reset();

    size_t copied = 0;
    {
        default_resource_window_t window{&probe};
        for (const auto& [name, uid] : DEFAULT_FUNCTIONS) {
            auto copy = registry.get_function(uid)->get_copy(&target);
            if (copy != nullptr && copy->name() == name && copy->num_kernels() > 0) {
                ++copied;
            }
        }
    }

    REQUIRE(copied == DEFAULT_FUNCTIONS.size());
    INFO("allocations taken from the process-global default resource: " << probe.allocations());
    REQUIRE(probe.allocations() == 0);
    REQUIRE(target.allocations() > 0);
    REQUIRE(target.outstanding() == 0);
}
