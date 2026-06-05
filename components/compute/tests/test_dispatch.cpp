#include <catch2/catch.hpp>
#include <components/compute/function.hpp>
#include <components/compute/gpu_aggregate.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {
    struct dummy_state : kernel_state {
        int value;
    };

    core::result_wrapper_t<kernel_state_ptr> dummy_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<dummy_state>();
        c->value = 0;
        return c;
    }

    core::error_t dummy_consume(kernel_context&, const data_chunk_t&) { return core::error_t::no_error(); }

    core::error_t dummy_merge(aggregate_kernel_context&, kernel_state&&, kernel_state&) {
        return core::error_t::no_error();
    }

    core::error_t dummy_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    kernel_signature_t int_signature() {
        return kernel_signature_t(function_type_t::aggregate,
                                  {exact_type_matcher(logical_type::INTEGER)},
                                  {output_type::fixed(logical_type::INTEGER)});
    }
} // namespace

TEST_CASE("components::compute::kernel_target::default_cpu") {
    aggregate_kernel k{int_signature(), dummy_init, dummy_consume, dummy_merge, dummy_finalize};
    REQUIRE(k.target() == kernel_target::cpu);
    REQUIRE_FALSE(k.is_gpu_target());
    REQUIRE(k.target_data() == nullptr);
}

TEST_CASE("components::compute::kernel_target::gpu_factory") {
    auto k = make_gpu_aggregate_kernel(int_signature(),
                                       dummy_init,
                                       dummy_consume,
                                       dummy_merge,
                                       dummy_finalize,
                                       gpu_aggregate_op::sum);
    REQUIRE(k.target() == kernel_target::gpu_opencl);
    REQUIRE(k.is_gpu_target());

    const auto* desc = gpu_aggregate_descriptor_of(k);
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::sum);
    REQUIRE_FALSE(desc->is_custom());
}

TEST_CASE("components::compute::kernel_target::manual_set") {
    aggregate_kernel k{int_signature(), dummy_init, dummy_consume, dummy_merge, dummy_finalize};
    k.set_target(kernel_target::gpu_opencl);
    k.set_target_data(gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op::max));
    REQUIRE(k.is_gpu_target());

    const auto* desc = gpu_aggregate_descriptor_of(k);
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::max);
}

TEST_CASE("components::compute::function::has_kernel_for") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("dispatch_test", arity::unary(), function_doc{}, 2);
    REQUIRE_FALSE(fn->has_kernel_for(kernel_target::cpu));
    REQUIRE_FALSE(fn->has_kernel_for(kernel_target::gpu_opencl));
    REQUIRE_FALSE(fn->has_gpu_kernel());

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});
    REQUIRE(fn->has_kernel_for(kernel_target::cpu));
    REQUIRE_FALSE(fn->has_kernel_for(kernel_target::gpu_opencl));
    REQUIRE_FALSE(fn->has_gpu_kernel());

    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_signature(),
                                                    dummy_init,
                                                    dummy_consume,
                                                    dummy_merge,
                                                    dummy_finalize,
                                                    gpu_aggregate_op::sum));
    REQUIRE(fn->has_kernel_for(kernel_target::cpu));
    REQUIRE(fn->has_kernel_for(kernel_target::gpu_opencl));
    REQUIRE(fn->has_gpu_kernel());
}

TEST_CASE("components::compute::dispatch_best::prefers_cpu_by_default") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("disp_cpu_default", arity::unary(), function_doc{}, 2);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_signature(),
                                                    dummy_init,
                                                    dummy_consume,
                                                    dummy_merge,
                                                    dummy_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types);
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::cpu);
}

TEST_CASE("components::compute::dispatch_best::picks_gpu_when_preferred") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("disp_gpu_pref", arity::unary(), function_doc{}, 2);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_signature(),
                                                    dummy_init,
                                                    dummy_consume,
                                                    dummy_merge,
                                                    dummy_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);

    const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::sum);
}

TEST_CASE("components::compute::dispatch_best::falls_back_to_cpu_when_no_gpu_match") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("disp_fallback", arity::unary(), function_doc{}, 2);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});

    kernel_signature_t bigint_sig(function_type_t::aggregate,
                                  {exact_type_matcher(logical_type::BIGINT)},
                                  {output_type::fixed(logical_type::BIGINT)});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(std::move(bigint_sig),
                                                    dummy_init,
                                                    dummy_consume,
                                                    dummy_merge,
                                                    dummy_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::cpu);
}

TEST_CASE("components::compute::dispatch_best::no_match_returns_error") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("disp_no_match", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::STRING_LITERAL}};
    auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE_FALSE(best());
}

TEST_CASE("components::compute::dispatch_best::arity_mismatch") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("disp_arity", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_signature(),
                                           dummy_init,
                                           dummy_consume,
                                           dummy_merge,
                                           dummy_finalize});

    std::pmr::vector<complex_logical_type> too_many{complex_logical_type{logical_type::INTEGER},
                                                    complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, too_many);
    REQUIRE_FALSE(best());
}

TEST_CASE("components::compute::gpu_aggregate_descriptor::custom_callback") {
    bool callback_called = false;
    auto fn_ptr = [&callback_called](kernel_context&,
                                     gpu_runtime_handle_t&,
                                     const grouped_aggregate_input_t&,
                                     grouped_aggregate_output_t&) {
        callback_called = true;
        return core::error_t::no_error();
    };

    auto desc = gpu_aggregate_descriptor_t::custom(fn_ptr);
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::custom);
    REQUIRE(desc->is_custom());
    REQUIRE(static_cast<bool>(desc->custom_fn()));
}

TEST_CASE("components::compute::gpu_aggregate_descriptor::builtins_are_not_custom") {
    auto sum = gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op::sum);
    REQUIRE_FALSE(sum->is_custom());

    auto count_star = gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op::count_star);
    REQUIRE_FALSE(count_star->is_custom());
    REQUIRE(count_star->op() == gpu_aggregate_op::count_star);
}

TEST_CASE("components::compute::default_functions::register_both_cpu_and_gpu") {
    auto* reg = function_registry_t::get_default();
    REQUIRE(reg != nullptr);

    for (const auto& [name, uid] : reg->get_functions()) {
        auto* fn = reg->get_function(uid);
        REQUIRE(fn != nullptr);
        if (name == "sum" || name == "min" || name == "max" || name == "count" || name == "avg") {
            INFO("Function " << name << " must have a GPU kernel registered");
            REQUIRE(fn->has_gpu_kernel());
        }
    }
}

TEST_CASE("components::compute::default_functions::sum_dispatches_to_gpu") {
    auto* reg = function_registry_t::get_default();
    function* sum_fn = nullptr;
    for (const auto& [name, uid] : reg->get_functions()) {
        if (name == "sum") {
            sum_fn = reg->get_function(uid);
            break;
        }
    }
    REQUIRE(sum_fn != nullptr);

    std::pmr::vector<complex_logical_type> int_types{complex_logical_type{logical_type::INTEGER}};
    auto best = sum_fn->dispatch_best(reg->resource(), int_types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::sum);
}

TEST_CASE("components::compute::default_functions::count_star_gpu_signature") {
    auto* reg = function_registry_t::get_default();
    function* count_fn = nullptr;
    for (const auto& [name, uid] : reg->get_functions()) {
        if (name == "count") {
            count_fn = reg->get_function(uid);
            break;
        }
    }
    REQUIRE(count_fn != nullptr);

    std::pmr::vector<complex_logical_type> no_args{};
    auto best = count_fn->dispatch_best(reg->resource(), no_args, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::count_star);
}

TEST_CASE("components::compute::kernel_target::to_string") {
    REQUIRE(to_string(kernel_target::cpu) == "cpu");
    REQUIRE(to_string(kernel_target::gpu_opencl) == "gpu_opencl");
}

TEST_CASE("components::compute::gpu_aggregate_op::to_string") {
    REQUIRE(to_string(gpu_aggregate_op::sum) == "sum");
    REQUIRE(to_string(gpu_aggregate_op::min) == "min");
    REQUIRE(to_string(gpu_aggregate_op::max) == "max");
    REQUIRE(to_string(gpu_aggregate_op::count) == "count");
    REQUIRE(to_string(gpu_aggregate_op::count_star) == "count_star");
    REQUIRE(to_string(gpu_aggregate_op::avg) == "avg");
    REQUIRE(to_string(gpu_aggregate_op::custom) == "custom");
}
