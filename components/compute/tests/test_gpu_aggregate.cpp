#include <catch2/catch.hpp>
#include <components/compute/function.hpp>
#include <components/compute/gpu_aggregate.hpp>

using namespace components::compute;
using namespace components::types;
using namespace components::vector;

namespace {

    struct gpu_state : kernel_state {
        int value{0};
    };

    core::result_wrapper_t<kernel_state_ptr> gpu_init(kernel_context&, kernel_init_args) {
        auto c = std::make_unique<gpu_state>();
        c->value = 0;
        return c;
    }

    core::error_t gpu_consume(kernel_context&, const data_chunk_t&) { return core::error_t::no_error(); }
    core::error_t gpu_merge(aggregate_kernel_context&, kernel_state&&, kernel_state&) {
        return core::error_t::no_error();
    }
    core::error_t gpu_finalize(aggregate_kernel_context&) { return core::error_t::no_error(); }

    kernel_signature_t int_sig() {
        return kernel_signature_t(function_type_t::aggregate,
                                  {exact_type_matcher(logical_type::INTEGER)},
                                  {output_type::fixed(logical_type::INTEGER)});
    }

    kernel_signature_t bigint_sig() {
        return kernel_signature_t(function_type_t::aggregate,
                                  {exact_type_matcher(logical_type::BIGINT)},
                                  {output_type::fixed(logical_type::BIGINT)});
    }

    kernel_signature_t double_sig() {
        return kernel_signature_t(function_type_t::aggregate,
                                  {exact_type_matcher(logical_type::DOUBLE)},
                                  {output_type::fixed(logical_type::DOUBLE)});
    }

    aggregate_function* find_default(const std::string& name) {
        auto* reg = function_registry_t::get_default();
        for (const auto& [n, uid] : reg->get_functions()) {
            if (n == name) {
                return dynamic_cast<aggregate_function*>(reg->get_function(uid));
            }
        }
        return nullptr;
    }

} // namespace

TEST_CASE("components::compute::gpu_aggregate_op::roundtrip_builtin_for_each") {
    constexpr gpu_aggregate_op ops[] = {gpu_aggregate_op::sum,
                                        gpu_aggregate_op::min,
                                        gpu_aggregate_op::max,
                                        gpu_aggregate_op::count,
                                        gpu_aggregate_op::count_star,
                                        gpu_aggregate_op::avg};
    for (auto op : ops) {
        auto desc = gpu_aggregate_descriptor_t::builtin(op);
        REQUIRE(desc != nullptr);
        REQUIRE(desc->op() == op);
        REQUIRE_FALSE(desc->is_custom());
        REQUIRE_FALSE(static_cast<bool>(desc->custom_fn()));
        REQUIRE(to_string(op) != "unknown");
    }
}

TEST_CASE("components::compute::gpu_aggregate_op::to_string_unknown") {
    auto bogus = static_cast<gpu_aggregate_op>(0xFE);
    REQUIRE(to_string(bogus) == "unknown");
}

TEST_CASE("components::compute::kernel_target::to_string_unknown") {
    auto bogus = static_cast<kernel_target>(0xFE);
    REQUIRE(to_string(bogus) == "unknown");
}

TEST_CASE("components::compute::gpu_aggregate_descriptor::ctor_op_only") {
    gpu_aggregate_descriptor_t desc{gpu_aggregate_op::avg};
    REQUIRE(desc.op() == gpu_aggregate_op::avg);
    REQUIRE_FALSE(desc.is_custom());
    REQUIRE_FALSE(static_cast<bool>(desc.custom_fn()));
}

TEST_CASE("components::compute::gpu_aggregate_descriptor::ctor_custom_with_callback") {
    int call_count = 0;
    grouped_aggregate_gpu_fn fn = [&](kernel_context&,
                                      gpu_runtime_handle_t&,
                                      const grouped_aggregate_input_t&,
                                      grouped_aggregate_output_t&) {
        ++call_count;
        return core::error_t::no_error();
    };

    gpu_aggregate_descriptor_t desc{gpu_aggregate_op::custom, std::move(fn)};
    REQUIRE(desc.op() == gpu_aggregate_op::custom);
    REQUIRE(desc.is_custom());
    REQUIRE(static_cast<bool>(desc.custom_fn()));

    aggregate_kernel kernel{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    kernel_context ctx{default_exec_context(), kernel};
    struct dummy_runtime : gpu_runtime_handle_t {};
    dummy_runtime rt;
    grouped_aggregate_input_t input{};
    grouped_aggregate_output_t output{};
    REQUIRE_FALSE(desc.custom_fn()(ctx, rt, input, output).contains_error());
    REQUIRE(call_count == 1);
}

TEST_CASE("components::compute::make_gpu_aggregate_kernel::custom_fn_factory") {
    bool triggered = false;
    grouped_aggregate_gpu_fn cb = [&](kernel_context&,
                                      gpu_runtime_handle_t&,
                                      const grouped_aggregate_input_t&,
                                      grouped_aggregate_output_t&) {
        triggered = true;
        return core::error_t::no_error();
    };
    auto kernel = make_gpu_aggregate_kernel(int_sig(),
                                            gpu_init,
                                            gpu_consume,
                                            gpu_merge,
                                            gpu_finalize,
                                            std::move(cb));
    REQUIRE(kernel.target() == kernel_target::gpu_opencl);
    REQUIRE(kernel.is_gpu_target());

    const auto* desc = gpu_aggregate_descriptor_of(kernel);
    REQUIRE(desc != nullptr);
    REQUIRE(desc->is_custom());
    REQUIRE(desc->op() == gpu_aggregate_op::custom);
    REQUIRE(static_cast<bool>(desc->custom_fn()));

    kernel_context ctx{default_exec_context(), kernel};
    struct dummy_runtime : gpu_runtime_handle_t {};
    dummy_runtime rt;
    grouped_aggregate_input_t input{};
    grouped_aggregate_output_t output{};
    REQUIRE_FALSE(desc->custom_fn()(ctx, rt, input, output).contains_error());
    REQUIRE(triggered);
}

TEST_CASE("components::compute::gpu_aggregate_descriptor_of::cpu_kernel_returns_null") {
    aggregate_kernel k{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    REQUIRE(k.target() == kernel_target::cpu);
    REQUIRE(gpu_aggregate_descriptor_of(k) == nullptr);
}

TEST_CASE("components::compute::gpu_aggregate_descriptor_of::gpu_kernel_without_descriptor_returns_null") {
    aggregate_kernel k{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    k.set_target(kernel_target::gpu_opencl);
    REQUIRE(k.is_gpu_target());
    REQUIRE(k.target_data() == nullptr);
    REQUIRE(gpu_aggregate_descriptor_of(k) == nullptr);
}

TEST_CASE("components::compute::gpu_aggregate_descriptor_of::unrelated_target_data_returns_null") {
    struct alien_target_data : kernel_target_data_t {};
    aggregate_kernel k{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    k.set_target(kernel_target::gpu_opencl);
    k.set_target_data(std::make_shared<alien_target_data>());
    REQUIRE(k.target_data() != nullptr);
    REQUIRE(gpu_aggregate_descriptor_of(k) == nullptr);
}

TEST_CASE("components::compute::prefer_cpu_only::all_slots_are_cpu") {
    auto pref = prefer_cpu_only();
    REQUIRE(pref.size() == kernel_target_count);
    for (auto t : pref) {
        REQUIRE(t == kernel_target::cpu);
    }
}

TEST_CASE("components::compute::prefer_gpu_first::orders_gpu_before_cpu") {
    auto pref = prefer_gpu_first();
    REQUIRE(pref.size() == kernel_target_count);
    REQUIRE(pref[0] == kernel_target::gpu_opencl);
    REQUIRE(pref[1] == kernel_target::cpu);
}

TEST_CASE("components::compute::dispatch_best::prefer_cpu_only_with_only_gpu_kernel_returns_error") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("only_gpu", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types);
    REQUIRE_FALSE(best());
    REQUIRE(best.error().type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::dispatch_best::prefer_gpu_first_with_only_gpu_kernel_picks_gpu") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("solo_gpu", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::min));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::min);
}

TEST_CASE("components::compute::dispatch_best::picks_first_gpu_when_multiple_gpu_kernels_match") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("dup_gpu", arity::unary(), function_doc{}, 3);

    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::max));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
    REQUIRE(desc != nullptr);
    REQUIRE(desc->op() == gpu_aggregate_op::sum);
}

TEST_CASE("components::compute::dispatch_best::distinct_gpu_sigs_dispatched_per_input_type") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("per_type_gpu", arity::unary(), function_doc{}, 4);

    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(bigint_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::max));
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(double_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::avg));

    auto pref = prefer_gpu_first();

    {
        std::pmr::vector<complex_logical_type> t{complex_logical_type{logical_type::INTEGER}};
        auto best = fn->dispatch_best(resource, t, pref);
        REQUIRE(best());
        REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::sum);
    }
    {
        std::pmr::vector<complex_logical_type> t{complex_logical_type{logical_type::BIGINT}};
        auto best = fn->dispatch_best(resource, t, pref);
        REQUIRE(best());
        REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::max);
    }
    {
        std::pmr::vector<complex_logical_type> t{complex_logical_type{logical_type::DOUBLE}};
        auto best = fn->dispatch_best(resource, t, pref);
        REQUIRE(best());
        REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::avg);
    }
}

TEST_CASE("components::compute::dispatch_exact::returns_first_matching_kernel") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("exact_pick", arity::unary(), function_doc{}, 2);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto exact = fn->dispatch_exact(resource, types);
    REQUIRE(exact());
    REQUIRE(exact.value().get().target() == kernel_target::cpu);
}

TEST_CASE("components::compute::dispatch_exact::gpu_only_function_returns_gpu") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("exact_gpu_only", arity::unary(), function_doc{}, 1);
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::max));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto exact = fn->dispatch_exact(resource, types);
    REQUIRE(exact());
    REQUIRE(exact.value().get().target() == kernel_target::gpu_opencl);
    REQUIRE(gpu_aggregate_descriptor_of(exact.value().get())->op() == gpu_aggregate_op::max);
}

TEST_CASE("components::compute::dispatch_exact::no_matching_signature_returns_error") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("exact_no_match", arity::unary(), function_doc{}, 1);
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::DOUBLE}};
    auto exact = fn->dispatch_exact(resource, types);
    REQUIRE_FALSE(exact());
    REQUIRE(exact.error().type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::function::has_gpu_kernel::only_gpu_added") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("g_only", arity::unary(), function_doc{}, 1);
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));
    REQUIRE_FALSE(fn->has_kernel_for(kernel_target::cpu));
    REQUIRE(fn->has_kernel_for(kernel_target::gpu_opencl));
    REQUIRE(fn->has_gpu_kernel());
}

TEST_CASE("components::compute::aggregate_function::gpu_kernel_overflow_returns_error") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("overflow", arity::unary(), function_doc{}, 2);

    REQUIRE_FALSE(fn->add_kernel(resource,
                                 aggregate_kernel{int_sig(),
                                                  gpu_init,
                                                  gpu_consume,
                                                  gpu_merge,
                                                  gpu_finalize})
                      .contains_error());
    REQUIRE_FALSE(fn->add_kernel(resource,
                                 make_gpu_aggregate_kernel(int_sig(),
                                                           gpu_init,
                                                           gpu_consume,
                                                           gpu_merge,
                                                           gpu_finalize,
                                                           gpu_aggregate_op::sum))
                      .contains_error());

    auto extra = fn->add_kernel(resource,
                                make_gpu_aggregate_kernel(bigint_sig(),
                                                          gpu_init,
                                                          gpu_consume,
                                                          gpu_merge,
                                                          gpu_finalize,
                                                          gpu_aggregate_op::max));
    REQUIRE(extra.contains_error());
    REQUIRE(extra.type == core::error_code_t::kernel_error);
}

TEST_CASE("components::compute::aggregate_function::get_copy_preserves_gpu_kernel") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("copy_me", arity::unary(), function_doc{}, 2);
    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));

    auto copy = fn->get_copy(resource);
    REQUIRE(copy != nullptr);
    REQUIRE(copy->has_kernel_for(kernel_target::cpu));
    REQUIRE(copy->has_gpu_kernel());

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};
    auto best = copy->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::sum);
}

TEST_CASE("components::compute::aggregate_function::get_signatures_includes_gpu") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("sigs", arity::unary(), function_doc{}, 2);
    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(bigint_sig(),
                                                    gpu_init,
                                                    gpu_consume,
                                                    gpu_merge,
                                                    gpu_finalize,
                                                    gpu_aggregate_op::sum));
    REQUIRE(fn->get_signatures().size() == 2);
    REQUIRE(fn->num_kernels() == 2);
}

TEST_CASE("components::compute::default_functions::numeric_aggregates_all_dispatch_to_gpu") {
    auto* reg = function_registry_t::get_default();
    auto* resource = reg->resource();

    auto check = [&](const std::string& name, logical_type t, gpu_aggregate_op expected_op) {
        auto* fn = find_default(name);
        REQUIRE(fn != nullptr);
        std::pmr::vector<complex_logical_type> types{complex_logical_type{t}};
        auto best = fn->dispatch_best(resource, types, prefer_gpu_first());
        INFO("dispatch_best failed for " << name << " over type_id=" << static_cast<int>(t));
        REQUIRE(best());
        REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
        const auto* desc = gpu_aggregate_descriptor_of(best.value().get());
        REQUIRE(desc != nullptr);
        REQUIRE(desc->op() == expected_op);
    };

    check("sum", logical_type::INTEGER, gpu_aggregate_op::sum);
    check("sum", logical_type::BIGINT, gpu_aggregate_op::sum);
    check("sum", logical_type::DOUBLE, gpu_aggregate_op::sum);

    check("min", logical_type::INTEGER, gpu_aggregate_op::min);
    check("min", logical_type::DOUBLE, gpu_aggregate_op::min);

    check("max", logical_type::BIGINT, gpu_aggregate_op::max);
    check("max", logical_type::FLOAT, gpu_aggregate_op::max);

    check("count", logical_type::INTEGER, gpu_aggregate_op::count);
    check("count", logical_type::STRING_LITERAL, gpu_aggregate_op::count);

    check("avg", logical_type::INTEGER, gpu_aggregate_op::avg);
    check("avg", logical_type::DOUBLE, gpu_aggregate_op::avg);
}

TEST_CASE("components::compute::default_functions::sum_rejects_string_input") {
    auto* reg = function_registry_t::get_default();
    auto* fn = find_default("sum");
    REQUIRE(fn != nullptr);

    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::STRING_LITERAL}};
    auto best = fn->dispatch_best(reg->resource(), types, prefer_gpu_first());
    REQUIRE_FALSE(best());
}

TEST_CASE("components::compute::default_functions::count_nullary_dispatches_count_star") {
    auto* reg = function_registry_t::get_default();
    auto* fn = find_default("count");
    REQUIRE(fn != nullptr);
    REQUIRE(fn->fn_arity().varargs);

    std::pmr::vector<complex_logical_type> no_args{};
    auto best = fn->dispatch_best(reg->resource(), no_args, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::count_star);
}

TEST_CASE("components::compute::default_functions::count_unary_dispatches_count") {
    auto* reg = function_registry_t::get_default();
    auto* fn = find_default("count");
    REQUIRE(fn != nullptr);

    std::pmr::vector<complex_logical_type> one_arg{complex_logical_type{logical_type::INTEGER}};
    auto best = fn->dispatch_best(reg->resource(), one_arg, prefer_gpu_first());
    REQUIRE(best());
    REQUIRE(best.value().get().target() == kernel_target::gpu_opencl);
    REQUIRE(gpu_aggregate_descriptor_of(best.value().get())->op() == gpu_aggregate_op::count);
}

TEST_CASE("components::compute::gpu_aggregate_descriptor::shareable_across_kernels") {
    auto shared_desc = gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op::avg);

    aggregate_kernel k1{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    k1.set_target(kernel_target::gpu_opencl);
    k1.set_target_data(shared_desc);

    aggregate_kernel k2{bigint_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    k2.set_target(kernel_target::gpu_opencl);
    k2.set_target_data(shared_desc);

    const auto* d1 = gpu_aggregate_descriptor_of(k1);
    const auto* d2 = gpu_aggregate_descriptor_of(k2);
    REQUIRE(d1 != nullptr);
    REQUIRE(d2 != nullptr);
    REQUIRE(d1 == d2);
    REQUIRE(d1->op() == gpu_aggregate_op::avg);
}

TEST_CASE("components::compute::compute_kernel::target_toggling") {
    aggregate_kernel k{int_sig(), gpu_init, gpu_consume, gpu_merge, gpu_finalize};
    REQUIRE(k.target() == kernel_target::cpu);
    REQUIRE_FALSE(k.is_gpu_target());

    k.set_target(kernel_target::gpu_opencl);
    k.set_target_data(gpu_aggregate_descriptor_t::builtin(gpu_aggregate_op::sum));
    REQUIRE(k.is_gpu_target());
    REQUIRE(gpu_aggregate_descriptor_of(k) != nullptr);

    k.set_target(kernel_target::cpu);
    REQUIRE_FALSE(k.is_gpu_target());
    REQUIRE(gpu_aggregate_descriptor_of(k) == nullptr);
}

TEST_CASE("components::compute::kernel_target_count::matches_preference_size") {
    REQUIRE(kernel_target_count == 2);
    REQUIRE(prefer_cpu_only().size() == kernel_target_count);
    REQUIRE(prefer_gpu_first().size() == kernel_target_count);
}

namespace {

    kernel_signature_t numeric_unary_int_sig() {
        return kernel_signature_t(function_type_t::aggregate,
                                  {numeric_types_matcher()},
                                  {output_type::fixed(logical_type::BIGINT)});
    }

    struct sum_state : kernel_state {
        int64_t value{0};
    };

    core::result_wrapper_t<kernel_state_ptr> sum_init(kernel_context&, kernel_init_args) {
        auto s = std::make_unique<sum_state>();
        s->value = 0;
        return s;
    }

    core::error_t sum_int32_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<sum_state*>(ctx.state());
        const auto* data = in.data[0].data<int32_t>();
        for (uint64_t i = 0; i < in.size(); ++i) {
            acc->value += static_cast<int64_t>(data[i]);
        }
        return core::error_t::no_error();
    }

    core::error_t sum_int64_consume(kernel_context& ctx, const data_chunk_t& in) {
        auto* acc = static_cast<sum_state*>(ctx.state());
        const auto* data = in.data[0].data<int64_t>();
        for (uint64_t i = 0; i < in.size(); ++i) {
            acc->value += data[i];
        }
        return core::error_t::no_error();
    }

    core::error_t sum_merge_push(aggregate_kernel_context& ctx, kernel_state&& from, kernel_state&) {
        ctx.batch_results.emplace_back(ctx.batch_results.get_allocator().resource(),
                                       static_cast<sum_state&>(from).value);
        return core::error_t::no_error();
    }

    core::error_t sum_finalize_noop(aggregate_kernel_context&) { return core::error_t::no_error(); }

} // namespace

TEST_CASE("components::compute::cpu_fallback::gpu_signature_narrower_than_cpu_routes_to_cpu") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("fb_wide_cpu", arity::unary(), function_doc{}, 2);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{numeric_unary_int_sig(),
                                           sum_init,
                                           sum_int32_consume,
                                           sum_merge_push,
                                           sum_finalize_noop});
    (void) fn->add_kernel(resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    sum_init,
                                                    sum_int32_consume,
                                                    sum_merge_push,
                                                    sum_finalize_noop,
                                                    gpu_aggregate_op::sum));

    std::pmr::vector<complex_logical_type> int_types{complex_logical_type{logical_type::INTEGER}};
    auto best_int = fn->dispatch_best(resource, int_types, prefer_gpu_first());
    REQUIRE(best_int());
    REQUIRE(best_int.value().get().target() == kernel_target::gpu_opencl);

    std::pmr::vector<complex_logical_type> double_types{complex_logical_type{logical_type::DOUBLE}};
    auto best_double = fn->dispatch_best(resource, double_types, prefer_gpu_first());
    REQUIRE(best_double());
    REQUIRE(best_double.value().get().target() == kernel_target::cpu);
    REQUIRE(gpu_aggregate_descriptor_of(best_double.value().get()) == nullptr);
}

TEST_CASE("components::compute::cpu_fallback::function_execute_runs_fallback_consume_on_gpu_kernel") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("fb_exec", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(&resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    sum_init,
                                                    sum_int32_consume,
                                                    sum_merge_push,
                                                    sum_finalize_noop,
                                                    gpu_aggregate_op::sum));

    data_chunk_t chunk(&resource, {logical_type::INTEGER}, 4);
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 7));
    chunk.set_value(0, 1, logical_value_t(chunk.resource(), 11));
    chunk.set_value(0, 2, logical_value_t(chunk.resource(), 13));
    chunk.set_value(0, 3, logical_value_t(chunk.resource(), 17));
    chunk.set_cardinality(4);

    auto res = fn->execute(chunk);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int64_t>() == 48);
}

TEST_CASE("components::compute::cpu_fallback::function_execute_runs_fallback_on_batched_chunks") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("fb_batched", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(&resource,
                          make_gpu_aggregate_kernel(int_sig(),
                                                    sum_init,
                                                    sum_int32_consume,
                                                    sum_merge_push,
                                                    sum_finalize_noop,
                                                    gpu_aggregate_op::sum));

    std::vector<data_chunk_t> batch;
    {
        data_chunk_t c(&resource, {logical_type::INTEGER}, 3);
        c.set_value(0, 0, logical_value_t(c.resource(), 1));
        c.set_value(0, 1, logical_value_t(c.resource(), 2));
        c.set_value(0, 2, logical_value_t(c.resource(), 3));
        c.set_cardinality(3);
        batch.emplace_back(std::move(c));
    }
    {
        data_chunk_t c(&resource, {logical_type::INTEGER}, 2);
        c.set_value(0, 0, logical_value_t(c.resource(), 100));
        c.set_value(0, 1, logical_value_t(c.resource(), 200));
        c.set_cardinality(2);
        batch.emplace_back(std::move(c));
    }

    auto res = fn->execute(batch);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 2);
    REQUIRE(vals[0].value<int64_t>() == 6);
    REQUIRE(vals[1].value<int64_t>() == 300);
}

TEST_CASE("components::compute::cpu_fallback::strict_fallback_dispatch_when_gpu_kernel_absent") {
    auto* resource = std::pmr::get_default_resource();
    auto fn = std::make_unique<aggregate_function>("fb_nogpu", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(resource,
                          aggregate_kernel{int_sig(),
                                           sum_init,
                                           sum_int32_consume,
                                           sum_merge_push,
                                           sum_finalize_noop});

    REQUIRE_FALSE(fn->has_gpu_kernel());
    std::pmr::vector<complex_logical_type> types{complex_logical_type{logical_type::INTEGER}};

    auto cpu_pref = fn->dispatch_best(resource, types, prefer_cpu_only());
    REQUIRE(cpu_pref());
    REQUIRE(cpu_pref.value().get().target() == kernel_target::cpu);

    auto gpu_pref = fn->dispatch_best(resource, types, prefer_gpu_first());
    REQUIRE(gpu_pref());
    REQUIRE(gpu_pref.value().get().target() == kernel_target::cpu);
    REQUIRE(gpu_aggregate_descriptor_of(gpu_pref.value().get()) == nullptr);
}

TEST_CASE("components::compute::cpu_fallback::default_sum_executes_via_fallback_for_int32") {
    std::pmr::synchronized_pool_resource resource;
    auto* reg = function_registry_t::get_default();
    auto* sum_fn = find_default("sum");
    REQUIRE(sum_fn != nullptr);
    (void) reg;

    data_chunk_t chunk(&resource, {logical_type::INTEGER}, 5);
    chunk.set_value(0, 0, logical_value_t(chunk.resource(), 1));
    chunk.set_value(0, 1, logical_value_t(chunk.resource(), 2));
    chunk.set_value(0, 2, logical_value_t(chunk.resource(), 3));
    chunk.set_value(0, 3, logical_value_t(chunk.resource(), 4));
    chunk.set_value(0, 4, logical_value_t(chunk.resource(), 5));
    chunk.set_cardinality(5);

    exec_context_t ctx{&resource};
    auto res = sum_fn->execute(chunk, nullptr, ctx);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int32_t>() == 15);
}

namespace {

    struct gpu_query_result_t {
        std::pmr::vector<logical_value_t> values;
        std::pmr::vector<uint64_t> counts;
        explicit gpu_query_result_t(std::pmr::memory_resource* r)
            : values(r)
            , counts(r) {}
    };

    grouped_aggregate_gpu_fn make_sum_per_group_callback() {
        return [](kernel_context&,
                  gpu_runtime_handle_t&,
                  const grouped_aggregate_input_t& input,
                  grouped_aggregate_output_t& output) -> core::error_t {
            if (input.input == nullptr || input.group_ids == nullptr) {
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"null input", std::pmr::get_default_resource()});
            }
            if (output.values == nullptr) {
                return core::error_t(core::error_code_t::kernel_error,
                                     std::pmr::string{"null output", std::pmr::get_default_resource()});
            }
            auto* res = output.values->get_allocator().resource();
            std::vector<int64_t> sums(input.group_count, 0);
            std::vector<uint64_t> cnts(input.group_count, 0);
            const auto* data = input.input->data<int32_t>();
            for (uint64_t i = 0; i < input.row_count; ++i) {
                auto g = input.group_ids[i];
                if (g == UINT32_MAX || g >= input.group_count) {
                    continue;
                }
                sums[g] += static_cast<int64_t>(data[i]);
                cnts[g] += 1;
            }
            output.values->clear();
            output.values->reserve(input.group_count);
            for (size_t g = 0; g < input.group_count; ++g) {
                output.values->emplace_back(res, sums[g]);
            }
            if (output.counts != nullptr) {
                output.counts->assign(cnts.begin(), cnts.end());
            }
            return core::error_t::no_error();
        };
    }

    aggregate_kernel make_sum_gpu_query_kernel() {
        return make_gpu_aggregate_kernel(int_sig(),
                                         sum_init,
                                         sum_int32_consume,
                                         sum_merge_push,
                                         sum_finalize_noop,
                                         make_sum_per_group_callback());
    }

    struct fake_runtime : gpu_runtime_handle_t {};

} // namespace

TEST_CASE("components::compute::gpu_query::custom_callback_small_input_multiple_groups") {
    std::pmr::synchronized_pool_resource resource;
    auto kernel = make_sum_gpu_query_kernel();
    const auto* desc = gpu_aggregate_descriptor_of(kernel);
    REQUIRE(desc != nullptr);
    REQUIRE(desc->is_custom());

    exec_context_t exec_ctx{&resource};
    kernel_context ctx{exec_ctx, kernel};
    fake_runtime rt;

    constexpr uint64_t row_count = 5;
    vector_t input_vec(&resource, complex_logical_type{logical_type::INTEGER}, row_count);
    auto* data = input_vec.data<int32_t>();
    data[0] = 10;
    data[1] = 20;
    data[2] = 30;
    data[3] = 40;
    data[4] = 50;

    const uint32_t group_ids[row_count] = {0, 1, 0, 1, 0};

    grouped_aggregate_input_t input{};
    input.input = &input_vec;
    input.group_ids = group_ids;
    input.row_count = row_count;
    input.group_count = 2;

    gpu_query_result_t out(&resource);
    grouped_aggregate_output_t output{};
    output.values = &out.values;
    output.counts = &out.counts;

    auto status = desc->custom_fn()(ctx, rt, input, output);
    REQUIRE_FALSE(status.contains_error());
    REQUIRE(out.values.size() == 2);
    REQUIRE(out.counts.size() == 2);
    REQUIRE(out.values[0].value<int64_t>() == 90);
    REQUIRE(out.values[1].value<int64_t>() == 60);
    REQUIRE(out.counts[0] == 3);
    REQUIRE(out.counts[1] == 2);
}

TEST_CASE("components::compute::gpu_query::custom_callback_handles_inactive_group_id_sentinel") {
    std::pmr::synchronized_pool_resource resource;
    auto kernel = make_sum_gpu_query_kernel();
    const auto* desc = gpu_aggregate_descriptor_of(kernel);
    REQUIRE(desc != nullptr);

    exec_context_t exec_ctx{&resource};
    kernel_context ctx{exec_ctx, kernel};
    fake_runtime rt;

    constexpr uint64_t row_count = 6;
    vector_t input_vec(&resource, complex_logical_type{logical_type::INTEGER}, row_count);
    auto* data = input_vec.data<int32_t>();
    data[0] = 1;
    data[1] = 2;
    data[2] = 3;
    data[3] = 4;
    data[4] = 5;
    data[5] = 6;

    const uint32_t group_ids[row_count] = {0, UINT32_MAX, 0, UINT32_MAX, 0, 0};

    grouped_aggregate_input_t input{};
    input.input = &input_vec;
    input.group_ids = group_ids;
    input.row_count = row_count;
    input.group_count = 1;

    gpu_query_result_t out(&resource);
    grouped_aggregate_output_t output{};
    output.values = &out.values;
    output.counts = &out.counts;

    auto status = desc->custom_fn()(ctx, rt, input, output);
    REQUIRE_FALSE(status.contains_error());
    REQUIRE(out.values.size() == 1);
    REQUIRE(out.values[0].value<int64_t>() == 15);
    REQUIRE(out.counts[0] == 4);
}

TEST_CASE("components::compute::gpu_query::custom_callback_one_million_rows_single_group") {
    std::pmr::synchronized_pool_resource resource;
    auto kernel = make_sum_gpu_query_kernel();
    const auto* desc = gpu_aggregate_descriptor_of(kernel);
    REQUIRE(desc != nullptr);

    exec_context_t exec_ctx{&resource};
    kernel_context ctx{exec_ctx, kernel};
    fake_runtime rt;

    constexpr uint64_t row_count = 1'000'000;
    vector_t input_vec(&resource, complex_logical_type{logical_type::INTEGER}, row_count);
    auto* data = input_vec.data<int32_t>();
    for (uint64_t i = 0; i < row_count; ++i) {
        data[i] = static_cast<int32_t>(i % 1000);
    }

    std::vector<uint32_t> group_ids(row_count, 0u);

    grouped_aggregate_input_t input{};
    input.input = &input_vec;
    input.group_ids = group_ids.data();
    input.row_count = row_count;
    input.group_count = 1;

    gpu_query_result_t out(&resource);
    grouped_aggregate_output_t output{};
    output.values = &out.values;
    output.counts = &out.counts;

    auto status = desc->custom_fn()(ctx, rt, input, output);
    REQUIRE_FALSE(status.contains_error());

    int64_t expected = 0;
    for (uint64_t i = 0; i < row_count; ++i) {
        expected += static_cast<int64_t>(i % 1000);
    }
    REQUIRE(out.values.size() == 1);
    REQUIRE(out.values[0].value<int64_t>() == expected);
    REQUIRE(out.counts[0] == row_count);
}

TEST_CASE("components::compute::gpu_query::custom_callback_one_million_rows_many_groups") {
    std::pmr::synchronized_pool_resource resource;
    auto kernel = make_sum_gpu_query_kernel();
    const auto* desc = gpu_aggregate_descriptor_of(kernel);
    REQUIRE(desc != nullptr);

    exec_context_t exec_ctx{&resource};
    kernel_context ctx{exec_ctx, kernel};
    fake_runtime rt;

    constexpr uint64_t row_count = 1'000'000;
    constexpr size_t group_count = 8;
    vector_t input_vec(&resource, complex_logical_type{logical_type::INTEGER}, row_count);
    auto* data = input_vec.data<int32_t>();
    std::vector<uint32_t> group_ids(row_count);
    std::vector<int64_t> expected(group_count, 0);
    std::vector<uint64_t> expected_counts(group_count, 0);
    for (uint64_t i = 0; i < row_count; ++i) {
        const auto v = static_cast<int32_t>(i % 100);
        const auto g = static_cast<uint32_t>(i % group_count);
        data[i] = v;
        group_ids[i] = g;
        expected[g] += static_cast<int64_t>(v);
        expected_counts[g] += 1;
    }

    grouped_aggregate_input_t input{};
    input.input = &input_vec;
    input.group_ids = group_ids.data();
    input.row_count = row_count;
    input.group_count = group_count;

    gpu_query_result_t out(&resource);
    grouped_aggregate_output_t output{};
    output.values = &out.values;
    output.counts = &out.counts;

    auto status = desc->custom_fn()(ctx, rt, input, output);
    REQUIRE_FALSE(status.contains_error());

    REQUIRE(out.values.size() == group_count);
    REQUIRE(out.counts.size() == group_count);
    for (size_t g = 0; g < group_count; ++g) {
        REQUIRE(out.values[g].value<int64_t>() == expected[g]);
        REQUIRE(out.counts[g] == expected_counts[g]);
    }
}

TEST_CASE("components::compute::gpu_query::fallback_consume_processes_one_million_rows") {
    std::pmr::synchronized_pool_resource resource;
    auto fn = std::make_unique<aggregate_function>("big_fallback", arity::unary(), function_doc{}, 1);

    (void) fn->add_kernel(&resource,
                          make_gpu_aggregate_kernel(bigint_sig(),
                                                    sum_init,
                                                    sum_int64_consume,
                                                    sum_merge_push,
                                                    sum_finalize_noop,
                                                    gpu_aggregate_op::sum));

    constexpr uint64_t row_count = 1'000'000;
    data_chunk_t chunk(&resource, {logical_type::BIGINT}, row_count);
    auto* data = chunk.data[0].data<int64_t>();
    int64_t expected = 0;
    for (uint64_t i = 0; i < row_count; ++i) {
        const int64_t v = static_cast<int64_t>(i % 2048);
        data[i] = v;
        expected += v;
    }
    chunk.set_cardinality(row_count);

    auto res = fn->execute(chunk);
    REQUIRE_FALSE(res.has_error());
    auto& vals = std::get<std::pmr::vector<logical_value_t>>(res.value());
    REQUIRE(vals.size() == 1);
    REQUIRE(vals[0].value<int64_t>() == expected);
}
