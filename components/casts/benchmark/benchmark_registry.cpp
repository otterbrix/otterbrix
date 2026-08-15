#include <benchmark/benchmark.h>

#include <components/casts/default_casts.hpp>

#include <memory_resource>

using namespace components;
using namespace components::casts;
using types::complex_logical_type;
using types::logical_type;

namespace {

    cast_registry_t& default_registry() {
        static cast_registry_t registry = [] {
            cast_registry_t built{std::pmr::get_default_resource()};
            register_default_casts(built);
            return built;
        }();
        return registry;
    }

    // Both operands are candidates and one reaches the other, so the answer is found before the
    // third-type enumeration runs -- the cheap path.
    void common_type_operand_wins(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type left{logical_type::INTEGER};
        const complex_logical_type right{logical_type::BIGINT};
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.find_best_common_type(left, right));
        }
    }

    // Neither operand reaches the other, so every candidate target of the left operand is scored.
    // This is the case the sorted early exit is meant to cut short.
    void common_type_third_type(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type left{logical_type::UBIGINT};
        const complex_logical_type right{logical_type::INTEGER};
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.find_best_common_type(left, right));
        }
    }

    // A left operand whose every implicit entry carries a cost rule, so each candidate is scored
    // by evaluating that rule rather than reading a stored cost.
    void common_type_decimal_left(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type left = complex_logical_type::create_decimal(10, 2);
        const complex_logical_type right{logical_type::BOOLEAN};
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.find_best_common_type(left, right));
        }
    }

    // No common type: the search runs to exhaustion without ever setting a bound, so the early
    // exit can never fire. The worst case.
    void common_type_unreachable(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type left{logical_type::INTEGER};
        const complex_logical_type right{logical_type::DATE};
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.find_best_common_type(left, right));
        }
    }

    void common_type_lists(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type left =
            complex_logical_type::create_list(complex_logical_type{logical_type::INTEGER});
        const complex_logical_type right =
            complex_logical_type::create_list(complex_logical_type{logical_type::BIGINT});
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.find_best_common_type(left, right));
        }
    }

    void lookup_registered_leaf(benchmark::State& state) {
        const cast_registry_t& registry = default_registry();
        const complex_logical_type source{logical_type::INTEGER};
        const complex_logical_type target{logical_type::DOUBLE};
        for (auto _ : state) {
            benchmark::DoNotOptimize(registry.lookup(source, target));
        }
    }

    BENCHMARK(common_type_operand_wins);
    BENCHMARK(common_type_third_type);
    BENCHMARK(common_type_decimal_left);
    BENCHMARK(common_type_unreachable);
    BENCHMARK(common_type_lists);
    BENCHMARK(lookup_registered_leaf);

} // namespace

BENCHMARK_MAIN();
