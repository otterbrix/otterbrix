#include <benchmark/benchmark.h>

#include <components/casts/default_casts.hpp>
#include <core/pmr.hpp>

#include <cassert>
#include <memory_resource>

using namespace components;
using namespace components::casts;
using types::complex_logical_type;
using types::logical_type;

namespace {

    // THE BENCHMARK OWNS THE ARENA IT MEASURES ON. Rule 14 bans both spellings of the
    // process-global arena, and this was the last std::pmr::get_default_resource() call
    // outside a test in the tree. There is nothing above this function to borrow an arena
    // from -- it is a file-local static registry built once for the whole binary -- so the
    // arena is a static too, and core::pmr::otterbrix_resource is the same type the engine
    // and the python module use (a pool normally, resource_tracer_t under ASAN, which makes
    // a leak out of the registry a report rather than a block hidden inside a pool chunk).
    //
    // ORDER IS LOAD-BEARING: function-local statics are destroyed in REVERSE order of
    // construction, so `arena` -- constructed first, on the first call -- is destroyed after
    // `registry`, which is the only ordering in which the registry's pmr members still have
    // an arena to deallocate into. The lambda names `arena` without capturing it; a static
    // needs no capture.
    cast_registry_t& default_registry() {
        static core::pmr::otterbrix_resource arena;
        static cast_registry_t registry = [] {
            cast_registry_t built{&arena};
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
        // Literal, in-window pair: the refusal branch inside create_decimal is unreachable
        // here, so the arena it would build a message on is never touched. See decimal_key()
        // in default_casts.cpp for the same reasoning.
        auto left_result = complex_logical_type::create_decimal(std::pmr::null_memory_resource(), 10, 2);
        assert(!left_result.has_error() && "benchmark: DECIMAL(10,2) is inside the DECIMAL window");
        const complex_logical_type left = std::move(left_result.value());
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
