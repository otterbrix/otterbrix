#include "../classes.hpp"
#include <benchmark/benchmark.h>

//#define test_with_disk

using components::expressions::compare_type;

constexpr bool wal_on = true;
constexpr bool wal_off = false;
constexpr bool disk_on = true;
constexpr bool disk_off = false;
constexpr bool index_on = true;
constexpr bool index_off = false;

#define BENCHMARK_FUNC(FUNC, WAL_ON, DISK_ON)                                                                          \
    BENCHMARK(FUNC<WAL_ON, DISK_ON, index_off>)->Arg(100);                                                             \
    BENCHMARK(FUNC<WAL_ON, DISK_ON, index_on>)->Arg(100)

template<bool on_wal, bool on_disk, bool on_index>
void only_find_all(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_wal, on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto p = create_aggregate(dispatcher->resource(), database_name, collection_name);
            dispatcher->find(session, p.first, p.second);
        }
    }
}

template<bool on_wal, bool on_disk, bool on_index>
void only_find_eq(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_wal, on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto p = create_aggregate(dispatcher->resource(), database_name, collection_name, compare_type::eq, "count", 115);
            dispatcher->find(session, p.first, p.second);
        }
    }
}

template<bool on_wal, bool on_disk, bool on_index>
void only_find_gt(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = wr_dispatcher<on_wal, on_disk>();
    collection_name_t collection_name = get_collection_name<on_index>();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto p = create_aggregate(dispatcher->resource(), database_name, collection_name, compare_type::gt, "count", size_collection - 100);
            dispatcher->find(session, p.first, p.second);
        }
    }
}

// TODO: delete_insert_update_one uses removed document_t API, needs rewrite

BENCHMARK_FUNC(only_find_all, wal_off, disk_off);
BENCHMARK_FUNC(only_find_eq, wal_off, disk_off);
BENCHMARK_FUNC(only_find_gt, wal_off, disk_off);

#ifdef test_with_disk
BENCHMARK_FUNC(only_find_all, wal_on, disk_on);
BENCHMARK_FUNC(only_find_eq, wal_on, disk_on);
BENCHMARK_FUNC(only_find_gt, wal_on, disk_on);
#endif

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    init_spaces<wal_off, disk_off>();
#ifdef test_with_disk
    init_spaces<wal_on, disk_on>();
#endif
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
