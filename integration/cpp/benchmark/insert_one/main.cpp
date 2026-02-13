#include "../classes.hpp"

void insert_one(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    dispatcher->create_database(session, database_name);
    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    dispatcher->create_collection(session, database_name, collection_name, types);
    int n_row = 0;
    state.ResumeTiming();
    for (auto _ : state) {
        auto chunk = gen_data_chunk(static_cast<size_t>(state.range(0)), n_row, dispatcher->resource());
        n_row += state.range(0);
        auto ins = make_node_insert(dispatcher->resource(), {database_name, collection_name}, std::move(chunk));
        dispatcher->execute_plan(session, ins);
    }
}
BENCHMARK(insert_one)->Arg(1)->Arg(10)->Arg(20)->Arg(100)->Arg(500)->Arg(1000);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    unique_spaces::get();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
