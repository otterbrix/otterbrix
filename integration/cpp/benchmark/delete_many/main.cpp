#include "../classes.hpp"

void delete_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {database_name, collection_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::lt,
                                                                     components::expressions::key_t{dispatcher->resource(), "count"},
                                                                     core::parameter_id_t{1}));
                params->add_parameter(core::parameter_id_t{1}, 100 * i);
                dispatcher->delete_many(session, match, params);
            }
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {database_name, collection_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::gt,
                                                                     components::expressions::key_t{dispatcher->resource(), "count"},
                                                                     core::parameter_id_t{2}));
                params->add_parameter(core::parameter_id_t{2}, size_collection - 100 * i);
                dispatcher->delete_many(session, match, params);
            }
        }
    }
}
BENCHMARK(delete_many)->Arg(100);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    init_collection();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
