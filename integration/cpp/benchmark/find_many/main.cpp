#include "../classes.hpp"

void find_many(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                          {database_name, collection_name});
                plan->append_child(
                    make_node_match(dispatcher->resource(),
                                    {database_name, collection_name},
                                    make_compare_expression(dispatcher->resource(), compare_type::all_true)));
                dispatcher->find(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                          {database_name, collection_name});
                plan->append_child(
                    make_node_match(dispatcher->resource(),
                                    {database_name, collection_name},
                                    make_compare_expression(dispatcher->resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{dispatcher->resource(), "_id"},
                                                            core::parameter_id_t{1})));
                params->add_parameter(core::parameter_id_t{1}, std::to_string(i));
                dispatcher->find(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                          {database_name, collection_name});
                plan->append_child(
                    make_node_match(dispatcher->resource(),
                                    {database_name, collection_name},
                                    make_compare_expression(dispatcher->resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{dispatcher->resource(), "_id"},
                                                            core::parameter_id_t{2})));
                params->add_parameter(core::parameter_id_t{2}, std::to_string(size_collection - i));
                dispatcher->find(session, plan, params);
            }
        }
    }
}
BENCHMARK(find_many)->Arg(1)->Arg(10)->Arg(20)->Arg(100);

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
