#include "../classes.hpp"

static constexpr int count_insert_batch = 50;
static constexpr int size_insert_batch = 100;
static constexpr int count_find_one = 100;
static constexpr int count_find_many = 100;
static constexpr int count_update_one = 100;
static constexpr int count_update_many = 100;
static constexpr int count_delete_one = 100;
static constexpr int count_delete_many = 100;

void work(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    int n_state = 0;
    for (auto _ : state) {
        // create database and collection
        ++n_state;
        auto db_name = database_name + "_" + std::to_string(n_state);
        auto col_name = collection_name + "_" + std::to_string(n_state);
        dispatcher->create_database(session, db_name);
        auto types = gen_data_chunk(0, dispatcher->resource()).types();
        dispatcher->create_collection(session, db_name, col_name, types);

        auto n_row = 0;
        for (int i_insert = 0; i_insert < count_insert_batch; ++i_insert) {
            auto chunk = gen_data_chunk(size_insert_batch, n_row, dispatcher->resource());
            n_row += size_insert_batch;
            auto ins = make_node_insert(dispatcher->resource(), {db_name, col_name}, std::move(chunk));
            dispatcher->execute_plan(session, ins);
        }

        // find_one
        for (int i_find = 1; i_find < count_find_one; ++i_find) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(), compare_type::all_true)));
                dispatcher->find_one(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{"_id"},
                                                            core::parameter_id_t{1})));
                params->add_parameter(core::parameter_id_t{1}, gen_id(i_find));
                dispatcher->find_one(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{"_id"},
                                                            core::parameter_id_t{2})));
                params->add_parameter(core::parameter_id_t{2}, gen_id(size_collection - i_find));
                dispatcher->find_one(session, plan, params);
            }
        }

        // find_many
        for (int i_find = 1; i_find < count_find_many; ++i_find) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(), compare_type::all_true)));
                dispatcher->find(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{"_id"},
                                                            core::parameter_id_t{1})));
                params->add_parameter(core::parameter_id_t{1}, gen_id(i_find));
                dispatcher->find(session, plan, params);
            }
            {
                auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(), {db_name, col_name});
                plan->append_child(
                    make_node_match(std::pmr::get_default_resource(),
                                    {db_name, col_name},
                                    make_compare_expression(std::pmr::get_default_resource(),
                                                            compare_type::eq,
                                                            components::expressions::key_t{"_id"},
                                                            core::parameter_id_t{2})));
                params->add_parameter(core::parameter_id_t{2}, gen_id(size_collection - i_find));
                dispatcher->find(session, plan, params);
            }
        }

        // update_one
        for (int i_update = 0; i_update < count_update_one; ++i_update) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto match = make_node_match(std::pmr::get_default_resource(),
                                             {db_name, col_name},
                                             make_compare_expression(std::pmr::get_default_resource(),
                                                                     compare_type::eq,
                                                                     components::expressions::key_t{"_id"},
                                                                     core::parameter_id_t{1}));
                params->add_parameter(core::parameter_id_t{1}, gen_id(i_update));
                dispatcher->update_one(
                    session,
                    match,
                    params,
                    document_t::document_from_json(R"_({"$set": {"count": 0}})_", dispatcher->resource()),
                    false);
            }
            {
                auto match = make_node_match(std::pmr::get_default_resource(),
                                             {db_name, col_name},
                                             make_compare_expression(std::pmr::get_default_resource(),
                                                                     compare_type::eq,
                                                                     components::expressions::key_t{"_id"},
                                                                     core::parameter_id_t{2}));
                params->add_parameter(core::parameter_id_t{2}, gen_id(size_collection - i_update));
                dispatcher->update_one(
                    session,
                    match,
                    params,
                    document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(size_collection) + "}}",
                                                   dispatcher->resource()),
                    false);
            }
        }

        // update_many
        for (int i_update = 0; i_update < count_update_many; ++i_update) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto match = make_node_match(std::pmr::get_default_resource(),
                                             {db_name, col_name},
                                             make_compare_expression(std::pmr::get_default_resource(),
                                                                     compare_type::lt,
                                                                     components::expressions::key_t{"count"},
                                                                     core::parameter_id_t{1}));
                params->add_parameter(core::parameter_id_t{1}, count_update_many * i_update);
                dispatcher->update_one(
                    session,
                    match,
                    params,
                    document_t::document_from_json(R"_({"$set": {"count": 0}})_", dispatcher->resource()),
                    false);
            }
            {
                auto match = make_node_match(std::pmr::get_default_resource(),
                                             {db_name, col_name},
                                             make_compare_expression(std::pmr::get_default_resource(),
                                                                     compare_type::gt,
                                                                     components::expressions::key_t{"count"},
                                                                     core::parameter_id_t{2}));
                params->add_parameter(core::parameter_id_t{2}, size_collection - count_update_many * i_update);
                dispatcher->update_one(
                    session,
                    match,
                    params,
                    document_t::document_from_json("{\"$set\": {\"count\": " + std::to_string(size_collection) + "}}",
                                                   dispatcher->resource()),
                    false);
            }
        }

        // delete_one
        for (int i_delete = 0; i_delete < count_delete_one; ++i_delete) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {db_name, col_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::eq,
                                                                     components::expressions::key_t{"_id"},
                                                                     core::parameter_id_t{1}));
                params->add_parameter(core::parameter_id_t{1}, gen_id(i_delete));
                dispatcher->delete_one(session, match, params);
            }
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {db_name, col_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::eq,
                                                                     components::expressions::key_t{"_id"},
                                                                     core::parameter_id_t{2}));
                params->add_parameter(core::parameter_id_t{2}, gen_id(size_collection - i_delete));
                dispatcher->delete_one(session, match, params);
            }
        }

        // delete_many
        for (int i_delete = 0; i_delete < count_delete_many; ++i_delete) {
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {db_name, col_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::lt,
                                                                     components::expressions::key_t{"count"},
                                                                     core::parameter_id_t{1}));
                params->add_parameter(core::parameter_id_t{1}, 100 * i_delete);
                dispatcher->delete_many(session, match, params);
            }
            {
                auto match = components::logical_plan::make_node_match(
                    dispatcher->resource(),
                    {db_name, col_name},
                    components::expressions::make_compare_expression(dispatcher->resource(),
                                                                     compare_type::gt,
                                                                     components::expressions::key_t{"count"},
                                                                     core::parameter_id_t{2}));
                params->add_parameter(core::parameter_id_t{2}, size_collection - 100 * i_delete);
                dispatcher->delete_many(session, match, params);
            }
        }
    }
}
BENCHMARK(work);

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
