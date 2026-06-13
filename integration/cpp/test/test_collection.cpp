#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>

static const database_name_t database_name = "testdatabase";
static const collection_name_t collection_name = "testcollection";

using components::expressions::compare_type;
using components::expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

template<typename T>
using deleted_unique_ptr = std::unique_ptr<T, std::function<void(T*)>>;

TEST_CASE("integration::cpp::test_collection") {
    auto resource = std::pmr::synchronized_pool_resource();

    auto config = test_create_config("/tmp/test_collection");
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::vector<components::table::column_definition_t> columns;
    columns.reserve(types.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + database_name + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, database_name, collection_name, columns);
        }
    }

    INFO("insert") {
        {
            auto chunk = gen_data_chunk(50, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                database_name,
                collection_name,
                components::logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
    }

    INFO("insert_more") {
        auto chunk = gen_data_chunk(50, 50, dispatcher->resource());
        auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
            dispatcher->resource(),
            database_name,
            collection_name,
            components::logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 50);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(),
                                                           plan,
                                                           components::logical_plan::make_parameter_node(
                                                               dispatcher->resource())});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 100);
        }
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::regex,
                                                                 key{dispatcher->resource(), "count_str", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), "9$"));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }

        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_or);
            expr->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1}));
            expr->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::regex,
                                                                 key{dispatcher->resource(), "count_str", side_t::left},
                                                                 id_par{2}));
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), 90));
            params->add_parameter(id_par{2}, components::types::logical_value_t(dispatcher->resource(), "9$"));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }

        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr_and =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
            auto expr_or =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_or);
            expr_or->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1}));
            expr_or->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::regex,
                                                                 key{dispatcher->resource(), "count_str", side_t::left},
                                                                 id_par{2}));
            expr_and->append_child(expr_or);
            expr_and->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::lte,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{3}));

            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr_and)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), 90));
            params->add_parameter(id_par{2}, components::types::logical_value_t(dispatcher->resource(), "9$"));
            params->add_parameter(id_par{3}, components::types::logical_value_t(dispatcher->resource(), 30));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
        }
    }
    INFO("cursor") {
        auto session = otterbrix::session_id_t();
        auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                  core::dbname_t{database_name},
                                                                  core::relname_t{collection_name});
        auto cur = dispatcher->execute_plan(
            session,
            components::logical_plan::execution_plan_t{dispatcher->resource(),
                                                       plan,
                                                       components::logical_plan::make_parameter_node(
                                                           dispatcher->resource())});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 100);
    }
    INFO("find_one") {
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count_str", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), "1"));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), 10));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto plan = components::logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                      core::dbname_t{database_name},
                                                                      core::relname_t{collection_name});
            auto expr =
                components::expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
            expr->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1}));
            expr->append_child(
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::regex,
                                                                 key{dispatcher->resource(), "count_str", side_t::left},
                                                                 id_par{2}));
            plan->append_child(components::logical_plan::make_node_match(dispatcher->resource(),
                                                                         core::dbname_t{database_name},
                                                                         core::relname_t{collection_name},
                                                                         std::move(expr)));
            auto params = components::logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, components::types::logical_value_t(dispatcher->resource(), 90));
            params->add_parameter(id_par{2}, components::types::logical_value_t(dispatcher->resource(), "9$"));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), plan, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 99);
        }
    }
    INFO("drop_collection") {
        {
            auto session = otterbrix::session_id_t();
            auto drop = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                database_name,
                collection_name,
                components::logical_plan::make_node_drop_collection(dispatcher->resource()));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), drop, nullptr});
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto drop = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                database_name,
                collection_name,
                components::logical_plan::make_node_drop_collection(dispatcher->resource()));
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), drop, nullptr});
            REQUIRE(cur->is_error());
        }
    }
}
