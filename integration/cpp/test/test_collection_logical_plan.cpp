#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_builders.hpp>
#include <core/operations_helper.hpp>
#include <variant>

// Name the DML node's target so the executor registers its catalog lookup and
// enrich stamps table_oid() from the resolved entry.
#define WRAP_DML_TARGET(DB, REL, NODE) test_dml_target(NODE, DB, REL)

static const core::dbname_t table_database_name{"table_testdatabase"};
static const core::relname_t table_collection_name{"table_testcollection"};
static const core::relname_t table_other_collection_name{"table_othertestcollection"};
static const core::relname_t table_collection_left{"table_testcollection_left_join"};
static const core::relname_t table_collection_right{"table_testcollection_right_join"};

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::logical_plan") {
    auto config = test_create_config(integration_fixture_path("test_collection_logical_plan"));
    test_clear_directory(config);

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    std::pmr::vector<types::complex_logical_type> types_left(dispatcher->resource());
    std::pmr::vector<types::complex_logical_type> types_right(dispatcher->resource());

    types_left.emplace_back(types::logical_type::STRING_LITERAL, "name");
    types_left.emplace_back(types::logical_type::BIGINT, "key_1");
    types_left.emplace_back(types::logical_type::BIGINT, "key_2");

    types_right.emplace_back(types::logical_type::BIGINT, "value");
    types_right.emplace_back(types::logical_type::BIGINT, "key");

    std::vector<components::table::column_definition_t> columns;
    std::vector<components::table::column_definition_t> columns_left;
    std::vector<components::table::column_definition_t> columns_right;
    columns.reserve(types.size());
    columns_left.reserve(types_left.size());
    columns_right.reserve(types_right.size());
    for (const auto& type : types) {
        columns.emplace_back(type.alias(), type);
    }
    for (const auto& type : types_left) {
        columns_left.emplace_back(type.alias(), type);
    }
    for (const auto& type : types_right) {
        columns_right.emplace_back(type.alias(), type);
    }

    INFO("initialization");
    {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + table_database_name.t + ";");
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, table_database_name, table_collection_name, columns);
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, table_database_name, table_other_collection_name, columns);
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, table_database_name, table_collection_left, columns_left);
        }
        {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, table_database_name, table_collection_right, columns_right);
        }
    }

    INFO("insert");
    {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = WRAP_DML_TARGET(table_database_name,
                                   table_collection_name,
                                   logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == kNumInserts);
        }
    }

    INFO("find");
    {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 90),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 90.0),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("group by boolean");
    {
        auto session = otterbrix::session_id_t();
        auto aggregate =
            logical_plan::make_node_aggregate(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name});

        // Sort by count_bool ascending so false comes first, true second
        {
            std::vector<expressions::expression_ptr> sort = {
                expressions::make_sort_expression(dispatcher->resource(),
                                                  key(dispatcher->resource(), "count_bool"),
                                                  expressions::sort_order::asc)};
            aggregate->append_child(logical_plan::make_node_sort(dispatcher->resource(), std::move(sort)));
        }

        auto group = logical_plan::make_node_group(dispatcher->resource());

        group->append_expression(make_scalar_expression(dispatcher->resource(),
                                                        expressions::scalar_type::group_field,
                                                        key(dispatcher->resource(), "count_bool")));

        auto scalar_expr = make_scalar_expression(dispatcher->resource(),
                                                  expressions::scalar_type::get_field,
                                                  key(dispatcher->resource(), "count_bool"));
        scalar_expr->append_param(key(dispatcher->resource(), "count_bool"));
        group->append_expression(std::move(scalar_expr));

        auto count_expr =
            expressions::make_aggregate_expression(dispatcher->resource(), "count", key(dispatcher->resource(), "cnt"));
        count_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(count_expr));

        auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                               "sum",
                                                               key(dispatcher->resource(), "sum_val"));
        sum_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(sum_expr));

        auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                               "avg",
                                                               key(dispatcher->resource(), "avg_val"));
        avg_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(avg_expr));

        aggregate->append_child(std::move(group));
        auto cur = dispatcher->execute_plan(session,
                                            logical_plan::execution_plan_t{dispatcher->resource(), aggregate, nullptr});
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        // row 0: false (even indices 0,2,4,...98 → count values 1,3,5,...,99) → cnt=50, sum=2500, avg=50.0
        // row 1: true  (odd indices 1,3,5,...99 → count values 2,4,6,...,100) → cnt=50, sum=2550, avg=51.0
        // Note: gen_data_chunk produces count = i+1, count_bool = (i%2==0)
        // Even indices (0,2,...,98): count_bool=true, count=1,3,...,99 → sum=2500, avg=50.0
        // Odd indices (1,3,...,99): count_bool=false, count=2,4,...,100 → sum=2550, avg=51.0
        // After sort asc: false first (row 0), true second (row 1)
        auto cell0 = cur->get<bool>(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        REQUIRE(cell0.value() == false);
        auto cell1 = cur->get<bool>(0, 1);
        REQUIRE_FALSE(cell1.has_error());
        REQUIRE(cell1.value() == true);
        auto cell2 = cur->get<uint64_t>(1, 0);
        REQUIRE_FALSE(cell2.has_error());
        REQUIRE(cell2.value() == 50);
        auto cell3 = cur->get<uint64_t>(1, 1);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == 50);
        auto cell4 = cur->get<int64_t>(2, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == 2550);
        auto cell5 = cur->get<int64_t>(2, 1);
        REQUIRE_FALSE(cell5.has_error());
        REQUIRE(cell5.value() == 2500);
        // avg of integers is DOUBLE.
        auto cell6 = cur->get<double>(3, 0);
        REQUIRE_FALSE(cell6.has_error());
        REQUIRE(cell6.value() == Catch::Approx(51.0));
        auto cell7 = cur->get<double>(3, 1);
        REQUIRE_FALSE(cell7.has_error());
        REQUIRE(cell7.value() == Catch::Approx(50.0));
    }

    INFO("insert from select");
    {
        auto ins_node = logical_plan::make_node_insert(dispatcher->resource());
        ins_node->append_child(
            logical_plan::make_node_aggregate(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name}));
        auto ins = WRAP_DML_TARGET(table_database_name, table_other_collection_name, ins_node);
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == kNumInserts);
        }
    }

    INFO("delete");
    {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 90),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto del = WRAP_DML_TARGET(
                table_database_name,
                table_collection_name,
                logical_plan::make_node_delete(
                    dispatcher->resource(),
                    logical_plan::make_node_match(
                        dispatcher->resource(),
                        qualified_name_t{table_database_name, table_collection_name},
                        make_compare_expression(dispatcher->resource(),
                                                compare_type::gt,
                                                key{dispatcher->resource(), "count", side_t::left},
                                                id_par{0})),
                    logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t::all())));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 90),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), del, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 90),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("delete using");
    {
        // DELETE FROM table_other_collection_name USING table_collection_name:
        // the USING table is a source child sub-plan feeding the RIGHT side of the
        // delete join; the WHERE predicate compares target (left) against source (right).
        // Materialize table_collection_name (90 rows, count 1..90) as the source child.
        auto scan_session = otterbrix::session_id_t();
        auto scan_agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                          qualified_name_t{table_database_name, table_collection_name});
        auto scan_cur =
            dispatcher->execute_plan(scan_session,
                                     logical_plan::execution_plan_t{dispatcher->resource(), scan_agg, nullptr});
        REQUIRE(scan_cur->size() == 90);
        vector::data_chunk_t using_data = std::move(scan_cur->chunks().front());

        auto expr =
            components::expressions::make_compare_expression(dispatcher->resource(),
                                                             compare_type::eq,
                                                             key{dispatcher->resource(), "count", side_t::left},
                                                             key{dispatcher->resource(), "count", side_t::right});
        auto del_inner = logical_plan::make_node_delete(
            dispatcher->resource(),
            logical_plan::make_node_match(dispatcher->resource(),
                                          qualified_name_t{table_database_name, table_other_collection_name},
                                          std::move(expr)),
            logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t::all()));
        del_inner->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), std::move(using_data)));
        auto del = WRAP_DML_TARGET(table_database_name, table_other_collection_name, del_inner);
        {
            auto session = otterbrix::session_id_t();
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), del, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 90);
        }
    }

    INFO("update");
    {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::lt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 20),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::lt,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            // SET count = $2 — the value expression's own key names the target column.
            auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                   expressions::scalar_type::constant,
                                                                   expressions::key_t{dispatcher->resource(), "count"});
            update_expr->append_param(id_par{1});
            auto upd = WRAP_DML_TARGET(table_database_name,
                                       table_collection_name,
                                       make_node_update(dispatcher->resource(),
                                                        match,
                                                        logical_plan::make_node_limit(dispatcher->resource(),
                                                                                      logical_plan::limit_t::all()),
                                                        {update_expr}));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 20),
                                          components::vector::parameter_kind::literal) == id_par{0});
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{1});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), upd, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::lt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 20),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
    }

    INFO("update array element");
    {
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            std::pmr::vector<std::pmr::string> path(dispatcher->resource());
            path.emplace_back("count_array");
            path.emplace_back("1");
            auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                   expressions::scalar_type::constant,
                                                                   key{std::move(path)});
            update_expr->append_param(id_par{1});
            auto upd = WRAP_DML_TARGET(table_database_name,
                                       table_collection_name,
                                       make_node_update(dispatcher->resource(),
                                                        match,
                                                        logical_plan::make_node_limit(dispatcher->resource(),
                                                                                      logical_plan::limit_t::all()),
                                                        {update_expr}));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), uint64_t{9999}),
                                          components::vector::parameter_kind::literal) == id_par{1});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), upd, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         qualified_name_t{table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{0});
            agg->append_child(
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_collection_name},
                                              std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
            for (size_t i = 0; i < cur->size(); ++i) {
                auto cell30 = cur->get<uint64_t>(4, i, {0});
                REQUIRE_FALSE(cell30.has_error());
                REQUIRE(cell30.value() == 9999);
            }
        }
    }

    INFO("update from");
    {
        auto scan_session = otterbrix::session_id_t();
        auto scan_agg =
            logical_plan::make_node_aggregate(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_other_collection_name});
        auto scan_cur =
            dispatcher->execute_plan(scan_session,
                                     logical_plan::execution_plan_t{dispatcher->resource(), scan_agg, nullptr});
        REQUIRE(scan_cur->size() == 10);
        vector::data_chunk_t data = std::move(scan_cur->chunks().front());
        {
            auto session = otterbrix::session_id_t();

            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), int64_t{2}),
                                          components::vector::parameter_kind::literal) == id_par{0});

            auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                   expressions::scalar_type::multiply,
                                                                   expressions::key_t{dispatcher->resource(), "count"});
            update_expr->append_param(expressions::key_t{dispatcher->resource(), "count", expressions::side_t::right});
            update_expr->append_param(id_par{0});

            auto expr = components::expressions::make_compare_expression(
                dispatcher->resource(),
                compare_type::eq,
                key{{{"initial_table", "count"}, dispatcher->resource()}},
                key{{{"from_table", "count"}, dispatcher->resource()}});

            auto update_inner = logical_plan::make_node_update(
                dispatcher->resource(),
                logical_plan::make_node_match(dispatcher->resource(),
                                              qualified_name_t{table_database_name, table_other_collection_name},
                                              std::move(expr)),
                logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t::all()),
                {std::move(update_expr)});
            update_inner->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), std::move(data)));
            update_inner->set_result_alias("initial_table");
            update_inner->children().back()->set_result_alias("from_table");
            auto update = WRAP_DML_TARGET(table_database_name, table_other_collection_name, update_inner);
            auto cur = dispatcher->execute_plan(session,
                                                logical_plan::execution_plan_t{dispatcher->resource(), update, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg =
                logical_plan::make_node_aggregate(dispatcher->resource(),
                                                  qualified_name_t{table_database_name, table_other_collection_name});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), agg, nullptr});
            REQUIRE(cur->size() == 10);
            for (size_t num = 0; num < cur->size(); ++num) {
                auto cell8 = cur->get<int64_t>(0, num);
                REQUIRE_FALSE(cell8.has_error());
                REQUIRE(cell8.value() == static_cast<int64_t>(91 + num) * 2);
            }
        }
    }

    INFO("delete with limit 1");
    {
        // table_collection_name has 90 rows at this point, 19 with count==1000
        // Delete 1 row where count == 1000, LIMIT 1
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            auto limit = logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t(1));
            auto del = WRAP_DML_TARGET(table_database_name,
                                       table_collection_name,
                                       logical_plan::make_node_delete(dispatcher->resource(), match, limit));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), del, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 1);
        }
    }

    INFO("delete with limit");
    {
        // table_collection_name has 89 rows at this point
        // Delete rows where count == 1000, but limit to 5
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            auto limit = logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t(5));
            auto del = WRAP_DML_TARGET(table_database_name,
                                       table_collection_name,
                                       logical_plan::make_node_delete(dispatcher->resource(), match, limit));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), del, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 5);
        }
    }

    INFO("update with limit 1");
    {
        // Update one row where count == 1000, set count = 2000
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            // SET count = $2 — the value expression's own key names the target column.
            auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                   expressions::scalar_type::constant,
                                                                   expressions::key_t{dispatcher->resource(), "count"});
            update_expr->append_param(id_par{1});
            auto limit = logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t(1));
            auto upd =
                WRAP_DML_TARGET(table_database_name,
                                table_collection_name,
                                logical_plan::make_node_update(dispatcher->resource(), match, limit, {update_expr}));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 2000),
                                          components::vector::parameter_kind::literal) == id_par{1});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), upd, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->affected_rows() == 1);
        }
    }

    INFO("update with limit N");
    {
        // Update up to 5 rows where count == 1000, set count = 3000
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                qualified_name_t{table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{0}));
            // SET count = $2 — the value expression's own key names the target column.
            auto update_expr = expressions::make_scalar_expression(dispatcher->resource(),
                                                                   expressions::scalar_type::constant,
                                                                   expressions::key_t{dispatcher->resource(), "count"});
            update_expr->append_param(id_par{1});
            auto limit = logical_plan::make_node_limit(dispatcher->resource(), logical_plan::limit_t(5));
            auto upd =
                WRAP_DML_TARGET(table_database_name,
                                table_collection_name,
                                logical_plan::make_node_update(dispatcher->resource(), match, limit, {update_expr}));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 1000),
                                          components::vector::parameter_kind::literal) == id_par{0});
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), 3000),
                                          components::vector::parameter_kind::literal) == id_par{1});
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), upd, params});
            REQUIRE(cur->is_success());
            // There were 18 rows with count==1000 after delete limit 1 removed 1, delete limit 5 removed 5, and update limit 1 changed 1
            // So 12 remain with count==1000, limit 5 should update 5
            REQUIRE(cur->affected_rows() == 5);
        }
    }

    INFO("join with outside data");
    {
        vector::data_chunk_t chunk_left(dispatcher->resource(), types_left, 101);
        vector::data_chunk_t chunk_right(dispatcher->resource(), types_right, 100);
        chunk_left.set_cardinality(101);
        chunk_right.set_cardinality(100);

        for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
            chunk_left.set_value(0, static_cast<size_t>(num), std::string_view{"Name " + std::to_string(num)});
            chunk_left.set_value(1, static_cast<size_t>(num), num);
            chunk_left.set_value(2, static_cast<size_t>(num), reversed);
        }
        for (int64_t num = 0; num < 100; ++num) {
            chunk_right.set_value(0, static_cast<size_t>(num), (num + 25) * 2 * 10);
            chunk_right.set_value(1, static_cast<size_t>(num), (num + 25) * 2);
        }
        {
            auto session = otterbrix::session_id_t();
            auto ins_left = WRAP_DML_TARGET(table_database_name,
                                            table_collection_left,
                                            logical_plan::make_node_insert(dispatcher->resource(), chunk_left));
            auto cur =
                dispatcher->execute_plan(session,
                                         logical_plan::execution_plan_t{dispatcher->resource(), ins_left, nullptr});
        }
        {
            auto session = otterbrix::session_id_t();
            auto ins_right = WRAP_DML_TARGET(table_database_name,
                                             table_collection_right,
                                             logical_plan::make_node_insert(dispatcher->resource(), chunk_right));
            auto cur =
                dispatcher->execute_plan(session,
                                         logical_plan::execution_plan_t{dispatcher->resource(), ins_right, nullptr});
        }
        INFO("right is raw data");
        {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), logical_plan::join_type::inner);
            join->append_child(
                logical_plan::make_node_aggregate(dispatcher->resource(),
                                                  qualified_name_t{table_database_name, table_collection_left}));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
            {
                join->append_expression(
                    expressions::make_compare_expression(dispatcher->resource(),
                                                         compare_type::eq,
                                                         expressions::key_t{dispatcher->resource(), "key_1"},
                                                         expressions::key_t{dispatcher->resource(), "key"}));
            }
            auto cur = dispatcher->execute_plan(session,
                                                logical_plan::execution_plan_t{dispatcher->resource(), join, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                auto cell9 = cur->get<int64_t>(1, static_cast<size_t>(num));
                REQUIRE_FALSE(cell9.has_error());
                REQUIRE(cell9.value() == (num + 25) * 2);
                auto cell10 = cur->get<int64_t>(4, static_cast<size_t>(num));
                REQUIRE_FALSE(cell10.has_error());
                REQUIRE(cell10.value() == (num + 25) * 2);
                auto cell11 = cur->get<int64_t>(3, static_cast<size_t>(num));
                REQUIRE_FALSE(cell11.has_error());
                REQUIRE(cell11.value() == (num + 25) * 2 * 10);
                auto cell12 = cur->get<std::string_view>(0, static_cast<size_t>(num));
                REQUIRE_FALSE(cell12.has_error());
                REQUIRE(cell12.value() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("left is raw data");
        {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
            join->append_child(
                logical_plan::make_node_aggregate(dispatcher->resource(),
                                                  qualified_name_t{table_database_name, table_collection_right}));
            {
                join->append_expression(
                    expressions::make_compare_expression(dispatcher->resource(),
                                                         compare_type::eq,
                                                         expressions::key_t{dispatcher->resource(), "key_1"},
                                                         expressions::key_t{dispatcher->resource(), "key"}));
            }
            auto cur = dispatcher->execute_plan(session,
                                                logical_plan::execution_plan_t{dispatcher->resource(), join, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                auto cell13 = cur->get<int64_t>(1, static_cast<size_t>(num));
                REQUIRE_FALSE(cell13.has_error());
                REQUIRE(cell13.value() == (num + 25) * 2);
                auto cell14 = cur->get<int64_t>(4, static_cast<size_t>(num));
                REQUIRE_FALSE(cell14.has_error());
                REQUIRE(cell14.value() == (num + 25) * 2);
                auto cell15 = cur->get<int64_t>(3, static_cast<size_t>(num));
                REQUIRE_FALSE(cell15.has_error());
                REQUIRE(cell15.value() == (num + 25) * 2 * 10);
                auto cell16 = cur->get<std::string_view>(0, static_cast<size_t>(num));
                REQUIRE_FALSE(cell16.has_error());
                REQUIRE(cell16.value() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("both are raw data");
        {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
            {
                join->append_expression(expressions::make_compare_expression(
                    dispatcher->resource(),
                    compare_type::eq,
                    expressions::key_t{dispatcher->resource(), "key_1", side_t::left},
                    expressions::key_t{dispatcher->resource(), "key", side_t::right}));
            }
            auto cur = dispatcher->execute_plan(session,
                                                logical_plan::execution_plan_t{dispatcher->resource(), join, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                auto cell17 = cur->get<int64_t>(1, static_cast<size_t>(num));
                REQUIRE_FALSE(cell17.has_error());
                REQUIRE(cell17.value() == (num + 25) * 2);
                auto cell18 = cur->get<int64_t>(4, static_cast<size_t>(num));
                REQUIRE_FALSE(cell18.has_error());
                REQUIRE(cell18.value() == (num + 25) * 2);
                auto cell19 = cur->get<int64_t>(3, static_cast<size_t>(num));
                REQUIRE_FALSE(cell19.has_error());
                REQUIRE(cell19.value() == (num + 25) * 2 * 10);
                auto cell20 = cur->get<std::string_view>(0, static_cast<size_t>(num));
                REQUIRE_FALSE(cell20.has_error());
                REQUIRE(cell20.value() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("both are raw data with complex join expr");
        {
            auto session = otterbrix::session_id_t();
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), int64_t{75}),
                                          components::vector::parameter_kind::literal) == core::parameter_id_t(0));
            auto join = logical_plan::make_node_join(dispatcher->resource(), logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
            {
                auto and_expr =
                    expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
                and_expr->append_child(expressions::make_compare_expression(
                    dispatcher->resource(),
                    compare_type::eq,
                    expressions::key_t{dispatcher->resource(), "key_1", side_t::left},
                    expressions::key_t{dispatcher->resource(), "key", side_t::right}));
                and_expr->append_child(expressions::make_compare_expression(
                    dispatcher->resource(),
                    compare_type::gt,
                    expressions::key_t{dispatcher->resource(), "key", side_t::right},
                    core::parameter_id_t(0)));

                join->append_expression(std::move(and_expr));
            }
            auto cur =
                dispatcher->execute_plan(session, logical_plan::execution_plan_t{dispatcher->resource(), join, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);

            for (int index = 0, num = 13; index < 13; ++index, ++num) {
                auto cell21 = cur->get<int64_t>(1, static_cast<size_t>(index));
                REQUIRE_FALSE(cell21.has_error());
                REQUIRE(cell21.value() == (num + 25) * 2);
                auto cell22 = cur->get<int64_t>(4, static_cast<size_t>(index));
                REQUIRE_FALSE(cell22.has_error());
                REQUIRE(cell22.value() == (num + 25) * 2);
                auto cell23 = cur->get<int64_t>(3, static_cast<size_t>(index));
                REQUIRE_FALSE(cell23.has_error());
                REQUIRE(cell23.value() == (num + 25) * 2 * 10);
                auto cell24 = cur->get<std::string_view>(0, static_cast<size_t>(index));
                REQUIRE_FALSE(cell24.has_error());
                REQUIRE(cell24.value() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("join raw data with aggregate");
        {
            auto session = otterbrix::session_id_t();
            auto aggregate = logical_plan::make_node_aggregate(dispatcher->resource(), qualified_name_t{});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            {
                {
                    std::vector<expressions::expression_ptr> sort = {
                        expressions::make_sort_expression(dispatcher->resource(),
                                                          key(dispatcher->resource(), "avg"),
                                                          expressions::sort_order::desc)};
                    aggregate->append_child(logical_plan::make_node_sort(dispatcher->resource(), std::move(sort)));
                }
                {
                    auto group = logical_plan::make_node_group(dispatcher->resource());

                    group->append_expression(make_scalar_expression(dispatcher->resource(),
                                                                    expressions::scalar_type::group_field,
                                                                    key(dispatcher->resource(), "key_1")));

                    auto scalar_expr = make_scalar_expression(dispatcher->resource(),
                                                              expressions::scalar_type::get_field,
                                                              key(dispatcher->resource(), "key_1"));
                    scalar_expr->append_param(key(dispatcher->resource(), "key_1"));
                    group->append_expression(std::move(scalar_expr));

                    auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                             "count",
                                                                             key(dispatcher->resource(), "count"));
                    count_expr->append_param(key(dispatcher->resource(), "name"));
                    group->append_expression(std::move(count_expr));

                    auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           "sum",
                                                                           key(dispatcher->resource(), "sum"));
                    sum_expr->append_param(key(dispatcher->resource(), "value"));
                    group->append_expression(std::move(sum_expr));

                    auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           "avg",
                                                                           key(dispatcher->resource(), "avg"));
                    avg_expr->append_param(key(dispatcher->resource(), "key"));
                    group->append_expression(std::move(avg_expr));

                    auto min_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           "min",
                                                                           key(dispatcher->resource(), "min"));
                    min_expr->append_param(key(dispatcher->resource(), "value"));
                    group->append_expression(std::move(min_expr));

                    auto max_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                           "max",
                                                                           key(dispatcher->resource(), "max"));
                    max_expr->append_param(key(dispatcher->resource(), "value"));
                    group->append_expression(std::move(max_expr));

                    aggregate->append_child(std::move(group));
                }
                {
                    aggregate->append_child(logical_plan::make_node_match(
                        dispatcher->resource(),
                        qualified_name_t{},
                        make_compare_expression(dispatcher->resource(),
                                                compare_type::lt,
                                                key(dispatcher->resource(), "key_1", side_t::left),
                                                core::parameter_id_t(0))));
                }
                REQUIRE(params->add_parameter(components::vector::make_scalar(dispatcher->resource(), int64_t{75}),
                                              components::vector::parameter_kind::literal) == core::parameter_id_t(0));
            }
            {
                auto join = logical_plan::make_node_join(dispatcher->resource(), logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
                {
                    join->append_expression(expressions::make_compare_expression(
                        dispatcher->resource(),
                        compare_type::eq,
                        expressions::key_t{dispatcher->resource(), "key_1", side_t::left},
                        expressions::key_t{dispatcher->resource(), "key", side_t::right}));
                }
                aggregate->append_child(join);
            }
            auto cur =
                dispatcher->execute_plan(session,
                                         logical_plan::execution_plan_t{dispatcher->resource(), aggregate, params});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);

            REQUIRE(cur->chunks().front().data[1].type().type() == types::logical_type::UBIGINT);
            REQUIRE(cur->chunks().front().data[1].type().alias() == "count");
            REQUIRE(cur->chunks().front().data[2].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunks().front().data[2].type().alias() == "sum");
            REQUIRE(cur->chunks().front().data[3].type().type() == types::logical_type::DOUBLE);
            REQUIRE(cur->chunks().front().data[3].type().alias() == "avg");
            REQUIRE(cur->chunks().front().data[4].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunks().front().data[4].type().alias() == "min");
            REQUIRE(cur->chunks().front().data[5].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunks().front().data[5].type().alias() == "max");

            for (int num = 0, reversed = 12; num < 13; ++num, --reversed) {
                auto cell25 = cur->get<uint64_t>(1, static_cast<size_t>(num));
                REQUIRE_FALSE(cell25.has_error());
                REQUIRE(cell25.value() == 1);
                auto cell26 = cur->get<int64_t>(2, static_cast<size_t>(num));
                REQUIRE_FALSE(cell26.has_error());
                REQUIRE(cell26.value() == (reversed + 25) * 2 * 10);
                auto cell27 = cur->get<double>(3, static_cast<size_t>(num));
                REQUIRE_FALSE(cell27.has_error());
                REQUIRE(cell27.value() ==
                        Catch::Approx(static_cast<double>((reversed + 25) * 2)));
                auto cell28 = cur->get<int64_t>(4, static_cast<size_t>(num));
                REQUIRE_FALSE(cell28.has_error());
                REQUIRE(cell28.value() == (reversed + 25) * 2 * 10);
                auto cell29 = cur->get<int64_t>(5, static_cast<size_t>(num));
                REQUIRE_FALSE(cell29.has_error());
                REQUIRE(cell29.value() == (reversed + 25) * 2 * 10);
            }
        }
        INFO("just raw data");
        {
            auto session = otterbrix::session_id_t();
            auto node = logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left);
            auto cur = dispatcher->execute_plan(session,
                                                logical_plan::execution_plan_t{dispatcher->resource(), node, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == chunk_left.size());
            REQUIRE(cur->column_count() == chunk_left.column_count());

            for (size_t i = 0; i < chunk_left.column_count(); i++) {
                for (size_t j = 0; j < chunk_left.size(); j++) {
                    auto location = cur->locate(j);
                    REQUIRE_FALSE(location.has_error());
                    REQUIRE(vector::cells_equal(chunk_left.data[i],
                                                j,
                                                cur->chunks()[location.value().chunk].data[i],
                                                location.value().row));
                }
            }
        }
    }
}
