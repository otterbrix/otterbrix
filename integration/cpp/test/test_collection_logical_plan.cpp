#include "test_config.hpp"
#include <catch2/catch.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/tests/generaty.hpp>
#include <core/operations_helper.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <variant>

static const database_name_t table_database_name = "table_testdatabase";
static const collection_name_t table_collection_name = "table_testcollection";
static const collection_name_t table_other_collection_name = "table_othertestcollection";
static const collection_name_t table_collection_left = "table_testcollection_left_join";
static const collection_name_t table_collection_right = "table_testcollection_right_join";

using namespace components;
using namespace components::cursor;
using expressions::compare_type;
using expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::logical_plan") {
    auto config = test_create_config("/tmp/test_collection_logical_plan");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

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

    INFO("initialization") {
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, table_database_name);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, table_database_name, table_collection_name, types);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, table_database_name, table_other_collection_name, types);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, table_database_name, table_collection_left, types_left);
        }
        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, table_database_name, table_collection_right, types_right);
        }
    }

    INFO("insert") {
        auto chunk = gen_data_chunk(kNumInserts, dispatcher->resource());
        auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                  {table_database_name, table_collection_name},
                                                  std::move(chunk));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, table_database_name, table_collection_name) == kNumInserts);
        }
    }

    INFO("find") {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto cur = dispatcher->execute_plan(session, agg);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 90.0));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
    }

    INFO("group by boolean") {
        auto session = otterbrix::session_id_t();
        auto aggregate = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                           {table_database_name, table_collection_name});

        // Sort by count_bool ascending so false comes first, true second
        {
            std::vector<expressions::expression_ptr> sort = {
                expressions::make_sort_expression(key(dispatcher->resource(), "count_bool"),
                                                  expressions::sort_order::asc)};
            aggregate->append_child(
                logical_plan::make_node_sort(dispatcher->resource(), {}, std::move(sort)));
        }

        auto group = logical_plan::make_node_group(dispatcher->resource(), {});

        auto scalar_expr = make_scalar_expression(dispatcher->resource(),
                                                  expressions::scalar_type::get_field,
                                                  key(dispatcher->resource(), "count_bool"));
        scalar_expr->append_param(key(dispatcher->resource(), "count_bool"));
        group->append_expression(std::move(scalar_expr));

        auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                 expressions::aggregate_type::count,
                                                                 key(dispatcher->resource(), "cnt"));
        count_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(count_expr));

        auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                               expressions::aggregate_type::sum,
                                                               key(dispatcher->resource(), "sum_val"));
        sum_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(sum_expr));

        auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                               expressions::aggregate_type::avg,
                                                               key(dispatcher->resource(), "avg_val"));
        avg_expr->append_param(key(dispatcher->resource(), "count"));
        group->append_expression(std::move(avg_expr));

        aggregate->append_child(std::move(group));
        auto cur = dispatcher->execute_plan(session, aggregate);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);

        // row 0: false (even indices 0,2,4,...98 → count values 1,3,5,...,99) → cnt=50, sum=2500, avg=50.0
        // row 1: true  (odd indices 1,3,5,...99 → count values 2,4,6,...,100) → cnt=50, sum=2550, avg=51.0
        // Note: gen_data_chunk produces count = i+1, count_bool = (i%2==0)
        // Even indices (0,2,...,98): count_bool=true, count=1,3,...,99 → sum=2500, avg=50.0
        // Odd indices (1,3,...,99): count_bool=false, count=2,4,...,100 → sum=2550, avg=51.0
        // After sort asc: false first (row 0), true second (row 1)
        REQUIRE(cur->chunk_data().value(0, 0).value<bool>() == false);
        REQUIRE(cur->chunk_data().value(0, 1).value<bool>() == true);
        REQUIRE(cur->chunk_data().value(1, 0).value<uint64_t>() == 50);
        REQUIRE(cur->chunk_data().value(1, 1).value<uint64_t>() == 50);
        REQUIRE(cur->chunk_data().value(2, 0).value<int64_t>() == 2550);
        REQUIRE(cur->chunk_data().value(2, 1).value<int64_t>() == 2500);
        REQUIRE(core::is_equals(cur->chunk_data().value(3, 0).value<double>(), 51.0));
        REQUIRE(core::is_equals(cur->chunk_data().value(3, 1).value<double>(), 50.0));
    }

    INFO("insert from select") {
        auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                  {table_database_name, table_other_collection_name});
        ins->append_child(logical_plan::make_node_aggregate(dispatcher->resource(),
                                                            {table_database_name, table_collection_name}));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, table_database_name, table_collection_name) == kNumInserts);
        }
    }

    INFO("delete") {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto del = logical_plan::make_node_delete_many(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                logical_plan::make_node_match(
                    dispatcher->resource(),
                    {table_database_name, table_collection_name},
                    make_compare_expression(dispatcher->resource(),
                                            compare_type::gt,
                                            key{dispatcher->resource(), "count", side_t::left},
                                            id_par{1})));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(session, del, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::gt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 90));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
    }

    INFO("delete using") {
        auto expr =
            components::expressions::make_compare_expression(dispatcher->resource(),
                                                             compare_type::eq,
                                                             key{dispatcher->resource(), "count", side_t::left},
                                                             key{dispatcher->resource(), "count", side_t::right});
        auto del = logical_plan::make_node_delete_many(
            dispatcher->resource(),
            {table_database_name, table_other_collection_name},
            {table_database_name, table_collection_name},
            logical_plan::make_node_match(dispatcher->resource(),
                                          {table_database_name, table_other_collection_name},
                                          std::move(expr)));
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, del);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 90);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, table_database_name, table_other_collection_name) == 10);
        }
    }

    INFO("update") {
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::lt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 20));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::lt,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            expressions::update_expr_ptr update_expr =
                new expressions::update_expr_set_t(expressions::key_t{dispatcher->resource(), "count"});
            update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
            auto upd = make_node_update_many(dispatcher->resource(),
                                             {table_database_name, table_collection_name},
                                             match,
                                             {update_expr});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 20));
            params->add_parameter(id_par{2}, types::logical_value_t(dispatcher->resource(), 1000));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::lt,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 20));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 0);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
    }

    INFO("update array element") {
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            std::pmr::vector<std::pmr::string> path(dispatcher->resource());
            path.emplace_back("count_array");
            path.emplace_back("0");
            expressions::update_expr_ptr update_expr =
                new expressions::update_expr_set_t(key{std::move(path)});
            update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
            auto upd = make_node_update_many(dispatcher->resource(),
                                             {table_database_name, table_collection_name},
                                             match,
                                             {update_expr});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            params->add_parameter(id_par{2}, types::logical_value_t(dispatcher->resource(), uint64_t{9999}));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_collection_name});
            auto expr =
                components::expressions::make_compare_expression(dispatcher->resource(),
                                                                 compare_type::eq,
                                                                 key{dispatcher->resource(), "count", side_t::left},
                                                                 id_par{1});
            agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                            {table_database_name, table_collection_name},
                                                            std::move(expr)));
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            auto cur = dispatcher->execute_plan(session, agg, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 19);
            for (size_t i = 0; i < cur->size(); ++i) {
                auto arr = cur->chunk_data().value(4, i);
                REQUIRE(arr.children()[0].value<uint64_t>() == 9999);
            }
        }
    }

    INFO("update from") {
        auto scan_session = otterbrix::session_id_t();
        auto scan_agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                          {table_database_name, table_other_collection_name});
        auto scan_cur = dispatcher->execute_plan(scan_session, scan_agg);
        REQUIRE(scan_cur->size() == 10);
        vector::data_chunk_t data = std::move(scan_cur->chunk_data());
        {
            auto session = otterbrix::session_id_t();

            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), int64_t{2}));

            expressions::update_expr_ptr update_expr =
                new expressions::update_expr_set_t(expressions::key_t{dispatcher->resource(), "count"});
            expressions::update_expr_ptr calculate_expr =
                new expressions::update_expr_calculate_t(expressions::update_expr_type::mult);
            calculate_expr->left() = new expressions::update_expr_get_value_t(
                expressions::key_t{dispatcher->resource(), "count", expressions::side_t::right});
            calculate_expr->right() = new expressions::update_expr_get_const_value_t(id_par{1});
            update_expr->left() = std::move(calculate_expr);

            auto expr = components::expressions::make_compare_expression(
                dispatcher->resource(),
                compare_type::eq,
                key{{{"initial_table", "count"}, dispatcher->resource()}},
                key{{{"from_table", "count"}, dispatcher->resource()}});

            auto update = logical_plan::make_node_update_many(
                dispatcher->resource(),
                {table_database_name, table_other_collection_name},
                logical_plan::make_node_match(dispatcher->resource(),
                                              {table_database_name, table_other_collection_name},
                                              std::move(expr)),
                {std::move(update_expr)},
                false);
            update->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), std::move(data)));
            update->set_result_alias("initial_table");
            update->children().back()->set_result_alias("from_table");
            auto cur = dispatcher->execute_plan(session, update, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                         {table_database_name, table_other_collection_name});
            auto cur = dispatcher->execute_plan(session, agg);
            REQUIRE(cur->size() == 10);
            for (size_t num = 0; num < cur->size(); ++num) {
                REQUIRE(cur->chunk_data().value(0, num).value<int64_t>() == static_cast<int64_t>(91 + num) * 2);
            }
        }
    }

    INFO("delete with limit 1") {
        // table_collection_name has 90 rows at this point, 19 with count==1000
        // Delete 1 row where count == 1000, LIMIT 1
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            auto limit = logical_plan::make_node_limit(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                logical_plan::limit_t(1));
            auto del = logical_plan::make_node_delete(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                match, limit);
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            auto cur = dispatcher->execute_plan(session, del, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, table_database_name, table_collection_name) == 89);
        }
    }

    INFO("delete with limit") {
        // table_collection_name has 89 rows at this point
        // Delete rows where count == 1000, but limit to 5
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            auto limit = logical_plan::make_node_limit(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                logical_plan::limit_t(5));
            auto del = logical_plan::make_node_delete(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                match, limit);
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            auto cur = dispatcher->execute_plan(session, del, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 5);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->size(session, table_database_name, table_collection_name) == 84);
        }
    }

    INFO("update with limit 1") {
        // Update one row where count == 1000, set count = 2000
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            expressions::update_expr_ptr update_expr =
                new expressions::update_expr_set_t(expressions::key_t{dispatcher->resource(), "count"});
            update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
            auto limit = logical_plan::make_node_limit(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                logical_plan::limit_t(1));
            auto upd = logical_plan::make_node_update(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                match, limit,
                {update_expr});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            params->add_parameter(id_par{2}, types::logical_value_t(dispatcher->resource(), 2000));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
        }
    }

    INFO("update with limit N") {
        // Update up to 5 rows where count == 1000, set count = 3000
        {
            auto session = otterbrix::session_id_t();
            auto match = logical_plan::make_node_match(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                make_compare_expression(dispatcher->resource(),
                                        compare_type::eq,
                                        key{dispatcher->resource(), "count", side_t::left},
                                        id_par{1}));
            expressions::update_expr_ptr update_expr =
                new expressions::update_expr_set_t(expressions::key_t{dispatcher->resource(), "count"});
            update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
            auto limit = logical_plan::make_node_limit(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                logical_plan::limit_t(5));
            auto upd = logical_plan::make_node_update(
                dispatcher->resource(),
                {table_database_name, table_collection_name},
                match, limit,
                {update_expr});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(id_par{1}, types::logical_value_t(dispatcher->resource(), 1000));
            params->add_parameter(id_par{2}, types::logical_value_t(dispatcher->resource(), 3000));
            auto cur = dispatcher->execute_plan(session, upd, params);
            REQUIRE(cur->is_success());
            // There were 18 rows with count==1000 after delete limit 1 removed 1, delete limit 5 removed 5, and update limit 1 changed 1
            // So 12 remain with count==1000, limit 5 should update 5
            REQUIRE(cur->size() == 5);
        }
    }

    INFO("join with outside data") {
        vector::data_chunk_t chunk_left(dispatcher->resource(), types_left, 101);
        vector::data_chunk_t chunk_right(dispatcher->resource(), types_right, 100);
        chunk_left.set_cardinality(101);
        chunk_right.set_cardinality(100);

        for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
            chunk_left.set_value(0,
                                 static_cast<size_t>(num),
                                 types::logical_value_t{dispatcher->resource(), "Name " + std::to_string(num)});
            chunk_left.set_value(1, static_cast<size_t>(num), types::logical_value_t{dispatcher->resource(), num});
            chunk_left.set_value(2, static_cast<size_t>(num), types::logical_value_t{dispatcher->resource(), reversed});
        }
        for (int64_t num = 0; num < 100; ++num) {
            chunk_right.set_value(0, static_cast<size_t>(num), types::logical_value_t{dispatcher->resource(), (num + 25) * 2 * 10});
            chunk_right.set_value(1, static_cast<size_t>(num), types::logical_value_t{dispatcher->resource(), (num + 25) * 2});
        }
        {
            auto session = otterbrix::session_id_t();
            auto ins_left = logical_plan::make_node_insert(dispatcher->resource(),
                                                           {table_database_name, table_collection_left},
                                                           chunk_left);
            auto cur = dispatcher->execute_plan(session, ins_left);
        }
        {
            auto session = otterbrix::session_id_t();
            auto ins_right = logical_plan::make_node_insert(dispatcher->resource(),
                                                            {table_database_name, table_collection_right},
                                                            chunk_right);
            auto cur = dispatcher->execute_plan(session, ins_right);
        }
        INFO("right is raw data") {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                 {table_database_name, table_collection_left}));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
            {
                join->append_expression(
                    expressions::make_compare_expression(dispatcher->resource(),
                                                         compare_type::eq,
                                                         expressions::key_t{dispatcher->resource(), "key_1"},
                                                         expressions::key_t{dispatcher->resource(), "key"}));
            }
            auto cur = dispatcher->execute_plan(session, join);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(1, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(4, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(3, static_cast<size_t>(num)).value<int64_t>() ==
                        (num + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(0, static_cast<size_t>(num)).value<std::string_view>() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("left is raw data") {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
            join->append_child(logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                 {table_database_name, table_collection_right}));
            {
                join->append_expression(
                    expressions::make_compare_expression(dispatcher->resource(),
                                                         compare_type::eq,
                                                         expressions::key_t{dispatcher->resource(), "key_1"},
                                                         expressions::key_t{dispatcher->resource(), "key"}));
            }
            auto cur = dispatcher->execute_plan(session, join);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(1, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(4, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(3, static_cast<size_t>(num)).value<int64_t>() ==
                        (num + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(0, static_cast<size_t>(num)).value<std::string_view>() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("both are raw data") {
            auto session = otterbrix::session_id_t();
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
            join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
            {
                join->append_expression(expressions::make_compare_expression(
                    dispatcher->resource(),
                    compare_type::eq,
                    expressions::key_t{dispatcher->resource(), "key_1", side_t::left},
                    expressions::key_t{dispatcher->resource(), "key", side_t::right}));
            }
            auto cur = dispatcher->execute_plan(session, join);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 26);

            for (int num = 0; num < 26; ++num) {
                REQUIRE(cur->chunk_data().value(1, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(4, static_cast<size_t>(num)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(3, static_cast<size_t>(num)).value<int64_t>() ==
                        (num + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(0, static_cast<size_t>(num)).value<std::string_view>() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("both are raw data with complex join expr") {
            auto session = otterbrix::session_id_t();
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            params->add_parameter(core::parameter_id_t(1), types::logical_value_t(dispatcher->resource(), int64_t{75}));
            auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
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
                    core::parameter_id_t(1)));

                join->append_expression(std::move(and_expr));
            }
            auto cur = dispatcher->execute_plan(session, join, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);

            for (int index = 0, num = 13; index < 13; ++index, ++num) {
                REQUIRE(cur->chunk_data().value(1, static_cast<size_t>(index)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(4, static_cast<size_t>(index)).value<int64_t>() == (num + 25) * 2);
                REQUIRE(cur->chunk_data().value(3, static_cast<size_t>(index)).value<int64_t>() ==
                        (num + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(0, static_cast<size_t>(index)).value<std::string_view>() ==
                        "Name " + std::to_string((num + 25) * 2));
            }
        }
        INFO("join raw data with aggregate") {
            auto session = otterbrix::session_id_t();
            auto aggregate = logical_plan::make_node_aggregate(dispatcher->resource(), {});
            auto params = logical_plan::make_parameter_node(dispatcher->resource());
            {
                {
                    std::vector<expressions::expression_ptr> sort = {
                        expressions::make_sort_expression(key(dispatcher->resource(), "avg"),
                                                          expressions::sort_order::desc)};
                    aggregate->append_child(logical_plan::make_node_sort(dispatcher->resource(), {}, std::move(sort)));
                }
                {
                    auto group = logical_plan::make_node_group(dispatcher->resource(), {});
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
                        {},
                        make_compare_expression(dispatcher->resource(),
                                                compare_type::lt,
                                                key(dispatcher->resource(), "key_1", side_t::left),
                                                core::parameter_id_t(1))));
                }
                params->add_parameter(core::parameter_id_t(1),
                                      types::logical_value_t(dispatcher->resource(), int64_t{75}));
            }
            {
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
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
            auto cur = dispatcher->execute_plan(session, aggregate, params);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 13);

            REQUIRE(cur->chunk_data().data[1].type().type() == types::logical_type::UBIGINT);
            REQUIRE(cur->chunk_data().data[1].type().alias() == "count");
            REQUIRE(cur->chunk_data().data[2].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunk_data().data[2].type().alias() == "sum");
            REQUIRE(cur->chunk_data().data[3].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunk_data().data[3].type().alias() == "avg");
            REQUIRE(cur->chunk_data().data[4].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunk_data().data[4].type().alias() == "min");
            REQUIRE(cur->chunk_data().data[5].type().type() == types::logical_type::BIGINT);
            REQUIRE(cur->chunk_data().data[5].type().alias() == "max");

            for (int num = 0, reversed = 12; num < 13; ++num, --reversed) {
                REQUIRE(cur->chunk_data().value(1, static_cast<size_t>(num)).value<uint64_t>() == 1);
                REQUIRE(cur->chunk_data().value(2, static_cast<size_t>(num)).value<int64_t>() ==
                        (reversed + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(3, static_cast<size_t>(num)).value<int64_t>() ==
                        static_cast<int64_t>((reversed + 25) * 2));
                REQUIRE(cur->chunk_data().value(4, static_cast<size_t>(num)).value<int64_t>() ==
                        (reversed + 25) * 2 * 10);
                REQUIRE(cur->chunk_data().value(5, static_cast<size_t>(num)).value<int64_t>() ==
                        (reversed + 25) * 2 * 10);
            }
        }
        INFO("just raw data") {
            auto session = otterbrix::session_id_t();
            auto node = logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left);
            auto cur = dispatcher->execute_plan(session, node);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == chunk_left.size());
            REQUIRE(cur->chunk_data().column_count() == chunk_left.column_count());

            for (size_t i = 0; i < chunk_left.column_count(); i++) {
                for (size_t j = 0; j < chunk_left.size(); j++) {
                    REQUIRE(chunk_left.value(i, j) == cur->chunk_data().value(i, j));
                }
            }
        }
    }
}
