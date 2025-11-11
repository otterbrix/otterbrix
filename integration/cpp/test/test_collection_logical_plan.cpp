#include "test_config.hpp"
#include <catch2/catch.hpp>
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
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <variant>

static const database_name_t doc_database_name = "doc_testdatabase";
static const collection_name_t doc_collection_name = "doc_testcollection";
static const collection_name_t doc_other_collection_name = "doc_othertestcollection";
static const collection_name_t doc_collection_left = "doc_testcollection_left_join";
static const collection_name_t doc_collection_right = "doc_testcollection_right_join";
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

    types_left.emplace_back(types::logical_type::STRING_LITERAL, "_id");
    types_left.emplace_back(types::logical_type::STRING_LITERAL, "name");
    types_left.emplace_back(types::logical_type::BIGINT, "key_1");
    types_left.emplace_back(types::logical_type::BIGINT, "key_2");

    types_right.emplace_back(types::logical_type::STRING_LITERAL, "_id");
    types_right.emplace_back(types::logical_type::BIGINT, "value");
    types_right.emplace_back(types::logical_type::BIGINT, "key");

    INFO("initialization") {
        INFO("documents") {
            {
                auto session = otterbrix::session_id_t();
                dispatcher->create_database(session, doc_database_name);
            }
            {
                auto session = otterbrix::session_id_t();
                dispatcher->create_collection(session, doc_database_name, doc_collection_name);
            }
            {
                auto session = otterbrix::session_id_t();
                dispatcher->create_collection(session, doc_database_name, doc_other_collection_name);
            }
            {
                auto session = otterbrix::session_id_t();
                dispatcher->create_collection(session, doc_database_name, doc_collection_left);
            }
            {
                auto session = otterbrix::session_id_t();
                dispatcher->create_collection(session, doc_database_name, doc_collection_right);
            }
        }
        INFO("table") {
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
    }

    INFO("insert") {
        INFO("documents") {
            std::pmr::vector<components::document::document_ptr> documents(dispatcher->resource());
            for (int num = 0; num < kNumInserts; ++num) {
                documents.push_back(gen_doc(num + 1, dispatcher->resource()));
            }
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {doc_database_name, doc_collection_name},
                                                      documents);
            {
                auto session = otterbrix::session_id_t();
                auto cur = dispatcher->execute_plan(session, ins);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
            }
            {
                auto session = otterbrix::session_id_t();
                REQUIRE(dispatcher->size(session, doc_database_name, doc_collection_name) == kNumInserts);
            }
        }
        INFO("table") {
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
    }

    INFO("find") {
        INFO("documents") {
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto cur = dispatcher->execute_plan(session, agg);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            // same query, but now count is compared with different numeric type
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90.0));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
        }
        INFO("table") {
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
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            // same query, but now count is compared with different numeric type
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90.0));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
        }
    }

    INFO("insert from select") {
        INFO("documents") {
            auto ins =
                logical_plan::make_node_insert(dispatcher->resource(), {doc_database_name, doc_other_collection_name});
            ins->append_child(
                logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name}));
            {
                auto session = otterbrix::session_id_t();
                auto cur = dispatcher->execute_plan(session, ins);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
            }
            {
                auto session = otterbrix::session_id_t();
                REQUIRE(dispatcher->size(session, doc_database_name, doc_collection_name) == kNumInserts);
            }
        }
        INFO("table") {
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
    }

    INFO("delete") {
        INFO("documents") {
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            {
                auto session = otterbrix::session_id_t();
                auto del = logical_plan::make_node_delete_many(
                    dispatcher->resource(),
                    {doc_database_name, doc_collection_name},
                    logical_plan::make_node_match(dispatcher->resource(),
                                                  {doc_database_name, doc_collection_name},
                                                  make_compare_expression(dispatcher->resource(),
                                                                          compare_type::gt,
                                                                          side_t::left,
                                                                          key{"count"},
                                                                          id_par{1})));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, del, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 0);
            }
        }
        INFO("table") {
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            {
                auto session = otterbrix::session_id_t();
                auto del = logical_plan::make_node_delete_many(
                    dispatcher->resource(),
                    {table_database_name, table_collection_name},
                    logical_plan::make_node_match(dispatcher->resource(),
                                                  {table_database_name, table_collection_name},
                                                  make_compare_expression(dispatcher->resource(),
                                                                          compare_type::gt,
                                                                          side_t::left,
                                                                          key{"count"},
                                                                          id_par{1})));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, del, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::gt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(90));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 0);
            }
        }
    }

    INFO("delete using") {
        INFO("documents") {
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         key{"count"});
            auto del = logical_plan::make_node_delete_many(
                dispatcher->resource(),
                {doc_database_name, doc_other_collection_name},
                {doc_database_name, doc_collection_name},
                logical_plan::make_node_match(dispatcher->resource(),
                                              {doc_database_name, doc_other_collection_name},
                                              std::move(expr)));
            {
                auto session = otterbrix::session_id_t();
                auto cur = dispatcher->execute_plan(session, del);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 90);
            }
            {
                auto session = otterbrix::session_id_t();
                REQUIRE(dispatcher->size(session, doc_database_name, doc_other_collection_name) == 10);
            }
        }
        INFO("table") {
            auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                         compare_type::eq,
                                                                         key{"count"},
                                                                         key{"count"});
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
    }

    INFO("update") {
        INFO("documents") {
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::lt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->size() == 19);
            }
            {
                auto session = otterbrix::session_id_t();
                auto match = logical_plan::make_node_match(dispatcher->resource(),
                                                           {doc_database_name, doc_collection_name},
                                                           make_compare_expression(dispatcher->resource(),
                                                                                   compare_type::lt,
                                                                                   side_t::left,
                                                                                   key{"count"},
                                                                                   id_par{1}));
                expressions::update_expr_ptr update_expr =
                    new expressions::update_expr_set_t(expressions::key_t{"count"});
                update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
                auto upd = make_node_update_many(dispatcher->resource(),
                                                 {doc_database_name, doc_collection_name},
                                                 match,
                                                 {update_expr});
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                params->add_parameter(id_par{2}, types::logical_value_t(1000));
                auto cur = dispatcher->execute_plan(session, upd, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 19);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::lt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 0);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg =
                    logical_plan::make_node_aggregate(dispatcher->resource(), {doc_database_name, doc_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(1000));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 19);
            }
        }
        INFO("table") {
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::lt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->size() == 19);
            }
            {
                auto session = otterbrix::session_id_t();
                auto match = logical_plan::make_node_match(dispatcher->resource(),
                                                           {table_database_name, table_collection_name},
                                                           make_compare_expression(dispatcher->resource(),
                                                                                   compare_type::lt,
                                                                                   side_t::left,
                                                                                   key{"count"},
                                                                                   id_par{1}));
                expressions::update_expr_ptr update_expr =
                    new expressions::update_expr_set_t(expressions::key_t{"count"});
                update_expr->left() = new expressions::update_expr_get_const_value_t(id_par{2});
                auto upd = make_node_update_many(dispatcher->resource(),
                                                 {table_database_name, table_collection_name},
                                                 match,
                                                 {update_expr});
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                params->add_parameter(id_par{2}, types::logical_value_t(1000));
                auto cur = dispatcher->execute_plan(session, upd, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 19);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::lt,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(20));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 0);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {table_database_name, table_collection_name});
                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             side_t::left,
                                                                             key{"count"},
                                                                             id_par{1});
                agg->append_child(logical_plan::make_node_match(dispatcher->resource(),
                                                                {table_database_name, table_collection_name},
                                                                std::move(expr)));
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(1000));
                auto cur = dispatcher->execute_plan(session, agg, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 19);
            }
        }
    }

    INFO("update from") {
        INFO("documents") {
            std::pmr::vector<document_ptr> data;
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {doc_database_name, doc_other_collection_name});
                auto cur = dispatcher->execute_plan(session, agg);
                REQUIRE(cur->size() == 10);
                data.reserve(cur->size());
                for (int num = 0; num < cur->size(); ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    data.emplace_back(cur->get_document());
                }
            }
            {
                auto session = otterbrix::session_id_t();

                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(2));

                expressions::update_expr_ptr update_expr =
                    new expressions::update_expr_set_t(expressions::key_t{"count"});
                expressions::update_expr_ptr calculate_expr =
                    new expressions::update_expr_calculate_t(expressions::update_expr_type::mult);
                calculate_expr->left() =
                    new expressions::update_expr_get_value_t(expressions::key_t{"count"}, expressions::side_t::right);
                calculate_expr->right() = new expressions::update_expr_get_const_value_t(id_par{1});
                update_expr->left() = std::move(calculate_expr);

                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             key{"count"},
                                                                             key{"count"});

                auto update = logical_plan::make_node_update_many(
                    dispatcher->resource(),
                    {doc_database_name, doc_other_collection_name},
                    logical_plan::make_node_match(dispatcher->resource(),
                                                  {doc_database_name, doc_other_collection_name},
                                                  std::move(expr)),
                    {std::move(update_expr)},
                    false);
                update->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), data));
                auto cur = dispatcher->execute_plan(session, update, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 10);
            }
            {
                auto session = otterbrix::session_id_t();
                auto agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                             {doc_database_name, doc_other_collection_name});
                auto cur = dispatcher->execute_plan(session, agg);
                REQUIRE(cur->size() == 10);
                for (int num = 0; num < cur->size(); ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE((91 + num) * 2 == cur->get_document()->get_long("count"));
                }
            }
        }
        INFO("table") {
            auto scan_session = otterbrix::session_id_t();
            auto scan_agg = logical_plan::make_node_aggregate(dispatcher->resource(),
                                                              {table_database_name, table_other_collection_name});
            auto scan_cur = dispatcher->execute_plan(scan_session, scan_agg);
            REQUIRE(scan_cur->size() == 10);
            vector::data_chunk_t data = std::move(scan_cur->chunk_data());
            {
                auto session = otterbrix::session_id_t();

                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(id_par{1}, types::logical_value_t(int64_t{2}));

                expressions::update_expr_ptr update_expr =
                    new expressions::update_expr_set_t(expressions::key_t{"count"});
                expressions::update_expr_ptr calculate_expr =
                    new expressions::update_expr_calculate_t(expressions::update_expr_type::mult);
                calculate_expr->left() =
                    new expressions::update_expr_get_value_t(expressions::key_t{"count"}, expressions::side_t::right);
                calculate_expr->right() = new expressions::update_expr_get_const_value_t(id_par{1});
                update_expr->left() = std::move(calculate_expr);

                auto expr = components::expressions::make_compare_expression(dispatcher->resource(),
                                                                             compare_type::eq,
                                                                             key{"count"},
                                                                             key{"count"});

                auto update = logical_plan::make_node_update_many(
                    dispatcher->resource(),
                    {table_database_name, table_other_collection_name},
                    logical_plan::make_node_match(dispatcher->resource(),
                                                  {table_database_name, table_other_collection_name},
                                                  std::move(expr)),
                    {std::move(update_expr)},
                    false);
                update->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), std::move(data)));
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
                for (int num = 0; num < cur->size(); ++num) {
                    REQUIRE(cur->chunk_data().value(0, num).value<int64_t>() == (91 + num) * 2);
                }
            }
        }
    }

    INFO("join with outside data") {
        INFO("documents") {
            std::pmr::vector<components::document::document_ptr> documents_left(dispatcher->resource());
            std::pmr::vector<components::document::document_ptr> documents_right(dispatcher->resource());
            for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
                auto doc_left = make_document(dispatcher->resource());
                doc_left->set("_id", gen_id(num + 1, dispatcher->resource()));
                doc_left->set("name", "Name " + std::to_string(num));
                doc_left->set("key_1", num);
                doc_left->set("key_2", reversed);
                documents_left.emplace_back(std::move(doc_left));
            }
            for (int64_t num = 0; num < 100; ++num) {
                auto doc_right = make_document(dispatcher->resource());
                doc_right->set("_id", gen_id(num + 1001, dispatcher->resource()));
                doc_right->set("value", (num + 25) * 2 * 10);
                doc_right->set("key", (num + 25) * 2);
                documents_right.emplace_back(std::move(doc_right));
            }
            {
                auto session = otterbrix::session_id_t();
                auto ins_left = logical_plan::make_node_insert(dispatcher->resource(),
                                                               {doc_database_name, doc_collection_left},
                                                               documents_left);
                auto cur = dispatcher->execute_plan(session, ins_left);
            }
            {
                auto session = otterbrix::session_id_t();
                auto ins_right = logical_plan::make_node_insert(dispatcher->resource(),
                                                                {doc_database_name, doc_collection_right},
                                                                documents_right);
                auto cur = dispatcher->execute_plan(session, ins_right);
            }
            INFO("right is raw data") {
                auto session = otterbrix::session_id_t();
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                     {doc_database_name, doc_collection_left}));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
                {
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document()->get_long("key_1") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("key") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("value") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_string("name") ==
                            std::pmr::string("Name " + std::to_string((num + 25) * 2)));
                }
            }
            INFO("left is raw data") {
                auto session = otterbrix::session_id_t();
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
                join->append_child(logical_plan::make_node_aggregate(dispatcher->resource(),
                                                                     {doc_database_name, doc_collection_right}));
                {
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document()->get_long("key_1") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("key") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("value") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_string("name") ==
                            std::pmr::string("Name " + std::to_string((num + 25) * 2)));
                }
            }
            INFO("both are raw data") {
                auto session = otterbrix::session_id_t();
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
                {
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document()->get_long("key_1") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("key") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("value") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_string("name") ==
                            std::pmr::string("Name " + std::to_string((num + 25) * 2)));
                }
            }
            INFO("both are raw data with complex join expr") {
                auto session = otterbrix::session_id_t();
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(core::parameter_id_t(1), types::logical_value_t(int64_t{75}));
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
                {
                    auto and_expr =
                        expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
                    and_expr->append_child(expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::eq,
                                                                                expressions::key_t{"key_1"},
                                                                                expressions::key_t{"key"}));
                    and_expr->append_child(expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                side_t::right,
                                                                                expressions::key_t{"key"},
                                                                                core::parameter_id_t(1)));

                    join->append_expression(std::move(and_expr));
                }
                auto cur = dispatcher->execute_plan(session, join, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 13);

                for (int num = 13; num < 26; ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document()->get_long("key_1") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("key") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("value") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_string("name") ==
                            std::pmr::string("Name " + std::to_string((num + 25) * 2)));
                }
            }
            INFO("join raw data with aggregate") {
                auto session = otterbrix::session_id_t();
                auto aggregate = logical_plan::make_node_aggregate(dispatcher->resource(), {});
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                {
                    {
                        std::vector<expressions::expression_ptr> sort = {
                            expressions::make_sort_expression(key("avg"), expressions::sort_order::desc)};
                        aggregate->append_child(
                            logical_plan::make_node_sort(dispatcher->resource(), {}, std::move(sort)));
                    }
                    {
                        // test data does not have any overlaping values, so group here is for raw_data support
                        // not for a functionality
                        auto group = logical_plan::make_node_group(dispatcher->resource(), {});
                        auto scalar_expr = make_scalar_expression(dispatcher->resource(),
                                                                  expressions::scalar_type::get_field,
                                                                  key("key_1"));
                        scalar_expr->append_param(key("key_1"));
                        group->append_expression(std::move(scalar_expr));

                        auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                                 expressions::aggregate_type::count,
                                                                                 key("count"));
                        count_expr->append_param(key("name"));
                        group->append_expression(std::move(count_expr));

                        auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::sum,
                                                                               key("sum"));
                        sum_expr->append_param(key("value"));
                        group->append_expression(std::move(sum_expr));

                        auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::avg,
                                                                               key("avg"));
                        avg_expr->append_param(key("key"));
                        group->append_expression(std::move(avg_expr));

                        auto min_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::min,
                                                                               key("min"));
                        min_expr->append_param(key("value"));
                        group->append_expression(std::move(min_expr));

                        auto max_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::max,
                                                                               key("max"));
                        max_expr->append_param(key("value"));
                        group->append_expression(std::move(max_expr));

                        aggregate->append_child(std::move(group));
                    }
                    {
                        aggregate->append_child(
                            logical_plan::make_node_match(dispatcher->resource(),
                                                          {},
                                                          make_compare_expression(dispatcher->resource(),
                                                                                  compare_type::lt,
                                                                                  side_t::left,
                                                                                  key("key_1"),
                                                                                  core::parameter_id_t(1))));
                    }
                    params->add_parameter(core::parameter_id_t(1), types::logical_value_t(int64_t{75}));
                }
                {
                    auto join =
                        logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                    join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_left));
                    join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), documents_right));
                    {
                        join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                     compare_type::eq,
                                                                                     expressions::key_t{"key_1"},
                                                                                     expressions::key_t{"key"}));
                    }
                    aggregate->append_child(join);
                }
                auto cur = dispatcher->execute_plan(session, aggregate, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 13);

                for (int num = 12; num >= 0; --num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document()->get_long("count") == 1);
                    REQUIRE(cur->get_document()->get_long("sum") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_long("avg") == (num + 25) * 2);
                    REQUIRE(cur->get_document()->get_long("min") == (num + 25) * 2 * 10);
                    REQUIRE(cur->get_document()->get_long("max") == (num + 25) * 2 * 10);
                }
            }
            INFO("just raw data") {
                auto session = otterbrix::session_id_t();
                auto node = logical_plan::make_node_raw_data(dispatcher->resource(), documents_left);
                auto cur = dispatcher->execute_plan(session, node);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == documents_left.size());

                for (int num = 0; num < cur->size(); ++num) {
                    REQUIRE(cur->has_next());
                    cur->next_document();
                    REQUIRE(cur->get_document() == documents_left[num]);
                }
            }
        }
        INFO("table") {
            vector::data_chunk_t chunk_left(dispatcher->resource(), types_left, 101);
            vector::data_chunk_t chunk_right(dispatcher->resource(), types_right, 100);
            chunk_left.set_cardinality(101);
            chunk_right.set_cardinality(100);

            for (int64_t num = 0, reversed = 100; num < 101; ++num, --reversed) {
                chunk_left.set_value(0, num, types::logical_value_t{gen_id(num + 1)});
                chunk_left.set_value(1, num, types::logical_value_t{"Name " + std::to_string(num)});
                chunk_left.set_value(2, num, types::logical_value_t{num});
                chunk_left.set_value(3, num, types::logical_value_t{reversed});
            }
            for (int64_t num = 0; num < 100; ++num) {
                chunk_right.set_value(0, num, types::logical_value_t{gen_id(num + 1001)});
                chunk_right.set_value(1, num, types::logical_value_t{(num + 25) * 2 * 10});
                chunk_right.set_value(2, num, types::logical_value_t{(num + 25) * 2});
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
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() ==
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
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() ==
                            "Name " + std::to_string((num + 25) * 2));
                }
            }
            INFO("both are raw data") {
                auto session = otterbrix::session_id_t();
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
                {
                    join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                 compare_type::eq,
                                                                                 expressions::key_t{"key_1"},
                                                                                 expressions::key_t{"key"}));
                }
                auto cur = dispatcher->execute_plan(session, join);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 26);

                for (int num = 0; num < 26; ++num) {
                    REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(1, num).value<std::string_view>() ==
                            "Name " + std::to_string((num + 25) * 2));
                }
            }
            INFO("both are raw data with complex join expr") {
                auto session = otterbrix::session_id_t();
                auto params = logical_plan::make_parameter_node(dispatcher->resource());
                params->add_parameter(core::parameter_id_t(1), types::logical_value_t(int64_t{75}));
                auto join = logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
                join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
                {
                    auto and_expr =
                        expressions::make_compare_union_expression(dispatcher->resource(), compare_type::union_and);
                    and_expr->append_child(expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::eq,
                                                                                expressions::key_t{"key_1"},
                                                                                expressions::key_t{"key"}));
                    and_expr->append_child(expressions::make_compare_expression(dispatcher->resource(),
                                                                                compare_type::gt,
                                                                                side_t::right,
                                                                                expressions::key_t{"key"},
                                                                                core::parameter_id_t(1)));

                    join->append_expression(std::move(and_expr));
                }
                auto cur = dispatcher->execute_plan(session, join, params);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == 13);

                for (int index = 0, num = 13; index < 13; ++index, ++num) {
                    REQUIRE(cur->chunk_data().value(2, index).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(5, index).value<int64_t>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(4, index).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(1, index).value<std::string_view>() ==
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
                            expressions::make_sort_expression(key("avg"), expressions::sort_order::desc)};
                        aggregate->append_child(
                            logical_plan::make_node_sort(dispatcher->resource(), {}, std::move(sort)));
                    }
                    {
                        // test data does not have any overlaping values, so group here is for raw_data support
                        // not for a functionality
                        auto group = logical_plan::make_node_group(dispatcher->resource(), {});
                        auto scalar_expr = make_scalar_expression(dispatcher->resource(),
                                                                  expressions::scalar_type::get_field,
                                                                  key("key_1"));
                        scalar_expr->append_param(key("key_1"));
                        group->append_expression(std::move(scalar_expr));

                        auto count_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                                 expressions::aggregate_type::count,
                                                                                 key("count"));
                        count_expr->append_param(key("name"));
                        group->append_expression(std::move(count_expr));

                        auto sum_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::sum,
                                                                               key("sum"));
                        sum_expr->append_param(key("value"));
                        group->append_expression(std::move(sum_expr));

                        auto avg_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::avg,
                                                                               key("avg"));
                        avg_expr->append_param(key("key"));
                        group->append_expression(std::move(avg_expr));

                        auto min_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::min,
                                                                               key("min"));
                        min_expr->append_param(key("value"));
                        group->append_expression(std::move(min_expr));

                        auto max_expr = expressions::make_aggregate_expression(dispatcher->resource(),
                                                                               expressions::aggregate_type::max,
                                                                               key("max"));
                        max_expr->append_param(key("value"));
                        group->append_expression(std::move(max_expr));

                        aggregate->append_child(std::move(group));
                    }
                    {
                        aggregate->append_child(
                            logical_plan::make_node_match(dispatcher->resource(),
                                                          {},
                                                          make_compare_expression(dispatcher->resource(),
                                                                                  compare_type::lt,
                                                                                  side_t::left,
                                                                                  key("key_1"),
                                                                                  core::parameter_id_t(1))));
                    }
                    params->add_parameter(core::parameter_id_t(1), types::logical_value_t(int64_t{75}));
                }
                {
                    auto join =
                        logical_plan::make_node_join(dispatcher->resource(), {}, logical_plan::join_type::inner);
                    join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_left));
                    join->append_child(logical_plan::make_node_raw_data(dispatcher->resource(), chunk_right));
                    {
                        join->append_expression(expressions::make_compare_expression(dispatcher->resource(),
                                                                                     compare_type::eq,
                                                                                     expressions::key_t{"key_1"},
                                                                                     expressions::key_t{"key"}));
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
                REQUIRE(cur->chunk_data().data[3].type().type() == types::logical_type::DOUBLE);
                REQUIRE(cur->chunk_data().data[3].type().alias() == "avg");
                REQUIRE(cur->chunk_data().data[4].type().type() == types::logical_type::BIGINT);
                REQUIRE(cur->chunk_data().data[4].type().alias() == "min");
                REQUIRE(cur->chunk_data().data[5].type().type() == types::logical_type::BIGINT);
                REQUIRE(cur->chunk_data().data[5].type().alias() == "max");

                for (int num = 12; num >= 0; --num) {
                    REQUIRE(cur->chunk_data().value(1, num).value<uint64_t>() == 1);
                    REQUIRE(cur->chunk_data().value(2, num).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(3, num).value<double>() == (num + 25) * 2);
                    REQUIRE(cur->chunk_data().value(4, num).value<int64_t>() == (num + 25) * 2 * 10);
                    REQUIRE(cur->chunk_data().value(5, num).value<int64_t>() == (num + 25) * 2 * 10);
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
}
