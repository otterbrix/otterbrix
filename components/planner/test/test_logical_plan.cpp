#include <actor-zeta.hpp>
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/planner.hpp>
#include <components/tests/generaty.hpp>

#include <limits>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

constexpr auto database_name = "database";
constexpr auto collection_name = "collection";

TEST_CASE("components::planner::create_database") {
    auto resource = core::pmr::otterbrix_resource();
    auto plan = make_node_create_database(
        &resource,
        qualified_name_t{core::dbname_t{std::string{database_name}}, core::schema_t{}, core::relname_t{}});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($create_database: database)_");
}

TEST_CASE("components::planner::drop_database") {
    auto resource = core::pmr::otterbrix_resource();
    auto plan = make_node_drop(&resource, drop_target_kind::database);
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    // drop nodes carry only OIDs; enrich is not run in this unit test
    // harness so namespace_oid stays INVALID_OID (== 0).
    REQUIRE(node->to_string() == R"_($drop_database: <oid:0>)_");
}

TEST_CASE("components::planner::create_collection") {
    auto resource = core::pmr::otterbrix_resource();
    auto plan = make_node_create_collection(&resource, core::relname_t{collection_name});
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    REQUIRE(node->to_string() == R"_($create_collection: collection)_");
}

TEST_CASE("components::planner::drop_collection") {
    auto resource = core::pmr::otterbrix_resource();
    auto plan = make_node_drop(&resource, drop_target_kind::collection);
    components::planner::planner_t planner;
    auto node = planner.create_plan(&resource, plan);
    // drop nodes carry only OIDs; enrich is not run in this unit test
    // harness so table_oid stays INVALID_OID (== 0).
    REQUIRE(node->to_string() == R"_($drop_collection: <oid:0>)_");
}

TEST_CASE("components::planner::match") {
    auto resource = core::pmr::otterbrix_resource();
    auto node_match = make_node_match(&resource,
                                      qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}},
                                      make_compare_expression(&resource,
                                                              compare_type::eq,
                                                              key(&resource, "key", side_t::left),
                                                              core::parameter_id_t(1)));
    REQUIRE(node_match->to_string() == R"_($match: {"key": {$eq: #1}})_");
}

TEST_CASE("components::planner::group") {
    auto resource = core::pmr::otterbrix_resource();
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "count"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(&resource, "sum", key(&resource, "total"));
        auto expr_multiply = make_scalar_expression(&resource, scalar_type::multiply);
        expr_multiply->append_param(key(&resource, "price"));
        expr_multiply->append_param(key(&resource, "quantity"));
        agg_expr->append_param(std::move(expr_multiply));
        expressions.emplace_back(std::move(agg_expr));
        agg_expr = make_aggregate_expression(&resource, "avg", key(&resource, "avg_quantity"));
        agg_expr->append_param(key(&resource, "quantity"));
        expressions.emplace_back(std::move(agg_expr));
        auto node_group = make_node_group(&resource, expressions);
        REQUIRE(
            node_group->to_string() ==
            R"_($group: {count: "date", total: {$sum: {$multiply: ["price", "quantity"]}}, avg_quantity: {$avg: "quantity"}})_");
    }
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "count"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        scalar_expr = make_scalar_expression(&resource, scalar_type::multiply, key(&resource, "count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key(&resource, "count"));
        expressions.emplace_back(std::move(scalar_expr));
        auto node_group = make_node_group(&resource, expressions);
        REQUIRE(node_group->to_string() == R"_($group: {count: "date", count_4: {$multiply: [#1, "count"]}})_");
    }
}

TEST_CASE("components::planner::sort") {
    auto resource = core::pmr::otterbrix_resource();
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "key"), sort_order::asc});
        auto node_sort = make_node_sort(&resource, expressions);
        REQUIRE(node_sort->to_string() == R"_($sort: {key: 1})_");
    }
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "key1"), sort_order::asc});
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "key2"), sort_order::desc});
        auto node_sort = make_node_sort(&resource, expressions);
        REQUIRE(node_sort->to_string() == R"_($sort: {key1: 1, key2: -1})_");
    }
}

TEST_CASE("components::planner::aggregate") {
    auto resource = core::pmr::otterbrix_resource();
    auto aggregate =
        make_node_aggregate(&resource,
                            qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}});

    aggregate->append_child(
        make_node_match(&resource,
                        qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}},
                        make_compare_expression(&resource,
                                                compare_type::eq,
                                                key(&resource, "key", side_t::left),
                                                core::parameter_id_t(1))));

    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "count"));
        scalar_expr->append_param(key(&resource, "date"));
        expressions.emplace_back(std::move(scalar_expr));
        scalar_expr = make_scalar_expression(&resource, scalar_type::multiply, key(&resource, "count_4"));
        scalar_expr->append_param(core::parameter_id_t(1));
        scalar_expr->append_param(key(&resource, "count"));
        expressions.emplace_back(std::move(scalar_expr));
        aggregate->append_child(make_node_group(&resource, expressions));
    }
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "name"), sort_order::asc});
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "count"), sort_order::desc});
        aggregate->append_child(make_node_sort(&resource, expressions));
    }

    components::planner::planner_t planner;
    auto node_aggregate = planner.create_plan(&resource, aggregate);

    REQUIRE(node_aggregate->to_string() == R"_($aggregate: {)_"
                                           R"_($match: {"key": {$eq: #1}}, )_"
                                           R"_($group: {count: "date", count_4: {$multiply: [#1, "count"]}}, )_"
                                           R"_($sort: {name: 1, count: -1})_"
                                           R"_(})_");
}

TEST_CASE("components::planner::aggregate_having") {
    // HAVING is a first-class $having node lowered as an aggregate child, rendered by
    // node_aggregate's to_string BETWEEN $group and $sort (post-aggregation filter, the
    // SQL step between GROUP BY and ORDER BY). The compare rides at expressions()[0].
    auto resource = core::pmr::otterbrix_resource();
    auto aggregate =
        make_node_aggregate(&resource,
                            qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}});

    // $group: GROUP BY k + hidden SUM(x) aggregate exposed as "sum_x"
    {
        std::vector<expression_ptr> expressions;
        auto scalar_expr = make_scalar_expression(&resource, scalar_type::get_field, key(&resource, "k"));
        scalar_expr->append_param(key(&resource, "k"));
        expressions.emplace_back(std::move(scalar_expr));
        auto agg_expr = make_aggregate_expression(&resource, "sum", key(&resource, "sum_x"));
        agg_expr->append_param(key(&resource, "x"));
        expressions.emplace_back(std::move(agg_expr));
        aggregate->append_child(make_node_group(&resource, expressions));
    }

    // $having: SUM(x) > #1  (compare carried at expressions()[0], mirroring node_match)
    aggregate->append_child(make_node_having(&resource,
                                             make_compare_expression(&resource,
                                                                     compare_type::gt,
                                                                     key(&resource, "sum_x", side_t::left),
                                                                     core::parameter_id_t(1))));

    // $sort: ORDER BY k
    {
        std::vector<expression_ptr> expressions;
        expressions.emplace_back(new sort_expression_t{&resource, key(&resource, "k"), sort_order::asc});
        aggregate->append_child(make_node_sort(&resource, expressions));
    }

    components::planner::planner_t planner;
    auto node_aggregate = planner.create_plan(&resource, aggregate);

    const std::string plan = node_aggregate->to_string();
    const auto pos_group = plan.find("$group:");
    const auto pos_having = plan.find("$having:");
    const auto pos_sort = plan.find("$sort:");
    REQUIRE(pos_group != std::string::npos);
    REQUIRE(pos_having != std::string::npos);
    REQUIRE(pos_sort != std::string::npos);
    // $having renders AFTER $group and BEFORE $sort in the aggregate plan string.
    REQUIRE(pos_group < pos_having);
    REQUIRE(pos_having < pos_sort);
}

TEST_CASE("components::planner::insert") {
    auto resource = core::pmr::otterbrix_resource();
    {
        auto chunk = gen_data_chunk(0, &resource);
        auto plan = make_node_insert(&resource, std::move(chunk));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        // insert prints OID-based identity; enrich is not run in this
        // unit test harness so table_oid stays INVALID_OID (== 0).
        REQUIRE(node->to_string() == R"_($insert: <oid:0> {$raw_data: {$rows: 0}})_");
    }
    {
        auto chunk = gen_data_chunk(1, &resource);
        auto plan = make_node_insert(&resource, std::move(chunk));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: <oid:0> {$raw_data: {$rows: 1}})_");
    }
    {
        auto chunk = gen_data_chunk(5, &resource);
        auto plan = make_node_insert(&resource, std::move(chunk));
        components::planner::planner_t planner;
        auto node = planner.create_plan(&resource, plan);
        REQUIRE(node->to_string() == R"_($insert: <oid:0> {$raw_data: {$rows: 5}})_");
    }
}

TEST_CASE("components::planner::limit") {
    auto resource = core::pmr::otterbrix_resource();
    {
        auto limit = limit_t::limit_one();
        auto node_limit = make_node_limit(&resource, limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 1)_");
    }
    {
        auto limit = limit_t::all();
        auto node_limit = make_node_limit(&resource, limit);
        REQUIRE(node_limit->to_string().empty());
    }
    {
        auto limit = limit_t(5);
        auto node_limit = make_node_limit(&resource, limit);
        REQUIRE(node_limit->to_string() == R"_($limit: 5)_");
    }
    {
        auto node_limit = make_node_limit(&resource, limit_t::all_from(2));
        REQUIRE(node_limit->to_string() == R"_($offset: 2)_");
    }
    {
        // As written, not validated: a negative count prints as written.
        auto node_limit = make_node_limit(&resource, limit_t(-1));
        REQUIRE(node_limit->to_string() == R"_($limit: -1)_");
    }
}

TEST_CASE("components::planner::limit_t_keeps_what_was_written") {
    REQUIRE_FALSE(limit_t::all().has_count());
    REQUIRE_FALSE(limit_t::all_from(3).has_count());
    REQUIRE(limit_t::all_from(3).offset() == 3);
    REQUIRE(limit_t(0).has_count());
    REQUIRE(limit_t(0).count() == 0);
    REQUIRE(limit_t(-1).has_count());
    REQUIRE(limit_t(-1).count() == -1);
    REQUIRE(limit_t(2, -7).offset() == -7);
    REQUIRE(limit_t::limit_one().count() == 1);
}

TEST_CASE("components::planner::limit_t_is_effective") {
    REQUIRE_FALSE(limit_t::all().is_effective());
    REQUIRE_FALSE(limit_t().is_effective());
    REQUIRE(limit_t::all_from(1).is_effective());
    REQUIRE(limit_t(0).is_effective());
    REQUIRE(limit_t(5).is_effective());
    // an invalid window is still a window: operator_limit has to start to refuse it
    REQUIRE(limit_t(-1).is_effective());
    REQUIRE(limit_t::all_from(-1).is_effective());
}

TEST_CASE("components::planner::limit_t_validate") {
    auto resource = core::pmr::otterbrix_resource();
    REQUIRE_FALSE(limit_t::all().validate(&resource).contains_error());
    REQUIRE_FALSE(limit_t(0).validate(&resource).contains_error());
    REQUIRE_FALSE(limit_t(3, 4).validate(&resource).contains_error());
    REQUIRE_FALSE(limit_t::all_from(4).validate(&resource).contains_error());

    auto negative_limit = limit_t(-1).validate(&resource);
    REQUIRE(negative_limit.contains_error());
    REQUIRE(negative_limit.what == "LIMIT must not be negative");
    REQUIRE(negative_limit.type == core::error_code_t::invalid_parameter);

    auto negative_offset = limit_t::all_from(-1).validate(&resource);
    REQUIRE(negative_offset.contains_error());
    REQUIRE(negative_offset.what == "OFFSET must not be negative");
    REQUIRE(negative_offset.type == core::error_code_t::invalid_parameter);

    // OFFSET is checked first
    auto both = limit_t(-1, -1).validate(&resource);
    REQUIRE(both.contains_error());
    REQUIRE(both.what == "OFFSET must not be negative");
}

TEST_CASE("components::planner::limit_t_read_cap") {
    REQUIRE_FALSE(limit_t::all().read_cap().is_capped());
    REQUIRE_FALSE(limit_t::all_from(5).read_cap().is_capped());
    REQUIRE(limit_t(0).read_cap().is_capped());
    REQUIRE(limit_t(0).read_cap().rows() == 0);
    REQUIRE(limit_t(3).read_cap().rows() == 3);
    // the cap is what a source has to produce for the window [offset, offset + count) to be filled
    REQUIRE(limit_t(3, 4).read_cap().rows() == 7);
    // the sum of two int64 fits the unsigned cap, so nothing wraps
    REQUIRE(limit_t(std::numeric_limits<int64_t>::max(), 5).read_cap().rows() ==
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 5);
    // an invalid window caps nothing: it is refused when the Limit node starts
    REQUIRE_FALSE(limit_t(-1).read_cap().is_capped());
    REQUIRE_FALSE(limit_t(-5, 2).read_cap().is_capped());
    REQUIRE_FALSE(limit_t(5, -2).read_cap().is_capped());
}

TEST_CASE("components::planner::row_cap_t") {
    REQUIRE_FALSE(row_cap_t::uncapped().is_capped());
    REQUIRE(row_cap_t::uncapped().admits(0));
    REQUIRE(row_cap_t::uncapped().admits(std::numeric_limits<uint64_t>::max() - 1));
    REQUIRE(row_cap_t::uncapped().to_disk() == -1);

    REQUIRE(row_cap_t(0).is_capped());
    REQUIRE_FALSE(row_cap_t(0).admits(0));
    REQUIRE(row_cap_t(0).to_disk() == 0);

    REQUIRE(row_cap_t(2).admits(0));
    REQUIRE(row_cap_t(2).admits(1));
    REQUIRE_FALSE(row_cap_t(2).admits(2));
    REQUIRE(row_cap_t(2).rows() == 2);
    REQUIRE(row_cap_t(2).to_disk() == 2);

    // the disk protocol is signed and -1 means "no cap": a huge cap saturates below it
    REQUIRE(row_cap_t(std::numeric_limits<uint64_t>::max() - 1).to_disk() == std::numeric_limits<int64_t>::max());
    REQUIRE(row_cap_t(static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1).to_disk() ==
            std::numeric_limits<int64_t>::max());
}

TEST_CASE("components::planner::delete") {
    auto resource = core::pmr::otterbrix_resource();
    auto match = make_node_match(&resource,
                                 qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}},
                                 make_compare_expression(&resource,
                                                         compare_type::eq,
                                                         key(&resource, "key", side_t::left),
                                                         core::parameter_id_t(1)));
    components::logical_plan::storage_parameters parameters{&resource};
    {
        auto node = make_node_delete(&resource, match, make_node_limit(&resource, limit_t::all()));
        components::planner::planner_t planner;
        auto node_delete = planner.create_plan(&resource, node);
        REQUIRE(node_delete->to_string() == R"_($delete: <oid:0> {$match: {"key": {$eq: #1}}})_");
    }
    {
        auto node = make_node_delete(&resource, match, make_node_limit(&resource, limit_t::limit_one()));
        components::planner::planner_t planner;
        auto node_delete = planner.create_plan(&resource, node);
        REQUIRE(node_delete->to_string() == R"_($delete: <oid:0> {$match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}

TEST_CASE("components::planner::update") {
    auto resource = core::pmr::otterbrix_resource();
    auto match = make_node_match(&resource,
                                 qualified_name_t{core::schema_t{database_name}, core::relname_t{collection_name}},
                                 make_compare_expression(&resource,
                                                         compare_type::eq,
                                                         key(&resource, "key", side_t::left),
                                                         core::parameter_id_t(1)));

    // SET count = $0 — the value expression's own key names the target column.
    expression_ptr update =
        make_scalar_expression(&resource, scalar_type::constant, components::expressions::key_t{&resource, "count"});
    static_cast<scalar_expression_t*>(update.get())->append_param(core::parameter_id_t(0));

    components::logical_plan::storage_parameters parameters{&resource};
    {
        auto node = make_node_update(&resource, match, make_node_limit(&resource, limit_t::all()), {update});
        components::planner::planner_t planner;
        auto node_update = planner.create_plan(&resource, node);
        REQUIRE(node_update->to_string() == R"_($update: <oid:0> {$match: {"key": {$eq: #1}}})_");
    }
    {
        auto node = make_node_update(&resource, match, make_node_limit(&resource, limit_t::limit_one()), {update});
        components::planner::planner_t planner;
        auto node_update = planner.create_plan(&resource, node);
        REQUIRE(node_update->to_string() == R"_($update: <oid:0> {$match: {"key": {$eq: #1}}, $limit: 1})_");
    }
}

// At plan time every OID is still INVALID_OID, so node_drop_t::hash_impl must key on the
// written names/missing_ok/behavior too — hashing only enrich-stamped OIDs would make
// `DROP TABLE a`, `DROP TABLE b` and `DROP TABLE IF EXISTS a` collide.
TEST_CASE("components::planner::node_drop_hash_folds_names_and_flags") {
    auto resource = core::pmr::otterbrix_resource();
    auto base = [&]() {
        auto n = make_node_drop(&resource, drop_target_kind::collection);
        n->set_target(qualified_name_t{core::schema_t{"db"}, core::relname_t{"t"}});
        return n;
    };

    auto a = base();

    SECTION("a different relname hashes differently") {
        auto b = base();
        b->set_target(qualified_name_t{core::schema_t{"db"}, core::relname_t{"u"}});
        REQUIRE(a->hash() != b->hash());
    }
    SECTION("a different dbname hashes differently") {
        auto b = base();
        b->set_target(qualified_name_t{core::schema_t{"other"}, core::relname_t{"t"}});
        REQUIRE(a->hash() != b->hash());
    }
    SECTION("RESTRICT hashes differently from CASCADE") {
        auto b = base();
        // The node default is restrict_, so the differing side is CASCADE.
        b->set_behavior(components::catalog::drop_behavior_t::cascade_);
        REQUIRE(a->hash() != b->hash());
    }
    SECTION("IF EXISTS hashes differently from the plain statement") {
        auto b = base();
        b->set_if_exists(true);
        REQUIRE(a->hash() != b->hash());
    }
    SECTION("a different index name hashes differently") {
        auto i1 = make_node_drop(&resource, drop_target_kind::index);
        i1->set_target(qualified_name_t{core::schema_t{"db"}, core::relname_t{"t"}});
        i1->set_index_name(core::indexname_t{"idx1"});
        auto i2 = make_node_drop(&resource, drop_target_kind::index);
        i2->set_target(qualified_name_t{core::schema_t{"db"}, core::relname_t{"t"}});
        i2->set_index_name(core::indexname_t{"idx2"});
        REQUIRE(i1->hash() != i2->hash());
    }
}
