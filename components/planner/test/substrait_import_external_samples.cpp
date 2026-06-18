#include <components/logical_plan/substrait_adapter/from_substrait.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>

#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

using namespace components::logical_plan;

namespace {

    std::string read_binary_file(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in.good()) {
            throw std::runtime_error("failed to open input file: " + path.string());
        }
        std::ostringstream buffer;
        buffer << in.rdbuf();
        return buffer.str();
    }

    void require(bool condition, const std::string& message) {
        if (!condition) {
            throw std::runtime_error(message);
        }
    }

    void require_type(const node_ptr& node, node_type expected, const std::string& context) {
        require(node != nullptr, context + ": null node");
        require(node->type() == expected,
                context + ": expected " + to_string(expected) + ", got " + to_string(node->type()));
    }

    const node_ptr& require_any_child(const node_ptr& node, node_type expected, const std::string& context) {
        require_type(node, node_type::aggregate_t, context + " root");
        for (const auto& child : node->children()) {
            if (child->type() == expected) {
                return child;
            }
        }
        throw std::runtime_error(context + ": expected child " + to_string(expected) + ", got tree " + node->to_string());
    }

    std::size_t count_children(const node_ptr& node, node_type expected) {
        std::size_t count = 0;
        for (const auto& child : node->children()) {
            if (child->type() == expected) {
                ++count;
            }
        }
        return count;
    }

    void require_sort_order(const node_ptr& sort_node,
                            std::size_t index,
                            components::expressions::sort_order expected,
                            const std::string& context) {
        require_type(sort_node, node_type::sort_t, context + " sort");
        require(sort_node->expressions().size() > index, context + ": missing sort expression at index " + std::to_string(index));
        const auto* sort_expr = static_cast<const components::expressions::sort_expression_t*>(
            sort_node->expressions().at(index).get());
        require(sort_expr->order() == expected,
                context + ": unexpected sort direction at index " + std::to_string(index));
    }

    void require_pipeline_children(const node_ptr& plan,
                                   const std::string& context,
                                   bool needs_match,
                                   bool needs_join) {
        require_type(plan, node_type::aggregate_t, context + " root");
        if (needs_match) {
            const auto& match = require_any_child(plan, node_type::match_t, context);
            require(!match->expressions().empty(), context + ": filter expression was not restored");
        }
        if (needs_join) {
            const auto& join = require_any_child(plan, node_type::join_t, context);
            require(join->children().size() == 2, context + ": expected two join inputs");
            require(!join->expressions().empty(), context + ": join condition was not restored");
        }
        require(count_children(plan, node_type::group_t) >= 1, context + ": expected projection/group expression");
        require_any_child(plan, node_type::sort_t, context);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, context);
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() > 0, context + ": expected positive LIMIT in tree " + plan->to_string());
    }

    void check_collection(const node_ptr& node, const std::string& expected, const std::string& context) {
        require(node->collection_full_name().collection == expected,
                context + ": expected collection '" + expected + "', got '" +
                    node->collection_full_name().collection + "'");
    }

    void check_read_named_table(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "read_named_table");
        check_collection(plan, "coll", "read_named_table");
        for (const auto& child : plan->children()) {
            require(child->type() == node_type::group_t,
                    "read_named_table: SELECT * may only add a DataFusion projection wrapper");
        }
    }

    void check_filter_gt(const node_ptr& plan) {
        check_collection(plan, "coll", "filter_gt");
        const auto& match = require_any_child(plan, node_type::match_t, "filter_gt");
        require(!match->expressions().empty(), "filter_gt: filter expression was not restored");
    }

    void check_project_single_field(const node_ptr& plan) {
        check_collection(plan, "coll", "project_single_field");
        const auto& group = require_any_child(plan, node_type::group_t, "project_single_field");
        require(group->expressions().size() == 1, "project_single_field: expected one projected expression");
    }

    void check_project_arithmetic_multi(const node_ptr& plan) {
        check_collection(plan, "coll", "project_arithmetic_multi");
        const auto& group = require_any_child(plan, node_type::group_t, "project_arithmetic_multi");
        require(group->expressions().size() == 3,
                "project_arithmetic_multi: expected id plus two arithmetic projections");
    }

    void check_filter_between(const node_ptr& plan) {
        check_collection(plan, "coll", "filter_between");
        const auto& match = require_any_child(plan, node_type::match_t, "filter_between");
        require(!match->expressions().empty(), "filter_between: AND filter expression was not restored");
        require_any_child(plan, node_type::group_t, "filter_between");
    }

    void check_sort_single_field(const node_ptr& plan) {
        check_collection(plan, "coll", "sort_single_field");
        const auto& sort = require_any_child(plan, node_type::sort_t, "sort_single_field");
        require(sort->expressions().size() == 1, "sort_single_field: expected one sort expression");
        require_sort_order(sort, 0, components::expressions::sort_order::asc, "sort_single_field");
    }

    void check_sort_two_fields(const node_ptr& plan) {
        check_collection(plan, "sales", "sort_two_fields");
        const auto& sort = require_any_child(plan, node_type::sort_t, "sort_two_fields");
        require(sort->expressions().size() == 2, "sort_two_fields: expected two sort expressions");
        require_sort_order(sort, 0, components::expressions::sort_order::asc, "sort_two_fields");
        require_sort_order(sort, 1, components::expressions::sort_order::desc, "sort_two_fields");
        const auto& group = require_any_child(plan, node_type::group_t, "sort_two_fields");
        require(group->expressions().size() == 2, "sort_two_fields: expected two selected fields");
    }

    void check_fetch_limit_10(const node_ptr& plan) {
        check_collection(plan, "limit_table", "fetch_limit_10");
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "fetch_limit_10");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 10,
                "fetch_limit_10: expected LIMIT 10, got " + std::to_string(typed_limit->limit().limit()) +
                    " in tree " + plan->to_string());
    }

    void check_join_inner(const node_ptr& plan) {
        const auto& join = require_any_child(plan, node_type::join_t, "join_inner");
        const auto* typed_join = static_cast<const node_join_t*>(join.get());
        require(typed_join->type() == join_type::inner, "join_inner: expected inner join");
        require(join->children().size() == 2, "join_inner: expected two join inputs");
        check_collection(join->children().at(0), "left_table", "join_inner left");
        check_collection(join->children().at(1), "right_table", "join_inner right");
        require(!join->expressions().empty(), "join_inner: join condition was not restored");
    }

    void check_aggregate_count(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "aggregate_count");
        check_collection(plan, "sales", "aggregate_count");
        const auto& group = require_any_child(plan, node_type::group_t, "aggregate_count");
        require(group->expressions().size() == 2, "aggregate_count: expected group key and count measure");
    }

    void check_aggregate_multi_measure(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "aggregate_multi_measure");
        check_collection(plan, "sales", "aggregate_multi_measure");
        const auto& group = require_any_child(plan, node_type::group_t, "aggregate_multi_measure");
        require(group->expressions().size() == 5,
                "aggregate_multi_measure: expected group key plus count/sum/min/max measures");
    }

    void check_pipeline_filter_project_sort_limit(const node_ptr& plan) {
        check_collection(plan, "coll", "pipeline_filter_project_sort_limit");
        require_pipeline_children(plan, "pipeline_filter_project_sort_limit", true, false);
    }

    void check_aggregate_filter_sort_limit(const node_ptr& plan) {
        check_collection(plan, "sales", "aggregate_filter_sort_limit");
        require_pipeline_children(plan, "aggregate_filter_sort_limit", true, false);
        require(count_children(plan, node_type::group_t) >= 1,
                "aggregate_filter_sort_limit: expected aggregate/group output");
    }

    void check_join_project_sort_limit(const node_ptr& plan) {
        require_pipeline_children(plan, "join_project_sort_limit", false, true);
    }

    void check_join_filter_aggregate_sort_limit(const node_ptr& plan) {
        require_pipeline_children(plan, "join_filter_aggregate_sort_limit", true, true);
        const auto& group = require_any_child(plan, node_type::group_t, "join_filter_aggregate_sort_limit");
        require(group->expressions().size() >= 2,
                "join_filter_aggregate_sort_limit: expected group key and count measure");
    }

    void check_large_orders_region_top(const node_ptr& plan) {
        require_pipeline_children(plan, "large_orders_region_top", true, true);
        const auto& group = require_any_child(plan, node_type::group_t, "large_orders_region_top");
        require(group->expressions().size() >= 3,
                "large_orders_region_top: expected region plus count/sum measures");
        const auto& sort = require_any_child(plan, node_type::sort_t, "large_orders_region_top");
        require(sort->expressions().size() == 2, "large_orders_region_top: expected two sort expressions");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "large_orders_region_top");
        require_sort_order(sort, 1, components::expressions::sort_order::asc, "large_orders_region_top");
    }

    void check_large_customer_tier_stats(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "large_customer_tier_stats root");
        require_any_child(plan, node_type::join_t, "large_customer_tier_stats");
        require_any_child(plan, node_type::match_t, "large_customer_tier_stats");
        const auto& group = require_any_child(plan, node_type::group_t, "large_customer_tier_stats");
        require(group->expressions().size() >= 4,
                "large_customer_tier_stats: expected tier plus count/max/min measures");
        const auto& sort = require_any_child(plan, node_type::sort_t, "large_customer_tier_stats");
        require(sort->expressions().size() == 2, "large_customer_tier_stats: expected two sort expressions");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "large_customer_tier_stats");
        require_sort_order(sort, 1, components::expressions::sort_order::asc, "large_customer_tier_stats");
    }

    void check_large_projection_filter_sort_limit(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_projection_filter_sort_limit");
        require_pipeline_children(plan, "large_projection_filter_sort_limit", true, false);
        const auto& group = require_any_child(plan, node_type::group_t, "large_projection_filter_sort_limit");
        require(group->expressions().size() == 3,
                "large_projection_filter_sort_limit: expected order_id/customer_id/gross_amount projections");
        const auto& sort = require_any_child(plan, node_type::sort_t, "large_projection_filter_sort_limit");
        require(sort->expressions().size() == 2, "large_projection_filter_sort_limit: expected two sort expressions");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "large_projection_filter_sort_limit");
        require_sort_order(sort, 1, components::expressions::sort_order::asc, "large_projection_filter_sort_limit");
    }

    void check_coll_filter(const node_ptr& plan) {
        const auto& match = require_any_child(plan, node_type::match_t, "coll_filter");
        require(!match->expressions().empty(), "coll_filter: filter expression was not restored");
        require_any_child(plan, node_type::group_t, "coll_filter");
    }

    void check_coll_filter_sort(const node_ptr& plan) {
        check_coll_filter(plan);
        const auto& sort = require_any_child(plan, node_type::sort_t, "coll_filter_sort");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "coll_filter_sort");
    }

    void check_coll_filter_sort_asc(const node_ptr& plan) {
        check_coll_filter(plan);
        const auto& sort = require_any_child(plan, node_type::sort_t, "coll_filter_sort_asc");
        require_sort_order(sort, 0, components::expressions::sort_order::asc, "coll_filter_sort_asc");
    }

    void check_coll_filter_sort_limit_asc(const node_ptr& plan) {
        check_coll_filter_sort_asc(plan);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "coll_filter_sort_limit_asc");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 1, "coll_filter_sort_limit_asc: expected LIMIT 1");
    }

    void check_project_subtract_multiply(const node_ptr& plan) {
        check_collection(plan, "coll", "project_subtract_multiply");
        const auto& group = require_any_child(plan, node_type::group_t, "project_subtract_multiply");
        require(group->expressions().size() == 3,
                "project_subtract_multiply: expected id plus two arithmetic projections");
    }

    void check_sort_desc_limit_2(const node_ptr& plan) {
        check_collection(plan, "coll", "sort_desc_limit_2");
        const auto& sort = require_any_child(plan, node_type::sort_t, "sort_desc_limit_2");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "sort_desc_limit_2");
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "sort_desc_limit_2");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 2, "sort_desc_limit_2: expected LIMIT 2");
    }

    void check_sales_filter_sort(const node_ptr& plan) {
        check_collection(plan, "sales", "sales_filter_sort");
        require_any_child(plan, node_type::match_t, "sales_filter_sort");
        const auto& sort = require_any_child(plan, node_type::sort_t, "sales_filter_sort");
        require_sort_order(sort, 0, components::expressions::sort_order::desc, "sales_filter_sort");
        require_any_child(plan, node_type::group_t, "sales_filter_sort");
    }

    void check_sales_project_amount_math(const node_ptr& plan) {
        check_collection(plan, "sales", "sales_project_amount_math");
        const auto& group = require_any_child(plan, node_type::group_t, "sales_project_amount_math");
        require(group->expressions().size() == 3,
                "sales_project_amount_math: expected category plus two arithmetic projections");
    }

    void check_sales_aggregate_sum_only(const node_ptr& plan) {
        check_collection(plan, "sales", "sales_aggregate_sum_only");
        const auto& group = require_any_child(plan, node_type::group_t, "sales_aggregate_sum_only");
        require(group->expressions().size() == 2,
                "sales_aggregate_sum_only: expected group key plus sum measure");
    }

    void check_sales_aggregate_min_max_sort(const node_ptr& plan) {
        check_collection(plan, "sales", "sales_aggregate_min_max_sort");
        const auto& group = require_any_child(plan, node_type::group_t, "sales_aggregate_min_max_sort");
        require(group->expressions().size() == 3,
                "sales_aggregate_min_max_sort: expected group key plus min/max measures");
        const auto& sort = require_any_child(plan, node_type::sort_t, "sales_aggregate_min_max_sort");
        require_sort_order(sort, 0, components::expressions::sort_order::asc, "sales_aggregate_min_max_sort");
    }

    void check_sales_global_stats(const node_ptr& plan) {
        check_collection(plan, "sales", "sales_global_stats");
        const auto& group = require_any_child(plan, node_type::group_t, "sales_global_stats");
        require(group->expressions().size() == 3,
                "sales_global_stats: expected count/sum/max aggregate measures");
    }

    void check_large_orders_count(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_orders_count");
        const auto& group = require_any_child(plan, node_type::group_t, "large_orders_count");
        require(group->expressions().size() == 1, "large_orders_count: expected one count measure");
    }

    void check_large_orders_quantity_stats(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_orders_quantity_stats");
        const auto& group = require_any_child(plan, node_type::group_t, "large_orders_quantity_stats");
        require(group->expressions().size() >= 3,
                "large_orders_quantity_stats: expected quantity plus count/sum measures");
        require_any_child(plan, node_type::sort_t, "large_orders_quantity_stats");
    }

    void check_large_orders_filter_top_amounts(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_orders_filter_top_amounts");
        require_pipeline_children(plan, "large_orders_filter_top_amounts", true, false);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "large_orders_filter_top_amounts");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 8, "large_orders_filter_top_amounts: expected LIMIT 8");
    }

    void check_large_orders_project_gross(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_orders_project_gross");
        require_any_child(plan, node_type::match_t, "large_orders_project_gross");
        const auto& group = require_any_child(plan, node_type::group_t, "large_orders_project_gross");
        require(group->expressions().size() == 2,
                "large_orders_project_gross: expected order_id and gross_amount projections");
        require_any_child(plan, node_type::sort_t, "large_orders_project_gross");
    }

    void check_large_customers_region_count(const node_ptr& plan) {
        check_collection(plan, "large_customers", "large_customers_region_count");
        const auto& group = require_any_child(plan, node_type::group_t, "large_customers_region_count");
        require(group->expressions().size() == 2,
                "large_customers_region_count: expected region plus count measure");
        require_any_child(plan, node_type::sort_t, "large_customers_region_count");
    }

    void check_large_customers_tier_filter(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "large_customers_tier_filter root");
        const auto& group = require_any_child(plan, node_type::group_t, "large_customers_tier_filter");
        require(group->expressions().size() >= 2,
                "large_customers_tier_filter: expected customer_id and tier projections");
        const auto& sort = require_any_child(plan, node_type::sort_t, "large_customers_tier_filter");
        require_sort_order(sort, 0, components::expressions::sort_order::asc, "large_customers_tier_filter");
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "large_customers_tier_filter");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 5, "large_customers_tier_filter: expected LIMIT 5");
    }

    void check_large_join_customer_orders(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "large_join_customer_orders root");
        require_any_child(plan, node_type::join_t, "large_join_customer_orders");
        require_any_child(plan, node_type::match_t, "large_join_customer_orders");
        const auto& group = require_any_child(plan, node_type::group_t, "large_join_customer_orders");
        require(group->expressions().size() >= 2,
                "large_join_customer_orders: expected restored projection expressions");
        require_any_child(plan, node_type::sort_t, "large_join_customer_orders");
    }

    void check_large_join_region_quantity_stats(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "large_join_region_quantity_stats root");
        require_any_child(plan, node_type::join_t, "large_join_region_quantity_stats");
        require_any_child(plan, node_type::match_t, "large_join_region_quantity_stats");
        const auto& group = require_any_child(plan, node_type::group_t, "large_join_region_quantity_stats");
        require(group->expressions().size() >= 3,
                "large_join_region_quantity_stats: expected region/quantity/count expressions");
        require_any_child(plan, node_type::sort_t, "large_join_region_quantity_stats");
    }

    void check_large_join_project_sort_limit(const node_ptr& plan) {
        require_pipeline_children(plan, "large_join_project_sort_limit", true, true);
        const auto& group = require_any_child(plan, node_type::group_t, "large_join_project_sort_limit");
        require(group->expressions().size() == 3,
                "large_join_project_sort_limit: expected region/order_id/gross_amount projections");
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "large_join_project_sort_limit");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() == 6, "large_join_project_sort_limit: expected LIMIT 6");
    }

    void check_large_orders_customer_window_sample(const node_ptr& plan) {
        check_collection(plan, "large_orders", "large_orders_customer_window_sample");
        require_any_child(plan, node_type::match_t, "large_orders_customer_window_sample");
        require_any_child(plan, node_type::sort_t, "large_orders_customer_window_sample");
        const auto& group = require_any_child(plan, node_type::group_t, "large_orders_customer_window_sample");
        require(group->expressions().size() >= 3,
                "large_orders_customer_window_sample: expected customer_id plus count/sum measures");
    }

    void check_group_only_any(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "group_only_any root");
        const auto& group = require_any_child(plan, node_type::group_t, "group_only_any");
        require(!group->expressions().empty(), "group_only_any: expected restored group/projection expressions");
    }

    void check_group_sort_any(const node_ptr& plan) {
        check_group_only_any(plan);
        require_any_child(plan, node_type::sort_t, "group_sort_any");
    }

    void check_group_sort_limit_any(const node_ptr& plan) {
        check_group_sort_any(plan);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "group_sort_limit_any");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() > 0, "group_sort_limit_any: expected positive LIMIT");
    }

    void check_filter_group_sort_any(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "filter_group_sort_any root");
        const auto& match = require_any_child(plan, node_type::match_t, "filter_group_sort_any");
        require(!match->expressions().empty(), "filter_group_sort_any: expected restored filter expression");
        require_any_child(plan, node_type::group_t, "filter_group_sort_any");
        require_any_child(plan, node_type::sort_t, "filter_group_sort_any");
    }

    void check_filter_group_sort_limit_any(const node_ptr& plan) {
        check_filter_group_sort_any(plan);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "filter_group_sort_limit_any");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() > 0, "filter_group_sort_limit_any: expected positive LIMIT");
    }

    void check_join_filter_group_sort_any(const node_ptr& plan) {
        require_type(plan, node_type::aggregate_t, "join_filter_group_sort_any root");
        const auto& join = require_any_child(plan, node_type::join_t, "join_filter_group_sort_any");
        require(join->children().size() == 2, "join_filter_group_sort_any: expected two join inputs");
        require_any_child(plan, node_type::match_t, "join_filter_group_sort_any");
        require_any_child(plan, node_type::group_t, "join_filter_group_sort_any");
        require_any_child(plan, node_type::sort_t, "join_filter_group_sort_any");
    }

    void check_join_filter_group_sort_limit_any(const node_ptr& plan) {
        check_join_filter_group_sort_any(plan);
        const auto& limit_node = require_any_child(plan, node_type::limit_t, "join_filter_group_sort_limit_any");
        const auto* typed_limit = static_cast<const node_limit_t*>(limit_node.get());
        require(typed_limit->limit().limit() > 0, "join_filter_group_sort_limit_any: expected positive LIMIT");
    }

    using check_fn = std::function<void(const node_ptr&)>;

    const std::unordered_map<std::string, check_fn>& checks() {
        static const std::unordered_map<std::string, check_fn> value = {
            {"read_named_table", check_read_named_table},
            {"filter_gt", check_filter_gt},
            {"filter_lt_sort_id", check_coll_filter_sort_asc},
            {"filter_gt_project_sort", check_coll_filter_sort_asc},
            {"filter_range_limit_id", check_coll_filter_sort_limit_asc},
            {"filter_and_sort_id", check_coll_filter_sort},
            {"project_single_field", check_project_single_field},
            {"project_arithmetic_multi", check_project_arithmetic_multi},
            {"project_subtract_multiply", check_project_subtract_multiply},
            {"filter_between", check_filter_between},
            {"sort_single_field", check_sort_single_field},
            {"sort_desc_limit_2", check_sort_desc_limit_2},
            {"sort_two_fields", check_sort_two_fields},
            {"fetch_limit_10", check_fetch_limit_10},
            {"join_inner", check_join_inner},
            {"aggregate_count", check_aggregate_count},
            {"aggregate_multi_measure", check_aggregate_multi_measure},
            {"sales_filter_sort", check_sales_filter_sort},
            {"sales_project_amount_math", check_sales_project_amount_math},
            {"sales_aggregate_sum_only", check_sales_aggregate_sum_only},
            {"sales_aggregate_min_max_sort", check_sales_aggregate_min_max_sort},
            {"sales_global_stats", check_sales_global_stats},
            {"pipeline_filter_project_sort_limit", check_pipeline_filter_project_sort_limit},
            {"aggregate_filter_sort_limit", check_aggregate_filter_sort_limit},
            {"join_project_sort_limit", check_join_project_sort_limit},
            {"join_filter_aggregate_sort_limit", check_join_filter_aggregate_sort_limit},
            {"large_orders_region_top", check_large_orders_region_top},
            {"large_customer_tier_stats", check_large_customer_tier_stats},
            {"large_projection_filter_sort_limit", check_large_projection_filter_sort_limit},
            {"large_orders_count", check_large_orders_count},
            {"large_orders_quantity_stats", check_large_orders_quantity_stats},
            {"large_orders_filter_top_amounts", check_large_orders_filter_top_amounts},
            {"large_orders_project_gross", check_large_orders_project_gross},
            {"large_orders_customer_window_sample", check_large_orders_customer_window_sample},
            {"large_customers_region_count", check_large_customers_region_count},
            {"large_customers_tier_filter", check_large_customers_tier_filter},
            {"large_join_customer_orders", check_large_join_customer_orders},
            {"large_join_region_quantity_stats", check_large_join_region_quantity_stats},
            {"large_join_project_sort_limit", check_large_join_project_sort_limit},
            {"coll_all_order_asc", check_group_sort_any},
            {"coll_double_order_desc", check_group_sort_any},
            {"coll_square_filter_sort", check_filter_group_sort_any},
            {"coll_range_project_math", check_filter_group_sort_any},
            {"coll_limit_two_asc", check_group_sort_limit_any},
            {"sales_filter_project_asc", check_filter_group_sort_any},
            {"sales_sort_amount_asc_limit", check_group_sort_limit_any},
            {"sales_count_total", check_group_only_any},
            {"sales_category_count_sum_sort", check_group_sort_any},
            {"sales_filter_aggregate_sum", check_filter_group_sort_any},
            {"sales_project_subtract", check_group_sort_any},
            {"large_orders_quantity_max_min", check_group_sort_any},
            {"large_orders_customer_top_quantity", check_filter_group_sort_limit_any},
            {"large_orders_project_amount_quantity", check_filter_group_sort_limit_any},
            {"large_customers_tier_count", check_group_sort_any},
            {"large_customers_region_tier_count", check_group_sort_any},
            {"large_join_region_amount_top", check_join_filter_group_sort_limit_any},
            {"large_join_tier_quantity_sum", check_join_filter_group_sort_any},
            {"large_join_customer_score", check_join_filter_group_sort_limit_any},
            {"large_join_region_count_amount", check_join_filter_group_sort_any},
        };
        return value;
    }

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2) {
            throw std::runtime_error("usage: substrait_import_external_samples <datafusion-export-dir>");
        }

        std::pmr::synchronized_pool_resource resource;
        const auto export_dir = std::filesystem::path(argv[1]);

        std::unordered_set<std::string> expected_names;
        for (const auto& [name, _] : checks()) {
            expected_names.insert(name);
        }
        for (const auto& entry : std::filesystem::directory_iterator(export_dir)) {
            if (entry.path().extension() == ".bin") {
                const auto name = entry.path().stem().string();
                require(expected_names.contains(name),
                        "no OtterBrix import checker registered for external producer sample: " + name);
            }
        }

        for (const auto& [name, check] : checks()) {
            const auto path = export_dir / (name + ".bin");
            const auto binary = read_binary_file(path);
            auto restored = substrait_adapter::from_substrait_binary(
                &resource,
                binary,
                substrait_adapter::import_profile_t::external_canonical);

            require(restored.plan != nullptr, name + ": importer returned null plan");
            check(restored.plan);
            std::cout << "PASS OtterBrix import " << name << ".bin: " << restored.plan->to_string() << '\n';
        }

        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "FAIL: " << ex.what() << '\n';
        return 1;
    }
}
