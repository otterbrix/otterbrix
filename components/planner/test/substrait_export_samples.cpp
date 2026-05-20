    #include <components/catalog/catalog.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_create_type.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_type.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/substrait_adapter/to_substrait.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <vector>

using namespace components::logical_plan;
using namespace components::expressions;
using key = components::expressions::key_t;

namespace {

    collection_full_name_t collection_name(std::string_view name) {
        return {std::string("db"), std::string(name)};
    }

    void write_text_file(const std::filesystem::path& path, const std::string& data) {
        std::ofstream out(path);
        if (!out.good()) {
            throw std::runtime_error("failed to open output file: " + path.string());
        }
        out << data;
    }

    void write_binary_file(const std::filesystem::path& path, const std::string& data) {
        std::ofstream out(path, std::ios::binary);
        if (!out.good()) {
            throw std::runtime_error("failed to open output file: " + path.string());
        }
        out.write(data.data(), static_cast<std::streamsize>(data.size()));
    }

    void register_table(components::catalog::catalog& cat,
                        std::pmr::memory_resource* resource,
                        const collection_full_name_t& name,
                        std::vector<components::types::complex_logical_type> fields) {
        std::vector<components::table::column_definition_t> columns;
        std::vector<components::types::field_description> descriptions;
        columns.reserve(fields.size());
        descriptions.reserve(fields.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            auto column_name = fields[i].has_alias() ? fields[i].alias() : ("col_" + std::to_string(i));
            columns.emplace_back(std::move(column_name), std::move(fields[i]));
            descriptions.emplace_back(i + 1);
        }
        std::pmr::vector<components::catalog::field_id_t> primary_key(resource);
        primary_key.emplace_back(1);
        components::catalog::schema schema(resource, columns, descriptions, primary_key);
        auto err = cat.create_table({resource, name}, {resource, schema});
        if (err.contains_error()) {
            throw std::runtime_error("failed to register table " + name.collection + ": " + std::string(err.what));
        }
    }

    node_ptr build_read_plan(std::pmr::memory_resource* resource) {
        return make_node_aggregate(resource, collection_name("coll"));
    }

    node_ptr build_sales_read_plan(std::pmr::memory_resource* resource) {
        return make_node_aggregate(resource, collection_name("sales"));
    }

    node_ptr build_metrics_read_plan(std::pmr::memory_resource* resource) {
        return make_node_aggregate(resource, collection_name("metrics"));
    }

    node_ptr build_filter_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource, collection_name("coll"), make_compare_expression(resource, compare_type::all_true)));
        return plan;
    }

    node_ptr build_project_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        std::vector<expression_ptr> exprs;
        auto field = make_scalar_expression(resource, scalar_type::get_field, key(resource, "id_alias"));
        field->append_param(key(resource, "id"));
        exprs.emplace_back(std::move(field));
        plan->append_child(make_node_group(resource, collection_name("coll"), exprs));
        return plan;
    }

    expression_ptr make_field_projection(std::pmr::memory_resource* resource,
                                          std::string_view output_name,
                                          std::string_view input_name) {
        auto field = make_scalar_expression(resource, scalar_type::get_field, key(resource, std::string(output_name)));
        field->append_param(key(resource, std::string(input_name)));
        return field;
    }

    expression_ptr make_binary_scalar(std::pmr::memory_resource* resource,
                                      scalar_type type,
                                      std::string_view output_name,
                                      std::string_view left_name,
                                      std::string_view right_name) {
        auto expr = make_scalar_expression(resource, type, key(resource, std::string(output_name)));
        expr->append_param(key(resource, std::string(left_name)));
        expr->append_param(key(resource, std::string(right_name)));
        return expr;
    }

    node_ptr build_project_arithmetic_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "id", "id"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "id_plus_id", "id", "id"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "id_times_id", "id", "id"));
        plan->append_child(make_node_group(resource, collection_name("coll"), exprs));
        return plan;
    }

    node_ptr build_impossible_filter_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource,
                                           collection_name("coll"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "id"),
                                                                   key(resource, "id"))));
        return plan;
    }

    node_ptr build_all_false_filter_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource, collection_name("coll"), make_compare_expression(resource, compare_type::all_false)));
        return plan;
    }

    node_ptr build_sort_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(new sort_expression_t{key(resource, "id"), sort_order::asc});
        plan->append_child(make_node_sort(resource, collection_name("coll"), exprs));
        return plan;
    }

    node_ptr build_fetch_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_limit(resource, collection_name("coll"), limit_t(10)));
        return plan;
    }

    node_ptr build_join_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, {});
        auto join = make_node_join(resource, {}, join_type::left);
        join->append_child(make_node_aggregate(resource, collection_name("left_table")));
        join->append_child(make_node_aggregate(resource, collection_name("right_table")));
        join->append_expression(make_compare_expression(resource, compare_type::all_true));
        plan->append_child(join);
        return plan;
    }

    node_ptr build_aggregate_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;

        auto group_key = make_scalar_expression(resource, scalar_type::get_field, key(resource, "category"));
        group_key->append_param(key(resource, "category"));
        exprs.emplace_back(std::move(group_key));

        auto cnt = make_aggregate_expression(resource, "count", key(resource, "cnt"));
        cnt->append_param(key(resource, "amount"));
        exprs.emplace_back(std::move(cnt));

        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_filter_project_sort_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource,
                                           collection_name("coll"),
                                           make_compare_expression(resource, compare_type::all_true)));

        std::vector<expression_ptr> project_exprs;
        auto id_alias = make_scalar_expression(resource, scalar_type::get_field, key(resource, "id_alias"));
        id_alias->append_param(key(resource, "id"));
        project_exprs.emplace_back(std::move(id_alias));
        plan->append_child(make_node_group(resource, collection_name("coll"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "id_alias"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("coll"), sort_exprs));
        plan->append_child(make_node_limit(resource, collection_name("coll"), limit_t(2)));
        return plan;
    }

    node_ptr build_aggregate_sort_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = build_aggregate_plan(resource);
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "cnt"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), sort_exprs));
        plan->append_child(make_node_limit(resource, collection_name("sales"), limit_t(2)));
        return plan;
    }

    node_ptr build_aggregate_amount_stats_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "category", "category"));

        auto cnt = make_aggregate_expression(resource, "count", key(resource, "cnt"));
        cnt->append_param(key(resource, "amount"));
        exprs.emplace_back(std::move(cnt));

        auto sum = make_aggregate_expression(resource, "sum", key(resource, "total_amount"));
        sum->append_param(key(resource, "amount"));
        exprs.emplace_back(std::move(sum));

        auto min = make_aggregate_expression(resource, "min", key(resource, "min_amount"));
        min->append_param(key(resource, "amount"));
        exprs.emplace_back(std::move(min));

        auto max = make_aggregate_expression(resource, "max", key(resource, "max_amount"));
        max->append_param(key(resource, "amount"));
        exprs.emplace_back(std::move(max));

        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "category"), sort_order::asc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_filter_field_gt_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        return plan;
    }

    node_ptr build_metrics_filter_field_lt_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::lt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        return plan;
    }

    node_ptr build_metrics_filter_field_eq_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::eq,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        return plan;
    }

    node_ptr build_metrics_filter_field_gte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        return plan;
    }

    node_ptr build_metrics_filter_field_lte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::lte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        return plan;
    }

    node_ptr build_metrics_filter_union_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_or);
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        condition->append_child(make_compare_expression(resource, compare_type::lt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));
        return plan;
    }

    node_ptr build_metrics_filter_union_and_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_and);
        condition->append_child(make_compare_expression(resource, compare_type::gte, key(resource, "x"), key(resource, "y")));
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));
        return plan;
    }

    node_ptr build_metrics_filter_union_not_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_not);
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));
        return plan;
    }

    node_ptr build_metrics_project_math_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "sum_xy", "x", "y"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::subtract, "delta_xy", "x", "y"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "product_xy", "x", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_project_delta_product_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::subtract, "delta_xy", "x", "y"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "product_xy", "x", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_sort_two_fields_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(new sort_expression_t{key(resource, "bucket"), sort_order::asc});
        exprs.emplace_back(new sort_expression_t{key(resource, "x"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_sort_x_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(new sort_expression_t{key(resource, "x"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_sales_sort_amount_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(new sort_expression_t{key(resource, "amount"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_sales_project_amount_math_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "category", "category"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "amount_double", "amount", "amount"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "amount_squared", "amount", "amount"));
        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_sales_project_category_amount_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "category", "category"));
        exprs.emplace_back(make_field_projection(resource, "amount", "amount"));
        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_sales_project_category_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "category", "category"));
        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_sales_project_amount_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "amount", "amount"));
        plan->append_child(make_node_group(resource, collection_name("sales"), exprs));
        return plan;
    }

    node_ptr build_sales_project_category_amount_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = build_sales_project_category_amount_plan(resource);
        plan->append_child(make_node_limit(resource, collection_name("sales"), limit_t(2)));
        return plan;
    }

    node_ptr build_sales_project_category_sort_asc_plan(std::pmr::memory_resource* resource) {
        auto plan = build_sales_project_category_amount_plan(resource);
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "category"), sort_order::asc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), sort_exprs));
        return plan;
    }

    node_ptr build_sales_filter_amount_self_gte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        plan->append_child(make_node_match(resource,
                                           collection_name("sales"),
                                           make_compare_expression(resource,
                                                                   compare_type::gte,
                                                                   key(resource, "amount"),
                                                                   key(resource, "amount"))));
        return plan;
    }

    node_ptr build_sales_filter_amount_self_gt_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        plan->append_child(make_node_match(resource,
                                           collection_name("sales"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "amount"),
                                                                   key(resource, "amount"))));
        return plan;
    }

    node_ptr build_coll_project_sort_id_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "id", "id"));
        plan->append_child(make_node_group(resource, collection_name("coll"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "id"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("coll"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_project_identity_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        exprs.emplace_back(make_field_projection(resource, "y", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_project_bucket_x_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_project_bucket_x_sort_bucket_plan(std::pmr::memory_resource* resource) {
        auto plan = build_metrics_project_bucket_x_plan(resource);
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "bucket"), sort_order::asc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_fetch_limit_2_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_limit(resource, collection_name("metrics"), limit_t(2)));
        return plan;
    }

    node_ptr build_metrics_filter_all_false_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource, collection_name("metrics"), make_compare_expression(resource, compare_type::all_false)));
        return plan;
    }

    node_ptr build_metrics_filter_project_gt_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));

        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        exprs.emplace_back(make_field_projection(resource, "y", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_project_lte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::lte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));

        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        exprs.emplace_back(make_field_projection(resource, "y", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_sort_x_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "x"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_filter_or_sort_x_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_or);
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        condition->append_child(make_compare_expression(resource, compare_type::lt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "x"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_coll_filter_self_gte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource,
                                           collection_name("coll"),
                                           make_compare_expression(resource,
                                                                   compare_type::gte,
                                                                   key(resource, "id"),
                                                                   key(resource, "id"))));
        return plan;
    }

    node_ptr build_coll_filter_self_gt_empty_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource,
                                           collection_name("coll"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "id"),
                                                                   key(resource, "id"))));
        return plan;
    }

    node_ptr build_coll_project_math_sort_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "id", "id"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "id_plus_id", "id", "id"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "id_times_id", "id", "id"));
        plan->append_child(make_node_group(resource, collection_name("coll"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "id_plus_id"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("coll"), sort_exprs));
        return plan;
    }

    node_ptr build_coll_filter_true_project_math_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("coll"));
        plan->append_child(make_node_match(resource, collection_name("coll"), make_compare_expression(resource, compare_type::all_true)));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "id", "id"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "id_plus_id", "id", "id"));
        plan->append_child(make_node_group(resource, collection_name("coll"), project_exprs));
        return plan;
    }

    node_ptr build_sales_filter_amount_self_lte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        plan->append_child(make_node_match(resource,
                                           collection_name("sales"),
                                           make_compare_expression(resource,
                                                                   compare_type::lte,
                                                                   key(resource, "amount"),
                                                                   key(resource, "amount"))));
        return plan;
    }

    node_ptr build_sales_filter_amount_self_lt_empty_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        plan->append_child(make_node_match(resource,
                                           collection_name("sales"),
                                           make_compare_expression(resource,
                                                                   compare_type::lt,
                                                                   key(resource, "amount"),
                                                                   key(resource, "amount"))));
        return plan;
    }

    node_ptr build_sales_project_sort_amount_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "category", "category"));
        project_exprs.emplace_back(make_field_projection(resource, "amount", "amount"));
        plan->append_child(make_node_group(resource, collection_name("sales"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "amount"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_project_sum_delta_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "sum_xy", "x", "y"));
        exprs.emplace_back(make_binary_scalar(resource, scalar_type::subtract, "delta_xy", "x", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_project_gte_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        exprs.emplace_back(make_field_projection(resource, "y", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_project_lt_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::lt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "x", "x"));
        exprs.emplace_back(make_field_projection(resource, "y", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_and_project_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_and);
        condition->append_child(make_compare_expression(resource, compare_type::gte, key(resource, "x"), key(resource, "y")));
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_filter_not_project_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        auto condition = make_compare_union_expression(resource, compare_type::union_not);
        condition->append_child(make_compare_expression(resource, compare_type::gt, key(resource, "x"), key(resource, "y")));
        plan->append_child(make_node_match(resource, collection_name("metrics"), condition));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        return plan;
    }

    node_ptr build_metrics_project_sort_sum_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "sum_xy", "x", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "sum_xy"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_filter_lte_sort_y_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::lte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "y"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_filter_gte_sort_x_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gte,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "x"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_sales_project_amount_square_sort_desc_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("sales"));
        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "category", "category"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::multiply, "amount_squared", "amount", "amount"));
        plan->append_child(make_node_group(resource, collection_name("sales"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "amount_squared"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("sales"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_aggregate_full_stats_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        std::vector<expression_ptr> exprs;
        exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));

        auto cnt = make_aggregate_expression(resource, "count", key(resource, "cnt"));
        cnt->append_param(key(resource, "metric_id"));
        exprs.emplace_back(std::move(cnt));

        auto sum = make_aggregate_expression(resource, "sum", key(resource, "sum_x"));
        sum->append_param(key(resource, "x"));
        exprs.emplace_back(std::move(sum));

        auto avg = make_aggregate_expression(resource, "avg", key(resource, "avg_y"));
        avg->append_param(key(resource, "y"));
        exprs.emplace_back(std::move(avg));

        auto min = make_aggregate_expression(resource, "min", key(resource, "min_y"));
        min->append_param(key(resource, "y"));
        exprs.emplace_back(std::move(min));

        auto max = make_aggregate_expression(resource, "max", key(resource, "max_x"));
        max->append_param(key(resource, "x"));
        exprs.emplace_back(std::move(max));

        plan->append_child(make_node_group(resource, collection_name("metrics"), exprs));
        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "bucket"), sort_order::asc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        return plan;
    }

    node_ptr build_metrics_filter_project_sort_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, collection_name("metrics"));
        plan->append_child(make_node_match(resource,
                                           collection_name("metrics"),
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "x"),
                                                                   key(resource, "y"))));

        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "metric_id", "metric_id"));
        project_exprs.emplace_back(make_field_projection(resource, "bucket", "bucket"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::subtract, "delta_xy", "x", "y"));
        plan->append_child(make_node_group(resource, collection_name("metrics"), project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "delta_xy"), sort_order::desc});
        plan->append_child(make_node_sort(resource, collection_name("metrics"), sort_exprs));
        plan->append_child(make_node_limit(resource, collection_name("metrics"), limit_t(2)));
        return plan;
    }

    node_ptr build_join_project_sort_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, {});
        auto join = make_node_join(resource, {}, join_type::left);
        join->append_child(make_node_aggregate(resource, collection_name("left_table")));
        join->append_child(make_node_aggregate(resource, collection_name("right_table")));
        join->append_expression(make_compare_expression(resource, compare_type::all_true));
        plan->append_child(join);

        std::vector<expression_ptr> project_exprs;
        auto left_id = make_scalar_expression(resource, scalar_type::get_field, key(resource, "left_id"));
        left_id->append_param(key(resource, "left_id", side_t::left));
        project_exprs.emplace_back(std::move(left_id));
        auto right_id = make_scalar_expression(resource, scalar_type::get_field, key(resource, "right_id"));
        right_id->append_param(key(resource, "right_id", side_t::right));
        project_exprs.emplace_back(std::move(right_id));
        plan->append_child(make_node_group(resource, {}, project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "left_id"), sort_order::asc});
        plan->append_child(make_node_sort(resource, {}, sort_exprs));
        plan->append_child(make_node_limit(resource, {}, limit_t(3)));
        return plan;
    }

    node_ptr build_join_filter_project_sort_limit_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_aggregate(resource, {});
        auto join = make_node_join(resource, {}, join_type::inner);
        join->append_child(make_node_aggregate(resource, collection_name("left_table")));
        join->append_child(make_node_aggregate(resource, collection_name("right_table")));
        join->append_expression(make_compare_expression(resource,
                                                        compare_type::gt,
                                                        key(resource, "right_id", side_t::right),
                                                        key(resource, "left_id", side_t::left)));
        plan->append_child(join);
        plan->append_child(make_node_match(resource,
                                           {},
                                           make_compare_expression(resource,
                                                                   compare_type::gt,
                                                                   key(resource, "right_id"),
                                                                   key(resource, "left_id"))));

        std::vector<expression_ptr> project_exprs;
        project_exprs.emplace_back(make_field_projection(resource, "left_id", "left_id"));
        project_exprs.emplace_back(make_field_projection(resource, "right_id", "right_id"));
        project_exprs.emplace_back(make_binary_scalar(resource, scalar_type::add, "pair_sum", "left_id", "right_id"));
        plan->append_child(make_node_group(resource, {}, project_exprs));

        std::vector<expression_ptr> sort_exprs;
        sort_exprs.emplace_back(new sort_expression_t{key(resource, "pair_sum"), sort_order::desc});
        plan->append_child(make_node_sort(resource, {}, sort_exprs));
        plan->append_child(make_node_limit(resource, {}, limit_t(2)));
        return plan;
    }

    components::vector::data_chunk_t build_data_chunk(std::pmr::memory_resource* resource) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.emplace_back(components::types::logical_type::INTEGER, "id");
        types.emplace_back(components::types::logical_type::STRING_LITERAL, "name");

        components::vector::data_chunk_t chunk(resource, types, 2);
        chunk.set_cardinality(2);
        chunk.set_value(0, 0, components::types::logical_value_t(resource, int32_t{1}));
        chunk.set_value(1, 0, components::types::logical_value_t(resource, std::string("alice")));
        chunk.set_value(0, 1, components::types::logical_value_t(resource, int32_t{2}));
        chunk.set_value(1, 1, components::types::logical_value_t(resource, std::string("bob")));
        return chunk;
    }

    node_ptr build_data_plan(std::pmr::memory_resource* resource) {
        return make_node_raw_data(resource, build_data_chunk(resource));
    }

    node_ptr build_insert_plan(std::pmr::memory_resource* resource) {
        return make_node_insert(resource, collection_name("coll"), build_data_chunk(resource));
    }

    node_match_ptr build_match_by_id(std::pmr::memory_resource* resource) {
        return make_node_match(resource,
                               collection_name("coll"),
                               make_compare_expression(resource,
                                                       compare_type::eq,
                                                       key(resource, "id", side_t::left),
                                                       core::parameter_id_t(1)));
    }

    node_ptr build_delete_plan(std::pmr::memory_resource* resource) {
        return make_node_delete_many(resource, collection_name("coll"), build_match_by_id(resource));
    }

    node_ptr build_update_plan(std::pmr::memory_resource* resource) {
        std::pmr::vector<update_expr_ptr> updates(resource);
        update_expr_ptr update = new update_expr_set_t(key(resource, "id"));
        update->left() = new update_expr_get_const_value_t(core::parameter_id_t(2));
        updates.emplace_back(std::move(update));
        return make_node_update_many(resource, collection_name("coll"), build_match_by_id(resource), updates, false);
    }

    node_ptr build_create_index_plan(std::pmr::memory_resource* resource) {
        auto plan = make_node_create_index(resource, collection_name("coll"), "idx_id", index_type::single);
        plan->keys().emplace_back(resource, "id");
        return plan;
    }

    node_ptr build_create_type_plan(std::pmr::memory_resource* resource) {
        std::vector<components::types::complex_logical_type> fields;
        fields.emplace_back(components::types::logical_type::INTEGER, "id");
        fields.emplace_back(components::types::logical_type::STRING_LITERAL, "name");
        return make_node_create_type(resource, components::types::complex_logical_type::create_struct("sample_type", fields));
    }

    node_ptr build_function_plan(std::pmr::memory_resource* resource) {
        std::pmr::vector<param_storage> args(resource);
        args.emplace_back(core::parameter_id_t(1));
        args.emplace_back(key(resource, "id"));
        return make_node_function(resource, std::string("sample_function"), std::move(args));
    }

} // namespace

int main(int argc, char** argv) {
    try {
        const auto output_dir = argc > 1 ? std::filesystem::path(argv[1]) : std::filesystem::path("build/substrait_exports");
        std::filesystem::create_directories(output_dir);

        std::pmr::synchronized_pool_resource resource;
        components::catalog::catalog cat(&resource);
        cat.create_namespace({"db"});

        register_table(cat,
                       &resource,
                       collection_name("coll"),
                       {components::types::complex_logical_type(components::types::logical_type::INTEGER, "id")});
        register_table(cat,
                       &resource,
                       collection_name("left_table"),
                       {components::types::complex_logical_type(components::types::logical_type::INTEGER, "left_id")});
        register_table(cat,
                       &resource,
                       collection_name("right_table"),
                       {components::types::complex_logical_type(components::types::logical_type::INTEGER, "right_id")});
        register_table(cat,
                       &resource,
                       collection_name("sales"),
                       {components::types::complex_logical_type(components::types::logical_type::STRING_LITERAL, "category"),
                        components::types::complex_logical_type(components::types::logical_type::INTEGER, "amount")});
        register_table(cat,
                       &resource,
                       collection_name("metrics"),
                       {components::types::complex_logical_type(components::types::logical_type::INTEGER, "metric_id"),
                        components::types::complex_logical_type(components::types::logical_type::STRING_LITERAL, "bucket"),
                        components::types::complex_logical_type(components::types::logical_type::INTEGER, "x"),
                        components::types::complex_logical_type(components::types::logical_type::INTEGER, "y")});

        const std::vector<std::pair<const char*, node_ptr>> exports = {
            {"read_named_table", build_read_plan(&resource)},
            {"sales_read_named_table", build_sales_read_plan(&resource)},
            {"metrics_read_named_table", build_metrics_read_plan(&resource)},
            {"filter_true", build_filter_plan(&resource)},
            {"project_single_field", build_project_plan(&resource)},
            {"project_arithmetic_multi", build_project_arithmetic_plan(&resource)},
            {"filter_impossible", build_impossible_filter_plan(&resource)},
            {"filter_all_false", build_all_false_filter_plan(&resource)},
            {"sort_single_field", build_sort_plan(&resource)},
            {"fetch_limit_10", build_fetch_plan(&resource)},
            {"join_left", build_join_plan(&resource)},
            {"aggregate_count", build_aggregate_plan(&resource)},
            {"pipeline_filter_project_sort_limit", build_filter_project_sort_limit_plan(&resource)},
            {"aggregate_sort_limit", build_aggregate_sort_limit_plan(&resource)},
            {"aggregate_amount_stats", build_aggregate_amount_stats_plan(&resource)},
            {"metrics_filter_field_gt", build_metrics_filter_field_gt_plan(&resource)},
            {"metrics_filter_field_lt", build_metrics_filter_field_lt_plan(&resource)},
            {"metrics_filter_field_eq", build_metrics_filter_field_eq_plan(&resource)},
            {"metrics_filter_field_gte", build_metrics_filter_field_gte_plan(&resource)},
            {"metrics_filter_field_lte", build_metrics_filter_field_lte_plan(&resource)},
            {"metrics_filter_union_or", build_metrics_filter_union_plan(&resource)},
            {"metrics_filter_union_and", build_metrics_filter_union_and_plan(&resource)},
            {"metrics_filter_union_not", build_metrics_filter_union_not_plan(&resource)},
            {"metrics_project_math", build_metrics_project_math_plan(&resource)},
            {"metrics_project_delta_product", build_metrics_project_delta_product_plan(&resource)},
            {"metrics_sort_two_fields", build_metrics_sort_two_fields_plan(&resource)},
            {"metrics_sort_x_desc", build_metrics_sort_x_desc_plan(&resource)},
            {"sales_sort_amount_desc", build_sales_sort_amount_desc_plan(&resource)},
            {"sales_project_amount_math", build_sales_project_amount_math_plan(&resource)},
            {"sales_project_category_amount", build_sales_project_category_amount_plan(&resource)},
            {"sales_project_category", build_sales_project_category_plan(&resource)},
            {"sales_project_amount", build_sales_project_amount_plan(&resource)},
            {"sales_project_category_amount_limit", build_sales_project_category_amount_limit_plan(&resource)},
            {"sales_project_category_sort_asc", build_sales_project_category_sort_asc_plan(&resource)},
            {"sales_filter_amount_self_gte", build_sales_filter_amount_self_gte_plan(&resource)},
            {"sales_filter_amount_self_gt", build_sales_filter_amount_self_gt_plan(&resource)},
            {"coll_project_sort_id_desc", build_coll_project_sort_id_desc_plan(&resource)},
            {"metrics_project_identity", build_metrics_project_identity_plan(&resource)},
            {"metrics_project_bucket_x", build_metrics_project_bucket_x_plan(&resource)},
            {"metrics_project_bucket_x_sort_bucket", build_metrics_project_bucket_x_sort_bucket_plan(&resource)},
            {"metrics_fetch_limit_2", build_metrics_fetch_limit_2_plan(&resource)},
            {"metrics_filter_all_false", build_metrics_filter_all_false_plan(&resource)},
            {"metrics_filter_project_gt", build_metrics_filter_project_gt_plan(&resource)},
            {"metrics_filter_project_lte", build_metrics_filter_project_lte_plan(&resource)},
            {"metrics_filter_sort_x_desc", build_metrics_filter_sort_x_desc_plan(&resource)},
            {"metrics_filter_or_sort_x_desc", build_metrics_filter_or_sort_x_desc_plan(&resource)},
            {"coll_filter_self_gte", build_coll_filter_self_gte_plan(&resource)},
            {"coll_filter_self_gt_empty", build_coll_filter_self_gt_empty_plan(&resource)},
            {"coll_project_math_sort_desc", build_coll_project_math_sort_desc_plan(&resource)},
            {"coll_filter_true_project_math", build_coll_filter_true_project_math_plan(&resource)},
            {"sales_filter_amount_self_lte", build_sales_filter_amount_self_lte_plan(&resource)},
            {"sales_filter_amount_self_lt_empty", build_sales_filter_amount_self_lt_empty_plan(&resource)},
            {"sales_project_sort_amount_desc", build_sales_project_sort_amount_desc_plan(&resource)},
            {"metrics_project_sum_delta", build_metrics_project_sum_delta_plan(&resource)},
            {"metrics_filter_project_gte", build_metrics_filter_project_gte_plan(&resource)},
            {"metrics_filter_project_lt", build_metrics_filter_project_lt_plan(&resource)},
            {"metrics_filter_and_project", build_metrics_filter_and_project_plan(&resource)},
            {"metrics_filter_not_project", build_metrics_filter_not_project_plan(&resource)},
            {"metrics_project_sort_sum_desc", build_metrics_project_sort_sum_desc_plan(&resource)},
            {"metrics_filter_lte_sort_y_desc", build_metrics_filter_lte_sort_y_desc_plan(&resource)},
            {"metrics_filter_gte_sort_x_desc", build_metrics_filter_gte_sort_x_desc_plan(&resource)},
            {"sales_project_amount_square_sort_desc", build_sales_project_amount_square_sort_desc_plan(&resource)},
            {"metrics_aggregate_full_stats", build_metrics_aggregate_full_stats_plan(&resource)},
            {"metrics_filter_project_sort_limit", build_metrics_filter_project_sort_limit_plan(&resource)},
            {"join_project_sort_limit", build_join_project_sort_limit_plan(&resource)},
            {"join_filter_project_sort_limit", build_join_filter_project_sort_limit_plan(&resource)},
            {"data_virtual_table", build_data_plan(&resource)},
            {"insert_values", build_insert_plan(&resource)},
            {"delete_filter", build_delete_plan(&resource)},
            {"update_filter", build_update_plan(&resource)},
            {"create_database", make_node_create_database(&resource, collection_name(""))},
            {"drop_database", make_node_drop_database(&resource, collection_name(""))},
            {"create_collection", make_node_create_collection(&resource, collection_name("new_coll"))},
            {"drop_collection", make_node_drop_collection(&resource, collection_name("old_coll"))},
            {"create_index", build_create_index_plan(&resource)},
            {"drop_index", make_node_drop_index(&resource, collection_name("coll"), "idx_id")},
            {"create_type", build_create_type_plan(&resource)},
            {"drop_type", make_node_drop_type(&resource, std::string("sample_type"))},
            {"function_extension", build_function_plan(&resource)},
        };

        for (const auto& [name, plan] : exports) {
            auto json = substrait_adapter::to_substrait_json(
                plan,
                substrait_adapter::export_profile_t::external_canonical,
                &cat);
            auto bin = substrait_adapter::to_substrait_binary(
                plan,
                substrait_adapter::export_profile_t::external_canonical,
                &cat);
            const auto json_path = output_dir / (std::string(name) + ".json");
            const auto bin_path = output_dir / (std::string(name) + ".bin");
            write_text_file(json_path, json);
            write_binary_file(bin_path, bin);
            std::cout << "wrote " << json_path.string() << "\n";
            std::cout << "wrote " << bin_path.string() << "\n";
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "substrait_export_samples failed: " << e.what() << "\n";
        return 1;
    }
}
