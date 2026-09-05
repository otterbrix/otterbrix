#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;

using v = components::types::logical_value_t;
using vec = std::vector<v>;

// Transformer now wraps SELECT in sequence_t(resolve_*..., aggregate); descend
// to the aggregate consumer when inspecting its rendered form.
#define TEST_SIMPLE_SELECT(QUERY, RESULT, PARAMS)                                                                      \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
            auto _wrap = transformer.transform(pg_cell_to_node_cast(select)).finalize();                               \
            REQUIRE(!_wrap.has_error());                                                                               \
            auto result = _wrap.value();                                                                               \
            auto node = result.sub_queries.back();                                                                     \
            if (node->type() == components::logical_plan::node_type::sequence_t) {                                     \
                node = node->children().back();                                                                        \
            }                                                                                                          \
            auto agg = result.parameters;                                                                              \
            REQUIRE(node->to_string() == RESULT);                                                                      \
            REQUIRE(agg->parameters().parameters.size() == PARAMS.size());                                             \
            for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                               \
                REQUIRE(agg->parameter(core::parameter_id_t(uint16_t(i))) == PARAMS.at(i));                            \
            }                                                                                                          \
        }                                                                                                              \
    }

TEST_CASE("components::sql::select_from_where") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection;)_", R"_($aggregate: {})_", vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection LIMIT 101;)_",
                       R"_($aggregate: {$limit: 101})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection LIMIT ALL;)_",
                       R"_($aggregate: {$limit: -1})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM UID.TestDatabase.TestSchema.TestCollection;)_", R"_($aggregate: {})_", vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND name = 'doc 10' AND "count" = 2;)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE ((((number = 10 AND name = 'doc 10'))));)_",
                       R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}]}})_",
                       vec({v(&resource, 10l), v(&resource, "doc 10")}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 OR name = 'doc 10' OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #2}]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND name = 'doc 10' OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE (number = 10 AND name = 'doc 10') OR "count" = 2;)_",
        R"_($aggregate: {$match: {$or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = 10 AND (name = 'doc 10' OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, $or: ["name": {$eq: #1}, "count": {$eq: #2}]]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE ((number = 10 AND name = 'doc 10') OR "count" = 2) AND )_"
        R"_(((number = 10 AND name = 'doc 10') OR "count" = 2) AND )_"
        R"_(((number = 10 AND name = 'doc 10') OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: [)_"
        R"_($or: [$and: ["number": {$eq: #0}, "name": {$eq: #1}], "count": {$eq: #2}], )_"
        R"_($or: [$and: ["number": {$eq: #3}, "name": {$eq: #4}], "count": {$eq: #5}], )_"
        R"_($or: [$and: ["number": {$eq: #6}, "name": {$eq: #7}], "count": {$eq: #8}])_"
        R"_(]}})_",
        vec({v(&resource, 10l),
             v(&resource, "doc 10"),
             v(&resource, 2l),
             v(&resource, 10l),
             v(&resource, "doc 10"),
             v(&resource, 2l),
             v(&resource, 10l),
             v(&resource, "doc 10"),
             v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number == 10;)_",
                       R"_($aggregate: {$match: {"number": {$eq: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number != 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number <> 10;)_",
                       R"_($aggregate: {$match: {"number": {$ne: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number < 10;)_",
                       R"_($aggregate: {$match: {"number": {$lt: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number <= 10;)_",
                       R"_($aggregate: {$match: {"number": {$lte: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number > 10;)_",
                       R"_($aggregate: {$match: {"number": {$gt: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number >= 10;)_",
                       R"_($aggregate: {$match: {"number": {$gte: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT(number >= 10);)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT number >= 10;)_",
                       R"_($aggregate: {$match: {$not: ["number": {$gte: #0}]}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE NOT (number = 10) AND NOT(name = 'doc 10' OR "count" = 2);)_",
        R"_($aggregate: {$match: {$and: [$not: ["number": {$eq: #0}], )_"
        R"_($not: [$or: ["name": {$eq: #1}, "count": {$eq: #2}]]]}})_",
        vec({v(&resource, 10l), v(&resource, "doc 10"), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE name LIKE 'pattern';)_",
                       R"_($aggregate: {$match: {$function: {name: {"regexp_like"}, args: {"name", #0, #1}}}})_",
                       vec({v(&resource, "pattern"), v(&resource, "l")}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT (column_name).field FROM TestCollection WHERE (column_name).field > 9.99;)_",
        R"_($aggregate: {$match: {"column_name/field": {$gt: #0}}, $group: {column_name/field}, $select: {}})_",
        vec({v(&resource, 9.99)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT ((column_name).sub_type).* FROM TestCollection WHERE ((column_name).sub_type).field1 > ((column_name).sub_type).field2;)_",
        R"_($aggregate: {$match: {"column_name/sub_type/field1": )_"
        R"_({$gt: "column_name/sub_type/field2"}}, $group: {column_name/sub_type/*}, $select: {}})_",
        vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestCollection WHERE array_field[1] = 10;)_",
                       R"_($aggregate: {$match: {"array_field/1": {$eq: #0}}})_",
                       vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE name IS NULL;)_",
        R"_($aggregate: {$match: {"name": {$is_null: #0}}})_",
        vec({v(&resource, components::types::complex_logical_type{components::types::logical_type::NA})}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE name IS NOT NULL;)_",
        R"_($aggregate: {$match: {"name": {$is_not_null: #0}}})_",
        vec({v(&resource, components::types::complex_logical_type{components::types::logical_type::NA})}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE count IN (1, 2, 3);)_",
                       R"_($aggregate: {$match: {$or: ["count": {$eq: #0}, "count": {$eq: #1}, "count": {$eq: #2}]}})_",
                       vec({v(&resource, 1l), v(&resource, 2l), v(&resource, 3l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE count NOT IN (1, 2);)_",
                       R"_($aggregate: {$match: {$and: ["count": {$ne: #0}, "count": {$ne: #1}]}})_",
                       vec({v(&resource, 1l), v(&resource, 2l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE name LIKE '%test%';)_",
                       R"_($aggregate: {$match: {$function: {name: {"regexp_like"}, args: {"name", #0, #1}}}})_",
                       vec({v(&resource, "%test%"), v(&resource, "l")}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE name LIKE 'pre_fix';)_",
                       R"_($aggregate: {$match: {$function: {name: {"regexp_like"}, args: {"name", #0, #1}}}})_",
                       vec({v(&resource, "pre_fix"), v(&resource, "l")}));

    // NULL NOT LIKE p is UNKNOWN: the match itself inverts, and a NULL subject never reaches it, so
    // the negated form is the same call with an 'n' flag — no union_not, no is_not_null guard.
    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE name NOT LIKE '%test%';)_",
                       R"_($aggregate: {$match: {$function: {name: {"regexp_like"}, args: {"name", #0, #1}}}})_",
                       vec({v(&resource, "%test%"), v(&resource, "ln")}));

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE name IS NOT NULL AND count IN (1, 2);)_",
        R"_($aggregate: {$match: {$and: ["name": {$is_not_null: #0}, $or: ["count": {$eq: #1}, "count": {$eq: #2}]]}})_",
        vec({v(&resource, components::types::complex_logical_type{components::types::logical_type::NA}),
             v(&resource, 1l),
             v(&resource, 2l)}));
}

TEST_CASE("components::sql::select_from_order_by") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number ASC;)_",
                       R"_($aggregate: {$sort: {number: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number DESC;)_",
                       R"_($aggregate: {$sort: {number: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number, name;)_",
                       R"_($aggregate: {$sort: {number: 1, name: 1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number ASC, name DESC;)_",
                       R"_($aggregate: {$sort: {number: 1, name: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number ASC, name DESC LIMIT 200;)_",
                       R"_($aggregate: {$sort: {number: 1, name: -1}, $limit: 200})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection ORDER BY number, "count" ASC, name, value DESC;)_",
                       R"_($aggregate: {$sort: {number: 1, count: 1, name: 1, value: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number > 10 ORDER BY number ASC, name DESC;)_",
        R"_($aggregate: {$match: {"number": {$gt: #0}}, $sort: {number: 1, name: -1}})_",
        vec({v(&resource, 10l)}));

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestCollection ORDER BY (struct_type).field1 DESC;)_",
                       R"_($aggregate: {$sort: {struct_type/field1: -1}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT * FROM TestCollection ORDER BY array_field[1] DESC;)_",
                       R"_($aggregate: {$sort: {array_field/1: -1}})_",
                       vec());
}

TEST_CASE("components::sql::group_by") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_SELECT(R"_(SELECT field FROM TestCollection GROUP BY field;)_",
                       R"_($aggregate: {$group: {group_by: field, field}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(
        R"_(SELECT name, name1, 9.99 FROM TestCollection GROUP BY name, name1;)_",
        R"_($aggregate: {$group: {group_by: name, group_by: name1, name, name1, {$constant: #0}}, $select: {}})_",
        vec({v(&resource, 9.99)}));
}

TEST_CASE("components::sql::select_from_fields") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_SELECT(R"_(SELECT number, name, "count" FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, name, count}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT (struct_type).* FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {struct_type/*}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT (struct_type).field_3 FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {struct_type/field_3}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT array_field[3] FROM TestCollection;)_",
                       R"_($aggregate: {$group: {array_field/3}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT matrix_field[3][2] FROM TestCollection;)_",
                       R"_($aggregate: {$group: {matrix_field/3/2}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT number, name as title FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, title: "name"}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(R"_(SELECT number, name title FROM TestDatabase.TestCollection;)_",
                       R"_($aggregate: {$group: {number, title: "name"}, $select: {}})_",
                       vec());

    TEST_SIMPLE_SELECT(
        R"_(SELECT number, 10 size, 'title' title, true "on", false "off" FROM TestDatabase.TestCollection;)_",
        R"_($aggregate: {$group: {number, size: {$constant: #0}, title: {$constant: #1}, )_"
        R"_(on: {$constant: #2}, off: {$constant: #3}}, $select: {}})_",
        vec({v(&resource, 10l), v(&resource, "title"), v(&resource, true), v(&resource, false)}));
}

TEST_CASE("components::sql::select_with_subquery") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_SIMPLE_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE col1 IN (SELECT col2 FROM TestDatabase2.TestCollection2);)_",
        R"_($aggregate: {$match: {"col1": {$any: #0}}})_",
        vec({v(&resource, nullptr)}));
}
// ЗАПИСЬ #344 — the DISTINCT flag of an aggregate call survives ONLY the select-list
// arm (transform_select reads func->agg_distinct); resolve_having_operand and
// transform_a_expr_func both drop it. Consequences pinned here:
//   * a HAVING aggregate not present in SELECT is minted WITHOUT distinct, so
//     `HAVING count(DISTINCT x)` silently computes count(x);
//   * HAVING-to-projection matching ignores distinct, so `HAVING count(DISTINCT x)`
//     binds to a projected count(x) (and vice versa) instead of minting its own
//     aggregate;
//   * an aggregate nested as a function argument loses the flag in
//     transform_a_expr_func.
TEST_CASE("components::sql::aggregate_distinct_survives_having_and_nesting") {
    using components::expressions::aggregate_expression_t;
    using components::expressions::expression_group;
    using components::expressions::expression_i;
    using components::expressions::expression_ptr;
    using components::expressions::function_expression_t;
    using components::expressions::param_storage;
    using components::expressions::scalar_expression_t;

    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    struct call_t {
        std::string name;
        bool distinct;
    };

    // Collect every named call (function or aggregate) in the tree, depth-first.
    struct collector_t {
        std::vector<call_t>* out;
        void walk(const expression_i* expr) const {
            if (expr == nullptr) {
                return;
            }
            switch (expr->group()) {
                case expression_group::aggregate: {
                    const auto* agg = static_cast<const aggregate_expression_t*>(expr);
                    out->push_back({agg->function_name(), agg->is_distinct()});
                    for (const auto& p : agg->params()) {
                        walk_param(p);
                    }
                    break;
                }
                case expression_group::function: {
                    const auto* fn = static_cast<const function_expression_t*>(expr);
                    out->push_back({fn->name(), fn->is_distinct()});
                    for (const auto& p : fn->args()) {
                        walk_param(p);
                    }
                    break;
                }
                case expression_group::scalar: {
                    for (const auto& p : static_cast<const scalar_expression_t*>(expr)->params()) {
                        walk_param(p);
                    }
                    break;
                }
                default:
                    break;
            }
        }
        void walk_param(const param_storage& p) const {
            if (std::holds_alternative<expression_ptr>(p)) {
                walk(std::get<expression_ptr>(p).get());
            }
        }
    };

    // Transform QUERY and return every named call found in the aggregate sub-tree
    // (the group node absorbs the select-list expressions, so one walk over the
    // aggregate's children sees projection and HAVING aggregates alike).
    auto calls_of = [&](const char* query) {
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto wrap = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(!wrap.has_error());
        auto node = wrap.value().sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        std::vector<call_t> calls;
        collector_t collector{&calls};
        struct node_walker_t {
            const collector_t* collector;
            void walk(const components::logical_plan::node_ptr& n) const {
                for (const auto& e : n->expressions()) {
                    collector->walk(e.get());
                }
                for (const auto& child : n->children()) {
                    walk(child);
                }
            }
        };
        node_walker_t{&collector}.walk(node);
        return calls;
    };

    auto counts_named = [](const std::vector<call_t>& calls, const char* name) {
        std::vector<call_t> found;
        for (const auto& c : calls) {
            if (c.name == name) {
                found.push_back(c);
            }
        }
        return found;
    };

    SECTION("HAVING count(DISTINCT x) mints a DISTINCT aggregate") {
        auto counts = counts_named(
            calls_of("SELECT k FROM db.t GROUP BY k HAVING count(DISTINCT x) > 1;"),
            "count");
        REQUIRE(counts.size() == 1);
        REQUIRE(counts.front().distinct);
    }

    SECTION("HAVING count(DISTINCT x) does not bind to a projected count(x)") {
        auto counts = counts_named(
            calls_of("SELECT count(x) AS c FROM db.t GROUP BY k HAVING count(DISTINCT x) > 1;"),
            "count");
        REQUIRE(counts.size() == 2);
        REQUIRE(counts[0].distinct != counts[1].distinct);
    }

    SECTION("HAVING count(x) does not bind to a projected count(DISTINCT x)") {
        auto counts = counts_named(
            calls_of("SELECT count(DISTINCT x) AS c FROM db.t GROUP BY k HAVING count(x) > 1;"),
            "count");
        REQUIRE(counts.size() == 2);
        REQUIRE(counts[0].distinct != counts[1].distinct);
    }

    SECTION("HAVING count(x) still reuses a projected count(x)") {
        auto counts = counts_named(
            calls_of("SELECT count(x) AS c FROM db.t GROUP BY k HAVING count(x) > 1;"),
            "count");
        REQUIRE(counts.size() == 1);
        REQUIRE_FALSE(counts.front().distinct);
    }

    SECTION("an aggregate nested as a function argument keeps DISTINCT") {
        auto counts = counts_named(
            calls_of("SELECT abs(count(DISTINCT x)) FROM db.t GROUP BY k;"),
            "count");
        REQUIRE(counts.size() == 1);
        REQUIRE(counts.front().distinct);
    }
}
