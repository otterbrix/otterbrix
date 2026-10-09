#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <tuple>

#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
#include <core/pmr.hpp>

#include <algorithm>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

using namespace components;
using expressions::side_t;

namespace {
    std::string to_std(const std::pmr::string& str) { return std::string(str.data(), str.size()); }

    std::string side_name(side_t side) {
        switch (side) {
            case side_t::left:
                return "left";
            case side_t::right:
                return "right";
            default:
                return "undefined";
        }
    }

    struct found_key_t {
        side_t side{side_t::undefined};
        std::string path;
        std::string qualifier;
    };

    std::string last_segment(const std::string& path) {
        const auto pos = path.rfind('/');
        return pos == std::string::npos ? path : path.substr(pos + 1);
    }

    void collect_keys(const expressions::expression_ptr& expr, std::vector<found_key_t>& out);

    void take_key(const expressions::key_t& key, std::vector<found_key_t>& out) {
        out.push_back({key.side(), key.as_string(), to_std(key.qualifier())});
    }

    void collect_param(const expressions::param_storage& param, std::vector<found_key_t>& out) {
        if (expressions::is_key(param)) {
            take_key(expressions::as_key(param), out);
        } else if (expressions::is_expr(param)) {
            collect_keys(expressions::as_expr(param), out);
        }
    }

    void collect_keys(const expressions::expression_ptr& expr, std::vector<found_key_t>& out) {
        if (!expr) {
            return;
        }
        switch (expr->group()) {
            case expressions::expression_group::compare: {
                const auto* compare = static_cast<const expressions::compare_expression_t*>(expr.get());
                collect_param(compare->left(), out);
                collect_param(compare->right(), out);
                for (const auto& child : compare->children()) {
                    collect_keys(child, out);
                }
                break;
            }
            case expressions::expression_group::scalar: {
                const auto* scalar = static_cast<const expressions::scalar_expression_t*>(expr.get());
                take_key(scalar->key(), out);
                for (const auto& param : scalar->params()) {
                    collect_param(param, out);
                }
                break;
            }
            case expressions::expression_group::aggregate: {
                const auto* aggregate = static_cast<const expressions::aggregate_expression_t*>(expr.get());
                take_key(aggregate->key(), out);
                for (const auto& param : aggregate->params()) {
                    collect_param(param, out);
                }
                break;
            }
            case expressions::expression_group::sort: {
                collect_param(static_cast<const expressions::sort_expression_t*>(expr.get())->operand(), out);
                break;
            }
            case expressions::expression_group::function: {
                const auto* function = static_cast<const expressions::function_expression_t*>(expr.get());
                for (const auto& arg : function->args()) {
                    collect_param(arg, out);
                }
                break;
            }
            default:
                break;
        }
    }

    void collect_keys(const logical_plan::node_ptr& node, std::vector<found_key_t>& out) {
        if (!node) {
            return;
        }
        for (const auto& expr : node->expressions()) {
            collect_keys(expr, out);
        }
        for (const auto& child : node->children()) {
            collect_keys(child, out);
        }
    }

    // A type names itself in type_name; every other catalog-resolve kind uses relname.
    void collect_catalog_targets(const logical_plan::catalog_resolves_t& resolves, std::vector<std::string>& out) {
        const auto add = [&out](const logical_plan::node_catalog_resolve_ptr& node, const char* kind) {
            if (!node) {
                return;
            }
            for (const auto& entry : node->entries()) {
                const std::string& name = entry.relname.empty() ? entry.type_name : entry.relname;
                out.push_back(std::string{kind} + ":" + entry.dbname + "." + name);
            }
        };
        add(resolves.namespaces, "namespace");
        add(resolves.tables, "table");
        add(resolves.types, "type");
        add(resolves.constraints, "constraint");
    }

    void collect_join_arities(const logical_plan::node_ptr& node, std::vector<size_t>& out) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::join_t) {
            out.push_back(node->children().size());
        }
        for (const auto& child : node->children()) {
            collect_join_arities(child, out);
        }
    }

    void collect_joins(const logical_plan::node_ptr& node, std::vector<std::string>& out) {
        if (!node) {
            return;
        }
        if (node->type() == logical_plan::node_type::join_t) {
            const auto* join = static_cast<const logical_plan::node_join_t*>(node.get());
            const char* kind = join->type() == logical_plan::join_type::inner   ? "inner"
                               : join->type() == logical_plan::join_type::left  ? "left"
                               : join->type() == logical_plan::join_type::right ? "right"
                               : join->type() == logical_plan::join_type::full  ? "full"
                               : join->type() == logical_plan::join_type::cross ? "cross"
                                                                                : "invalid";
            std::vector<found_key_t> keys;
            for (const auto& expr : node->expressions()) {
                collect_keys(expr, keys);
            }
            std::string text{kind};
            text += "(";
            for (size_t i = 0; i < keys.size(); ++i) {
                text += (i ? " " : "") + keys[i].path + ":" + side_name(keys[i].side);
            }
            out.push_back(text + ")");
        }
        for (const auto& child : node->children()) {
            collect_joins(child, out);
        }
    }

    struct probe_t {
        std::string sql;
        std::string column;
        bool rejected{false};
        std::string stage;
        core::error_code_t code{core::error_code_t::none};
        std::string message;
        std::vector<found_key_t> matches;
        std::vector<std::string> catalog_targets;
        std::vector<size_t> join_arities;
        std::vector<std::string> joins;
    };

    probe_t probe(const std::string& sql, const std::string& column) {
        probe_t out;
        out.sql = sql;
        out.column = column;

        auto resource = core::pmr::otterbrix_resource();
        std::pmr::monotonic_buffer_resource arena(&resource);

        List* raw = nullptr;
        try {
            raw = raw_parser(&arena, sql.c_str());
        } catch (const parser_exception_t& error) {
            out.rejected = true;
            out.stage = "parser";
            out.message = error.what();
            return out;
        }
        if (raw == nullptr || raw->lst.empty()) {
            out.rejected = true;
            out.stage = "parser";
            out.message = "empty parse tree";
            return out;
        }

        sql::transform::transformer transformer(&resource, sql.c_str());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw)));
        if (result.has_error()) {
            out.rejected = true;
            out.stage = "transformer";
            out.code = result.get_error().type;
            out.message = to_std(result.get_error().what);
            return out;
        }

        auto plan = result.finalize();
        if (plan.has_error()) {
            out.rejected = true;
            out.stage = "finalize";
            out.code = plan.error().type;
            out.message = to_std(plan.error().what);
            return out;
        }
        const auto& root = plan.value().sub_queries.back();
        collect_join_arities(root, out.join_arities);
        collect_joins(root, out.joins);
        std::vector<found_key_t> keys;
        collect_keys(root, keys);
        for (const auto& key : keys) {
            // An empty name asks for every key, for cases that watch a statement's shape, not one reference.
            if (column.empty() || last_segment(key.path) == column) {
                out.matches.push_back(key);
            }
        }
        collect_catalog_targets(plan.value().catalog_resolves, out.catalog_targets);
        return out;
    }

    probe_t transform_only(const std::string& sql) { return probe(sql, std::string{}); }

    std::string describe(const probe_t& probe) {
        std::ostringstream text;
        text << "query: " << probe.sql << "\n  reference under test: " << probe.column << "\n  observed: ";
        if (probe.rejected) {
            text << "rejected by " << probe.stage << " (code " << static_cast<int>(probe.code)
                 << "): " << probe.message;
            return text.str();
        }
        if (probe.matches.empty()) {
            text << "accepted, no key carries the reference; catalog targets:";
            for (const auto& target : probe.catalog_targets) {
                text << " " << target;
            }
            for (const auto& join : probe.joins) {
                text << "\n    join: " << join;
            }
            return text.str();
        }
        text << "accepted";
        for (const auto& join : probe.joins) {
            text << "\n    join: " << join;
        }
        for (const auto& key : probe.matches) {
            text << "\n    key path \"" << key.path << "\", side " << side_name(key.side) << ", qualifier \""
                 << key.qualifier << "\"";
        }
        return text.str();
    }

    void
    require_resolved_path(const probe_t& probe, side_t side, const std::string& qualifier, const std::string& path) {
        INFO(describe(probe));
        REQUIRE_FALSE(probe.rejected);
        REQUIRE(probe.matches.size() == 1);
        CHECK(probe.matches.front().path == path);
        CHECK(side_name(probe.matches.front().side) == side_name(side));
        CHECK(probe.matches.front().qualifier == qualifier);
    }

    void require_resolved(const probe_t& probe, side_t side, const std::string& qualifier) {
        require_resolved_path(probe, side, qualifier, probe.column);
    }

    void require_rejected(const probe_t& probe, core::error_code_t code) {
        INFO(describe(probe));
        REQUIRE(probe.rejected);
        CHECK(static_cast<int>(probe.code) == static_cast<int>(code));
    }

    void require_rejected_saying(const probe_t& probe, core::error_code_t code, const std::string& fragment) {
        require_rejected(probe, code);
        INFO(describe(probe));
        CHECK(probe.message.find(fragment) != std::string::npos);
    }

    void require_rejected_by_parser(const probe_t& probe) {
        INFO(describe(probe));
        REQUIRE(probe.rejected);
        CHECK(probe.stage == "parser");
    }

    struct database_t;
    components::cursor::cursor_t_ptr run_ok(database_t& db, const std::string& sql);

    struct database_t {
        explicit database_t(const std::string& path)
            : config(test_create_config(path)) {
            test_clear_directory(config);
            space = std::make_unique<test_spaces>(config);
        }

        components::cursor::cursor_t_ptr run(const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return space->dispatcher()->execute_sql(session, sql);
        }

        void seed(std::initializer_list<const char*> statements) {
            for (const char* sql : statements) {
                run_ok(*this, sql);
            }
        }

        configuration::config config;
        std::unique_ptr<test_spaces> space;
    };

    components::cursor::cursor_t_ptr run_ok(database_t& db, const std::string& sql) {
        auto cursor = db.run(sql);
        INFO("query: " << sql);
        INFO("error: " << (cursor->is_error() ? to_std(cursor->get_error().what) : "none"));
        REQUIRE(cursor->is_success());
        return cursor;
    }

    core::error_t run_refused(database_t& db, const std::string& sql) {
        auto cursor = db.run(sql);
        INFO("query: " << sql);
        REQUIRE(cursor->is_error());
        return cursor->get_error();
    }

    // A merged column reading the padded copy of an outer join shows up here as fewer values.
    size_t rows_with_a_value(const components::cursor::cursor_t_ptr& cursor) {
        size_t count = 0;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            if (!cursor->value(0, row).is_null()) {
                ++count;
            }
        }
        return count;
    }
} // namespace

TEST_CASE("name_resolution::column_ref::bare_column_stays_undefined") {
    auto result = probe("SELECT id FROM t;", "id");
    require_resolved(result, side_t::undefined, "");
}

TEST_CASE("name_resolution::column_ref::relname_qualifier") {
    auto result = probe("SELECT t.id FROM t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::empty_element_slot_is_not_wildcard") {
    auto result = probe("SELECT d.t.id FROM t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::two_part_from_answers_bare_relname") {
    auto result = probe("SELECT t.id FROM d.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::two_part_from_answers_db_qualifier") {
    auto result = probe("SELECT d.t.id FROM d.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::wrong_db_is_rejected") {
    auto result = probe("SELECT x.t.id FROM d.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::schema_filled_in_reference_only") {
    auto result = probe("SELECT d.s.t.id FROM d.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::three_part_from_answers_bare_relname") {
    auto result = probe("SELECT t.id FROM d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::skip_middle_slot") {
    auto result = probe("SELECT d.t.id FROM d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::schema_alone_does_not_qualify") {
    auto result = probe("SELECT s.t.id FROM d.s.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::schema_alone_does_not_qualify_under_uid") {
    auto result = probe("SELECT s.t.id FROM u.d.s.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::column_ref::four_part_column_reference") {
    auto result = probe("SELECT d.s.t.id FROM d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::column_ref::four_part_reference_under_uid_element") {
    auto result = probe("SELECT d.s.t.id FROM u.d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::column_ref::five_part_column_reference") {
    auto result = probe("SELECT u.d.s.t.id FROM u.d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::column_ref::six_part_column_reference_rejected") {
    auto result = probe("SELECT u.d.s.t.x.id FROM u.d.s.t;", "id");
    INFO(describe(result));
    REQUIRE(result.rejected);
}

TEST_CASE("name_resolution::from_name::five_part_from_reference_rejected") {
    auto result = probe("SELECT id FROM a.b.c.d.e;", "id");
    require_rejected_by_parser(result);
}

// ALTER TYPE follows CREATE/DROP TYPE — a type always lives in public.
TEST_CASE("name_resolution::type_name::alter_type_keeps_its_database") {
    auto result = transform_only("ALTER TYPE shop.addr_t ADD ATTRIBUTE zip TEXT;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:public.addr_t") !=
          result.catalog_targets.end());
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:shop.addr_t") ==
          result.catalog_targets.end());
}

TEST_CASE("name_resolution::type_name::unqualified_alter_type_has_no_database") {
    auto result = transform_only("ALTER TYPE addr_t ADD ATTRIBUTE zip TEXT;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:public.addr_t") !=
          result.catalog_targets.end());
}

TEST_CASE("name_resolution::type_name::alter_type_in_a_database_is_refused_like_create_type") {
    database_t db(integration_fixture_path("test_name_resolution/alter_type_database"));
    db.seed({"CREATE DATABASE shop;"});
    auto create = run_refused(db, "CREATE TYPE shop.mood AS (a BIGINT);");
    auto alter = run_refused(db, "ALTER TYPE shop.mood ADD ATTRIBUTE b BIGINT;");
    CHECK(create.type == core::error_code_t::invalid_parameter);
    CHECK(alter.type == core::error_code_t::invalid_parameter);
    CHECK(std::string(alter.what).find("a type always lives in \"public\"") != std::string::npos);
    CHECK(std::string(create.what).find("a type always lives in \"public\"") != std::string::npos);
}

TEST_CASE("name_resolution::type_name::type_name_over_three_parts_rejected") {
    auto result = transform_only("CREATE TYPE a.b.c.d AS (x INT);");
    require_rejected_by_parser(result);
}

TEST_CASE("name_resolution::ambiguity::bare_relname_across_databases") {
    auto result = probe("SELECT t.id FROM d1.t CROSS JOIN d2.t;", "id");
    require_rejected(result, core::error_code_t::ambiguous_name);
}

TEST_CASE("name_resolution::ambiguity::skipped_slot_made_it_ambiguous") {
    auto result = probe("SELECT d.t.id FROM d.s1.t CROSS JOIN d.s2.t;", "id");
    require_rejected(result, core::error_code_t::ambiguous_name);
}

TEST_CASE("name_resolution::ambiguity::database_qualifier_picks_a_side") {
    auto result = probe("SELECT d1.t.id FROM d1.t CROSS JOIN d2.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::ambiguity::database_qualifier_picks_the_right_side") {
    auto result = probe("SELECT d2.t.id FROM d1.t CROSS JOIN d2.t;", "id");
    require_resolved(result, side_t::right, "t");
}

TEST_CASE("name_resolution::alias::alias_answers") {
    auto result = probe("SELECT x.id FROM d.t AS x;", "id");
    require_resolved(result, side_t::left, "x");
}

TEST_CASE("name_resolution::alias::relname_hidden_by_alias") {
    auto result = probe("SELECT t.id FROM d.t AS x;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::alias::qualified_relname_hidden_by_alias") {
    auto result = probe("SELECT d.t.id FROM d.t AS x;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::alias::alias_cannot_be_qualified") {
    auto result = probe("SELECT d.x.id FROM d.t AS x;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::from_element::subquery_alias_answers") {
    auto result = probe("SELECT s.c FROM (SELECT * FROM d.inner_t) s;", "c");
    require_resolved(result, side_t::left, "s");
}

TEST_CASE("name_resolution::from_element::subquery_alias_cannot_be_qualified") {
    auto result = probe("SELECT d.s.c FROM (SELECT * FROM d.inner_t) s;", "c");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::from_element::subquery_without_alias_rejected") {
    // The grammar already refuses this (gram.y:12668), inherited from PostgreSQL.
    auto result = probe("SELECT c FROM (SELECT * FROM d.inner_t);", "c");
    require_rejected_by_parser(result);
}

TEST_CASE("name_resolution::from_element::cte_answers_its_name") {
    auto result = probe("WITH w AS (SELECT * FROM d.inner_t) SELECT w.c FROM w;", "c");
    require_resolved(result, side_t::left, "w");
}

TEST_CASE("name_resolution::from_element::cte_name_is_not_qualifiable") {
    auto result = probe("WITH w AS (SELECT * FROM d.inner_t) SELECT w.c FROM d.w;", "c");
    require_resolved(result, side_t::left, "w");
    INFO(describe(result));
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:d.w") !=
          result.catalog_targets.end());
}

TEST_CASE("name_resolution::from_element::subquery_on_the_left_of_a_join") {
    auto result = transform_only("SELECT s.c FROM (SELECT * FROM d.inner_t) s JOIN d.other o ON s.jk = o.jk;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.join_arities.size() == 1);
    CHECK(result.join_arities.front() == 2);
}

TEST_CASE("name_resolution::from_element::subquery_on_the_right_of_a_composite_left") {
    auto result = transform_only("SELECT a.jk FROM d.a a JOIN d.b b ON a.jk = b.jk "
                                 "JOIN (SELECT * FROM d.inner_t) c ON a.jk = c.jk;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.join_arities.size() == 2);
    for (auto arity : result.join_arities) {
        CHECK(arity == 2);
    }
}

TEST_CASE("name_resolution::from_element::table_function_on_the_right_of_a_composite_left") {
    auto result = transform_only("SELECT a.jk FROM d.a a JOIN d.b b ON a.jk = b.jk "
                                 "JOIN generate_series(1, 3) g ON a.jk = g.g;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.join_arities.size() == 2);
    for (auto arity : result.join_arities) {
        CHECK(arity == 2);
    }
}

TEST_CASE("name_resolution::field_access::around_an_unqualified_column") {
    auto result = probe("SELECT (custom_type).f3.f1 FROM d.t;", "f1");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.matches.size() == 1);
    CHECK(result.matches.front().path == "custom_type/f3/f1");
}

TEST_CASE("name_resolution::field_access::around_a_qualified_column") {
    auto result = probe("SELECT (t.custom_type).f3.f1 FROM d.t;", "f1");
    require_resolved_path(result, side_t::left, "t", "custom_type/f3/f1");
}

TEST_CASE("name_resolution::field_access::nested_pairs_reach_the_same_field") {
    auto result = probe("SELECT ((t.custom_type).f3).f1 FROM d.t;", "f1");
    require_resolved_path(result, side_t::left, "t", "custom_type/f3/f1");
}

TEST_CASE("name_resolution::field_access::same_field_from_a_predicate") {
    // WHERE reads the reference through a different code path than the select list does.
    auto result = probe("SELECT id FROM d.t WHERE (t.custom_type).f3.f1 = 1;", "f1");
    require_resolved_path(result, side_t::left, "t", "custom_type/f3/f1");
}

TEST_CASE("name_resolution::star::qualified_star") {
    auto result = probe("SELECT t.* FROM d.s.t;", "*");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.matches.size() == 1);
    CHECK(result.matches.front().path == "t/*");
}

TEST_CASE("name_resolution::star::alias_star") {
    auto result = probe("SELECT x.* FROM d.s.t AS x;", "*");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.matches.size() == 1);
    CHECK(result.matches.front().path == "x/*");
}

TEST_CASE("name_resolution::star::star_on_name_hidden_by_alias") {
    auto result = probe("SELECT t.* FROM d.s.t AS x;", "*");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::order_and_group::order_by_obeys_the_alias") {
    auto result = probe("SELECT x.id FROM d.t AS x ORDER BY d.t.id;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::order_and_group::group_by_resolves_through_the_alias") {
    auto result = probe("SELECT x.id FROM d.t AS x GROUP BY x.id;", "id");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE_FALSE(result.matches.empty());
    for (const auto& key : result.matches) {
        CHECK(key.path == "id");
        CHECK(side_name(key.side) == side_name(side_t::left));
    }
}

TEST_CASE("name_resolution::message::message_names_the_reference") {
    auto result = probe("SELECT nosuch.id FROM shop.orders;", "id");
    require_rejected_saying(result, core::error_code_t::table_not_exists, "nosuch");
}

TEST_CASE("name_resolution::message::message_shows_how_the_element_is_spelled") {
    auto result = probe("SELECT sales.orders.id FROM shop.sales.orders;", "id");
    require_rejected_saying(result, core::error_code_t::table_not_exists, "shop.sales.orders");
}

TEST_CASE("name_resolution::message::message_suggests_the_alias") {
    auto result = probe("SELECT orders.id FROM shop.orders AS placed;", "id");
    require_rejected_saying(result, core::error_code_t::table_not_exists, "placed");
}

TEST_CASE("name_resolution::using::equates_the_two_sides") {
    auto result = transform_only("SELECT a.id FROM d.a a JOIN d.b b USING (id);");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "inner(id:left id:right)");
}

TEST_CASE("name_resolution::using::several_columns") {
    auto result = transform_only("SELECT a.v FROM d.a a JOIN d.b b USING (id, k);");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "inner(id:left id:right k:left k:right)");
}

TEST_CASE("name_resolution::using::left_join_keeps_its_type") {
    // LEFT JOIN already produced join_type::left, so cardinality alone looked plausible even here.
    auto result = transform_only("SELECT a.v FROM d.a a LEFT JOIN d.b b USING (id);");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "left(id:left id:right)");
}

TEST_CASE("name_resolution::using::composite_left_side") {
    // The predicate is built by side, not by name: the left side here is itself a join with no name.
    auto result = transform_only("SELECT a.v FROM d.a a JOIN d.b b ON a.jk = b.jk JOIN d.c c USING (id);");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 2);
    CHECK(result.joins.front() == "inner(id:left id:right)");
}

TEST_CASE("name_resolution::using::star_is_refused") {
    auto result = transform_only("SELECT * FROM d.a a JOIN d.b b USING (id);");
    INFO(describe(result));
    REQUIRE(result.rejected);
    CHECK(static_cast<int>(result.code) == static_cast<int>(core::error_code_t::unimplemented_yet));
}

TEST_CASE("name_resolution::using::natural_join_is_refused") {
    // NATURAL needs the column lists of both sides, which the transformer does not have.
    auto result = transform_only("SELECT a.v FROM d.a a NATURAL JOIN d.b b;");
    INFO(describe(result));
    REQUIRE(result.rejected);
    CHECK(static_cast<int>(result.code) == static_cast<int>(core::error_code_t::unimplemented_yet));
}

TEST_CASE("name_resolution::using::plain_on_join_unchanged") {
    auto result = transform_only("SELECT a.v FROM d.a a JOIN d.b b ON a.id = b.id;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "inner(id:left id:right)");
}

TEST_CASE("name_resolution::using::cross_join_still_has_no_predicate") {
    auto result = transform_only("SELECT a.v FROM d.a a CROSS JOIN d.b b;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "cross()");
}

TEST_CASE("name_resolution::using::row_count_is_not_a_cross_product") {
    auto config = test_create_config(integration_fixture_path("test_name_resolution/using"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const char* ddl : {"CREATE DATABASE ud;",
                            "CREATE TABLE ud.a (id INT, av INT);",
                            "CREATE TABLE ud.b (id INT, bv INT);",
                            "INSERT INTO ud.a (id, av) VALUES (1,10),(2,20),(3,30),(4,40);",
                            "INSERT INTO ud.b (id, bv) VALUES (3,300),(4,400),(5,500),(6,600);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, ddl)->is_success());
    }
    {
        INFO("four rows each, two ids in common — a cross product would be sixteen");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a.av, b.bv FROM ud.a a JOIN ud.b b USING (id);");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        INFO("the same join written with ON — unchanged");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a.av, b.bv FROM ud.a a JOIN ud.b b ON a.id = b.id;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        INFO("the common column answers to its bare name — it is merged, not ambiguous");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM ud.a a JOIN ud.b b USING (id);");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        INFO("both copies keep answering to their own qualification");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a.id, b.id FROM ud.a a JOIN ud.b b USING (id);");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        INFO("a column missing from one side is the validator's to refuse");
        auto session = otterbrix::session_id_t();
        CHECK_FALSE(
            dispatcher->execute_sql(session, "SELECT a.av FROM ud.a a JOIN ud.b b USING (nosuch);")->is_success());
    }
    {
        INFO("LEFT JOIN: the merged column is the left copy, so unmatched rows keep their id");
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM ud.a a LEFT JOIN ud.b b USING (id);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        CHECK(rows_with_a_value(cur) == 4);
    }
}

TEST_CASE("name_resolution::duplicate_name::same_table_twice_without_aliases") {
    auto result = transform_only("SELECT t.id FROM d.t JOIN d.t ON t.jk = t.jk;");
    INFO(describe(result));
    REQUIRE(result.rejected);
    CHECK(static_cast<int>(result.code) == static_cast<int>(core::error_code_t::ambiguous_name));
}

TEST_CASE("name_resolution::duplicate_name::same_alias_twice") {
    auto result = transform_only("SELECT x.id FROM d.a AS x JOIN d.b AS x ON x.jk = x.jk;");
    INFO(describe(result));
    REQUIRE(result.rejected);
    CHECK(static_cast<int>(result.code) == static_cast<int>(core::error_code_t::ambiguous_name));
}

TEST_CASE("name_resolution::duplicate_name::same_table_twice_with_aliases_is_fine") {
    auto result = transform_only("SELECT a.id FROM d.t AS a JOIN d.t AS b ON a.jk = b.jk;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
}

TEST_CASE("name_resolution::duplicate_name::same_name_different_database_is_fine") {
    auto result = transform_only("SELECT d1.t.id FROM d1.t JOIN d2.t ON d1.t.jk = d2.t.jk;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
}

TEST_CASE("name_resolution::validator::unqualified_column_is_placed_by_the_validator") {
    database_t db(integration_fixture_path("test_name_resolution/validator_unqualified"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, bv INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, bv) VALUES (1,100),(2,200);"});

    auto cur = run_ok(db, "SELECT a.av FROM vd.a a JOIN vd.b b ON a.id = b.id WHERE av > 15;");
    CHECK(cur->size() == 1);
}

TEST_CASE("name_resolution::validator::one_relation_on_both_sides_is_not_ambiguous") {
    database_t db(integration_fixture_path("test_name_resolution/validator_same_schema"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);"});

    auto cur = run_ok(db, "SELECT av FROM vd.a WHERE id > 1;");
    CHECK(cur->size() == 1);
}

TEST_CASE("name_resolution::validator::qualified_reference_inside_a_derived_table") {
    database_t db(integration_fixture_path("test_name_resolution/validator_derived"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.inner_t (k INT, v INT);",
             "INSERT INTO vd.inner_t (k, v) VALUES (1,100),(2,200),(3,300);"});

    auto cur = run_ok(db, "SELECT sub.v FROM (SELECT inner_t.v FROM vd.inner_t WHERE inner_t.k > 1) sub;");
    CHECK(cur->size() == 2);
}

TEST_CASE("name_resolution::validator::qualifier_inside_a_derived_join_still_selects") {
    database_t db(integration_fixture_path("test_name_resolution/validator_derived_join"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, v INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, v) VALUES (1,100),(2,200);"});

    // `v` belongs to y, not x. Naming it through x has to be refused.
    std::ignore = run_refused(db, "SELECT sub.v FROM (SELECT x.v FROM vd.a x JOIN vd.b y ON x.id = y.id) sub;");
}

TEST_CASE("name_resolution::validator::subquery_qualifier_does_not_reach_a_neighbour") {
    database_t db(integration_fixture_path("test_name_resolution/validator_neighbour"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, bv INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, bv) VALUES (1,100),(2,200);"});

    std::ignore = run_refused(db, "SELECT s.bv FROM (SELECT x.id, x.av FROM vd.a x) s JOIN vd.b b ON s.id = b.id;");
}

TEST_CASE("name_resolution::validator::table_function_alias_names_the_relation") {
    database_t db(integration_fixture_path("test_name_resolution/table_function_alias"));
    db.seed({"CREATE DATABASE vd;"});

    auto cur = run_ok(db, "SELECT g.g FROM generate_series(1, 3) g;");
    CHECK(cur->size() == 3);
}

TEST_CASE("name_resolution::validator::table_function_shares_the_from_namespace") {
    database_t db(integration_fixture_path("test_name_resolution/table_function_namespace"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.g (id INT, w INT);",
             "INSERT INTO vd.g (id, w) VALUES (1,100);",
             "CREATE TABLE vd.s (generate_series INT, z INT);",
             "INSERT INTO vd.s (generate_series, z) VALUES (1,7),(2,8);"});
    {
        INFO("the function's alias collides with a table's visible name");
        CHECK(run_refused(db, "SELECT g.g FROM vd.g, generate_series(1, 3) g;").type ==
              core::error_code_t::ambiguous_name);
    }
    {
        INFO("unaliased, the function's column name is the function name — and so is the table's column");
        CHECK(run_refused(db, "SELECT generate_series FROM vd.s, generate_series(1, 3);").type ==
              core::error_code_t::ambiguous_name);
    }
    {
        INFO("an alias takes no qualification");
        CHECK(run_refused(db, "SELECT g.g.g FROM generate_series(1, 3) g;").type ==
              core::error_code_t::table_not_exists);
    }
}

TEST_CASE("name_resolution::using::merged_column_follows_the_join_type") {
    database_t db(integration_fixture_path("test_name_resolution/using_outer"));
    db.seed({"CREATE DATABASE ud;",
             "CREATE TABLE ud.a (id INT, av INT);",
             "CREATE TABLE ud.b (id INT, bv INT);",
             "INSERT INTO ud.a (id, av) VALUES (1,10),(2,20),(3,30);",
             "INSERT INTO ud.b (id, bv) VALUES (3,300),(4,400);"});
    {
        INFO("RIGHT: b's rows survive, so the merged id is the right copy");
        auto cur = run_ok(db, "SELECT id FROM ud.a a RIGHT JOIN ud.b b USING (id);");
        REQUIRE(cur->size() == 2);
        CHECK(rows_with_a_value(cur) == 2);
    }
    {
        INFO("FULL keeps working through either copy — the merge is what is missing, not the join");
        auto cur = run_ok(db, "SELECT a.id, b.id FROM ud.a a FULL JOIN ud.b b USING (id);");
        CHECK(cur->size() == 4);
    }
    {
        INFO("FULL's merged column is COALESCE, which the bare name cannot stand for yet");
        auto error = run_refused(db, "SELECT id FROM ud.a a FULL JOIN ud.b b USING (id);");
        CHECK(error.type == core::error_code_t::unimplemented_yet);
        CHECK(to_std(error.what).find("COALESCE") != std::string::npos);
    }
}

TEST_CASE("name_resolution::validator::ambiguous_column_says_it_is_ambiguous") {
    database_t db(integration_fixture_path("test_name_resolution/validator_ambiguous"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, bv INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, bv) VALUES (1,100),(2,200);"});

    CHECK(run_refused(db, "SELECT a.av FROM vd.a a JOIN vd.b b ON a.id = b.id WHERE id > 1;").type ==
          core::error_code_t::ambiguous_name);
}

TEST_CASE("name_resolution::quoted::quoted_alias_answers_as_written") {
    require_resolved(probe("SELECT \"X\".id FROM d.t AS \"X\";", "id"), side_t::left, "X");
}

TEST_CASE("name_resolution::quoted::unquoted_reference_misses_a_quoted_alias") {
    // `X` reaches the resolver as `x`, and the element answers to `X`.
    require_rejected(probe("SELECT X.id FROM d.t AS \"X\";", "id"), core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::quoted::quoted_db_qualifier") {
    require_resolved(probe("SELECT \"MyDb\".t.id FROM \"MyDb\".t;", "id"), side_t::left, "t");
}

TEST_CASE("name_resolution::quoted::unquoted_db_qualifier_misses_a_quoted_one") {
    require_rejected(probe("SELECT MyDb.t.id FROM \"MyDb\".t;", "id"), core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::quoted::quoted_relname_in_a_mixed_name") {
    // Only the quoted segment keeps its case; `d` folds as usual.
    require_resolved(probe("SELECT \"MyTable\".id FROM d.\"MyTable\";", "id"), side_t::left, "MyTable");
}

TEST_CASE("name_resolution::quoted::quoted_column_keeps_its_case") {
    database_t db(integration_fixture_path("test_name_resolution/quoted_column"));
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.q (\"Id\" INT, v INT);",
             "INSERT INTO vd.q (\"Id\", v) VALUES (1,10);"});
    {
        INFO("declared quoted, so only the quoted spelling names it");
        auto cur = run_ok(db, "SELECT \"Id\" FROM vd.q;");
        CHECK(cur->size() == 1);
    }
    {
        INFO("unquoted `Id` folds to `id`, which the table does not have");
        std::ignore = run_refused(db, "SELECT Id FROM vd.q;");
    }
}

TEST_CASE("name_resolution::quoted::unquoted_column_is_folded") {
    database_t db(integration_fixture_path("test_name_resolution/unquoted_column"));
    db.seed({"CREATE DATABASE vd;", "CREATE TABLE vd.u (Id INT, v INT);", "INSERT INTO vd.u (Id, v) VALUES (1,10);"});
    {
        INFO("declared unquoted, so every unquoted spelling reaches it");
        run_ok(db, "SELECT Id FROM vd.u;");
        run_ok(db, "SELECT id FROM vd.u;");
        run_ok(db, "SELECT ID FROM vd.u;");
    }
    {
        INFO("the quoted spelling is a different name, and the table has no such column");
        std::ignore = run_refused(db, "SELECT \"Id\" FROM vd.u;");
    }
}

TEST_CASE("name_resolution::regression::chained_join_reads_the_middle_table") {
    database_t db(integration_fixture_path("test_name_resolution/chain"));
    db.seed({"CREATE DATABASE cd;",
             "CREATE TABLE cd.l (jk INT, v INT);",
             "CREATE TABLE cd.m (jk INT, v INT);",
             "CREATE TABLE cd.n (jk INT, nv INT);",
             "INSERT INTO cd.l (jk, v) VALUES (1,100);",
             "INSERT INTO cd.m (jk, v) VALUES (1,200);",
             "INSERT INTO cd.n (jk, nv) VALUES (1,300);"});

    auto cur = run_ok(db, "SELECT m.v FROM cd.l JOIN cd.m ON l.jk = m.jk JOIN cd.n ON m.jk = n.jk;");
    REQUIRE(cur->size() == 1);
    CHECK(cur->value(0, 0).value<int32_t>() == 200);
}

TEST_CASE("name_resolution::regression::cross_database_join_is_not_a_cross_product") {
    database_t db(integration_fixture_path("test_name_resolution/cross_db"));
    db.seed({"CREATE DATABASE d1;",
             "CREATE DATABASE d2;",
             "CREATE TABLE d1.t (jk INT, v INT);",
             "CREATE TABLE d2.t (jk INT, v INT);",
             "INSERT INTO d1.t (jk, v) VALUES (1,10),(2,20),(3,30);",
             "INSERT INTO d2.t (jk, v) VALUES (2,200),(3,300),(4,400);"});

    auto cur = run_ok(db, "SELECT d1.t.v FROM d1.t JOIN d2.t ON d1.t.jk = d2.t.jk;");
    INFO("three rows each, two keys in common — an identity predicate would give nine");
    CHECK(cur->size() == 2);
}

namespace {
    template<typename Fn>
    auto with_parsed(const std::string& sql, Fn&& read) {
        auto resource = core::pmr::otterbrix_resource();
        std::pmr::monotonic_buffer_resource arena(&resource);
        List* raw = raw_parser(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        REQUIRE_FALSE(raw->lst.empty());
        auto& node = sql::transform::pg_cell_to_node_cast(linitial(raw));
        return read(sql::transform::pg_ptr_cast<SelectStmt>(&node), &resource);
    }

    qualified_name_t from_slots(const std::string& sql) {
        return with_parsed(sql, [](SelectStmt* select, std::pmr::memory_resource*) {
            auto* item = sql::transform::pg_ptr_cast<Node>(select->fromClause->lst.front().data);
            return sql::transform::rangevar_to_qualified_name(sql::transform::pg_ptr_cast<RangeVar>(item));
        });
    }

    struct reference_slots_t {
        std::string uid, db, schema, table, column;
    };

    reference_slots_t reference_slots(const std::string& sql) {
        sql::transform::name_collection_t names;
        names.left_name =
            qualified_name_t{core::uid_t{"u"}, core::dbname_t{"d"}, core::schema_t{"s"}, core::relname_t{"t"}};
        return with_parsed(sql, [&names](SelectStmt* select, std::pmr::memory_resource* resource) {
            auto* target = sql::transform::pg_ptr_cast<ResTarget>(select->targetList->lst.front().data);
            auto parsed = sql::transform::columnref_to_field(resource,
                                                             sql::transform::pg_ptr_cast<ColumnRef>(target->val),
                                                             names);
            REQUIRE_FALSE(parsed.has_error());
            const auto& ref = parsed.value();
            return reference_slots_t{ref.table.unique_identifier.t,
                                     ref.table.database.t,
                                     ref.table.schema.t,
                                     ref.table.collection.t,
                                     ref.field.as_string()};
        });
    }
} // namespace

TEST_CASE("name_resolution::from_name::from_arities_fill_the_slots") {
    // The shorter forms drop the middle slots: two segments are db.relname, not schema.relname.
    {
        auto name = from_slots("SELECT 1 FROM t;");
        CHECK(name.collection.t == "t");
        CHECK(name.database.t.empty());
        CHECK(name.schema.t.empty());
        CHECK(name.unique_identifier.t.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM d.t;");
        CHECK(name.database.t == "d");
        CHECK(name.collection.t == "t");
        CHECK(name.schema.t.empty());
        CHECK(name.unique_identifier.t.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM d.s.t;");
        CHECK(name.database.t == "d");
        CHECK(name.schema.t == "s");
        CHECK(name.collection.t == "t");
        CHECK(name.unique_identifier.t.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM u.d.s.t;");
        CHECK(name.unique_identifier.t == "u");
        CHECK(name.database.t == "d");
        CHECK(name.schema.t == "s");
        CHECK(name.collection.t == "t");
    }
}

TEST_CASE("name_resolution::column_ref::column_arities_fill_the_slots") {
    // Not a suffix of the FROM table: three segments are db.table.col, skipping schema, while
    // three segments in FROM are db.schema.relname -- only the count from the right is shared.
    {
        auto ref = reference_slots("SELECT col FROM u.d.s.t;");
        CHECK(ref.column == "col");
        CHECK(ref.table.empty());
        CHECK(ref.db.empty());
        CHECK(ref.schema.empty());
        CHECK(ref.uid.empty());
    }
    {
        auto ref = reference_slots("SELECT t.col FROM u.d.s.t;");
        CHECK(ref.table == "t");
        CHECK(ref.db.empty());
        CHECK(ref.schema.empty());
        CHECK(ref.uid.empty());
    }
    {
        auto ref = reference_slots("SELECT d.t.col FROM u.d.s.t;");
        CHECK(ref.db == "d");
        CHECK(ref.table == "t");
        CHECK(ref.schema.empty());
        CHECK(ref.uid.empty());
    }
    {
        auto ref = reference_slots("SELECT d.s.t.col FROM u.d.s.t;");
        CHECK(ref.db == "d");
        CHECK(ref.schema == "s");
        CHECK(ref.table == "t");
        CHECK(ref.uid.empty());
    }
    {
        auto ref = reference_slots("SELECT u.d.s.t.col FROM u.d.s.t;");
        CHECK(ref.uid == "u");
        CHECK(ref.db == "d");
        CHECK(ref.schema == "s");
        CHECK(ref.table == "t");
    }
}

TEST_CASE("name_resolution::star::qualified_star_keeps_its_side") {
    database_t db(integration_fixture_path("test_name_resolution/qualified_star_side"));
    db.seed({"CREATE DATABASE d1;",
             "CREATE DATABASE d2;",
             "CREATE TABLE d1.t (id BIGINT, a BIGINT);",
             "CREATE TABLE d2.t (id BIGINT, b BIGINT);",
             "INSERT INTO d1.t (id, a) VALUES (1, 10);",
             "INSERT INTO d2.t (id, b) VALUES (1, 20);"});

    auto right = run_ok(db, "SELECT d2.t.* FROM d1.t JOIN d2.t ON d1.t.id = d2.t.id;");
    REQUIRE(right->size() == 1);
    REQUIRE(right->column_count() == 2);
    CHECK(right->value(1, 0).value<int64_t>() == 20);

    auto left = run_ok(db, "SELECT d1.t.* FROM d1.t JOIN d2.t ON d1.t.id = d2.t.id;");
    REQUIRE(left->size() == 1);
    REQUIRE(left->column_count() == 2);
    CHECK(left->value(1, 0).value<int64_t>() == 10);
}

TEST_CASE("name_resolution::alter::alter_table_if_exists_on_a_missing_table_is_a_no_op") {
    database_t db(integration_fixture_path("test_name_resolution/alter_if_exists"));
    db.seed({"CREATE DATABASE d;"});
    run_ok(db, "ALTER TABLE IF EXISTS d.missing ADD COLUMN z BIGINT;");
    auto refused = run_refused(db, "ALTER TABLE d.missing ADD COLUMN z BIGINT;");
    CHECK(refused.type == core::error_code_t::table_not_exists);
}

// An unqualified REFERENCES target lives in the database of the table that owns the key — in ALTER TABLE as in
// CREATE TABLE, whether that table is written qualified or found by resolve.
TEST_CASE("name_resolution::fk_target::alter_resolves_the_target_in_the_owner_database") {
    database_t db(integration_fixture_path("test_name_resolution/fk_owner_database"));
    db.seed({"CREATE DATABASE shop;",
             "CREATE DATABASE other;",
             "CREATE TABLE shop.customers (id BIGINT PRIMARY KEY);",
             "CREATE TABLE other.customers (id BIGINT PRIMARY KEY);",
             "INSERT INTO shop.customers (id) VALUES (1);",
             "INSERT INTO other.customers (id) VALUES (2);",
             "CREATE TABLE shop.orders (cid BIGINT);",
             "CREATE TABLE shop.returns (cid BIGINT);"});

    run_ok(db, "ALTER TABLE orders ADD CONSTRAINT fk_orders FOREIGN KEY (cid) REFERENCES customers (id);");
    run_ok(db, "ALTER TABLE shop.returns ADD CONSTRAINT fk_returns FOREIGN KEY (cid) REFERENCES customers (id);");
    for (const std::string table : {"shop.orders", "shop.returns"}) {
        run_ok(db, "INSERT INTO " + table + " (cid) VALUES (1);");
        auto refused = run_refused(db, "INSERT INTO " + table + " (cid) VALUES (2);");
        CHECK(refused.type != core::error_code_t::none);
    }
}

TEST_CASE("name_resolution::fk_target::alter_does_not_reach_another_database") {
    database_t db(integration_fixture_path("test_name_resolution/fk_other_database"));
    db.seed({"CREATE DATABASE shop;",
             "CREATE DATABASE other;",
             "CREATE TABLE other.customers (id BIGINT PRIMARY KEY);",
             "CREATE TABLE shop.orders (cid BIGINT);"});

    for (const std::string sql :
         {"ALTER TABLE orders ADD CONSTRAINT fk FOREIGN KEY (cid) REFERENCES customers (id);",
          "ALTER TABLE shop.orders ADD CONSTRAINT fk FOREIGN KEY (cid) REFERENCES customers (id);"}) {
        // The refusal a missing REFERENCES target gets anywhere (CREATE TABLE, a qualified ALTER).
        auto refused = run_refused(db, sql);
        INFO(sql << ": " << to_std(refused.what));
        CHECK(refused.type == core::error_code_t::invalid_constraint);
        CHECK(to_std(refused.what).find("customers\" does not exist") != std::string::npos);
    }
}

TEST_CASE("name_resolution::alter::alter_table_if_exists_add_constraint_on_a_missing_table_is_a_no_op") {
    database_t db(integration_fixture_path("test_name_resolution/alter_if_exists_constraint"));
    db.seed({"CREATE DATABASE d;"});
    run_ok(db, "ALTER TABLE IF EXISTS d.missing ADD CONSTRAINT uq UNIQUE (z);");
    auto refused = run_refused(db, "ALTER TABLE d.missing ADD CONSTRAINT uq UNIQUE (z);");
    CHECK(refused.type != core::error_code_t::none);
}

TEST_CASE("name_resolution::alter::alter_table_if_exists_rename_column_on_a_missing_table_is_a_no_op") {
    database_t db(integration_fixture_path("test_name_resolution/alter_if_exists_rename"));
    db.seed({"CREATE DATABASE d;"});
    run_ok(db, "ALTER TABLE IF EXISTS d.missing RENAME COLUMN a TO b;");
    auto refused = run_refused(db, "ALTER TABLE d.missing RENAME COLUMN a TO b;");
    CHECK(refused.type != core::error_code_t::none);
}

TEST_CASE("name_resolution::fk_target::schema_or_uid_segment_is_refused") {
    database_t db(integration_fixture_path("test_name_resolution/fk_segments"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.p (id BIGINT PRIMARY KEY);", "CREATE TABLE d.c (id BIGINT);"});
    for (const std::string sql : {"CREATE TABLE d.c1 (id BIGINT REFERENCES d.s.p (id));",
                                  "CREATE TABLE d.c2 (id BIGINT, FOREIGN KEY (id) REFERENCES u.d.s.p (id));",
                                  "ALTER TABLE d.c ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES d.s.p (id);"}) {
        auto refused = run_refused(db, sql);
        INFO(sql << ": " << to_std(refused.what));
        CHECK(refused.type == core::error_code_t::invalid_parameter);
        CHECK(to_std(refused.what).find("uid or schema segment") != std::string::npos);
    }
    run_ok(db, "CREATE TABLE d.c3 (id BIGINT REFERENCES d.p (id));");
}

// A write target keeps its schema segment, as a read does: in a local database it is refused with the read's words;
// a database the catalog does not know leaves the whole name to the host.
TEST_CASE("name_resolution::write_target::schema_segment_in_a_local_database_is_refused") {
    database_t db(integration_fixture_path("test_name_resolution/write_schema_segment"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.t (id BIGINT);", "INSERT INTO d.t (id) VALUES (1);"});
    for (const std::string sql :
         {"INSERT INTO d.s.t (id) VALUES (2);", "UPDATE d.s.t SET id = 2;", "DELETE FROM d.s.t WHERE id = 1;"}) {
        auto refused = run_refused(db, sql);
        INFO(sql);
        CHECK(to_std(refused.what) ==
              "schema \"s\" does not exist: a relation lives in a database — write d.s.t as [database.]name");
    }
    auto rows = run_ok(db, "SELECT id FROM d.t;");
    CHECK(rows->size() == 1);
}

// IF EXISTS is a statement-level switch: it turns only "the statement's target does not exist" into success.
TEST_CASE("name_resolution::if_exists::drop_of_a_missing_target") {
    database_t db(integration_fixture_path("test_name_resolution/if_exists_drop"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.t (id BIGINT);"});
    for (const std::string kind_and_name : {"TABLE d.missing",
                                            "VIEW d.missing",
                                            "SEQUENCE d.missing",
                                            "TYPE missing",
                                            "DATABASE missing",
                                            "INDEX d.t.missing"}) {
        const auto space = kind_and_name.find(' ');
        const auto kind = kind_and_name.substr(0, space);
        const auto name = kind_and_name.substr(space + 1);
        INFO(kind_and_name);
        run_ok(db, "DROP " + kind + " IF EXISTS " + name + ";");
        auto refused = run_refused(db, "DROP " + kind + " " + name + ";");
        CHECK(to_std(refused.what).find("missing") != std::string::npos);
    }
}

TEST_CASE("name_resolution::if_exists::alter_forms_on_a_missing_table") {
    database_t db(integration_fixture_path("test_name_resolution/if_exists_alter"));
    db.seed({"CREATE DATABASE d;"});
    for (const std::string tail : {"ADD COLUMN z BIGINT",
                                   "ADD CONSTRAINT uq UNIQUE (z)",
                                   "ADD CONSTRAINT fk FOREIGN KEY (z) REFERENCES d.p (id)",
                                   "RENAME COLUMN a TO b"}) {
        INFO(tail);
        run_ok(db, "ALTER TABLE IF EXISTS d.missing " + tail + ";");
        auto refused = run_refused(db, "ALTER TABLE d.missing " + tail + ";");
        CHECK(refused.type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("name_resolution::if_exists::alter_of_an_existing_table_keeps_other_errors") {
    database_t db(integration_fixture_path("test_name_resolution/if_exists_other"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.t (id BIGINT);"});
    for (const std::string tail : {"ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES d.missing (id)",
                                   "ADD CONSTRAINT uq UNIQUE (nosuch)",
                                   "RENAME COLUMN nosuch TO b"}) {
        INFO(tail);
        auto refused = run_refused(db, "ALTER TABLE IF EXISTS d.t " + tail + ";");
        CHECK(refused.type != core::error_code_t::none);
    }
}

TEST_CASE("name_resolution::alter::add_column_resolves_its_type") {
    database_t db(integration_fixture_path("test_name_resolution/add_column_type"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.t (id BIGINT);", "CREATE TYPE mood AS ENUM ('sad', 'ok');"});
    auto refused = run_refused(db, "ALTER TABLE d.t ADD COLUMN z nosuchtype;");
    INFO(to_std(refused.what));
    CHECK(refused.type != core::error_code_t::none);
    run_ok(db, "ALTER TABLE d.t ADD COLUMN m mood;");
    run_ok(db, "INSERT INTO d.t (id, m) VALUES (1, 'ok');");
    auto rows = run_ok(db, "SELECT m FROM d.t;");
    REQUIRE(rows->size() == 1);
}

// DROP MATERIALIZED VIEW goes through the same path as DROP TABLE / DROP VIEW.
TEST_CASE("name_resolution::drop_matview::drop_survives_restart") {
    const auto path = integration_fixture_path("test_name_resolution/drop_matview");
    {
        database_t db(path);
        db.seed({"CREATE DATABASE d;",
                 "CREATE TABLE d.t (a BIGINT);",
                 "CREATE MATERIALIZED VIEW d.mv AS SELECT a FROM d.t WITH NO DATA;"});
        run_ok(db, "DROP MATERIALIZED VIEW d.mv;");
        auto gone = run_refused(db, "SELECT * FROM d.mv;");
        CHECK(gone.type == core::error_code_t::table_not_exists);
        run_ok(db, "DROP MATERIALIZED VIEW IF EXISTS d.mv;");
        auto missing = run_refused(db, "DROP MATERIALIZED VIEW d.mv;");
        CHECK(missing.type == core::error_code_t::table_not_exists);
    }
    auto config = test_create_config(path);
    test_spaces space(config);
    auto cur = space.dispatcher()->execute_sql(otterbrix::session_id_t(), "SELECT * FROM d.mv;");
    REQUIRE(cur->is_error());
    auto again = space.dispatcher()->execute_sql(otterbrix::session_id_t(),
                                                 "CREATE MATERIALIZED VIEW d.mv AS SELECT a FROM d.t WITH NO DATA;");
    INFO(to_std(again->is_error() ? again->get_error().what : std::pmr::string{"<ok>"}));
    REQUIRE(again->is_success());
}

TEST_CASE("name_resolution::drop_matview::kind_mismatch_is_refused") {
    database_t db(integration_fixture_path("test_name_resolution/drop_matview_kind"));
    db.seed({"CREATE DATABASE d;",
             "CREATE TABLE d.t (a BIGINT);",
             "CREATE VIEW d.v AS SELECT a FROM d.t;",
             "CREATE MATERIALIZED VIEW d.mv AS SELECT a FROM d.t WITH NO DATA;"});
    for (const auto& [sql, text] : std::vector<std::pair<std::string, std::string>>{
             {"DROP MATERIALIZED VIEW d.t;", "\"t\" is not a materialized view"},
             {"DROP MATERIALIZED VIEW d.v;", "\"v\" is not a materialized view"},
             {"DROP VIEW d.mv;", "\"mv\" is not a view"}}) {
        auto refused = run_refused(db, sql);
        INFO(sql << ": " << to_std(refused.what));
        CHECK(to_std(refused.what).find(text) != std::string::npos);
        CHECK(refused.type == core::error_code_t::schema_error);
    }
    // PostgreSQL 18 DropErrorMsgWrongType: the hint names the DROP for the kind the name has.
    CHECK(to_std(run_refused(db, "DROP VIEW d.t;").what).find("HINT: Use DROP TABLE to remove a table.") !=
          std::string::npos);
    CHECK(to_std(run_refused(db, "DROP MATERIALIZED VIEW d.v;").what).find("HINT: Use DROP VIEW to remove a view.") !=
          std::string::npos);
    run_ok(db, "SELECT * FROM d.mv;");
    run_ok(db, "SELECT * FROM d.v;");
}

// The matview depends on its source table ('n'): RESTRICT refuses, CASCADE takes the matview too.
TEST_CASE("name_resolution::drop_matview::source_table_restrict_and_cascade") {
    database_t db(integration_fixture_path("test_name_resolution/drop_matview_source"));
    db.seed({"CREATE DATABASE d;",
             "CREATE TABLE d.t (a BIGINT);",
             "CREATE MATERIALIZED VIEW d.mv AS SELECT a FROM d.t WITH NO DATA;"});
    auto restricted = run_refused(db, "DROP TABLE d.t;");
    INFO(to_std(restricted.what));
    run_ok(db, "SELECT * FROM d.mv;");
    run_ok(db, "DROP TABLE d.t CASCADE;");
    auto gone = run_refused(db, "SELECT * FROM d.mv;");
    CHECK(gone.type == core::error_code_t::table_not_exists);
    run_ok(db, "CREATE TABLE d.t (a BIGINT);");
    run_ok(db, "CREATE MATERIALIZED VIEW d.mv AS SELECT a FROM d.t WITH NO DATA;");
    run_ok(db, "DROP MATERIALIZED VIEW d.mv CASCADE;");
}

// A subcommand's IF EXISTS skips that subcommand alone; the others still apply (PostgreSQL: a notice, no error).
TEST_CASE("name_resolution::if_exists::subcommand_skips_only_itself") {
    database_t db(integration_fixture_path("test_name_resolution/if_exists_subcommand"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.t (id BIGINT, CONSTRAINT uq_id UNIQUE (id));"});

    run_ok(db, "ALTER TABLE d.t DROP COLUMN IF EXISTS a, ADD COLUMN b BIGINT;");
    run_ok(db, "SELECT b FROM d.t;");
    auto atomic = run_refused(db, "ALTER TABLE d.t DROP COLUMN a, ADD COLUMN c BIGINT;");
    INFO(to_std(atomic.what));
    auto no_c = run_refused(db, "SELECT c FROM d.t;");
    CHECK(no_c.type != core::error_code_t::none);

    run_ok(db, "ALTER TABLE d.t DROP CONSTRAINT IF EXISTS nope, ADD COLUMN e BIGINT;");
    run_ok(db, "SELECT e FROM d.t;");
    auto atomic_constraint = run_refused(db, "ALTER TABLE d.t DROP CONSTRAINT nope, ADD COLUMN f BIGINT;");
    INFO(to_std(atomic_constraint.what));
    auto no_f = run_refused(db, "SELECT f FROM d.t;");
    CHECK(no_f.type != core::error_code_t::none);

    run_ok(db, "ALTER TABLE d.t DROP COLUMN IF EXISTS a;");
    run_ok(db, "ALTER TABLE d.t DROP CONSTRAINT IF EXISTS uq_id, DROP CONSTRAINT IF EXISTS nope;");
    run_ok(db, "INSERT INTO d.t (id) VALUES (1), (1);");
}

TEST_CASE("name_resolution::if_exists::subcommand_on_a_computed_table") {
    database_t db(integration_fixture_path("test_name_resolution/if_exists_subcommand_computed"));
    db.seed({"CREATE DATABASE d;", "CREATE TABLE d.g ();", "INSERT INTO d.g (id, x) VALUES (1, 2);"});
    run_ok(db, "ALTER TABLE d.g DROP COLUMN IF EXISTS nosuch, DROP COLUMN x;");
    auto rows = run_ok(db, "SELECT * FROM d.g;");
    CHECK(rows->column_count() == 1);
    auto atomic = run_refused(db, "ALTER TABLE d.g DROP COLUMN nosuch, DROP COLUMN id;");
    INFO(to_std(atomic.what));
    auto still = run_ok(db, "SELECT * FROM d.g;");
    CHECK(still->column_count() == 1);
}
