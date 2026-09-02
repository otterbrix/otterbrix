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

    // Catalog lookups ride on the plan, one node per kind, each carrying every target of
    // that kind. A type names itself in type_name; every other kind uses relname.
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
        std::string stage; // "parser" or "transformer" when rejected
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

        collect_join_arities(result.node_ptr(), out.join_arities);
        collect_joins(result.node_ptr(), out.joins);
        std::vector<found_key_t> keys;
        collect_keys(result.node_ptr(), keys);
        for (const auto& key : keys) {
            // An empty name asks for every key, which is what the cases that
            // watch the shape of a statement rather than one reference want.
            if (column.empty() || last_segment(key.path) == column) {
                out.matches.push_back(key);
            }
        }
        // Reads the plan, so it goes last: finalize() hands over what transform() built.
        if (auto plan = result.finalize(); !plan.has_error()) {
            collect_catalog_targets(plan.value().catalog_resolves, out.catalog_targets);
        }
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

    // A running engine, for the cases whose answer only exists past the
    // transformer: which relation an unqualified name belongs to, and how many
    // rows a join actually produces.
    struct database_t {
        explicit database_t(const std::string& path)
            : config(test_create_config(path)) {
            test_clear_directory(config);
            config.disk.on = false;
            config.wal.on = false;
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

    // The statement and the engine's own words belong in the report: without
    // them a failed expectation reads as a bare "false".
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

    // How many rows carry a value in the first column. A merged column reading
    // the padded copy of an outer join shows up here and nowhere else.
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
    // The reference fills `db`, the element leaves it empty — that is
    // a mismatch, not a wildcard.
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
    // The reference fills `schema`, the two-part element does not.
    auto result = probe("SELECT d.s.t.id FROM d.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::three_part_from_answers_bare_relname") {
    auto result = probe("SELECT t.id FROM d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::skip_middle_slot") {
    // skipping `schema` is allowed, the filled slots match.
    auto result = probe("SELECT d.t.id FROM d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::qualification::schema_alone_does_not_qualify") {
    // `s` lands in the `db` slot, where the element has `d`. This is
    // the form a PostgreSQL user writes first, so the message has to show
    // how the element is spelled in FROM.
    auto result = probe("SELECT s.t.id FROM d.s.t;", "id");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::qualification::schema_alone_does_not_qualify_under_uid") {
    // The same trap one slot deeper.
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
    // the longest legal column form.
    auto result = probe("SELECT u.d.s.t.id FROM u.d.s.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::column_ref::six_part_column_reference_rejected") {
    // Over five segments there is no shape to read the reference as.
    auto result = probe("SELECT u.d.s.t.x.id FROM u.d.s.t;", "id");
    INFO(describe(result));
    REQUIRE(result.rejected);
}

TEST_CASE("name_resolution::from_name::five_part_from_reference_rejected") {
    auto result = probe("SELECT id FROM a.b.c.d.e;", "id");
    require_rejected_by_parser(result);
}

TEST_CASE("name_resolution::type_name::alter_type_keeps_its_database") {
    // The two-part name of a TYPE statement is `db.type`, same as everywhere
    // else. This used to hold only because a promotion copied the schema slot
    // into the database one after the fact.
    auto result = transform_only("ALTER TYPE shop.addr_t ADD ATTRIBUTE zip TEXT;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:shop.addr_t") !=
          result.catalog_targets.end());
}

TEST_CASE("name_resolution::type_name::unqualified_alter_type_has_no_database") {
    // The other side of the rule: with nothing to put in the database slot, the
    // statement must not acquire one out of thin air.
    auto result = transform_only("ALTER TYPE addr_t ADD ATTRIBUTE zip TEXT;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    CHECK(std::find(result.catalog_targets.begin(), result.catalog_targets.end(), "table:.addr_t") !=
          result.catalog_targets.end());
}

TEST_CASE("name_resolution::type_name::type_name_over_three_parts_rejected") {
    auto result = transform_only("CREATE TYPE a.b.c.d AS (x INT);");
    require_rejected_by_parser(result);
}

TEST_CASE("name_resolution::ambiguity::bare_relname_across_databases") {
    // CROSS JOIN so the only reference in the query is the one under
    // test: an ON clause would carry the same ambiguity and muddy the reading.
    auto result = probe("SELECT t.id FROM d1.t CROSS JOIN d2.t;", "id");
    require_rejected(result, core::error_code_t::ambiguous_name);
}

TEST_CASE("name_resolution::ambiguity::skipped_slot_made_it_ambiguous") {
    // Skipping `schema` is legal, but here it makes two elements
    // answer — which is a refusal rather than a guess.
    auto result = probe("SELECT d.t.id FROM d.s1.t CROSS JOIN d.s2.t;", "id");
    require_rejected(result, core::error_code_t::ambiguous_name);
}

TEST_CASE("name_resolution::ambiguity::database_qualifier_picks_a_side") {
    auto result = probe("SELECT d1.t.id FROM d1.t CROSS JOIN d2.t;", "id");
    require_resolved(result, side_t::left, "t");
}

TEST_CASE("name_resolution::ambiguity::database_qualifier_picks_the_right_side") {
    // The mirror of the row above: the same query has to reach the other side.
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
    // Every slot of a subquery element is empty, so a filled slot in
    // the reference cannot match.
    auto result = probe("SELECT d.s.c FROM (SELECT * FROM d.inner_t) s;", "c");
    require_rejected(result, core::error_code_t::table_not_exists);
}

TEST_CASE("name_resolution::from_element::subquery_without_alias_rejected") {
    // The grammar already refuses this (gram.y:12668), inherited
    // from PostgreSQL, so the case guards a rule that holds rather than
    // reporting one that is missing.
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
    // The same missing-copy question for the remaining element kind.
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
    // The PostgreSQL-idiomatic spelling: one pair of parentheses, then the field
    // chain continues without more.
    auto result = probe("SELECT (t.custom_type).f3.f1 FROM d.t;", "f1");
    require_resolved_path(result, side_t::left, "t", "custom_type/f3/f1");
}

TEST_CASE("name_resolution::field_access::nested_pairs_reach_the_same_field") {
    auto result = probe("SELECT ((t.custom_type).f3).f1 FROM d.t;", "f1");
    require_resolved_path(result, side_t::left, "t", "custom_type/f3/f1");
}

TEST_CASE("name_resolution::field_access::same_field_from_a_predicate") {
    // WHERE reads the reference through a different code path than the select
    // list does. Both have to arrive at the same key, or a predicate and a
    // projection over one field would disagree about which field it is.
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
    // Nothing in FROM carries that name.
    auto result = probe("SELECT nosuch.id FROM shop.orders;", "id");
    require_rejected_saying(result, core::error_code_t::table_not_exists, "nosuch");
}

TEST_CASE("name_resolution::message::message_shows_how_the_element_is_spelled") {
    // The element is there, the qualification is not the one written.
    // Without its spelling in the message the user cannot see why a form that
    // works in PostgreSQL fails here.
    auto result = probe("SELECT sales.orders.id FROM shop.sales.orders;", "id");
    require_rejected_saying(result, core::error_code_t::table_not_exists, "shop.sales.orders");
}

TEST_CASE("name_resolution::message::message_suggests_the_alias") {
    // The name is there but an alias hides it.
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
    // The nastier half of the defect: LEFT JOIN already produced join_type::left,
    // so the cardinality looked plausible while every left row matched every
    // right one.
    auto result = transform_only("SELECT a.v FROM d.a a LEFT JOIN d.b b USING (id);");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "left(id:left id:right)");
}

TEST_CASE("name_resolution::using::composite_left_side") {
    // Why the predicate is built by side and not by name: here the left side of
    // the outer join is a join, and has no name to write.
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
    // NATURAL needs the column lists of both sides, which the transformer does
    // not have. Refusing beats the silent cross product it used to produce.
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
    // The other side of the rule: with neither ON nor USING it really is a cross
    // product, and must stay one.
    auto result = transform_only("SELECT a.v FROM d.a a CROSS JOIN d.b b;");
    INFO(describe(result));
    REQUIRE_FALSE(result.rejected);
    REQUIRE(result.joins.size() == 1);
    CHECK(result.joins.front() == "cross()");
}

TEST_CASE("name_resolution::using::row_count_is_not_a_cross_product") {
    auto config = test_create_config("/tmp/test_name_resolution/using");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
        // Which copy the merged column stands for is only observable on an outer
        // join: rows 1 and 2 have no match in b, so the right copy is padded and
        // the merged `id` has to come from the left. Reading the padded copy
        // would show up here as two empty ids.
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
    database_t db("/tmp/test_name_resolution/validator_unqualified");
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, bv INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, bv) VALUES (1,100),(2,200);"});

    auto cur = run_ok(db, "SELECT a.av FROM vd.a a JOIN vd.b b ON a.id = b.id WHERE av > 15;");
    CHECK(cur->size() == 1);
}

TEST_CASE("name_resolution::validator::one_relation_on_both_sides_is_not_ambiguous") {
    database_t db("/tmp/test_name_resolution/validator_same_schema");
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);"});

    auto cur = run_ok(db, "SELECT av FROM vd.a WHERE id > 1;");
    CHECK(cur->size() == 1);
}

TEST_CASE("name_resolution::validator::qualified_reference_inside_a_derived_table") {
    database_t db("/tmp/test_name_resolution/validator_derived");
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.inner_t (k INT, v INT);",
             "INSERT INTO vd.inner_t (k, v) VALUES (1,100),(2,200),(3,300);"});

    auto cur = run_ok(db, "SELECT sub.v FROM (SELECT inner_t.v FROM vd.inner_t WHERE inner_t.k > 1) sub;");
    CHECK(cur->size() == 2);
}

TEST_CASE("name_resolution::validator::qualifier_inside_a_derived_join_still_selects") {
    database_t db("/tmp/test_name_resolution/validator_derived_join");
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, v INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, v) VALUES (1,100),(2,200);"});

    // `v` belongs to y, not x. Naming it through x has to be refused.
    std::ignore = run_refused(db, "SELECT sub.v FROM (SELECT x.v FROM vd.a x JOIN vd.b y ON x.id = y.id) sub;");
}

TEST_CASE("name_resolution::validator::subquery_qualifier_does_not_reach_a_neighbour") {
    database_t db("/tmp/test_name_resolution/validator_neighbour");
    db.seed({"CREATE DATABASE vd;",
             "CREATE TABLE vd.a (id INT, av INT);",
             "CREATE TABLE vd.b (id INT, bv INT);",
             "INSERT INTO vd.a (id, av) VALUES (1,10),(2,20);",
             "INSERT INTO vd.b (id, bv) VALUES (1,100),(2,200);"});

    // `bv` belongs to b, not to the derived table s. Naming it through s must be refused.
    std::ignore = run_refused(db, "SELECT s.bv FROM (SELECT x.id, x.av FROM vd.a x) s JOIN vd.b b ON s.id = b.id;");
}

TEST_CASE("name_resolution::validator::table_function_alias_names_the_relation") {
    database_t db("/tmp/test_name_resolution/table_function_alias");
    db.seed({"CREATE DATABASE vd;"});

    auto cur = run_ok(db, "SELECT g.g FROM generate_series(1, 3) g;");
    CHECK(cur->size() == 3);
}

TEST_CASE("name_resolution::validator::table_function_shares_the_from_namespace") {
    database_t db("/tmp/test_name_resolution/table_function_namespace");
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
    database_t db("/tmp/test_name_resolution/using_outer");
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
    database_t db("/tmp/test_name_resolution/validator_ambiguous");
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
    database_t db("/tmp/test_name_resolution/quoted_column");
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
    database_t db("/tmp/test_name_resolution/unquoted_column");
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
    database_t db("/tmp/test_name_resolution/chain");
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
    database_t db("/tmp/test_name_resolution/cross_db");
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

    // The slots a FROM name landed in, straight off the RangeVar.
    sql::transform::qualified_name from_slots(const std::string& sql) {
        return with_parsed(sql, [](SelectStmt* select, std::pmr::memory_resource*) {
            auto* item = sql::transform::pg_ptr_cast<Node>(select->fromClause->lst.front().data);
            return sql::transform::rangevar_to_qualified_name(sql::transform::pg_ptr_cast<RangeVar>(item));
        });
    }

    struct reference_slots_t {
        std::string uid, db, schema, table, column;
    };

    // The slots a column reference landed in. `names` carries one fully spelled
    // out element so that every arity of the lower table resolves against it and
    // the parse is what the case observes, not the match.
    reference_slots_t reference_slots(const std::string& sql) {
        sql::transform::name_collection_t names;
        names.left_name = sql::transform::qualified_name{"d", "t", "s", "u"};
        return with_parsed(sql, [&names](SelectStmt* select, std::pmr::memory_resource* resource) {
            auto* target = sql::transform::pg_ptr_cast<ResTarget>(select->targetList->lst.front().data);
            auto parsed = sql::transform::columnref_to_field(resource,
                                                             sql::transform::pg_ptr_cast<ColumnRef>(target->val),
                                                             names);
            REQUIRE_FALSE(parsed.has_error());
            const auto& ref = parsed.value();
            return reference_slots_t{ref.uid, ref.db, ref.schema, ref.table, ref.field.as_string()};
        });
    }
} // namespace

TEST_CASE("name_resolution::from_name::from_arities_fill_the_slots") {
    // A FROM name. The shorter forms drop the MIDDLE slots: two
    // segments are db.relname, not schema.relname — the deviation from PG that
    // the whole of matching is shaped around.
    {
        auto name = from_slots("SELECT 1 FROM t;");
        CHECK(name.relname == "t");
        CHECK(name.dbname.empty());
        CHECK(name.schemaname.empty());
        CHECK(name.uuid.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM d.t;");
        CHECK(name.dbname == "d");
        CHECK(name.relname == "t");
        CHECK(name.schemaname.empty());
        CHECK(name.uuid.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM d.s.t;");
        CHECK(name.dbname == "d");
        CHECK(name.schemaname == "s");
        CHECK(name.relname == "t");
        CHECK(name.uuid.empty());
    }
    {
        auto name = from_slots("SELECT 1 FROM u.d.s.t;");
        CHECK(name.uuid == "u");
        CHECK(name.dbname == "d");
        CHECK(name.schemaname == "s");
        CHECK(name.relname == "t");
    }
}

TEST_CASE("name_resolution::column_ref::column_arities_fill_the_slots") {
    // A column reference. Not a suffix of the FROM table: three segments are
    // db.table.col, skipping `schema`, while three segments in FROM are
    // db.schema.relname. Only the count from the right is shared.
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
