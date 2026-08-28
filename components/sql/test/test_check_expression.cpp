#include <catch2/catch_test_macros.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql::transform;
using namespace components::expressions;

// A stored CHECK constraint is SQL text, and it is compiled back into an expression by parsing it
// as the WHERE clause it resembles. That is what keeps the two in step: a predicate the engine
// accepts in a WHERE is accepted in a CHECK, without a second grammar that has to be taught each
// construct separately.

namespace {
    expression_ptr parse_check(std::pmr::memory_resource* resource, const std::string& text) {
        transformer tr(resource);
        auto parsed = tr.parse_where_expr(text);
        INFO("CHECK text: " << text);
        REQUIRE_FALSE(parsed.has_error());
        return parsed.value().expr;
    }
} // namespace

TEST_CASE("sql::check_expression::comparison_against_a_literal") {
    auto* resource = std::pmr::get_default_resource();
    auto expr = parse_check(resource, "age > 0");
    REQUIRE(expr);
    REQUIRE(expr->group() == expression_group::compare);
    CHECK(static_cast<compare_expression_t*>(expr.get())->type() == compare_type::gt);
}

TEST_CASE("sql::check_expression::comparison_over_two_columns") {
    auto* resource = std::pmr::get_default_resource();
    auto expr = parse_check(resource, "lo < hi");
    REQUIRE(expr);
    REQUIRE(expr->group() == expression_group::compare);
    auto* compare = static_cast<compare_expression_t*>(expr.get());
    CHECK(compare->type() == compare_type::lt);
    // Both sides name a column, so neither is a bound constant.
    CHECK(is_key(compare->left()));
    CHECK(is_key(compare->right()));
}

// The shape the hand-written compiler could not read: an operand that is computed rather than
// named. Parsing as a WHERE clause gets arithmetic for free, because a WHERE already has it.
TEST_CASE("sql::check_expression::arithmetic_operand") {
    auto* resource = std::pmr::get_default_resource();
    {
        auto expr = parse_check(resource, "n * -1 > 0");
        REQUIRE(expr);
        REQUIRE(expr->group() == expression_group::compare);
        auto* compare = static_cast<compare_expression_t*>(expr.get());
        CHECK(compare->type() == compare_type::gt);
        // The left operand is the multiplication, carried as a nested expression.
        REQUIRE(is_expr(compare->left()));
        const auto& nested = as_expr(compare->left());
        REQUIRE(nested->group() == expression_group::scalar);
        CHECK(static_cast<scalar_expression_t*>(nested.get())->type() == scalar_type::multiply);
    }
    {
        auto expr = parse_check(resource, "n + m > 10");
        REQUIRE(expr);
        CHECK(expr->group() == expression_group::compare);
    }
    {
        auto expr = parse_check(resource, "n * 2 > n + 1");
        REQUIRE(expr);
        CHECK(expr->group() == expression_group::compare);
    }
}

TEST_CASE("sql::check_expression::boolean_combinations") {
    auto* resource = std::pmr::get_default_resource();
    for (const auto& text : {"n > 0 AND n < 100", "n < 0 OR n > 100", "NOT (n = 0)", "n IS NOT NULL"}) {
        auto expr = parse_check(resource, text);
        INFO("CHECK text: " << text);
        CHECK(expr);
    }
}

TEST_CASE("sql::check_expression::richer_predicates") {
    auto* resource = std::pmr::get_default_resource();
    for (const auto& text : {"n BETWEEN 1 AND 10", "n IN (1, 2, 3)", "s LIKE 'a%'", "abs(n) > 0"}) {
        auto expr = parse_check(resource, text);
        INFO("CHECK text: " << text);
        CHECK(expr);
    }
}

// Text that is not an expression must be refused, not quietly turned into something that passes
// every row.
TEST_CASE("sql::check_expression::garbage_is_refused") {
    auto* resource = std::pmr::get_default_resource();
    for (const auto& text : {"", "age >", ") nonsense (", "SELECT 1"}) {
        transformer tr(resource);
        auto parsed = tr.parse_where_expr(text);
        INFO("CHECK text: " << text);
        CHECK(parsed.has_error());
    }
}

// The catalog stores the expression the user wrote, taken verbatim out of the statement rather
// than rebuilt from the parse tree. That is what makes the round trip exact — the stored bytes
// parse back to the same expression because they are the same bytes.
//
// Finding the span is a lexical walk from the CHECK keyword to the ')' that closes it, so the
// cases that matter are the ones where a parenthesis or a quote is not punctuation: inside a
// string, inside a comment, or escaped.
namespace {
    std::string sliced(std::pmr::memory_resource* resource, const std::string& statement) {
        std::pmr::monotonic_buffer_resource arena(resource);
        auto* parsed = raw_parser(&arena, statement.c_str());
        REQUIRE(parsed != nullptr);
        auto& node = pg_cell_to_node_cast(linitial(parsed));
        // Reach the Constraint node of `ALTER TABLE ... ADD CONSTRAINT ... CHECK (...)`.
        auto& alter = pg_cast<AlterTableStmt>(node);
        auto* command = pg_ptr_cast<AlterTableCmd>(alter.cmds->lst.front().data);
        auto* constraint = pg_ptr_cast<Constraint>(command->def);
        auto text = slice_check_expression(resource, statement.c_str(), constraint->location);
        INFO("statement: " << statement);
        REQUIRE_FALSE(text.has_error());
        return text.value();
    }
} // namespace

TEST_CASE("sql::check_expression::the_stored_text_is_what_was_written") {
    auto* resource = std::pmr::get_default_resource();
    const std::string head = "ALTER TABLE d.t ADD CONSTRAINT ck CHECK (";

    CHECK(sliced(resource, head + "n * -1 > 0);") == "n * -1 > 0");
    CHECK(sliced(resource, head + "(n) > 0);") == "(n) > 0");
    CHECK(sliced(resource, head + "n > 0 AND (m < 1 OR m > 9));") == "n > 0 AND (m < 1 OR m > 9)");
    CHECK(sliced(resource, head + "abs(n) > 0);") == "abs(n) > 0");
    CHECK(sliced(resource, head + "  n > 0  );") == "n > 0");

    // A parenthesis inside a string is not punctuation.
    CHECK(sliced(resource, head + "s = ')');") == "s = ')'");
    CHECK(sliced(resource, head + "s = '()()');") == "s = '()()'");
    // A quote doubled inside a string escapes itself.
    CHECK(sliced(resource, head + "s = 'it''s');") == "s = 'it''s'");
    CHECK(sliced(resource, head + "s <> 'a)b' AND n > 0);") == "s <> 'a)b' AND n > 0");
    // A quoted identifier follows the same rule.
    CHECK(sliced(resource, "ALTER TABLE d.t ADD CONSTRAINT ck CHECK (\"od)d\" > 0);") == "\"od)d\" > 0");
    // A comment may hide anything at all.
    CHECK(sliced(resource, head + "n > 0 /* ) not the end */ AND m > 0);") == "n > 0 /* ) not the end */ AND m > 0");
    CHECK(sliced(resource, head + "n > 0 -- ) not the end\n AND m > 0);") == "n > 0 -- ) not the end\n AND m > 0");
    // Whatever is written after the constraint must not be swept in.
    CHECK(sliced(resource, head + "n > 0) NOT VALID;") == "n > 0");
}

// Without the statement text there is nothing to slice, and a guess is worse than a refusal.
TEST_CASE("sql::check_expression::a_missing_statement_is_refused") {
    auto* resource = std::pmr::get_default_resource();
    CHECK(slice_check_expression(resource, nullptr, 0).has_error());
    CHECK(slice_check_expression(resource, "ALTER TABLE d.t ADD CONSTRAINT ck CHECK (n > 0);", -1).has_error());
    // A location past the end of the statement.
    CHECK(slice_check_expression(resource, "CHECK (n > 0)", 9999).has_error());
    // Nothing closes the parenthesis.
    CHECK(slice_check_expression(resource, "CHECK (n > 0", 0).has_error());
}
