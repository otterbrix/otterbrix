#include <catch2/catch.hpp>

#include <string>

#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>

#include <demo_ast.hpp>
#include <demo_extension.hpp>

using namespace components::sql::parser;

namespace {
    using demo_ext::demo_kind;

    bool is_op(const demo_ext::demo_node* node, demo_kind kind) { return node != nullptr && node->kind == kind; }

    long int_value(const demo_ext::demo_node* node) {
        REQUIRE(node != nullptr);
        REQUIRE(node->kind == demo_kind::number);
        return node->value;
    }

    demo_ext::demo_node* demo_payload(List* tree) {
        auto* root = extension_payload<demo_ext::demo_node>(tree, "demo");
        REQUIRE(root != nullptr);
        return root;
    }

    parser_extension_registry_t demo_registry() {
        parser_extension_registry_t registry;
        REQUIRE_FALSE(registry.add(make_demo_extension()).has_error());
        return registry;
    }
} // namespace

TEST_CASE("components::sql::correct_pg_query") {
    auto registry = demo_registry();

    std::pmr::monotonic_buffer_resource arena;
    auto* tree = raw_parser(&arena, "SELECT * FROM t;", registry);
    REQUIRE(nodeTag(linitial(tree)) == T_SelectStmt);
}

TEST_CASE("components::sql::extension_ast") {
    auto registry = demo_registry();

    std::pmr::monotonic_buffer_resource arena;

    SECTION("single literal") {
        auto* root = demo_payload(raw_parser(&arena, "DEMO 7", registry));
        CHECK(int_value(root) == 7);
    }

    SECTION("'+' binds looser than '*'") {
        auto* root = demo_payload(raw_parser(&arena, "DEMO 2 + 3 * 4", registry));
        REQUIRE(is_op(root, demo_kind::add));
        CHECK(int_value(root->lhs) == 2);
        REQUIRE(is_op(root->rhs, demo_kind::multiply));
        CHECK(int_value(root->rhs->lhs) == 3);
        CHECK(int_value(root->rhs->rhs) == 4);
    }

    SECTION("parentheses override precedence") {
        auto* root = demo_payload(raw_parser(&arena, "DEMO (2 + 3) * 4", registry));
        REQUIRE(is_op(root, demo_kind::multiply));
        REQUIRE(is_op(root->lhs, demo_kind::add));
        CHECK(int_value(root->rhs) == 4);
    }

    SECTION("left-associativity of subtraction") {
        // 10 - 3 - 2  ==  (10 - 3) - 2, i.e. the left operand is itself a '-'.
        auto* root = demo_payload(raw_parser(&arena, "DEMO 10 - 3 - 2", registry));
        REQUIRE(is_op(root, demo_kind::subtract));
        REQUIRE(is_op(root->lhs, demo_kind::subtract));
        CHECK(int_value(root->rhs) == 2);
    }

    SECTION("division is left-associative too") {
        // 20 / 5 / 2  ==  (20 / 5) / 2
        auto* root = demo_payload(raw_parser(&arena, "DEMO 20 / 5 / 2", registry));
        REQUIRE(is_op(root, demo_kind::divide));
        REQUIRE(is_op(root->lhs, demo_kind::divide));
        CHECK(int_value(root->rhs) == 2);
    }

    SECTION("the DEMO keyword is case-insensitive") {
        auto* root = demo_payload(raw_parser(&arena, "dEmO 7", registry));
        CHECK(int_value(root) == 7);
    }
}

TEST_CASE("components::sql::extension_transform") {
    auto registry = demo_registry();

    std::pmr::monotonic_buffer_resource arena;
    auto* demo_node = reinterpret_cast<Node*>(linitial(raw_parser(&arena, "DEMO 2 + 3 * 4", registry)));
    REQUIRE(nodeTag(demo_node) == T_ExtensionNode);

    components::sql::transform::transformer tr(&arena, nullptr, &registry);
    auto result = tr.transform(*demo_node);
    REQUIRE_FALSE(result.has_error());
    REQUIRE(result.node_ptr() != nullptr);
    CHECK(result.node_ptr()->type() == components::logical_plan::node_type::data_t);
}

TEST_CASE("components::sql::extension_error") {
    auto registry = demo_registry();

    std::pmr::monotonic_buffer_resource arena;
    CHECK_THROWS_AS(raw_parser(&arena, "DEMO 2 +", registry), parser_exception_t);
    CHECK_THROWS_AS(raw_parser(&arena, "DEMO (1 + 2", registry), parser_exception_t);
    CHECK_THROWS_AS(raw_parser(&arena, "DEMO", registry), parser_exception_t); // keyword, no expression
}

TEST_CASE("components::sql::extension_duplicate_rejected") {
    parser_extension_registry_t registry;
    REQUIRE_FALSE(registry.add(make_demo_extension()).has_error());

    auto duplicate = registry.add(make_demo_extension());
    REQUIRE(duplicate.has_error());
    CHECK(duplicate.error().type == core::error_code_t::already_exists);
}
