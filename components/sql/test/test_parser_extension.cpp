#include <catch2/catch.hpp>

#include <cctype>
#include <stdexcept>
#include <string>

#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/types/logical_value.hpp>

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

    // a toy ECHO extension, accepts ECHO <integer>
    struct echo_payload {
        int64_t value;
    };

    size_t match_keyword(const std::string& query, std::string_view keyword) {
        const size_t begin = query.find_first_not_of(" \t\r\n");
        if (begin == std::string::npos || query.size() - begin < keyword.size()) {
            return std::string::npos;
        }
        for (size_t i = 0; i < keyword.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(query[begin + i])) != keyword[i]) {
                return std::string::npos;
            }
        }
        const size_t end = begin + keyword.size();
        if (end < query.size() && (std::isalnum(static_cast<unsigned char>(query[end])) || query[end] == '_')) {
            return std::string::npos;
        }
        return end;
    }

    parse_extension_result_t echo_parse(std::pmr::memory_resource* resource, const std::string& query) {
        const size_t after_kw = match_keyword(query, "echo");
        if (after_kw == std::string::npos) {
            return NIL; // not ours, let the next extension win
        }

        const std::string arg = query.substr(after_kw);
        try {
            size_t consumed = 0;
            const long long value = std::stoll(arg, &consumed);
            if (arg.find_first_not_of(" \t\r\n", consumed) != std::string::npos) {
                throw std::invalid_argument("trailing tokens");
            }
            auto* payload = static_cast<echo_payload*>(resource->allocate(sizeof(echo_payload)));
            payload->value = static_cast<int64_t>(value);
            return list_make1(resource, make_extension_node(resource, "echo", payload));
        } catch (const std::exception&) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"echo: expected a single integer argument", resource});
        }
    }

    components::logical_plan::node_ptr echo_transform(std::pmr::memory_resource* resource,
                                                      ExtensionNode* node,
                                                      components::logical_plan::parameter_node_t* /*params*/) {
        const auto* payload = static_cast<const echo_payload*>(node->data);
        components::types::logical_value_t value(resource, payload->value);
        components::vector::data_chunk_t chunk(resource, {}, 1);
        chunk.set_cardinality(1);
        chunk.data.emplace_back(resource, value.type(), chunk.capacity());
        chunk.set_value(0, 0, std::move(value));
        return components::logical_plan::make_node_raw_data(resource, std::move(chunk));
    }

    parser_extension_t make_echo_extension() { return parser_extension_t{"echo", &echo_parse, &echo_transform}; }

    parser_extension_registry_t demo_registry() {
        parser_extension_registry_t registry;
        REQUIRE_FALSE(registry.add(make_demo_extension()).has_error());
        return registry;
    }

    parser_extension_registry_t demo_and_echo_registry() {
        parser_extension_registry_t registry;
        REQUIRE_FALSE(registry.add(make_demo_extension()).has_error());
        REQUIRE_FALSE(registry.add(make_echo_extension()).has_error());
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

TEST_CASE("components::sql::two_extensions_route_by_keyword") {
    auto registry = demo_and_echo_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("demo claims DEMO, echo does not") {
        auto* tree = raw_parser(&arena, "DEMO 2 + 3", registry);
        CHECK(extension_payload<demo_ext::demo_node>(tree, "demo") != nullptr);
        CHECK(extension_payload<echo_payload>(tree, "echo") == nullptr);
    }

    SECTION("echo claims ECHO, demo does not") {
        auto* tree = raw_parser(&arena, "ECHO 42", registry);
        auto* payload = extension_payload<echo_payload>(tree, "echo");
        REQUIRE(payload != nullptr);
        CHECK(payload->value == 42);
        CHECK(extension_payload<demo_ext::demo_node>(tree, "demo") == nullptr);
    }

    SECTION("keyword that only prefixes an identifier is not claimed") {
        // "ECHOES" must not be mistaken for the ECHO keyword, this is parse error
        CHECK_THROWS_AS(raw_parser(&arena, "ECHOES 1", registry), parser_exception_t);
    }

    SECTION("core SQL is claimed by neither extension") {
        auto* tree = raw_parser(&arena, "SELECT * FROM t;", registry);
        REQUIRE(nodeTag(linitial(tree)) == T_SelectStmt);
    }

    SECTION("a keyword nobody owns surfaces core parser error") {
        CHECK_THROWS_AS(raw_parser(&arena, "FOOBAR 1", registry), parser_exception_t);
    }
}

TEST_CASE("components::sql::two_extensions_error_handling") {
    auto registry = demo_and_echo_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("demo parse error") {
        CHECK_THROWS_AS(raw_parser(&arena, "DEMO 2 +", registry), parser_exception_t);
        CHECK_THROWS_AS(raw_parser(&arena, "DEMO (1 + 2", registry), parser_exception_t);
    }

    SECTION("echo parse error") {
        CHECK_THROWS_AS(raw_parser(&arena, "ECHO", registry), parser_exception_t);     // missing argument
        CHECK_THROWS_AS(raw_parser(&arena, "ECHO abc", registry), parser_exception_t); // not an integer
        CHECK_THROWS_AS(raw_parser(&arena, "ECHO 1 2", registry), parser_exception_t); // trailing tokens
    }
}

TEST_CASE("components::sql::two_extensions_transform_routing") {
    auto registry = demo_and_echo_registry();
    std::pmr::monotonic_buffer_resource arena;

    SECTION("echo node transform") {
        auto* node = reinterpret_cast<Node*>(linitial(raw_parser(&arena, "ECHO 99", registry)));
        REQUIRE(nodeTag(node) == T_ExtensionNode);
        components::sql::transform::transformer tr(&arena, nullptr, &registry);
        auto result = tr.transform(*node);
        REQUIRE_FALSE(result.has_error());
        REQUIRE(result.node_ptr() != nullptr);
        CHECK(result.node_ptr()->type() == components::logical_plan::node_type::data_t);
    }

    SECTION("demo node transform") {
        auto* node = reinterpret_cast<Node*>(linitial(raw_parser(&arena, "DEMO 2 + 3 * 4", registry)));
        REQUIRE(nodeTag(node) == T_ExtensionNode);
        components::sql::transform::transformer tr(&arena, nullptr, &registry);
        auto result = tr.transform(*node);
        REQUIRE_FALSE(result.has_error());
        CHECK(result.node_ptr()->type() == components::logical_plan::node_type::data_t);
    }

    SECTION("an unknown extension node is a transform error") {
        auto* ghost = make_extension_node(&arena, "unknown-and-unregistered-extension", nullptr);
        components::sql::transform::transformer tr(&arena, nullptr, &registry);
        auto result = tr.transform(*ghost);
        REQUIRE(result.has_error());
    }
}
