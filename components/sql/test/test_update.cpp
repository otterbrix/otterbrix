#include "parameter_equals.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::sql::transform;
using namespace components::expressions;

// Transformer now wraps UPDATE in sequence_t(resolve_*..., update); descend
// to the update consumer to inspect update_t shape.
#define TEST_SIMPLE_UPDATE(QUERY, RESULT, PARAMS, FIELDS)                                                              \
    SECTION(QUERY) {                                                                                                   \
        auto select = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        auto result = ([](auto _w) {                                                                                   \
            REQUIRE_FALSE(_w.has_error());                                                                             \
            return _w.value();                                                                                         \
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));                                            \
        auto node = result.sub_queries.back();                                                                         \
        if (node->type() == components::logical_plan::node_type::sequence_t) {                                         \
            node = node->children().back();                                                                            \
        }                                                                                                              \
        auto agg = result.parameters;                                                                                  \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().size() == PARAMS.size());                                                            \
        for (auto i = 0ul; i < PARAMS.size(); ++i) {                                                                   \
            REQUIRE(parameter_equals(*agg, core::parameter_id_t(uint32_t(i)), PARAMS.at(i)));                          \
        }                                                                                                              \
        auto updates = static_cast<components::logical_plan::node_update_t&>(*node).updates();                         \
        REQUIRE(updates.size() == FIELDS.size());                                                                      \
        for (size_t i = 0; i < updates.size(); ++i) {                                                                  \
            REQUIRE(*updates.at(i) == *FIELDS.at(i));                                                                  \
        }                                                                                                              \
    }

using components::vector::make_scalar;
using components::vector::make_string_literal;
using vec = std::vector<components::vector::vector_t>;
using fields = std::pmr::vector<expression_ptr>;

// A SET value expression, keyed by the column it is assigned to.
static expression_ptr
set_value(std::pmr::memory_resource* resource, components::expressions::key_t target, scalar_type type) {
    return make_scalar_expression(resource, type, target);
}

// SET <target> = $id
static expression_ptr
set_const(std::pmr::memory_resource* resource, components::expressions::key_t target, core::parameter_id_t id) {
    auto expr = set_value(resource, std::move(target), scalar_type::constant);
    static_cast<scalar_expression_t*>(expr.get())->append_param(id);
    return expr;
}

TEST_CASE("components::sql::update") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "count"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10;",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, 10l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "name"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET name = 'new name';",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_string_literal(&resource, "new name")}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "is_doc"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true;",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, true)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "count"}, core::parameter_id_t{0}));
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "name"}, core::parameter_id_t{1}));
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "is_doc"}, core::parameter_id_t{2}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true;",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, 10l),
                                make_string_literal(&resource, "new name"),
                                make_scalar(&resource, true)}),
                           f);
    }
}

TEST_CASE("components::sql::update_where") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "count"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET count = 10 WHERE id = 1;",
                           R"_($update: <oid:0> {$match: {"id": {$eq: #1}}})_",
                           vec({make_scalar(&resource, 10l), make_scalar(&resource, 1l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "name"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE(
            "UPDATE TestDatabase.TestCollection SET name = 'new name' WHERE name = 'old_name';",
            R"_($update: <oid:0> {$match: {"name": {$eq: #1}}})_",
            vec({make_string_literal(&resource, "new name"), make_string_literal(&resource, "old_name")}),
            f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "is_doc"}, core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET is_doc = true WHERE is_doc = false;",
                           R"_($update: <oid:0> {$match: {"is_doc": {$eq: #1}}})_",
                           vec({make_scalar(&resource, true), make_scalar(&resource, false)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "count"}, core::parameter_id_t{0}));
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "name"}, core::parameter_id_t{1}));
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "is_doc"}, core::parameter_id_t{2}));
        TEST_SIMPLE_UPDATE(
            "UPDATE TestDatabase.TestCollection SET count = 10, name = 'new name', is_doc = true "
            "WHERE id > 10 AND name = 'old_name' AND is_doc = false;",
            R"_($update: <oid:0> {$match: {$and: ["id": {$gt: #3}, "name": {$eq: #4}, "is_doc": {$eq: #5}]}})_",
            vec({make_scalar(&resource, 10l),
                 make_string_literal(&resource, "new name"),
                 make_scalar(&resource, true),
                 make_scalar(&resource, 10l),
                 make_string_literal(&resource, "old_name"),
                 make_scalar(&resource, false)}),
            f);
    }
}

TEST_CASE("components::sql::update_from") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    {
        fields f;
        auto price = set_value(&resource, components::expressions::key_t{&resource, "price"}, scalar_type::multiply);
        auto* price_expr = static_cast<scalar_expression_t*>(price.get());
        price_expr->append_param(components::expressions::key_t{&resource, "price", side_t::undefined});
        price_expr->append_param(core::parameter_id_t{0});
        f.emplace_back(std::move(price));
        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET price = price * 1.5;)_",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, 1.5)}),
                           f);
    }

    {
        fields f;
        auto product = make_scalar_expression(&resource, scalar_type::multiply);
        product->append_param(components::expressions::key_t{&resource, "price", side_t::right});
        product->append_param(components::expressions::key_t{&resource, "discount", side_t::left});
        auto price = set_value(&resource, components::expressions::key_t{&resource, "price"}, scalar_type::subtract);
        auto* price_expr = static_cast<scalar_expression_t*>(price.get());
        price_expr->append_param(components::expressions::key_t{&resource, "price", side_t::right});
        price_expr->append_param(expression_ptr{product});
        f.emplace_back(std::move(price));
        TEST_SIMPLE_UPDATE(
            R"_(UPDATE TestDatabase.TestCollection
SET price = OtherTestCollection.price - (OtherTestCollection.price * TestCollection.discount)
FROM OtherTestCollection
WHERE TestCollection.id = OtherTestCollection.id;)_",
            R"_($update: <oid:0> {$match: {"id": {$eq: "id"}}, $aggregate: {}})_",
            vec({}),
            f);
    }

    {
        fields f;
        components::expressions::key_t field{std::pmr::vector<std::pmr::string>{
            {std::pmr::string{"struct_type", &resource}, std::pmr::string{"field", &resource}},
            &resource}};
        auto sum = set_value(&resource, field, scalar_type::add);
        auto* sum_expr = static_cast<scalar_expression_t*>(sum.get());
        sum_expr->append_param(components::expressions::key_t{
            std::pmr::vector<std::pmr::string>{
                {std::pmr::string{"struct_type", &resource}, std::pmr::string{"field", &resource}},
                &resource},
            side_t::undefined});
        sum_expr->append_param(core::parameter_id_t{0});
        f.emplace_back(std::move(sum));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET struct_type.field = (struct_type).field + 1;",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, 1l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(
            set_const(&resource, components::expressions::key_t{&resource, "array_type"}, core::parameter_id_t{0}));
        const std::vector<components::vector::vector_t> elements{make_scalar(&resource, 1l),
                                                                 make_scalar(&resource, 2l),
                                                                 make_scalar(&resource, 3l),
                                                                 make_scalar(&resource, 4l)};
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET array_type = ARRAY[1,2,3,4];",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({components::vector::make_array(
                               &arena_resource,
                               components::types::complex_logical_type{components::types::logical_type::BIGINT},
                               elements)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(set_const(&resource,
                                 components::expressions::key_t{std::pmr::vector<std::pmr::string>{
                                     {std::pmr::string{"array_type", &resource}, std::pmr::string{"4", &resource}},
                                     &resource}},
                                 core::parameter_id_t{0}));
        TEST_SIMPLE_UPDATE("UPDATE TestDatabase.TestCollection SET array_type[4] = 196;",
                           R"_($update: <oid:0> {$match: {$all_true}})_",
                           vec({make_scalar(&resource, 196l)}),
                           f);
    }
}