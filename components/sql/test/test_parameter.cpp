#include "parameter_equals.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <tuple>

using namespace components;
using namespace components::sql;
using namespace components::sql::transform;
using namespace components::expressions;

using components::vector::make_null;
using components::vector::make_scalar;
using vec = std::vector<components::vector::vector_t>;

#define TEST_PARAMS_SELECT(QUERY, RESULT, BIND)                                                                        \
    {                                                                                                                  \
        SECTION(QUERY) {                                                                                               \
            auto select = linitial(raw_parser(&arena_resource, QUERY));                                                \
            auto binder = transformer.transform(pg_cell_to_node_cast(select));                                         \
            TEST_PARAMS(RESULT, BIND)                                                                                  \
        }                                                                                                              \
    }

// Transformer now wraps SELECT/UPDATE in sequence_t(resolve_*..., consumer);
// descend to the consumer when inspecting its rendered form.
#define TEST_PARAMS(RESULT, BIND)                                                                                      \
    {                                                                                                                  \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            binder.bind(i + 1, BIND.at(i));                                                                            \
        }                                                                                                              \
        auto result = ([](auto _w) {                                                                                   \
            REQUIRE_FALSE(_w.has_error());                                                                             \
            return _w.value();                                                                                         \
        }(binder.finalize()));                                                                                         \
        auto node = result.sub_queries.back();                                                                         \
        if (node->type() == components::logical_plan::node_type::sequence_t) {                                         \
            node = node->children().back();                                                                            \
        }                                                                                                              \
        auto agg = result.parameters;                                                                                  \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().size() == BIND.size());                                                              \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            REQUIRE(parameter_equals(*agg, core::parameter_id_t(uint32_t(i)), BIND.at(i)));                            \
        }                                                                                                              \
    }

#define TEST_SIMPLE_UPDATE(QUERY, RESULT, BIND, FIELDS)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(raw_parser(&arena_resource, QUERY));                                                      \
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));                                               \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            binder.bind(i + 1, BIND.at(i));                                                                            \
        }                                                                                                              \
        auto result = ([](auto _w) {                                                                                   \
            REQUIRE_FALSE(_w.has_error());                                                                             \
            return _w.value();                                                                                         \
        }(binder.finalize()));                                                                                         \
        auto node = result.sub_queries.back();                                                                         \
        if (node->type() == components::logical_plan::node_type::sequence_t) {                                         \
            node = node->children().back();                                                                            \
        }                                                                                                              \
        auto agg = result.parameters;                                                                                  \
        REQUIRE(node->to_string() == RESULT);                                                                          \
        REQUIRE(agg->parameters().size() == BIND.size());                                                              \
        for (auto i = 0ul; i < BIND.size(); ++i) {                                                                     \
            REQUIRE(parameter_equals(*agg, core::parameter_id_t(uint32_t(i)), BIND.at(i)));                            \
        }                                                                                                              \
    }

TEST_CASE("components::sql::select_bind") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_PARAMS_SELECT(
        R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 AND name = $2 AND "count" = $1;)_",
        R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
        vec({make_scalar(&resource, 10l), make_scalar(&resource, std::string("doc 10"))}));

    TEST_PARAMS_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 OR name = $2;)_",
                       R"_($aggregate: {$match: {$or: ["number": {$eq: #0}, "name": {$eq: #1}]}})_",
                       vec({make_scalar(&resource, 42l), make_scalar(&resource, std::string("abc"))}));

    TEST_PARAMS_SELECT(R"_(SELECT * FROM TestDatabase.TestCollection WHERE id > $1 AND flag = $2;)_",
                       R"_($aggregate: {$match: {$and: ["id": {$gt: #0}, "flag": {$eq: #1}]}})_",
                       vec({make_scalar(&resource, 5l), make_scalar(&resource, true)}));
}

TEST_CASE("components::sql::update_bind") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);
    using fields = std::pmr::vector<expression_ptr>;
    // A SET value keyed by its target column: SET <target> = $id
    auto set_const = [&](components::expressions::key_t target, core::parameter_id_t id) {
        auto expr = make_scalar_expression(&resource, scalar_type::constant, target);
        expr->append_param(id);
        return expression_ptr{expr};
    };

    {
        fields f;
        f.emplace_back(set_const(components::expressions::key_t{&resource, "count"}, core::parameter_id_t{0}));

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET count = $1 WHERE id = $2;)_",
                           R"_($update: <oid:0> {$match: {"id": {$eq: #1}}})_",
                           vec({make_scalar(&resource, 999l), make_scalar(&resource, 1l)}),
                           f);
    }

    {
        fields f;
        f.emplace_back(set_const(components::expressions::key_t{&resource, "name"}, core::parameter_id_t{0}));
        f.emplace_back(set_const(components::expressions::key_t{&resource, "flag"}, core::parameter_id_t{1}));

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET name = $1, flag = $2 WHERE "count" > $3;)_",
                           R"_($update: <oid:0> {$match: {"count": {$gt: #2}}})_",
                           vec({make_scalar(&resource, std::string("ok")),
                                make_scalar(&resource, true),
                                make_scalar(&resource, 100l)}),
                           f);
    }

    {
        fields f;
        auto rating =
            make_scalar_expression(&resource, scalar_type::add, components::expressions::key_t{&resource, "rating"});
        rating->append_param(components::expressions::key_t{&resource, "rating", side_t::undefined});
        rating->append_param(core::parameter_id_t{0});
        f.emplace_back(expression_ptr{rating});

        TEST_SIMPLE_UPDATE(R"_(UPDATE TestDatabase.TestCollection SET rating = rating + $1 WHERE flag = $2;)_",
                           R"_($update: <oid:0> {$match: {"flag": {$eq: #1}}})_",
                           vec({make_scalar(&resource, 5l), make_scalar(&resource, true)}),
                           f);
    }
}

TEST_CASE("components::sql::insert_bind") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("insert simple bind") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, name) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 42l));
        binder.bind(2, make_scalar(&resource, std::string("inserted")));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(binder.finalize()));
        auto node = result.sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }

        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, 42l)));
        REQUIRE(cell_equals(chunk.data[1], 0, make_scalar(&resource, "inserted")));
    }

    SECTION("insert with repeated param") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, parent_id) VALUES ($1, $1);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));

        REQUIRE(binder.all_bound() == false);
        binder.bind(1, make_scalar(&resource, 123l));
        REQUIRE(binder.all_bound() == true);

        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(binder.finalize()));
        auto node = result.sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }

        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, 123l)));
        REQUIRE(cell_equals(chunk.data[1], 0, make_scalar(&resource, 123l)));
    }

    SECTION("insert multi-bind") {
        auto select = linitial(raw_parser(&arena_resource,
                                          "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES "
                                          "($1, $2, $3), ($4, $5, $6);"));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        auto _wrap = binder.bind(1, make_scalar(&resource, 1ul))
                         .bind(2, make_scalar(&resource, "Name1"))
                         .bind(3, make_scalar(&resource, 10ul))
                         .bind(4, make_scalar(&resource, 2ul))
                         .bind(5, make_scalar(&resource, "Name2"))
                         .bind(6, make_scalar(&resource, 20ul))
                         .finalize();
        REQUIRE_FALSE(_wrap.has_error());
        auto result = _wrap.value();
        auto node = result.sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 2);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, 1ul)));
        REQUIRE(cell_equals(chunk.data[1], 0, make_scalar(&resource, "Name1")));
        REQUIRE(cell_equals(chunk.data[2], 0, make_scalar(&resource, 10ul)));
        REQUIRE(cell_equals(chunk.data[0], 1, make_scalar(&resource, 2ul)));
        REQUIRE(cell_equals(chunk.data[1], 1, make_scalar(&resource, "Name2")));
        REQUIRE(cell_equals(chunk.data[2], 1, make_scalar(&resource, 20ul)));
    }
}

TEST_CASE("components::sql::bind_row_of_column") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    components::vector::vector_t column(&resource, types::complex_logical_type{types::logical_type::BIGINT}, 3);
    column.set_value(0, int64_t{10});
    column.set_value(1, int64_t{20});
    column.set_value(2, int64_t{30});

    SECTION("select takes the requested row") {
        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE id = $1;)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, column, 2);
        auto finalized = binder.finalize();
        REQUIRE_FALSE(finalized.has_error());
        const auto& params = finalized.value().parameters;
        REQUIRE(params->parameters().size() == 1);
        REQUIRE(parameter_equals(*params, core::parameter_id_t(uint32_t(0)), make_scalar(&resource, int64_t{30})));
    }

    SECTION("insert takes the requested row") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, name) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, column, 2).bind(2, make_scalar(&resource, std::string("inserted")));
        auto finalized = binder.finalize();
        REQUIRE_FALSE(finalized.has_error());
        auto node = finalized.value().sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, int64_t{30})));
        REQUIRE(cell_equals(chunk.data[1], 0, make_scalar(&resource, std::string("inserted"))));
    }

    SECTION("a typed NULL of another type stays NULL in an insert column") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id) VALUES ($1), ($2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, int64_t{42}))
            .bind(2, make_null(&resource, types::complex_logical_type{types::logical_type::STRING_LITERAL}));
        auto finalized = binder.finalize();
        REQUIRE_FALSE(finalized.has_error());
        auto node = finalized.value().sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 2);
        REQUIRE(chunk.data[0].type().type() == types::logical_type::BIGINT);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, int64_t{42})));
        REQUIRE(chunk.data[0].is_null(1));
    }
}

TEST_CASE("components::sql::limit_offset_bind") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto consumer_string = [](const core::result_wrapper_t<logical_plan::execution_plan_t>& result,
                              std::string_view expected) {
        REQUIRE(!result.has_error());
        auto node = result.value().sub_queries[0];
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }
        REQUIRE(node->to_string() == expected);
    };

    SECTION("LIMIT $1 binds an integer") {
        auto stmt =
            linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection ORDER BY id LIMIT $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 5l));
        consumer_string(binder.finalize(), R"_($aggregate: {$sort: {id: 1}, $limit: 5})_");
    }

    SECTION("LIMIT $1 OFFSET $2 binds both") {
        auto stmt = linitial(
            raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection ORDER BY id LIMIT $1 OFFSET $2;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 7l)).bind(2, make_scalar(&resource, 3l));
        consumer_string(binder.finalize(), R"_($aggregate: {$sort: {id: 1}, $limit: 7, $offset: 3})_");
    }

    SECTION("LIMIT and WHERE share parameters") {
        auto stmt = linitial(
            raw_parser(&arena_resource,
                       "SELECT * FROM TestDatabase.TestCollection WHERE flag = $1 ORDER BY id LIMIT $2 OFFSET $3;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, true))
            .bind(2, make_scalar(&resource, 4l))
            .bind(3, make_scalar(&resource, 1l));
        consumer_string(binder.finalize(),
                        R"_($aggregate: {$match: {"flag": {$eq: #0}}, $sort: {id: 1}, $limit: 4, $offset: 1})_");
    }

    SECTION("a new LIMIT bind picks up the new value") {
        auto stmt =
            linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection ORDER BY id LIMIT $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 2l));
        consumer_string(binder.finalize(), R"_($aggregate: {$sort: {id: 1}, $limit: 2})_");

        auto next_binder = transformer.transform(pg_cell_to_node_cast(stmt));
        next_binder.bind(1, make_scalar(&resource, 11l));
        consumer_string(next_binder.finalize(), R"_($aggregate: {$sort: {id: 1}, $limit: 11})_");
    }

    SECTION("non-integer LIMIT parameter is rejected at finalize") {
        auto stmt =
            linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection ORDER BY id LIMIT $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, std::string("oops")));
        auto result = binder.finalize();
        REQUIRE(result.has_error());
    }

    SECTION("literal LIMIT with parameterized OFFSET") {
        auto stmt = linitial(
            raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection ORDER BY id LIMIT 5 OFFSET $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 2l));
        consumer_string(binder.finalize(), R"_($aggregate: {$sort: {id: 1}, $limit: 5, $offset: 2})_");
    }
}

TEST_CASE("components::sql::transform_result") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("not all bound") {
        auto stmt = linitial(
            raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection WHERE id = $1 AND name = $2;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 42l));
        auto result = binder.finalize();
        REQUIRE(result.has_error());
        REQUIRE(std::string(result.error().what).find("Not all parameters were bound: $2") != std::string::npos);
    }

    SECTION("a parameter the statement does not have is refused at bind") {
        auto stmt = linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection WHERE id = $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(stmt));
        binder.bind(1, make_scalar(&resource, 42l)).bind(7, make_scalar(&resource, 1l));
        REQUIRE(binder.has_error());
        REQUIRE(std::string(binder.get_error().what) == "Parameter with id=7 not found");
        REQUIRE(binder.finalize().has_error());
    }

    SECTION("new binds of one statement give new results") {
        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1 AND name = $2 AND "count" = $1;)_";
        auto select = linitial(raw_parser(&arena_resource, query));
        {
            auto binder = transformer.transform(pg_cell_to_node_cast(select));
            TEST_PARAMS(
                R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                vec({make_scalar(&resource, 10l), make_scalar(&resource, std::string("doc 10"))}));
        }
        {
            auto binder = transformer.transform(pg_cell_to_node_cast(select));
            TEST_PARAMS(
                R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                vec({make_scalar(&resource, 3.14), make_scalar(&resource, std::string("another doc 10"))}));
        }
        {
            auto binder = transformer.transform(pg_cell_to_node_cast(select));
            TEST_PARAMS(
                R"_($aggregate: {$match: {$and: ["number": {$eq: #0}, "name": {$eq: #1}, "count": {$eq: #0}]}})_",
                vec({make_scalar(&resource, false), make_scalar(&resource, std::string("another another doc 10"))}));
        }
    }

    SECTION("a second transform has parameters of its own") {
        auto resource = core::pmr::otterbrix_resource();
        std::pmr::monotonic_buffer_resource arena_resource(&resource);
        transform::transformer transformer(&resource);

        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1;)_";
        auto select = linitial(raw_parser(&arena_resource, query));

        auto first_binder = transformer.transform(pg_cell_to_node_cast(select));
        first_binder.bind(1, make_scalar(&resource, "doc"));
        auto first = ([](auto _w) {
                         REQUIRE_FALSE(_w.has_error());
                         return _w.value();
                     }(first_binder.finalize()))
                         .parameters;
        REQUIRE(parameter_equals(*first, core::parameter_id_t(uint32_t(0)), make_scalar(&resource, "doc")));

        auto second_binder = transformer.transform(pg_cell_to_node_cast(select));
        second_binder.bind(1, make_scalar(&resource, 100l));
        auto second = ([](auto _w) {
                          REQUIRE_FALSE(_w.has_error());
                          return _w.value();
                      }(second_binder.finalize()))
                          .parameters;
        REQUIRE(parameter_equals(*second, core::parameter_id_t(uint32_t(0)), make_scalar(&resource, 100l)));
        REQUIRE(parameter_equals(*first, core::parameter_id_t(uint32_t(0)), make_scalar(&resource, "doc")));
    }

    SECTION("the parameter count is the same after finalize") {
        auto select = linitial(raw_parser(&arena_resource, "SELECT * FROM TestDatabase.TestCollection WHERE id = $1;"));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        REQUIRE(binder.parameter_count() == 1);
        binder.bind(1, make_scalar(&resource, 1l));
        REQUIRE_FALSE(binder.finalize().has_error());
        REQUIRE(binder.parameter_count() == 1);
        REQUIRE(binder.all_bound());

        auto insert =
            linitial(raw_parser(&arena_resource, "INSERT INTO TestDatabase.TestCollection (id) VALUES ($1);"));
        auto insert_binder = transformer.transform(pg_cell_to_node_cast(insert));
        REQUIRE(insert_binder.parameter_count() == 1);
        insert_binder.bind(1, make_scalar(&resource, 1l));
        REQUIRE_FALSE(insert_binder.finalize().has_error());
        REQUIRE(insert_binder.parameter_count() == 1);
    }

    SECTION("a failed result has no parameters") {
        transform_result failed(&resource,
                                core::error_t{core::error_code_t::sql_parse_error, std::pmr::string{"x", &resource}});
        REQUIRE(failed.parameter_count() == 0);
        REQUIRE(failed.all_bound());
        REQUIRE(failed.finalize().has_error());
    }

    SECTION("a finalized result takes no more binds and cannot be finalized again") {
        auto query = R"_(SELECT * FROM TestDatabase.TestCollection WHERE number = $1;)_";
        auto select = linitial(raw_parser(&arena_resource, query));
        auto binder = transformer.transform(pg_cell_to_node_cast(select));
        binder.bind(1, make_scalar(&resource, 1l));
        REQUIRE_FALSE(binder.finalize().has_error());
        REQUIRE(binder.finalize().has_error());
        binder.bind(1, make_scalar(&resource, 2l));
        REQUIRE(binder.has_error());
    }

    SECTION("a new insert binding keeps the key translation") {
        auto query = R"_(INSERT INTO TestDatabase.TestCollection (id, parent_id) VALUES ($1, $2);)_";
        auto stmt = linitial(raw_parser(&arena_resource, query));

        auto first_binder = transformer.transform(pg_cell_to_node_cast(stmt));
        first_binder.bind(1, make_scalar(&resource, 123l)).bind(2, make_scalar(&resource, false));
        auto first = ([](auto _w) {
                         REQUIRE_FALSE(_w.has_error());
                         return _w.value();
                     }(first_binder.finalize()))
                         .sub_queries.back();
        if (first->type() == components::logical_plan::node_type::sequence_t) {
            first = first->children().back();
        }
        const auto keys = static_cast<logical_plan::node_insert_t*>(first.get())->key_translation();

        auto second_binder = transformer.transform(pg_cell_to_node_cast(stmt));
        second_binder.bind(1, make_scalar(&resource, true)).bind(2, make_scalar(&resource, std::string("doc 10")));
        auto node = ([](auto _w) {
                        REQUIRE_FALSE(_w.has_error());
                        return _w.value();
                    }(second_binder.finalize()))
                        .sub_queries.back();
        if (node->type() == components::logical_plan::node_type::sequence_t) {
            node = node->children().back();
        }

        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, make_scalar(&resource, true)));
        REQUIRE(cell_equals(chunk.data[1], 0, make_scalar(&resource, std::string("doc 10"))));
        REQUIRE(static_cast<logical_plan::node_insert_t*>(node.get())->key_translation() == keys);
    }
}
