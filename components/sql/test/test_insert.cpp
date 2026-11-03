#include "parameter_equals.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <core/counting_resource.hpp>

using namespace components::sql;
using namespace components::sql::transform;

namespace {
    // Transformer now wraps DML in sequence_t(resolve_*..., consumer); descend
    // to the consumer when the test wants to inspect insert_t-specific shape.
    components::logical_plan::node_ptr dml_consumer(components::logical_plan::node_ptr n) {
        if (n && n->type() == components::logical_plan::node_type::sequence_t) {
            return n->children().back();
        }
        return n;
    }
} // namespace

TEST_CASE("components::sql::insert_into") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("insert into with TestDatabase") {
        auto select =
            linitial(parse_ok(&arena_resource,
                              "INSERT INTO TestDatabase.TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, components::vector::make_scalar(&resource, int64_t{1})));
        REQUIRE(cell_equals(chunk.data[1], 0, components::vector::make_scalar(&resource, std::string_view{"Name"})));
        REQUIRE(cell_equals(chunk.data[2], 0, components::vector::make_scalar(&resource, int64_t{1})));
    }

    SECTION("insert into without TestDatabase") {
        auto select =
            linitial(parse_ok(&arena_resource, "INSERT INTO TestCollection (id, name, count) VALUES (1, 'Name', 1);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, components::vector::make_scalar(&resource, int64_t{1})));
        REQUIRE(cell_equals(chunk.data[1], 0, components::vector::make_scalar(&resource, std::string_view{"Name"})));
        REQUIRE(cell_equals(chunk.data[2], 0, components::vector::make_scalar(&resource, int64_t{1})));
    }

    SECTION("insert into with quoted") {
        auto select = linitial(
            parse_ok(&arena_resource, R"(INSERT INTO TestCollection (id, "name", "count") VALUES (1, 'Name', 1);)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, components::vector::make_scalar(&resource, int64_t{1})));
        REQUIRE(cell_equals(chunk.data[1], 0, components::vector::make_scalar(&resource, std::string_view{"Name"})));
        REQUIRE(cell_equals(chunk.data[2], 0, components::vector::make_scalar(&resource, int64_t{1})));
    }

    SECTION("insert into struct") {
        auto select = linitial(parse_ok(
            &arena_resource,
            R"(INSERT INTO TestCollection (struct_type.field_1, struct_type.field_3) VALUES(43, 'some text');)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        REQUIRE(cell_equals(chunk.data[0], 0, components::vector::make_scalar(&resource, int64_t{43})));
        REQUIRE(
            cell_equals(chunk.data[1], 0, components::vector::make_scalar(&resource, std::string_view{"some text"})));
    }

    SECTION("insert into array") {
        auto select =
            linitial(parse_ok(&arena_resource, R"(INSERT INTO TestCollection (array_type) VALUES(ARRAY[1, 2, 3]);)"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 1);
        std::vector<components::vector::vector_t> elements;
        for (int64_t value : {1, 2, 3}) {
            elements.push_back(components::vector::make_scalar(&resource, value));
        }
        REQUIRE(cell_equals(chunk.data[0],
                            0,
                            components::vector::make_array(
                                &resource,
                                components::types::complex_logical_type{components::types::logical_type::BIGINT},
                                elements)));
    }

    SECTION("insert into multi-documents") {
        auto select = linitial(parse_ok(&arena_resource,
                                        "INSERT INTO TestCollection (id, name, count) VALUES "
                                        "(1, 'Name1', 1), "
                                        "(2, 'Name2', 2), "
                                        "(3, 'Name3', 3), "
                                        "(4, 'Name4', 4), "
                                        "(5, 'Name5', 5);"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        const auto& chunk =
            static_cast<components::logical_plan::node_data_t*>(node->children().front().get())->data_chunk();
        REQUIRE(chunk.size() == 5);
        REQUIRE(cell_equals(chunk.data[0], 0, components::vector::make_scalar(&resource, int64_t{1})));
        REQUIRE(cell_equals(chunk.data[1], 0, components::vector::make_scalar(&resource, std::string_view{"Name1"})));
        REQUIRE(cell_equals(chunk.data[2], 0, components::vector::make_scalar(&resource, int64_t{1})));
        REQUIRE(cell_equals(chunk.data[0], 4, components::vector::make_scalar(&resource, int64_t{5})));
        REQUIRE(cell_equals(chunk.data[1], 4, components::vector::make_scalar(&resource, std::string_view{"Name5"})));
        REQUIRE(cell_equals(chunk.data[2], 4, components::vector::make_scalar(&resource, int64_t{5})));
    }

    SECTION("insert from select") {
        auto select = linitial(parse_ok(&arena_resource, R"_(INSERT INTO table2 (column1, column2, column3)
SELECT column1, column2, column3
FROM table1
WHERE condition = true;)_"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(select)).finalize()));
        auto node = dml_consumer(result.sub_queries.back());
        REQUIRE(node->type() == components::logical_plan::node_type::insert_t);
        REQUIRE(static_cast<const std::string&>(
                    static_cast<components::logical_plan::node_aggregate_t*>(node->children().front().get())
                        ->target()
                        .database) == "");
        REQUIRE(static_cast<const std::string&>(
                    static_cast<components::logical_plan::node_aggregate_t*>(node->children().front().get())
                        ->target()
                        .collection) == "table1");
    }
}

TEST_CASE("components::sql::insert_into takes nothing from the default resource") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    std::string sql = "INSERT INTO t (a, b) VALUES (1, 'x')";
    for (int row = 2; row <= 50; ++row) {
        sql += ", (" + std::to_string(row) + ", 'y')";
    }
    sql += ";";
    auto select = linitial(parse_ok(&arena_resource, sql.c_str()));

    core::pmr::default_resource_counter_t default_resource;
    auto result = transformer.transform(pg_cell_to_node_cast(select)).finalize();
    const auto default_allocations = default_resource.allocations();

    REQUIRE_FALSE(result.has_error());
    REQUIRE(default_allocations == 0);
}

namespace {
    std::size_t transform_insert_allocations(const std::string& row, int rows) {
        auto host = core::pmr::otterbrix_resource();
        core::pmr::counting_resource_t counter(&host);
        {
            std::pmr::monotonic_buffer_resource arena_resource(&host);
            transform::transformer transformer(&counter);
            std::string sql = "INSERT INTO t VALUES " + row;
            for (int i = 1; i < rows; ++i) {
                sql += ", " + row;
            }
            sql += ";";
            auto select = linitial(parse_ok(&arena_resource, sql.c_str()));
            counter.reset();
            auto result = transformer.transform(pg_cell_to_node_cast(select)).finalize();
            REQUIRE_FALSE(result.has_error());
        }
        return counter.allocations();
    }
} // namespace

TEST_CASE("components::sql::insert_into allocates nothing per plain literal cell") {
    SECTION("integers") {
        const auto rows100 = transform_insert_allocations("(1, 2, 3, 4, 5, 6)", 100);
        const auto rows200 = transform_insert_allocations("(1, 2, 3, 4, 5, 6)", 200);
        REQUIRE(rows200 - rows100 < 100);
    }
    SECTION("strings and doubles") {
        const auto rows100 = transform_insert_allocations("(1, 'abc', 2.5, 'de', 3.25, 7)", 100);
        const auto rows200 = transform_insert_allocations("(1, 'abc', 2.5, 'de', 3.25, 7)", 200);
        REQUIRE(rows200 - rows100 < 100);
    }
}
