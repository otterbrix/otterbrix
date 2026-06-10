#include <catch2/catch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;
using namespace components::sql::transform;
using namespace components::table;

namespace {
    // Transformer now wraps CREATE TABLE in sequence_t(resolve_*..., create_collection);
    // descend to the create_collection consumer for inspection.
    components::logical_plan::node_ptr ddl_consumer(components::logical_plan::node_ptr n) {
        if (n && n->type() == components::logical_plan::node_type::sequence_t) {
            return n->children().back();
        }
        return n;
    }
} // namespace

TEST_CASE("components::sql::constraints::not_null_and_default") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CREATE TABLE with NOT NULL") {
        auto stmt =
            raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER NOT NULL, name TEXT)")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
        REQUIRE(col_defs[1].name() == "name");
        REQUIRE(col_defs[1].is_not_null() == false);
    }

    SECTION("CREATE TABLE with DEFAULT") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER, name TEXT DEFAULT 'unknown')")
                        ->lst.front()
                        .data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].has_default_value() == false);
        REQUIRE(col_defs[1].name() == "name");
        REQUIRE(col_defs[1].has_default_value() == true);
        REQUIRE(col_defs[1].default_value().value<std::string_view>() == "unknown");
    }

    SECTION("CREATE TABLE with NOT NULL and DEFAULT combined") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER NOT NULL, score DOUBLE DEFAULT 0)")
                        ->lst.front()
                        .data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
        REQUIRE(col_defs[0].has_default_value() == false);
        REQUIRE(col_defs[1].name() == "score");
        REQUIRE(col_defs[1].has_default_value() == true);
    }

    SECTION("CREATE TABLE with PRIMARY KEY column-level") {
        auto stmt =
            raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER PRIMARY KEY, name TEXT)")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        // PRIMARY KEY implies NOT NULL
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
    }

    SECTION("CREATE TABLE with table-level PRIMARY KEY") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER, name TEXT, PRIMARY KEY (id))")
                        ->lst.front()
                        .data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& constraints = data->constraints();
        REQUIRE(constraints.size() == 1);
        REQUIRE(constraints[0].type == table_constraint_type::PRIMARY_KEY);
        REQUIRE(constraints[0].columns.size() == 1);
        REQUIRE(constraints[0].columns[0] == "id");
    }

    SECTION("CREATE TABLE with table-level UNIQUE") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER, email TEXT, UNIQUE (email))")
                        ->lst.front()
                        .data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& constraints = data->constraints();
        REQUIRE(constraints.size() == 1);
        REQUIRE(constraints[0].type == table_constraint_type::UNIQUE);
        REQUIRE(constraints[0].columns.size() == 1);
        REQUIRE(constraints[0].columns[0] == "email");
    }
}

TEST_CASE("components::sql::sequence") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CREATE SEQUENCE basic") {
        auto stmt = raw_parser(&arena_resource, "CREATE SEQUENCE db.my_seq")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        REQUIRE(node->type() == node_type::create_sequence_t);
        // CREATE SEQUENCE no longer carries db name in its to_string (namespace resolution is sibling-OID).
        REQUIRE(node->to_string() == "$create_sequence: my_seq");
    }

    SECTION("CREATE SEQUENCE with options") {
        auto stmt = raw_parser(&arena_resource, "CREATE SEQUENCE db.my_seq START 10 INCREMENT 2")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        REQUIRE(node->type() == node_type::create_sequence_t);
        auto seq = reinterpret_cast<node_create_sequence_ptr&>(node);
        REQUIRE(seq->start() == 10);
        REQUIRE(seq->increment() == 2);
    }

    SECTION("DROP SEQUENCE") {
        auto stmt = raw_parser(&arena_resource, "DROP SEQUENCE db.my_seq")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // DROP SEQUENCE is wrapped in sequence_t(resolve_ns, resolve_table, drop_sequence).
        REQUIRE(node->type() == node_type::sequence_t);
        REQUIRE(node->to_string() == "$sequence[3]");
    }
}

TEST_CASE("components::sql::view") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    SECTION("CREATE VIEW") {
        transform::transformer transformer(&resource);
        auto stmt = raw_parser(&arena_resource, "CREATE VIEW db.my_view AS SELECT * FROM db.tbl")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        REQUIRE(node->type() == node_type::sequence_t);
        REQUIRE(node->to_string() == "$sequence[2]");
    }

    SECTION("CREATE VIEW with raw_sql extracts query") {
        const char* sql = "CREATE VIEW db.my_view AS SELECT id, name FROM db.tbl WHERE id > 10";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::sequence_t);
        auto view_node = boost::static_pointer_cast<node_create_view_t>(result.sub_queries.back()->children().back());
        REQUIRE(view_node->type() == node_type::create_view_t);
        REQUIRE(view_node->query_sql() == "SELECT id, name FROM db.tbl WHERE id > 10");
    }

    SECTION("DROP VIEW") {
        transform::transformer transformer(&resource);
        auto stmt = raw_parser(&arena_resource, "DROP VIEW db.my_view")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // DROP VIEW is wrapped in sequence_t(resolve_ns, resolve_table, drop_view).
        REQUIRE(node->type() == node_type::sequence_t);
        REQUIRE(node->to_string() == "$sequence[3]");
    }
}

TEST_CASE("components::sql::check_constraint_whitelist") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // Table-level CHECK constraints go through extract_table_constraints → deparse_check_expr.
    // Column-level CHECK (inside T_ColumnDef) is a separate path not handled yet.

    SECTION("simple comparison is allowed") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x > 0))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE(data->constraints()[0].check_expression == "x > 0");
    }

    SECTION("IS NOT NULL is allowed") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x IS NOT NULL))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE_FALSE(data->constraints()[0].check_expression.empty());
    }

    SECTION("AND of comparisons is allowed") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x > 0 AND x < 100))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE_FALSE(data->constraints()[0].check_expression.empty());
    }

    // Forbidden node kinds must throw parser_exception_t.
    SECTION("function call in CHECK is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(abs(x) > 0))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    SECTION("subquery in CHECK is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x > (SELECT 1)))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }
}

TEST_CASE("components::sql::macro") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("DROP FUNCTION (macro)") {
        auto stmt = raw_parser(&arena_resource, "DROP FUNCTION db.my_macro()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // DROP FUNCTION is wrapped in sequence_t(resolve_ns, resolve_table, drop_macro).
        REQUIRE(node->type() == node_type::sequence_t);
        REQUIRE(node->to_string() == "$sequence[3]");
    }

    SECTION("DROP FUNCTION simple name") {
        auto stmt = raw_parser(&arena_resource, "DROP FUNCTION my_macro()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // No db prefix → only resolve_table sibling (no resolve_namespace), so 2 children.
        REQUIRE(node->type() == node_type::sequence_t);
    }
}

#include <components/logical_plan/node_create_database.hpp>

// PostgreSQL CREATE DATABASE / CREATE TABLE IF NOT EXISTS — parser & transformer
// propagate the if_not_exists flag through to the logical plan nodes. Dispatcher
// short-circuits on existing target without erroring (covered in integration tests).
TEST_CASE("components::sql::if_not_exists") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CREATE DATABASE without IF NOT EXISTS") {
        // Transformer wraps create_database in sequence_t(resolve_namespace, create_database).
        // dbname lives in the resolve_namespace sibling; flag is on the create_database node.
        auto stmt = raw_parser(&arena_resource, "CREATE DATABASE mydb")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto& d = reinterpret_cast<node_create_database_ptr&>(node);
        REQUIRE_FALSE(d->if_not_exists());
    }

    SECTION("CREATE DATABASE IF NOT EXISTS sets flag") {
        auto stmt = raw_parser(&arena_resource, "CREATE DATABASE IF NOT EXISTS mydb")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto& d = reinterpret_cast<node_create_database_ptr&>(node);
        REQUIRE(d->if_not_exists());
    }

    SECTION("CREATE TABLE IF NOT EXISTS sets flag on collection node") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE IF NOT EXISTS db.tbl (id INTEGER)")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto& cc = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(cc->relname() == "tbl");
        REQUIRE(cc->if_not_exists());
    }

    SECTION("CREATE TABLE without IF NOT EXISTS leaves flag false") {
        auto stmt = raw_parser(&arena_resource, "CREATE TABLE db.tbl (id INTEGER)")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto& cc = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE_FALSE(cc->if_not_exists());
    }
}
