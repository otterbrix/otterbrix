#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_drop.hpp>
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
    auto resource = core::pmr::otterbrix_resource();
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
    auto resource = core::pmr::otterbrix_resource();
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
        // DROP SEQUENCE registers a namespace + table lookup; the drop node itself
        // is the plan root and carries the names.
        REQUIRE(node->type() == node_type::drop_t);
        REQUIRE(result.catalog_resolves.namespaces->entries().size() == 1);
        REQUIRE(result.catalog_resolves.tables->entries().size() == 1);
    }
}

TEST_CASE("components::sql::view") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    SECTION("CREATE VIEW") {
        // The statement text is passed in, as every production entry point does.
        // It used to be omitted here, and the transformer answered by INVENTING a
        // body ("SELECT *") — which is the whole of defect D1, since the stored body
        // is re-parsed on every read of the view.
        const char* sql = "CREATE VIEW db.my_view AS SELECT * FROM db.tbl";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        REQUIRE(node->type() == node_type::create_view_t);
        REQUIRE(result.catalog_resolves.namespaces->entries().size() == 1);
    }

    SECTION("CREATE VIEW without the statement text is refused, not guessed") {
        transform::transformer transformer(&resource);
        auto stmt = raw_parser(&arena_resource, "CREATE VIEW db.my_view AS SELECT * FROM db.tbl")->lst.front().data;
        auto wrapped = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
        REQUIRE(wrapped.has_error());
    }

    SECTION("CREATE VIEW body is the text that was written, whatever the spacing") {
        // A newline after AS, and `AS(SELECT ...)` with no space: both defeated the
        // old " AS " substring search, which then fell back to "SELECT *".
        for (const char* sql : {"CREATE VIEW db.my_view AS\nSELECT id FROM db.tbl WHERE id > 10",
                                "CREATE VIEW db.my_view AS(SELECT id FROM db.tbl WHERE id > 10)"}) {
            transform::transformer transformer(&resource, sql);
            auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
            auto result = ([](auto _w) {
                REQUIRE_FALSE(_w.has_error());
                return _w.value();
            }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
            auto view_node = boost::static_pointer_cast<node_create_view_t>(result.sub_queries.back());
            INFO(sql);
            REQUIRE(view_node->query_sql().find("SELECT id FROM db.tbl WHERE id > 10") != std::string::npos);
        }
    }

    SECTION("CREATE VIEW keeps WITH CHECK OPTION out of the stored body") {
        const char* sql = "CREATE VIEW db.my_view AS SELECT id FROM db.tbl WITH CHECK OPTION";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto view_node = boost::static_pointer_cast<node_create_view_t>(result.sub_queries.back());
        REQUIRE(view_node->query_sql() == "SELECT id FROM db.tbl");
    }

    SECTION("CREATE VIEW with a column alias list is refused") {
        // The aliases rename the body's output columns and are carried nowhere, so
        // the view would promise names its stored body does not produce.
        const char* sql = "CREATE VIEW db.my_view (x) AS SELECT id FROM db.tbl";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        REQUIRE(transformer.transform(pg_cell_to_node_cast(stmt)).finalize().has_error());
    }

    SECTION("CREATE VIEW with raw_sql extracts query") {
        const char* sql = "CREATE VIEW db.my_view AS SELECT id, name FROM db.tbl WHERE id > 10";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto view_node = boost::static_pointer_cast<node_create_view_t>(result.sub_queries.back());
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
        // DROP VIEW registers a namespace + table lookup; the drop node is the root.
        REQUIRE(node->type() == node_type::drop_t);
        REQUIRE(result.catalog_resolves.namespaces->entries().size() == 1);
        REQUIRE(result.catalog_resolves.tables->entries().size() == 1);
    }
}

TEST_CASE("components::sql::check_constraint_whitelist") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // Table-level CHECK constraints go through extract_table_constraints → deparse_check_expr.
    // Column-level CHECK (inside T_ColumnDef) reaches the same deparser through
    // extract_column_constraints; the two extractors feed one list on the create node.

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

    SECTION("NOT of a comparison is allowed") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(NOT (x > 0)))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE(data->constraints()[0].check_expression == "NOT (x > 0)");
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

    // Shapes that DEPARSE cleanly and then cannot be evaluated. Each one used to be
    // accepted here and compiled to the constant TRUE at DML time, so the constraint
    // sat in the catalog judging nothing. The whitelist is what the DML-time
    // recogniser can read, not what the deparser can spell.
    SECTION("arithmetic in an operand is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (a INTEGER, b INTEGER, CHECK(a + b > 0))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    SECTION("arithmetic in the constant is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (a INTEGER, CHECK(a > 1 + 1))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    SECTION("comparing two columns is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (lo INTEGER, hi INTEGER, CHECK(lo <= hi))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    SECTION("a bare column reference is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (flag INTEGER, CHECK(flag))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    // BETWEEN is not a node kind of its own here: the grammar desugars it into an AND
    // of two comparisons (gram.y, a_expr BETWEEN), which IS an evaluable shape. Pinned
    // so the whitelist is not tightened past what the engine can actually enforce.
    SECTION("BETWEEN desugars into two comparisons and is allowed") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x BETWEEN 1 AND 10))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE(data->constraints()[0].check_expression == "(x >= 1) AND (x <= 10)");
    }

    SECTION("IN is rejected") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (x INTEGER, CHECK(x IN (1, 2)))"));
        auto result = transformer.transform(pg_cell_to_node_cast(stmt));
        REQUIRE(result.has_error());
    }

    // A string constant that CONTAINS a comparison operator is still a plain
    // column-against-constant comparison and must survive.
    SECTION("an operator inside a string literal is still a comparison") {
        auto stmt = linitial(raw_parser(&arena_resource, "CREATE TABLE t (s TEXT, CHECK(s = 'a > b'))"));
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = ddl_consumer(result.sub_queries.back());
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        REQUIRE(data->constraints().size() == 1);
        REQUIRE(data->constraints()[0].check_expression == "s = 'a > b'");
    }
}

TEST_CASE("components::sql::macro") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("DROP FUNCTION (macro)") {
        auto stmt = raw_parser(&arena_resource, "DROP FUNCTION db.my_macro()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // DROP FUNCTION registers a namespace + table lookup; the drop node is the root.
        REQUIRE(node->type() == node_type::drop_t);
        REQUIRE(result.catalog_resolves.namespaces->entries().size() == 1);
        REQUIRE(result.catalog_resolves.tables->entries().size() == 1);
    }

    SECTION("DROP FUNCTION simple name") {
        auto stmt = raw_parser(&arena_resource, "DROP FUNCTION my_macro()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(stmt)).finalize()));
        auto node = result.sub_queries.back();
        // No db prefix → the table lookup carries an empty dbname, and no namespace
        // lookup is registered at all.
        REQUIRE(node->type() == node_type::drop_t);
        REQUIRE(result.catalog_resolves.namespaces == nullptr);
        REQUIRE(result.catalog_resolves.tables->entries().size() == 1);
    }
}

#include <components/logical_plan/node_create_database.hpp>

// PostgreSQL CREATE DATABASE / CREATE TABLE IF NOT EXISTS — parser & transformer
// propagate the if_not_exists flag through to the logical plan nodes. Dispatcher
// short-circuits on existing target without erroring (covered in integration tests).
TEST_CASE("components::sql::if_not_exists") {
    auto resource = core::pmr::otterbrix_resource();
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

// A sequence bound is an int64, but the parse tree does not always keep it in the
// integer slot. `NumericOnly` builds a T_Float for FCONST, and scan.l's
// process_integer_literal sends EVERY literal outside int32 out as FCONST carrying
// the original digits — so `MAXVALUE 9223372036854775807` arrives as a T_Float too.
// transform_create_sequence called intVal() without looking at the tag, which reads
// the `char*` half of the Value union AS A NUMBER: the bound persisted into the
// catalog was the bit pattern of a pointer, different on every run.
TEST_CASE("components::sql::sequence_bounds_are_read_by_node_tag") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    auto plan_of = [&](const char* query) {
        auto stmt = raw_parser(&arena_resource, query)->lst.front().data;
        return transformer.transform(pg_cell_to_node_cast(stmt)).finalize();
    };
    auto sequence_of = [&](const char* query) {
        auto result = plan_of(query);
        REQUIRE_FALSE(result.has_error());
        auto node = ddl_consumer(result.value().sub_queries.back());
        REQUIRE(node->type() == node_type::create_sequence_t);
        return reinterpret_cast<node_create_sequence_ptr&>(node);
    };

    SECTION("a bound outside int32 is the value that was written, not a pointer") {
        // BEFORE: start() was the bit pattern of the char* holding "5000000000".
        auto seq = sequence_of("CREATE SEQUENCE db.big_seq START WITH 5000000000");
        CHECK(seq->start() == 5000000000LL);
    }

    SECTION("the int64 ceiling round-trips exactly") {
        auto seq = sequence_of("CREATE SEQUENCE db.max_seq MAXVALUE 9223372036854775807");
        CHECK(seq->max_value() == std::numeric_limits<int64_t>::max());
    }

    SECTION("a negative bound outside int32 keeps its sign") {
        auto seq = sequence_of("CREATE SEQUENCE db.neg_seq MINVALUE -5000000000 START WITH -4000000000");
        CHECK(seq->min_value() == -5000000000LL);
        CHECK(seq->start() == -4000000000LL);
    }

    SECTION("a fractional bound is refused, not rounded or read as a pointer") {
        // PostgreSQL runs the FCONST text through int8in and rejects "1.5"; so do we.
        auto result = plan_of("CREATE SEQUENCE db.frac_seq START WITH 1.5");
        REQUIRE(result.has_error());
        CHECK(std::string{result.error().what}.find("1.5") != std::string::npos);
        CHECK(std::string{result.error().what}.find("start") != std::string::npos);
    }

    SECTION("every bound option is checked, not just START") {
        REQUIRE(plan_of("CREATE SEQUENCE db.s1 INCREMENT BY 2.5").has_error());
        REQUIRE(plan_of("CREATE SEQUENCE db.s2 MINVALUE 0.5").has_error());
        REQUIRE(plan_of("CREATE SEQUENCE db.s3 MAXVALUE 1e6").has_error());
    }

    SECTION("a bound wider than int64 is refused rather than truncated") {
        REQUIRE(plan_of("CREATE SEQUENCE db.huge_seq MAXVALUE 99999999999999999999").has_error());
    }

    SECTION("plain int32 bounds are unchanged") {
        auto seq = sequence_of("CREATE SEQUENCE db.small_seq START 10 INCREMENT 2 MINVALUE 5 MAXVALUE 100");
        CHECK(seq->start() == 10);
        CHECK(seq->increment() == 2);
        CHECK(seq->min_value() == 5);
        CHECK(seq->max_value() == 100);
    }
}
