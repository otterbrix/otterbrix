// The session's current place (SET search_path = '<one place>'): short names are completed with it, a bare name
// looks in main.pg_catalog first and then in the place's schema; current_database() / current_schema() name it.

#include "session_sql_fixture.hpp"

#include <components/compute/function.hpp>

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

using session_sql::fixture_t;
using code = core::error_code_t;

namespace {

    std::vector<std::string> first_row(fixture_t& db, const otterbrix::session_id_t& session, const std::string& sql) {
        auto cursor = db.ok(session, sql);
        REQUIRE(cursor->size() == 1);
        const auto& chunk = cursor->chunks().front();
        std::vector<std::string> row;
        for (std::uint64_t column = 0; column < chunk.column_count(); ++column) {
            row.emplace_back(chunk.is_null(column, 0) ? std::string{"<null>"}
                                                      : std::string{chunk.get_value<std::string_view>(column, 0)});
        }
        return row;
    }

    std::string current_schema(fixture_t& db, const otterbrix::session_id_t& session) {
        return first_row(db, session, "SELECT current_schema();").front();
    }

    // main.public.t holds 1; schema s is empty.
    void seed(fixture_t& db, const otterbrix::session_id_t& session) {
        db.ok(session, "CREATE TABLE t (a BIGINT);");
        db.ok(session, "INSERT INTO t (a) VALUES (1);");
        db.ok(session, "CREATE SCHEMA s;");
    }

    // s.t holds 2.
    void seed_s_t(fixture_t& db) {
        const auto session = otterbrix::session_id_t();
        db.ok(session, "SET search_path = 's';");
        db.ok(session, "CREATE TABLE t (a BIGINT);");
        db.ok(session, "INSERT INTO t (a) VALUES (2);");
    }

    core::error_t plus_one(components::compute::kernel_context&,
                           const components::vector::data_chunk_t& in,
                           components::vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] + 1;
        }
        return core::error_t::no_error();
    }

    components::compute::function_ptr make_plus_one(std::pmr::memory_resource* resource, const std::string& name) {
        using namespace components::compute;
        function_doc doc{resource, "short_doc", "full_doc", {"arg"}, false};
        auto fn = core::pmr::make_polymorphic_unique<vector_function>(resource,
                                                                      name,
                                                                      arity::unary(),
                                                                      doc,
                                                                      size_t{1},
                                                                      volatility_t::volatile_);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::BIGINT)});
        vector_kernel kernel{std::move(sig), plus_one};
        REQUIRE_FALSE(fn->add_kernel(resource, std::move(kernel)).contains_error());
        return fn;
    }

} // namespace

TEST_CASE("integration::cpp::current_place::a_fresh_session_stands_in_main_public") {
    fixture_t db("test_current_place/r3_10");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE TABLE t (a BIGINT);");
    db.ok(session, "INSERT INTO t (a) VALUES (1);");
    CHECK(db.ints(session, "SELECT a FROM main.public.t;") == std::vector<std::int64_t>{1});
    CHECK(first_row(db, session, "SELECT current_database(), current_schema();") ==
          std::vector<std::string>{"main", "public"});
    CHECK(first_row(db, session, "SELECT CURRENT_CATALOG, CURRENT_SCHEMA;") ==
          std::vector<std::string>{"main", "public"});
}

TEST_CASE("integration::cpp::current_place::a_bare_name_never_reaches_another_schema") {
    fixture_t db("test_current_place/r3_11");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.only_here (a BIGINT);");
    db.check_refused(session,
                     "SELECT a FROM only_here;",
                     code::table_not_exists,
                     "relation \"only_here\" does not exist");
}

TEST_CASE("integration::cpp::current_place::a_set_place_is_the_sessions_own") {
    fixture_t db("test_current_place/r3_12");
    const auto a = otterbrix::session_id_t();
    const auto b = otterbrix::session_id_t();
    seed(db, a);
    db.ok(a, "SET search_path = 's';");
    db.ok(a, "CREATE TABLE t (a BIGINT);");
    db.ok(a, "INSERT INTO t (a) VALUES (2);");
    CHECK(db.ints(a, "SELECT a FROM t;") == std::vector<std::int64_t>{2});
    CHECK(current_schema(db, a) == "s");
    CHECK(db.ints(b, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::current_place::a_place_in_another_local_database") {
    fixture_t db("test_current_place/r3_13");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    seed_s_t(db);
    db.ok(session, "CREATE DATABASE d2;");
    db.ok(session, "SET search_path = 'd2.public';");
    db.ok(session, "CREATE TABLE t2 (a BIGINT);");
    CHECK(db.ints(session, "SELECT a FROM t2;").empty());
    CHECK(db.ints(session, "SELECT a FROM main.public.t;") == std::vector<std::int64_t>{1});
    CHECK(db.ints(session, "SELECT a FROM d2.public.t2;").empty());
    db.check_refused(session, "SELECT a FROM s.t;", code::table_not_exists, "relation \"s.t\" does not exist");
    CHECK(first_row(db, session, "SELECT current_database();") == std::vector<std::string>{"d2"});
}

TEST_CASE("integration::cpp::current_place::a_missing_place_is_refused_and_the_place_stays") {
    fixture_t db("test_current_place/r3_14");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    db.check_refused(session,
                     "SET search_path = 'nosuch';",
                     code::schema_not_exists,
                     "schema \"nosuch\" does not exist");
    db.check_refused(session,
                     "SET search_path = 'nodb.public';",
                     code::database_not_exists,
                     "database \"nodb\" does not exist");
    CHECK(db.ints(session, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
    CHECK(current_schema(db, session) == "public");
}

TEST_CASE("integration::cpp::current_place::a_list_or_too_many_parts_is_refused") {
    fixture_t db("test_current_place/r3_15");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    for (const std::string sql : {"SET search_path = public, s;", "SET search_path = 'a.b.c.d';"}) {
        db.check_refused(session,
                         sql,
                         code::invalid_parameter,
                         "search_path takes one place: [[connection.]database.]schema");
    }
    CHECK(db.ints(session, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::current_place::rollback_returns_the_place") {
    fixture_t db("test_current_place/r3_16");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    seed_s_t(db);
    db.ok(session, "BEGIN;");
    db.ok(session, "SET search_path = 's';");
    db.ok(session, "ROLLBACK;");
    CHECK(db.ints(session, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::current_place::set_local_lasts_until_the_transaction_ends") {
    fixture_t db("test_current_place/r3_17");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    seed_s_t(db);
    db.ok(session, "BEGIN;");
    db.ok(session, "SET LOCAL search_path = 's';");
    CHECK(db.ints(session, "SELECT a FROM t;") == std::vector<std::int64_t>{2});
    db.ok(session, "COMMIT;");
    CHECK(db.ints(session, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
}

TEST_CASE("integration::cpp::current_place::a_view_body_is_bound_where_it_was_created") {
    fixture_t db("test_current_place/r3_18");
    const auto a = otterbrix::session_id_t();
    const auto b = otterbrix::session_id_t();
    seed(db, a);
    seed_s_t(db);
    db.ok(a, "SET search_path = 's';");
    db.ok(a, "CREATE VIEW v AS SELECT a FROM t;");
    CHECK(db.ints(b, "SELECT a FROM s.v;") == std::vector<std::int64_t>{2});
}

TEST_CASE("integration::cpp::current_place::the_place_is_kept_by_name") {
    fixture_t db("test_current_place/r3_19");
    const auto a = otterbrix::session_id_t();
    const auto b = otterbrix::session_id_t();
    seed(db, a);
    seed_s_t(db);
    db.ok(a, "SET search_path = 's';");
    db.ok(b, "DROP SCHEMA s CASCADE;");
    db.check_refused(a, "SELECT a FROM t;", code::table_not_exists, "relation \"t\" does not exist");
    db.check_refused(a, "CREATE TABLE x (a BIGINT);", code::schema_not_exists, "schema \"s\" does not exist");
    CHECK(current_schema(db, a) == "<null>");
    db.ok(b, "CREATE SCHEMA s;");
    db.ok(b, "CREATE TABLE s.t (a BIGINT);");
    db.ok(b, "INSERT INTO s.t (a) VALUES (7);");
    CHECK(db.ints(a, "SELECT a FROM t;") == std::vector<std::int64_t>{7});
}

TEST_CASE("integration::cpp::current_place::a_bare_name_finds_pg_catalog") {
    fixture_t db("test_current_place/r3_20");
    const auto session = otterbrix::session_id_t();
    CHECK(db.strings(session, "SELECT relname FROM pg_class WHERE relname = 'pg_class';") ==
          std::vector<std::string>{"pg_class"});
}

TEST_CASE("integration::cpp::current_place::pg_catalog_comes_before_the_place") {
    fixture_t db("test_current_place/r3_21");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE TABLE pg_class (a BIGINT);");
    // Read from the catalog: its own row and the new table's; main.public.pg_class is empty.
    CHECK(db.ints(session, "SELECT count(*) FROM pg_class WHERE relname = 'pg_class';") ==
          std::vector<std::int64_t>{2});
    CHECK(db.ints(session, "SELECT count(*) FROM main.public.pg_class;") == std::vector<std::int64_t>{0});
}

TEST_CASE("integration::cpp::current_place::pg_catalog_is_one_whichever_database_names_it") {
    fixture_t db("test_current_place/r3_23");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d5;");
    db.ok(session, "SET search_path = 'd5.public';");
    for (const std::string sql : {"SELECT relname FROM pg_catalog.pg_class WHERE relname = 'pg_class';",
                                  "SELECT relname FROM pg_class WHERE relname = 'pg_class';",
                                  "SELECT relname FROM d5.pg_catalog.pg_class WHERE relname = 'pg_class';"}) {
        INFO(sql);
        CHECK(db.strings(session, sql) == std::vector<std::string>{"pg_class"});
    }
}

TEST_CASE("integration::cpp::current_place::alter_system_sets_the_place_of_a_fresh_session") {
    fixture_t db("test_current_place/r3_24");
    CHECK(current_schema(db, otterbrix::session_id_t()) == "public");
    db.ok(otterbrix::session_id_t(), "CREATE SCHEMA s9;");
    db.ok(otterbrix::session_id_t(), "ALTER SYSTEM SET search_path = 's9';");
    CHECK(current_schema(db, otterbrix::session_id_t()) == "s9");
    db.restart();
    CHECK(current_schema(db, otterbrix::session_id_t()) == "s9");
    CHECK(first_row(db, otterbrix::session_id_t(), "SELECT current_database();") == std::vector<std::string>{"main"});
}

TEST_CASE("integration::cpp::current_place::rollback_to_returns_the_savepoints_place") {
    fixture_t db("test_current_place/r3_26");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    db.ok(session, "CREATE DATABASE d2;");
    db.ok(session, "BEGIN;");
    db.ok(session, "SET search_path = 's';");
    db.ok(session, "SAVEPOINT p;");
    db.ok(session, "SET search_path = 'd2.public';");
    db.ok(session, "ROLLBACK TO p;");
    CHECK(current_schema(db, session) == "s");
    db.ok(session, "COMMIT;");
    CHECK(current_schema(db, session) == "s");
}

TEST_CASE("integration::cpp::current_place::alter_system_keeps_the_text_unchecked") {
    fixture_t db("test_current_place/r3_27");
    const auto session = otterbrix::session_id_t();
    seed(db, session);
    db.ok(session, "CREATE DATABASE d5;");
    db.ok(session, "SET search_path = 'd5.public';");
    db.ok(session, "ALTER SYSTEM SET search_path = 'nosuch';");
    const auto fresh = otterbrix::session_id_t();
    CHECK(current_schema(db, fresh) == "<null>");
    db.check_refused(fresh, "SELECT a FROM t;", code::table_not_exists, "relation \"t\" does not exist");
}

// functions, sequences and types follow the rule of tables.

TEST_CASE("integration::cpp::current_place::a_function_lives_in_its_place_and_its_schema_names_it") {
    fixture_t db("test_current_place/q1_function");
    const auto in_s = otterbrix::session_id_t();
    const auto in_public = otterbrix::session_id_t();
    db.ok(in_s, "CREATE SCHEMA s;");
    db.ok(in_s, "SET search_path = 's';");
    REQUIRE_FALSE(
        db.dispatcher()->register_udf(in_s, make_plus_one(db.dispatcher()->resource(), "plus_one")).contains_error());
    CHECK(db.ints(in_s, "SELECT plus_one(CAST(1 AS BIGINT));") == std::vector<std::int64_t>{2});
    CHECK(db.ints(in_s, "SELECT s.plus_one(CAST(1 AS BIGINT));") == std::vector<std::int64_t>{2});
    CHECK(db.ints(in_public, "SELECT s.plus_one(CAST(2 AS BIGINT));") == std::vector<std::int64_t>{3});
    const auto bare = db.refused(in_public, "SELECT plus_one(CAST(1 AS BIGINT));");
    CHECK(bare.type == code::unrecognized_function);
    CHECK(db.strings(in_public,
                     "SELECT n.nspname FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n "
                     "ON p.pronamespace = n.oid WHERE p.proname = 'plus_one';") == std::vector<std::string>{"s"});
}

TEST_CASE("integration::cpp::current_place::sequences_and_sql_functions_are_created_in_the_place") {
    fixture_t db("test_current_place/q1_sequence");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "SET search_path = 's';");
    db.ok(session, "CREATE SEQUENCE q;");
    db.ok(session, "CREATE FUNCTION f(x INT) RETURNS INT AS 'x -> x';");
    for (const std::string name : {"q", "f"}) {
        INFO(name);
        CHECK(db.strings(session,
                         "SELECT n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                         "ON c.relnamespace = n.oid WHERE c.relname = '" +
                             name + "';") == std::vector<std::string>{"s"});
    }
    db.ok(session, "DROP SEQUENCE q;");
    db.ok(session, "DROP FUNCTION f(INT);");
    CHECK(db.ints(session, "SELECT count(*) FROM pg_catalog.pg_class WHERE relname IN ('q', 'f');") ==
          std::vector<std::int64_t>{0});
}

TEST_CASE("integration::cpp::current_place::a_type_lives_in_its_place_and_its_schema_names_it") {
    fixture_t db("test_current_place/q1_type");
    const auto in_s = otterbrix::session_id_t();
    const auto in_public = otterbrix::session_id_t();
    db.ok(in_s, "CREATE SCHEMA s;");
    db.ok(in_s, "SET search_path = 's';");
    db.ok(in_s, "CREATE TYPE pair_t AS (x BIGINT, y BIGINT);");
    db.ok(in_s, "CREATE TABLE pairs (p pair_t);");
    CHECK(db.strings(in_public,
                     "SELECT n.nspname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n "
                     "ON t.typnamespace = n.oid WHERE t.typname = 'pair_t';") == std::vector<std::string>{"s"});
    db.ok(in_public, "CREATE TABLE more_pairs (p s.pair_t);");
    const auto bare = db.refused(in_public, "CREATE TABLE no_pairs (p pair_t);");
    CHECK(bare.type == code::undefined_object);
    db.ok(in_public, "CREATE TYPE public.pair_t AS (z BIGINT);");
    CHECK(db.ints(in_public, "SELECT count(*) FROM pg_catalog.pg_type WHERE typname = 'pair_t';") ==
          std::vector<std::int64_t>{2});
}

// information_schema is per database.

TEST_CASE("integration::cpp::current_place::every_database_has_its_information_schema") {
    fixture_t db("test_current_place/q3_information_schema");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d6;");
    CHECK(db.strings(session,
                     "SELECT n.nspname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_database d "
                     "ON n.nspdatabase = d.oid WHERE d.datname = 'd6' ORDER BY n.nspname;") ==
          std::vector<std::string>{"information_schema", "public"});
    db.ok(session, "CREATE TABLE d6.information_schema.probe (a BIGINT);");
    db.ok(session, "INSERT INTO d6.information_schema.probe (a) VALUES (6);");
    db.check_refused(session,
                     "SELECT a FROM information_schema.probe;",
                     code::table_not_exists,
                     "relation \"information_schema.probe\" does not exist");
    db.ok(session, "SET search_path = 'd6.public';");
    CHECK(db.ints(session, "SELECT a FROM information_schema.probe;") == std::vector<std::int64_t>{6});
    for (const auto& in : {session, otterbrix::session_id_t()}) {
        const auto error = db.refused(in, "DROP SCHEMA information_schema CASCADE;");
        CHECK(std::string{error.what}.find("built-in") != std::string::npos);
    }
}

// REFRESH MATERIALIZED VIEW d.s.mv refreshes database d, schema s.

TEST_CASE("integration::cpp::current_place::refresh_names_database_schema_and_matview") {
    fixture_t db("test_current_place/q6_refresh");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d;");
    db.ok(session, "SET search_path = 'd.public';");
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE d.public.src (a BIGINT);");
    db.ok(session, "CREATE MATERIALIZED VIEW d.public.mv AS SELECT a FROM d.public.src WITH NO DATA;");
    db.ok(session, "CREATE MATERIALIZED VIEW d.s.mv AS SELECT a FROM d.public.src WITH NO DATA;");
    db.ok(session, "INSERT INTO d.public.src (a) VALUES (1), (2);");
    db.ok(session, "REFRESH MATERIALIZED VIEW d.s.mv;");
    CHECK(db.ints(session, "SELECT count(*) FROM d.s.mv;") == std::vector<std::int64_t>{2});
    CHECK(db.ints(session, "SELECT count(*) FROM d.public.mv;") == std::vector<std::int64_t>{0});
    db.check_refused(session,
                     "REFRESH MATERIALIZED VIEW d.s.nomv;",
                     code::table_not_exists,
                     "relation \"d.s.nomv\" does not exist");
}

// A bare call reaches the user functions of the place only, wherever it stands in the statement.
TEST_CASE("integration::cpp::current_place::a_bare_call_in_set_and_returning_reaches_the_place_only") {
    fixture_t db("test_current_place/call_scope");
    const auto in_s = otterbrix::session_id_t();
    const auto in_public = otterbrix::session_id_t();
    db.ok(in_s, "CREATE SCHEMA s;");
    db.ok(in_s, "SET search_path = 's';");
    REQUIRE_FALSE(
        db.dispatcher()->register_udf(in_s, make_plus_one(db.dispatcher()->resource(), "plus_one")).contains_error());
    db.ok(in_public, "CREATE TABLE t (a BIGINT);");
    db.ok(in_public, "INSERT INTO t (a) VALUES (1);");
    for (const std::string& sql : {"UPDATE t SET a = plus_one(a);",
                                   "UPDATE t SET a = 5 RETURNING plus_one(a);",
                                   "INSERT INTO t (a) VALUES (2) RETURNING plus_one(a);",
                                   "DELETE FROM t WHERE a = 7 RETURNING plus_one(a);"}) {
        INFO(sql);
        CHECK(db.refused(in_public, sql).type == code::unrecognized_function);
    }
    CHECK(db.ints(in_public, "SELECT a FROM t;") == std::vector<std::int64_t>{1});
    db.ok(in_s, "CREATE TABLE s.u (a BIGINT);");
    db.ok(in_s, "INSERT INTO u (a) VALUES (1);");
    CHECK(db.ints(in_s, "UPDATE u SET a = plus_one(a) RETURNING plus_one(a);") == std::vector<std::int64_t>{3});
    CHECK(db.ints(in_public, "UPDATE s.u SET a = s.plus_one(a) RETURNING a;") == std::vector<std::int64_t>{3});
}

// A refused DROP names the schema with the database of the session's place, not main's.
TEST_CASE("integration::cpp::current_place::a_refused_drop_names_the_schema_with_the_places_database") {
    fixture_t db("test_current_place/cascade_message");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d;");
    db.ok(session, "SET search_path = 'd.public';");
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a BIGINT);");
    const auto error = db.refused(session, "DROP SCHEMA s;");
    CHECK(error.type == code::dependent_objects_still_exist);
    CHECK(std::string{error.what}.starts_with("cannot drop schema d.s because other objects depend on it"));
}

// REFRESH takes the matview the name found, by its oid; a name of a schema without one is refused even when another
// schema of the database has a matview of that name.
TEST_CASE("integration::cpp::current_place::refresh_of_a_schema_without_the_matview") {
    fixture_t db("test_current_place/refresh_oid");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d;");
    db.ok(session, "SET search_path = 'd.public';");
    db.ok(session, "CREATE SCHEMA s2;");
    db.ok(session, "CREATE TABLE d.public.src (a BIGINT);");
    db.ok(session, "CREATE MATERIALIZED VIEW d.public.mv AS SELECT a FROM d.public.src WITH NO DATA;");
    db.ok(session, "INSERT INTO d.public.src (a) VALUES (1);");
    db.check_refused(session,
                     "REFRESH MATERIALIZED VIEW d.s2.mv;",
                     code::table_not_exists,
                     "relation \"d.s2.mv\" does not exist");
    CHECK(db.ints(session, "SELECT count(*) FROM d.public.mv;") == std::vector<std::int64_t>{0});
    db.ok(session, "REFRESH MATERIALIZED VIEW mv;");
    CHECK(db.ints(session, "SELECT count(*) FROM d.public.mv;") == std::vector<std::int64_t>{1});
}

// DROP of an object of a database that is not there answers the same for every kind; IF EXISTS does nothing.
TEST_CASE("integration::cpp::current_place::drop_in_a_missing_database_answers_alike") {
    fixture_t db("test_current_place/drop_missing_database");
    const auto session = otterbrix::session_id_t();
    for (const auto& [kind, name] :
         std::initializer_list<std::pair<std::string, std::string>>{{"TABLE", "nodb.s.x"},
                                                                    {"VIEW", "nodb.s.x"},
                                                                    {"MATERIALIZED VIEW", "nodb.s.x"},
                                                                    {"SEQUENCE", "nodb.s.x"},
                                                                    {"INDEX", "nodb.s.t.x"},
                                                                    {"FUNCTION", "nodb.s.x(INT)"}}) {
        INFO(kind);
        db.check_refused(session,
                         "DROP " + kind + " " + name + ";",
                         code::database_not_exists,
                         "database \"nodb\" does not exist");
        db.ok(session, "DROP " + kind + " IF EXISTS " + name + ";");
    }
}

// A write, an ALTER and an ADD CONSTRAINT name the missing level of their target, as a CREATE does.
TEST_CASE("integration::cpp::current_place::a_write_into_a_missing_database_or_schema") {
    fixture_t db("test_current_place/write_missing");
    const auto session = otterbrix::session_id_t();
    for (const std::string& sql : {"INSERT INTO nodb.s.t (a) VALUES (1);",
                                   "UPDATE nodb.s.t SET a = 1;",
                                   "DELETE FROM nodb.s.t;",
                                   "ALTER TABLE nodb.s.t ADD COLUMN b BIGINT;",
                                   "ALTER TABLE nodb.s.t ADD CONSTRAINT c CHECK (a > 0);"}) {
        db.check_refused(session, sql, code::database_not_exists, "database \"nodb\" does not exist");
    }
    for (const std::string& sql : {"INSERT INTO nosuch.t (a) VALUES (1);",
                                   "UPDATE nosuch.t SET a = 1;",
                                   "DELETE FROM nosuch.t;",
                                   "ALTER TABLE nosuch.t ADD COLUMN b BIGINT;",
                                   "ALTER TABLE nosuch.t ADD CONSTRAINT c CHECK (a > 0);"}) {
        db.check_refused(session, sql, code::schema_not_exists, "schema \"nosuch\" does not exist");
    }
}

// A function of a database's schema, named with three parts, like a table.
TEST_CASE("integration::cpp::current_place::a_function_named_with_three_parts") {
    fixture_t db("test_current_place/function_three_parts");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE DATABASE d;");
    db.ok(session, "SET search_path = 'd.public';");
    db.ok(session, "CREATE SCHEMA s;");
    const auto in_main = otterbrix::session_id_t();
    db.ok(in_main, "CREATE FUNCTION d.s.f(x INT) RETURNS INT AS 'x -> x + 1';");
    CHECK(db.strings(in_main,
                     "SELECT n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                     "ON c.relnamespace = n.oid WHERE c.relname = 'f';") == std::vector<std::string>{"s"});
    db.ok(in_main, "DROP FUNCTION d.s.f(INT);");
    CHECK(db.ints(in_main, "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'f';") ==
          std::vector<std::int64_t>{0});
}

// A bare call in a CHECK is the table's schema's, whatever the place of the session that writes; a qualified one keeps
// its schema.
TEST_CASE("integration::cpp::current_place::a_check_calls_the_functions_of_its_tables_schema") {
    fixture_t db("test_current_place/check_scope");
    const auto in_s = otterbrix::session_id_t();
    const auto in_public = otterbrix::session_id_t();
    db.ok(in_s, "CREATE SCHEMA s;");
    db.ok(in_s, "SET search_path = 's';");
    REQUIRE_FALSE(
        db.dispatcher()->register_udf(in_s, make_plus_one(db.dispatcher()->resource(), "plus_one")).contains_error());
    db.ok(in_s, "CREATE TABLE t (a BIGINT CHECK (plus_one(a) > 1));");
    db.ok(in_s, "CREATE TABLE u (a BIGINT CHECK (s.plus_one(a) > 1));");
    db.ok(in_public, "CREATE TABLE s.v (a BIGINT CHECK (plus_one(a) > 1));");
    db.ok(in_public, "CREATE TABLE w (a BIGINT);");
    db.ok(in_public, "ALTER TABLE w ADD CONSTRAINT c CHECK (s.plus_one(a) > 1);");
    for (const std::string& table : {"s.t", "s.u", "s.v", "w"}) {
        INFO(table);
        db.ok(in_public, "INSERT INTO " + table + " (a) VALUES (5);");
        const auto refused = db.refused(in_public, "INSERT INTO " + table + " (a) VALUES (0);");
        CHECK(std::string{refused.what}.find("violated") != std::string::npos);
    }
    // A table of public: its CHECK calls public's functions even from the place s, where plus_one is.
    db.ok(in_s, "CREATE TABLE public.x (a BIGINT CHECK (plus_one(a) > 1));");
    CHECK(db.refused(in_s, "INSERT INTO public.x (a) VALUES (5);").type == code::unrecognized_function);
}

// A column depends on its user type, as in PostgreSQL 18: RESTRICT refuses the type's DROP, CASCADE drops the column
// and the views that read it. The dependency comes with CREATE TABLE and ADD COLUMN and goes with the column and its
// table.
TEST_CASE("integration::cpp::current_place::a_column_depends_on_its_type") {
    fixture_t db("test_current_place/column_type_dependency");
    const auto session = otterbrix::session_id_t();
    db.ok(session, "CREATE TYPE mood AS ENUM ('x', 'y');");
    db.ok(session, "CREATE TABLE t (id BIGINT, m mood);");
    db.ok(session, "INSERT INTO t (id) VALUES (1);");
    db.ok(session, "CREATE VIEW v AS SELECT id, m FROM t;");
    db.ok(session, "CREATE TABLE u (id BIGINT);");
    db.ok(session, "ALTER TABLE u ADD COLUMN m mood;");
    for (const std::string& sql : {"DROP TYPE mood;", "DROP TYPE mood RESTRICT;"}) {
        const auto refused = db.refused(session, sql);
        INFO(sql);
        CHECK(refused.type == code::dependent_objects_still_exist);
        CHECK(std::string{refused.what}.starts_with("cannot drop type mood because other objects depend on it"));
    }
    db.ok(session, "DROP TYPE mood CASCADE;");
    CHECK(db.strings(session,
                     "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON a.attrelid = c.oid "
                     "WHERE c.relname IN ('t', 'u') AND a.attisdropped = false ORDER BY a.attname;") ==
          std::vector<std::string>{"id", "id"});
    CHECK(db.ints(session, "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'v';") ==
          std::vector<std::int64_t>{0});
    CHECK(db.ints(session, "SELECT id FROM t;") == std::vector<std::int64_t>{1});
    CHECK_FALSE(std::string{db.refused(session, "SELECT m FROM t;").what}.empty());

    db.ok(session, "CREATE TYPE q AS ENUM ('a');");
    db.ok(session, "CREATE TABLE w (id BIGINT, x q);");
    db.ok(session, "DROP TABLE w;");
    db.ok(session, "CREATE TABLE z (id BIGINT, x q);");
    db.ok(session, "ALTER TABLE z DROP COLUMN x;");
    db.ok(session, "DROP TYPE q;");
}
