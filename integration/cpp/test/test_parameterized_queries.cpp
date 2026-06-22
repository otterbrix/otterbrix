#include "test_config.hpp"

#include <catch2/catch.hpp>

#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <components/types/logical_value.hpp>

using namespace components;
using namespace components::cursor;

// Mirrors integration/rust/otterbrix/tests/params.rs:
//   - parameterized queries ($N binding) for every supported value type,
//   - SQL-injection safety (payloads are stored/compared as data, never executed),
//   - error-code / message contracts (validation + DDL errors).
//
// Parameter binding goes through wrapper_dispatcher_t::execute_sql_with_params,
// whose signature is
//   cursor_t_ptr execute_sql_with_params(
//       const session_id_t&,
//       const std::string& query,
//       const std::vector<std::pair<size_t, logical_value_t>>& params);
// (see integration/cpp/wrapper_dispatcher.hpp:62 / .cpp:120-160). Each pair is
// {one-based placeholder index, value}; the dispatcher calls binder.bind(id, value)
// then binder.finalize(), surfacing bind/finalize failures as an error cursor.
//
// logical_value_t is built with the templated ctor `logical_value_t{resource, T}`
// where the C++ type of T selects the logical type via to_logical_type<T>()
// (components/types/logical_value.hpp:136). The same construction is used in
// components/sql/test/test_parameter.cpp (e.g. v(&resource, 10l) for BIGINT,
// v(&resource, 1ul) for UBIGINT, v(&resource, 3.14) for DOUBLE,
// v(&resource, true) for BOOLEAN, v(&resource, std::string("...")) for string).
// Type-to-SqlParamValue mapping:
//   Int64  -> int64_t  (literal suffix l)
//   UInt64 -> uint64_t (literal suffix ul)
//   Double -> double
//   Str    -> std::string
//   Bool   -> bool
namespace {
    using param_t = std::pair<size_t, types::logical_value_t>;
    using params_t = std::vector<param_t>;

    // Index of the column whose alias matches `name`, or column_count() if absent.
    uint64_t column_index(const cursor_t_ptr& cur, const std::string& name) {
        const auto& chunk = cur->chunk_data();
        for (uint64_t col = 0; col < chunk.column_count(); ++col) {
            if (chunk.data[col].type().alias() == name) {
                return col;
            }
        }
        return chunk.column_count();
    }

    test_spaces make_space(const std::string& subdir) {
        auto config = test_create_config("/tmp/test_parameterized_queries/" + subdir);
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        return test_spaces(config);
    }
} // namespace

TEST_CASE("integration::cpp::params::bind_each_type") {
    auto space = make_space("bind_each_type");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "alltypes");
    }

    INFO("bind Int64 / UInt64 / Double / Str / Bool via $N") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{
                {1, types::logical_value_t{resource, static_cast<int64_t>(-42)}},
                {2, types::logical_value_t{resource, static_cast<uint64_t>(7)}},
                {3, types::logical_value_t{resource, 3.5}},
                {4, types::logical_value_t{resource, std::string("hello")}},
                {5, types::logical_value_t{resource, true}},
            };
            auto cur = dispatcher->execute_sql_with_params(
                session,
                "INSERT INTO ParamDb.AllTypes (i, u, d, s, b) VALUES ($1, $2, $3, $4, $5);",
                params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.AllTypes;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);

            const uint64_t i_col = column_index(cur, "i");
            const uint64_t u_col = column_index(cur, "u");
            const uint64_t d_col = column_index(cur, "d");
            const uint64_t s_col = column_index(cur, "s");
            const uint64_t b_col = column_index(cur, "b");
            REQUIRE(i_col < cur->chunk_data().column_count());
            REQUIRE(u_col < cur->chunk_data().column_count());
            REQUIRE(d_col < cur->chunk_data().column_count());
            REQUIRE(s_col < cur->chunk_data().column_count());
            REQUIRE(b_col < cur->chunk_data().column_count());

            REQUIRE(cur->chunk_data().value(i_col, 0).value<int64_t>() == -42);
            REQUIRE(cur->chunk_data().value(u_col, 0).value<uint64_t>() == 7);
            REQUIRE(cur->chunk_data().value(d_col, 0).value<double>() == Approx(3.5));
            const auto s_cell = cur->chunk_data().value(s_col, 0);
            REQUIRE(s_cell.value<std::string_view>() == "hello");
            REQUIRE(cur->chunk_data().value(b_col, 0).value<bool>() == true);
        }
    }
}

TEST_CASE("integration::cpp::params::uint64_max") {
    auto space = make_space("uint64_max");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "u");
    }

    INFO("UInt64 max round-trips through a schema-free column") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{
                {1, types::logical_value_t{resource, static_cast<int64_t>(1)}},
                {2, types::logical_value_t{resource, std::numeric_limits<uint64_t>::max()}},
            };
            auto cur =
                dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.U (k, v) VALUES ($1, $2);", params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.U;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            const uint64_t v_col = column_index(cur, "v");
            REQUIRE(v_col < cur->chunk_data().column_count());
            REQUIRE(cur->chunk_data().value(v_col, 0).value<uint64_t>() == std::numeric_limits<uint64_t>::max());
        }
    }
}

TEST_CASE("integration::cpp::params::placeholders") {
    auto space = make_space("placeholders");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "place");
    }

    INFO("repeated placeholder bound once fills both columns") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{{1, types::logical_value_t{resource, static_cast<int64_t>(99)}}};
            auto cur = dispatcher->execute_sql_with_params(session,
                                                           "INSERT INTO ParamDb.Place (a, b) VALUES ($1, $1);",
                                                           params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.Place;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            const uint64_t a_col = column_index(cur, "a");
            const uint64_t b_col = column_index(cur, "b");
            REQUIRE(a_col < cur->chunk_data().column_count());
            REQUIRE(b_col < cur->chunk_data().column_count());
            REQUIRE(cur->chunk_data().value(a_col, 0).value<int64_t>() == 99);
            REQUIRE(cur->chunk_data().value(b_col, 0).value<int64_t>() == 99);
        }
    }

    INFO("multiple params in INSERT") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{
                {1, types::logical_value_t{resource, static_cast<int64_t>(7)}},
                {2, types::logical_value_t{resource, std::string("row7")}},
            };
            auto cur = dispatcher->execute_sql_with_params(session,
                                                           "INSERT INTO ParamDb.Place (id, name) VALUES ($1, $2);",
                                                           params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.Place WHERE name = 'row7';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            const uint64_t name_col = column_index(cur, "name");
            REQUIRE(name_col < cur->chunk_data().column_count());
            const auto name_cell = cur->chunk_data().value(name_col, 0);
            REQUIRE(name_cell.value<std::string_view>() == "row7");
        }
    }
}

TEST_CASE("integration::cpp::params::where_update_delete") {
    auto space = make_space("where_update_delete");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "rows");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "INSERT INTO ParamDb.Rows (name, count, flag) VALUES "
                                           "('a', 10, true), ('b', 20, false), ('c', 30, true);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    INFO("SELECT ... WHERE count > $1") {
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, static_cast<int64_t>(15)}}};
        auto cur = dispatcher->execute_sql_with_params(session, "SELECT * FROM ParamDb.Rows WHERE count > $1;", params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("SELECT ... WHERE count > $1 AND name = $2") {
        auto session = otterbrix::session_id_t();
        params_t params{
            {1, types::logical_value_t{resource, static_cast<int64_t>(15)}},
            {2, types::logical_value_t{resource, std::string("c")}},
        };
        auto cur = dispatcher->execute_sql_with_params(session,
                                                       "SELECT * FROM ParamDb.Rows WHERE count > $1 AND name = $2;",
                                                       params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const uint64_t name_col = column_index(cur, "name");
        REQUIRE(name_col < cur->chunk_data().column_count());
        const auto name_cell = cur->chunk_data().value(name_col, 0);
        REQUIRE(name_cell.value<std::string_view>() == "c");
    }

    INFO("SELECT ... WHERE flag = $1 (bool param)") {
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, true}}};
        auto cur = dispatcher->execute_sql_with_params(session, "SELECT * FROM ParamDb.Rows WHERE flag = $1;", params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    INFO("UPDATE ... SET count = $1 WHERE name = $2") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{
                {1, types::logical_value_t{resource, static_cast<int64_t>(777)}},
                {2, types::logical_value_t{resource, std::string("a")}},
            };
            auto cur = dispatcher->execute_sql_with_params(session,
                                                           "UPDATE ParamDb.Rows SET count = $1 WHERE name = $2;",
                                                           params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.Rows WHERE name = 'a';");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            const uint64_t count_col = column_index(cur, "count");
            REQUIRE(count_col < cur->chunk_data().column_count());
            REQUIRE(cur->chunk_data().value(count_col, 0).value<int64_t>() == 777);
        }
    }

    INFO("DELETE ... WHERE count = $1") {
        {
            auto session = otterbrix::session_id_t();
            params_t params{{1, types::logical_value_t{resource, static_cast<int64_t>(20)}}};
            auto cur =
                dispatcher->execute_sql_with_params(session, "DELETE FROM ParamDb.Rows WHERE count = $1;", params);
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.Rows;");
            REQUIRE(cur->is_success());
            // started with 3 rows; one ('a') was updated to count=777, one ('b') had count=20 and is now deleted.
            REQUIRE(cur->size() == 2);
        }
    }
}

TEST_CASE("integration::cpp::params::validation_errors") {
    auto space = make_space("validation_errors");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "val");
    }

    INFO("missing param: query has $2, only $1 bound") {
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, static_cast<int64_t>(1)}}};
        auto cur =
            dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.Val (id, name) VALUES ($1, $2);", params);
        REQUIRE(cur->is_error());
        REQUIRE_FALSE(cur->get_error().what.empty());
    }

    INFO("unknown param index: bind $99 for a single-placeholder query") {
        auto session = otterbrix::session_id_t();
        params_t params{{99, types::logical_value_t{resource, static_cast<int64_t>(1)}}};
        auto cur = dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.Val (id) VALUES ($1);", params);
        REQUIRE(cur->is_error());
        REQUIRE_FALSE(cur->get_error().what.empty());
    }

    INFO("zero param index is rejected") {
        auto session = otterbrix::session_id_t();
        params_t params{{0, types::logical_value_t{resource, static_cast<int64_t>(1)}}};
        auto cur = dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.Val (id) VALUES ($1);", params);
        REQUIRE(cur->is_error());
        REQUIRE_FALSE(cur->get_error().what.empty());
    }
}

TEST_CASE("integration::cpp::params::injection_quote_in_string_stored_verbatim") {
    auto space = make_space("injection_quote_verbatim");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "t");
    }

    const std::string nasty = "Robert'); DROP TABLE ParamDb.T;--";
    {
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, nasty}}};
        auto cur = dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.T (name) VALUES ($1);", params);
        REQUIRE(cur->is_success());
    }
    {
        // Table must still exist with exactly the one inserted row, stored verbatim.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.T;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const uint64_t name_col = column_index(cur, "name");
        REQUIRE(name_col < cur->chunk_data().column_count());
        const auto name_cell = cur->chunk_data().value(name_col, 0);
        REQUIRE(name_cell.value<std::string_view>() == nasty);
    }
}

TEST_CASE("integration::cpp::params::injection_or_1_eq_1_matches_no_rows") {
    auto space = make_space("injection_or_1_eq_1");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "t");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session,
                                    "INSERT INTO ParamDb.T (name, score) VALUES ('alice', 1), ('bob', 2), ('eve', 3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    {
        // The whole payload is a single string literal compared against `name`; no row equals it.
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, std::string("alice' OR '1'='1")}}};
        auto cur = dispatcher->execute_sql_with_params(session, "SELECT * FROM ParamDb.T WHERE name = $1;", params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::params::injection_semicolon_does_not_chain") {
    auto space = make_space("injection_semicolon");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "t");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "victim");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO ParamDb.Victim (x) VALUES (1), (2), (3);");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }

    const std::string payload = "x'; DELETE FROM ParamDb.Victim; --";
    {
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, payload}}};
        auto cur = dispatcher->execute_sql_with_params(session, "INSERT INTO ParamDb.T (name) VALUES ($1);", params);
        REQUIRE(cur->is_success());
    }
    {
        // Victim must be untouched: the chained DELETE inside the payload must not run.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.Victim;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    {
        // The payload was stored as data in T.
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM ParamDb.T;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const uint64_t name_col = column_index(cur, "name");
        REQUIRE(name_col < cur->chunk_data().column_count());
        const auto name_cell = cur->chunk_data().value(name_col, 0);
        REQUIRE(name_cell.value<std::string_view>() == payload);
    }
}

TEST_CASE("integration::cpp::params::injection_int_param_type_safety") {
    auto space = make_space("injection_int_param");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "t");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "INSERT INTO ParamDb.T (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
    }
    {
        // An Int64 param compares as an integer; it can only match the integer id.
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, static_cast<int64_t>(1)}}};
        auto cur = dispatcher->execute_sql_with_params(session, "SELECT * FROM ParamDb.T WHERE id = $1;", params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        const uint64_t name_col = column_index(cur, "name");
        REQUIRE(name_col < cur->chunk_data().column_count());
        const auto name_cell = cur->chunk_data().value(name_col, 0);
        REQUIRE(name_cell.value<std::string_view>() == "a");
    }
}

TEST_CASE("integration::cpp::params::injection_comment_marker_stored_literally") {
    auto space = make_space("injection_comment_marker");
    auto* dispatcher = space.dispatcher();
    auto* resource = dispatcher->resource();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, "CREATE DATABASE ParamDb;");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, "paramdb", "t");
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO ParamDb.T (name) VALUES ('a'), ('b');");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }
    {
        // "a'--" is a single literal string; the comment marker must NOT terminate the predicate,
        // so it matches neither 'a' nor 'b'.
        auto session = otterbrix::session_id_t();
        params_t params{{1, types::logical_value_t{resource, std::string("a'--")}}};
        auto cur = dispatcher->execute_sql_with_params(session, "SELECT * FROM ParamDb.T WHERE name = $1;", params);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::params::error_code_contracts") {
    auto space = make_space("error_code_contracts");
    auto* dispatcher = space.dispatcher();

    INFO("CREATE DATABASE twice -> database_already_exists") {
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE ErrDb;");
            REQUIRE(cur->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "CREATE DATABASE ErrDb;");
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == core::error_code_t::database_already_exists);
            REQUIRE_FALSE(cur->get_error().what.empty());
        }
    }

    INFO("DROP nonexistent database -> database_not_exists") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "DROP DATABASE NoSuchDb;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::database_not_exists);
        REQUIRE_FALSE(cur->get_error().what.empty());
    }

    INFO("SELECT from nonexistent table -> table_not_exists") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM ErrDb.NoSuchTable;");
        REQUIRE(cur->is_error());
        REQUIRE(cur->get_error().type == core::error_code_t::table_not_exists);
        // Unlike the DDL error paths above (which carry a descriptive .what), the
        // SELECT-from-nonexistent-table path surfaces table_not_exists with an empty
        // message, so we assert only the error code contract here.
    }
}
