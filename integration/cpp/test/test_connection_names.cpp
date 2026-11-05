// A three-part name whose first part is a connection names that connection's one database: connection.schema.table.
// Any other first part is a local database, as before. Four parts keep naming connection.database.schema.table.

#include "host_fixture.hpp"

#include <set>
#include <string>
#include <utility>
#include <vector>

using host_fixture::host_t;
using code = core::error_code_t;

namespace {

    struct remote_t {
        std::string connection;
        std::string database;
        std::string schema;
        std::string table;
        std::int64_t marker;
    };

    // Every table holds one row: its marker. The local database dup is there before the connection dup's database.
    struct catalog_t : host_t {
        explicit catalog_t(const std::string& leaf)
            : host_t(leaf, nullptr, {"my", "pg", "pg2", "multi", "dup"}) {
            const auto setup = otterbrix::session_id_t();
            for (const std::string sql : {"CREATE DATABASE local;",
                                          "SET search_path = 'local.public';",
                                          "CREATE SCHEMA eg;",
                                          "CREATE TABLE local.eg.regions (v BIGINT);",
                                          "INSERT INTO local.eg.regions (v) VALUES (1);",
                                          "CREATE DATABASE legacy;",
                                          "CREATE TABLE legacy.public.t (v BIGINT);",
                                          "INSERT INTO legacy.public.t (v) VALUES (2);",
                                          "CREATE DATABASE dup;",
                                          "SET search_path = 'dup.public';",
                                          "CREATE SCHEMA s;",
                                          "CREATE TABLE dup.s.t (v BIGINT);",
                                          "INSERT INTO dup.s.t (v) VALUES (3);"}) {
                db.ok(setup, sql);
            }
            for (const auto& remote : std::vector<remote_t>{{"my", "def", "bill", "orders", 11},
                                                            {"my", "def", "ops", "t", 12},
                                                            {"pg", "shop_db", "shop", "customers", 21},
                                                            {"pg", "shop_db", "sales", "t", 22},
                                                            {"pg2", "shop_db", "shop", "customers", 31},
                                                            {"multi", "a", "s", "t", 41},
                                                            {"multi", "b", "s", "t", 42},
                                                            {"dup", "x", "s", "t", 51}}) {
                add(remote);
            }
        }

        void add(const remote_t& remote) {
            const auto database = remote.connection + "." + remote.database;
            if (databases_.insert(database).second) {
                ok(remote_database(remote.connection, remote.database));
            }
            const auto schema = database + "." + remote.schema;
            if (schemas_.insert(schema).second) {
                ok(remote_schema(remote.connection, remote.database, remote.schema));
            }
            const auto table = schema + "." + remote.table;
            db.ok(otterbrix::session_id_t(), "CREATE TABLE " + table + " (v BIGINT);");
            backend(remote.connection).set_rows(table, {{remote.marker}});
        }

    private:
        std::set<std::string> databases_;
        std::set<std::string> schemas_;
    };

    std::string refusal(code type, const std::string& what) {
        return "refused " + std::to_string(static_cast<int>(type)) + ": " + what;
    }

    std::string refusal_of(const components::cursor::cursor_t_ptr& cursor) {
        return refusal(cursor->get_error().type, std::string{cursor->get_error().what});
    }

    // What `sql` answers in a fresh session whose place is `place` ("" keeps the default): "ok" when `expected` is
    // "ok" and it succeeds, otherwise the single bigint of its single row; the refusal either way.
    std::string answer(catalog_t& host, const std::string& place, const std::string& sql, const std::string& expected) {
        const auto session = otterbrix::session_id_t();
        if (!place.empty()) {
            auto set = host.db.run(session, "SET search_path = '" + place + "';");
            if (set->is_error()) {
                return refusal_of(set);
            }
        }
        auto cursor = host.db.run(session, sql);
        if (cursor->is_error()) {
            return refusal_of(cursor);
        }
        if (expected == "ok") {
            return "ok";
        }
        std::vector<std::int64_t> values;
        for (const auto& chunk : cursor->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                values.push_back(chunk.get_value<std::int64_t>(0, row));
            }
        }
        if (values.size() != 1) {
            return std::to_string(values.size()) + " rows";
        }
        return std::to_string(values.front());
    }

    struct case_t {
        std::string place;
        std::string sql;
        std::string expected;
    };

    void check(catalog_t& host, const std::vector<case_t>& cases) {
        for (const auto& c : cases) {
            INFO("place '" << c.place << "': " << c.sql);
            CHECK(answer(host, c.place, c.sql, c.expected) == c.expected);
        }
    }

    std::string read(const std::string& name) { return "SELECT v FROM " + name + ";"; }

} // namespace

TEST_CASE("integration::cpp::connection_names::a_name_resolves_to_one_table") {
    catalog_t host("test_connection_names/resolve");
    check(host,
          {
              {"", read("my.bill.orders"), "11"},
              {"", read("pg.shop.customers"), "21"},
              {"", read("pg2.shop.customers"), "31"},
              {"", read("local.eg.regions"), "1"},
              {"", read("legacy.public.t"), "2"},
              {"", read("pg.shop_db.shop.customers"), "21"},
              {"", read("multi.a.s.t"), "41"},
              {"", read("multi.b.s.t"), "42"},
              {"", read("dup.x.s.t"), "51"},
              {"pg.shop", read("customers"), "21"},
              {"pg.shop", read("sales.t"), "22"},
              {"my.bill", read("ops.t"), "12"},
              {"local.eg", read("regions"), "1"},
          });
}

TEST_CASE("integration::cpp::connection_names::a_name_that_names_nothing_or_two_is_refused") {
    catalog_t host("test_connection_names/refuse");
    check(
        host,
        {
            {"",
             read("nope.bill.orders"),
             refusal(code::table_not_exists, "relation \"nope.bill.orders\" does not exist")},
            {"",
             read("my.nobill.orders"),
             refusal(code::table_not_exists, "relation \"my.nobill.orders\" does not exist")},
            {"", read("my.bill.nope"), refusal(code::table_not_exists, "relation \"my.bill.nope\" does not exist")},
            {"",
             read("dup.s.t"),
             refusal(code::ambiguous_alias, "database \"dup\" is ambiguous: a connection and a local database")},
            {"", read("multi.s.t"), refusal(code::ambiguous_alias, "connection \"multi\" has more than one database")},
            {"nope.x", read("regions"), refusal(code::database_not_exists, "database \"nope\" does not exist")},
        });
}

TEST_CASE("integration::cpp::connection_names::a_place_in_a_connection_by_its_name") {
    catalog_t host("test_connection_names/place");
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'my.bill';");
    CHECK(host.db.strings(session, "SELECT current_database();") == std::vector<std::string>{"def"});
    CHECK(host.db.strings(session, "SELECT current_schema();") == std::vector<std::string>{"bill"});
    host.db.ok(session, "SET search_path = 'pg.shop';");
    CHECK(host.db.ints(session, "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'pg_class';") ==
          std::vector<std::int64_t>{1});
}

// current_connection() names the connection of the session's place: '' for a local place (a local database's
// datconnection), NULL for no place.
TEST_CASE("integration::cpp::connection_names::current_connection_names_the_places_connection") {
    catalog_t host("test_connection_names/current_connection");
    const auto session = otterbrix::session_id_t();
    constexpr auto NULL_TEXT = "<null>"; // how session_sql_fixture's strings() prints a NULL cell
    struct row_t {
        const char* place;
        const char* connection;
    };
    for (const auto& [place, connection] : {row_t{"my.bill", "my"},
                                            row_t{"pg.shop", "pg"},
                                            row_t{"pg2.shop", "pg2"},
                                            row_t{"local.eg", ""},
                                            row_t{"", NULL_TEXT}}) {
        INFO("place '" << place << "'");
        host.db.ok(session, std::string{"SET search_path = '"} + place + "';");
        CHECK(host.db.strings(session, "SELECT current_connection();") == std::vector<std::string>{connection});
    }
    check(host,
          {{"", "SELECT current_connection('my');",
            refusal(code::unrecognized_function, "function current_connection takes no arguments")}});
}

// A target written in three parts is the connection's table, as its four-part name is: a table made on the backend,
// and what a connection's database does not take refused alike.
TEST_CASE("integration::cpp::connection_names::a_three_part_target_is_its_four_part_target") {
    catalog_t host("test_connection_names/targets");
    check(host, {{"", "CREATE TABLE my.bill.made (v BIGINT);", "ok"}});
    host.backend("my").set_rows("my.def.bill.made", {{77}});
    check(host, {{"", read("my.bill.made"), "77"}});
    const std::vector<std::pair<std::string, std::string>> pairs = {
        {"CREATE VIEW my.bill.v3 AS SELECT 1 AS a;", "CREATE VIEW my.def.bill.v4 AS SELECT 1 AS a;"},
        {"CREATE SEQUENCE my.bill.s3;", "CREATE SEQUENCE my.def.bill.s4;"},
        {"CREATE TYPE my.bill.e3 AS ENUM ('a');", "CREATE TYPE my.def.bill.e4 AS ENUM ('a');"},
        {"CREATE TABLE my.bill.d3 (v BIGINT DEFAULT 1);", "CREATE TABLE my.def.bill.d4 (v BIGINT DEFAULT 1);"},
        {"CREATE INDEX i3 ON my.bill.orders (v);", "CREATE INDEX i4 ON my.def.bill.orders (v);"},
    };
    for (const auto& [three, four] : pairs) {
        INFO(three << " / " << four);
        CHECK(answer(host, "", three, "ok") == answer(host, "", four, "ok"));
        CHECK(answer(host, "", four, "ok") != "ok");
    }
}

// Every position a statement names a table in goes through the one rule.
TEST_CASE("integration::cpp::connection_names::every_position_of_a_name") {
    catalog_t host("test_connection_names/positions");
    const std::vector<std::string> reads = {
        "SELECT v FROM {t};",
        "SELECT x.v FROM {t} x JOIN {t} y ON x.v = y.v;",
        "SELECT v FROM (SELECT v FROM {t}) d;",
        "WITH c AS (SELECT v FROM {t}) SELECT v FROM c;",
        "SELECT v FROM {t} WHERE v IN (SELECT v FROM {t});",
        "SELECT v FROM {t} WHERE EXISTS (SELECT 1 FROM {t});",
        "SELECT (SELECT max(v) FROM {t});",
        "SELECT max(v) FROM {t} GROUP BY v HAVING max(v) = (SELECT max(v) FROM {t});",
    };
    std::vector<case_t> cases;
    for (const auto& [name, marker] :
         std::vector<std::pair<std::string, std::string>>{{"my.bill.orders", "11"}, {"local.eg.regions", "1"}}) {
        for (auto sql : reads) {
            for (auto at = sql.find("{t}"); at != std::string::npos; at = sql.find("{t}")) {
                sql.replace(at, 3, name);
            }
            cases.push_back({"", sql, marker});
        }
    }
    check(host, cases);
    check(host,
          {
              {"", "CREATE TABLE local.eg.sink (v BIGINT);", "ok"},
              {"", "INSERT INTO local.eg.sink (v) SELECT v FROM my.bill.orders;", "ok"},
              {"", read("local.eg.sink"), "11"},
              {"", "INSERT INTO my.bill.orders (v) VALUES (7);", "ok"},
              {"", "UPDATE my.bill.orders SET v = 8 WHERE v = 7;", "ok"},
              {"", "DELETE FROM my.bill.orders WHERE v = 8;", "ok"},
              {"", read("my.def.bill.orders"), "11"},
              {"", "CREATE TABLE my.bill.fresh (v BIGINT);", "ok"},
              {"", "DROP TABLE my.bill.fresh;", "ok"},
              {"", "DROP TABLE local.eg.sink;", "ok"},
          });
}
