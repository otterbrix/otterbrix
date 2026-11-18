// pg_class.relispartition, format_type(oid, typmod), and information_schema reachable from every place.

#include "host_fixture.hpp"

#include <string>
#include <vector>

using host_fixture::host_t;
using strings = host_fixture::strings;
using ints = std::vector<std::int64_t>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    std::string format_type(host_t& host, const std::string& oid, const std::string& typmod) {
        const auto names = host.db.strings(any(), "SELECT format_type(" + oid + "::oid, " + typmod + ");");
        REQUIRE(names.size() == 1);
        return names.front();
    }

    // The columns of m.t as format_type names their types, in order.
    const std::string column_types =
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON "
        "c.oid = a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = 't' AND "
        "n.nspname = 'm' AND a.attnum > 0 ORDER BY a.attnum;";

} // namespace

TEST_CASE("integration::cpp::catalog_compat::relispartition") {
    host_t host("catalog_compat/relispartition");
    host.db.ok(any(), "CREATE TABLE t (id BIGINT);");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relispartition = true;") == ints{0});
    CHECK(host.db.strings(any(),
                          "SELECT relname FROM pg_catalog.pg_class WHERE relname = 't' AND relispartition = false;") ==
          strings{"t"});
}

TEST_CASE("integration::cpp::catalog_compat::format_type_builtins") {
    host_t host("catalog_compat/format_type_builtins");
    CHECK(format_type(host, "16", "NULL") == "boolean");
    CHECK(format_type(host, "20", "-1") == "bigint");
    CHECK(format_type(host, "21", "NULL") == "smallint");
    CHECK(format_type(host, "23", "NULL") == "integer");
    CHECK(format_type(host, "25", "NULL") == "text");
    CHECK(format_type(host, "25", "5") == "text(5)");
    CHECK(format_type(host, "700", "NULL") == "real");
    CHECK(format_type(host, "701", "NULL") == "double precision");
    CHECK(format_type(host, "1700", "NULL") == "numeric");
    CHECK(format_type(host, "1700", std::to_string(((10 << 16) | 2) + 4)) == "numeric(10,2)");
    CHECK(format_type(host, "1082", "NULL") == "date");
    CHECK(format_type(host, "1083", "NULL") == "time without time zone");
    CHECK(format_type(host, "1114", "3") == "timestamp(3) without time zone");
    CHECK(format_type(host, "1184", "NULL") == "timestamp with time zone");
    CHECK(format_type(host, "1266", "2") == "time(2) with time zone");
    CHECK(format_type(host, "1186", "NULL") == "interval");
    CHECK(format_type(host, "1186", std::to_string((4096 << 16) | 0xFFFF)) == "interval second");
    CHECK(format_type(host, "2950", "NULL") == "uuid");
    CHECK(format_type(host, "999999", "NULL") == "???");
    CHECK(
        host.db.strings(any(), "SELECT format_type(typarray, NULL) FROM pg_catalog.pg_type WHERE typname = 'int4';") ==
        strings{"integer[]"});
    CHECK(host.db.strings(any(),
                          "SELECT format_type(reltype, NULL) FROM pg_catalog.pg_class WHERE relname = 'pg_class';") ==
          strings{"pg_class"});
    auto null_oid = host.db.ok(any(), "SELECT format_type(NULL, NULL);");
    REQUIRE(null_oid->size() == 1);
    auto is_null = null_oid->is_null(0, 0);
    REQUIRE_FALSE(is_null.has_error());
    CHECK(is_null.value());
}

// A user type is named with its schema unless the schema is the place's (PostgreSQL 18 TypeIsVisible).
TEST_CASE("integration::cpp::catalog_compat::format_type_user_types") {
    host_t host("catalog_compat/format_type_user_types");
    host.db.ok(any(), "CREATE SCHEMA m;");
    host.db.ok(any(), "CREATE TYPE m.mood AS ENUM ('sad', 'happy');");
    host.db.ok(any(),
               "CREATE TABLE m.t (id BIGINT, amount NUMERIC(10, 2), tags INTEGER[], mood m.mood, at TIMESTAMPTZ);");
    CHECK(host.db.strings(any(), column_types) ==
          strings{"bigint", "numeric(10,2)", "integer[]", "m.mood", "timestamp with time zone"});
    CHECK(
        host.db.strings(any(), "SELECT format_type(typarray, NULL) FROM pg_catalog.pg_type WHERE typname = 'mood';") ==
        strings{"m.mood[]"});
    CHECK(
        host.db.strings(any(),
                        "SELECT format_type(c.reltype, NULL) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                        "ON n.oid = c.relnamespace WHERE c.relname = 't' AND n.nspname = 'm';") == strings{"m.t"});
    const auto session = any();
    host.db.ok(session, "SET search_path = 'm';");
    CHECK(host.db.strings(session, column_types) ==
          strings{"bigint", "numeric(10,2)", "integer[]", "mood", "timestamp with time zone"});
}

// information_schema shows the session's current database: a connection's, which has no information_schema of its
// own, through main's.
TEST_CASE("integration::cpp::catalog_compat::information_schema_of_every_place") {
    host_t host("catalog_compat/information_schema");
    host.register_orders();
    host.db.ok(any(), "CREATE TABLE local_t (x BIGINT);");
    host.db.ok(any(), "CREATE DATABASE other;");
    host.db.ok(any(), "CREATE TABLE other.public.other_t (y BIGINT);");

    SECTION("a connection's database") {
        const auto session = any();
        host.db.ok(session, "SET search_path = 'm2.shop.public';");
        CHECK(host.db.strings(session,
                              "SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' ORDER BY "
                              "ordinal_position;") == strings{"id", "amount"});
        CHECK(host.db.strings(session,
                              "SELECT DISTINCT table_catalog FROM information_schema.columns WHERE table_name = "
                              "'orders';") == strings{"shop"});
        CHECK(host.db.ints(session, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'local_t';") ==
              ints{0});
        CHECK(host.db.strings(session,
                              "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public';") ==
              strings{"public"});
    }
    SECTION("another local database") {
        const auto session = any();
        host.db.ok(session, "SET search_path = 'other.public';");
        CHECK(host.db.ints(session, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'other_t';") ==
              ints{1});
        CHECK(host.db.ints(session, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'local_t';") ==
              ints{0});
    }
    SECTION("the default place") {
        CHECK(host.db.ints(any(), "SELECT count(*) FROM information_schema.columns WHERE table_name = 'local_t';") ==
              ints{1});
        CHECK(host.db.ints(any(), "SELECT count(*) FROM information_schema.columns WHERE table_name = 'orders';") ==
              ints{0});
    }
}
