#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/types/types.hpp>

// oid as PostgreSQL 18 defines it: uint32 storage; int2/int4/int8 -> oid implicit (int2/int4 keep the bit
// pattern, int8 refuses outside 0..4294967295); oid -> int4/int8 assignment; text in/out; comparisons, min and
// max, no arithmetic.

using namespace test_helpers;

namespace {

    configuration::config quiet_config(const char* leaf) {
        auto config = make_test_config(integration_fixture_path(std::string{"test_oid_type/"} + leaf));
        config.log.level = log_t::level::off;
        return config;
    }

    struct oid_space_t {
        explicit oid_space_t(const char* leaf)
            : config(quiet_config(leaf))
            , space(config) {}

        components::cursor::cursor_t_ptr run(const std::string& sql) { return exec(space.dispatcher(), sql); }

        template<typename T>
        T single(const std::string& sql) {
            auto cursor = run(sql);
            INFO(sql);
            INFO((cursor->is_error() ? std::string{cursor->get_error().what} : std::string{}));
            REQUIRE(cursor->is_success());
            REQUIRE(cursor->size() == 1);
            auto cell = cursor->get<T>(0, 0);
            REQUIRE_FALSE(cell.has_error());
            return cell.value();
        }

        configuration::config config;
        test_spaces space;
    };

} // namespace

TEST_CASE("integration::cpp::oid_type::catalog_oid_column") {
    oid_space_t db("catalog_oid_column");
    auto cursor = db.run("SELECT oid FROM pg_catalog.pg_type WHERE oid = 23;");
    REQUIRE(cursor->is_success());
    REQUIRE(cursor->size() == 1);
    CHECK(cursor->type_data().front().type() == components::types::logical_type::OID);
    auto cell = cursor->get<std::uint32_t>(0, 0);
    REQUIRE_FALSE(cell.has_error());
    CHECK(cell.value() == 23);
}

TEST_CASE("integration::cpp::oid_type::integer_to_oid") {
    oid_space_t db("integer_to_oid");
    CHECK(db.single<std::uint32_t>("SELECT CAST(CAST(-1 AS SMALLINT) AS oid);") == 4294967295U);
    CHECK(db.single<std::uint32_t>("SELECT CAST(CAST(-1 AS INTEGER) AS oid);") == 4294967295U);
    CHECK(db.single<std::uint32_t>("SELECT CAST(4294967295 AS oid);") == 4294967295U);
    CHECK(db.run("SELECT CAST(4294967296 AS oid);")->is_error());
    CHECK(db.run("SELECT CAST(CAST(-1 AS BIGINT) AS oid);")->is_error());
}

TEST_CASE("integration::cpp::oid_type::oid_to_integer") {
    oid_space_t db("oid_to_integer");
    CHECK(db.single<std::int32_t>("SELECT CAST(CAST(4294967295 AS oid) AS INTEGER);") == -1);
    CHECK(db.single<std::int64_t>("SELECT CAST(CAST(4294967295 AS oid) AS BIGINT);") == 4294967295LL);
}

TEST_CASE("integration::cpp::oid_type::text_io") {
    oid_space_t db("text_io");
    CHECK(db.single<std::uint32_t>("SELECT CAST('-1' AS oid);") == 4294967295U);
    CHECK(db.single<std::uint32_t>("SELECT CAST('4294967295' AS oid);") == 4294967295U);
    CHECK(db.run("SELECT CAST('4294967296' AS oid);")->is_error());
    CHECK(db.run("SELECT CAST('-2147483649' AS oid);")->is_error());
    CHECK(db.single<std::string_view>("SELECT CAST(CAST(4294967295 AS oid) AS TEXT);") == "4294967295");
}

TEST_CASE("integration::cpp::oid_type::no_arithmetic") {
    oid_space_t db("no_arithmetic");
    CHECK(db.run("SELECT oid + 1 FROM pg_catalog.pg_type;")->is_error());
    CHECK(db.run("SELECT -oid FROM pg_catalog.pg_type;")->is_error());
    CHECK(db.run("SELECT sum(oid) FROM pg_catalog.pg_type;")->is_error());
}

// Two plain integers still meet at an integer: an oid in between would refuse the addition.
TEST_CASE("integration::cpp::oid_type::integers_do_not_meet_at_oid") {
    oid_space_t db("integers_do_not_meet_at_oid");
    CHECK(db.single<std::int32_t>("SELECT CAST(2 AS SMALLINT) + CAST(3 AS INTEGER);") == 5);
    CHECK(db.single<std::int64_t>("SELECT CAST(2 AS INTEGER) + CAST(3 AS BIGINT);") == 5);
}

TEST_CASE("integration::cpp::oid_type::oid_column") {
    oid_space_t db("oid_column");
    REQUIRE(db.run("CREATE TABLE refs (id BIGINT, o oid);")->is_success());
    auto inserted = db.run("INSERT INTO refs (id, o) VALUES (1, 5), (2, 4294967295), (3, 70000);");
    INFO((inserted->is_error() ? std::string{inserted->get_error().what} : std::string{}));
    REQUIRE(inserted->is_success());
    CHECK(db.single<std::int64_t>("SELECT id FROM refs WHERE o = 5;") == 1);
    CHECK(db.single<std::int64_t>("SELECT id FROM refs WHERE o > 70000;") == 2);
    CHECK(db.single<std::uint32_t>("SELECT max(o) FROM refs;") == 4294967295U);
    CHECK(db.single<std::uint32_t>("SELECT min(o) FROM refs;") == 5);
    auto joined = db.run("SELECT t.typname FROM refs r JOIN pg_catalog.pg_type t ON t.oid = r.o;");
    REQUIRE(joined->is_success());
    CHECK(joined->size() == 0);
}
