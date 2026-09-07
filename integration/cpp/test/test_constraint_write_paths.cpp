#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/vector/indexing_vector.hpp>
#include <services/collection/executor.hpp>
#include <sstream>
#include <string>

namespace {
    // column_index answers a result_wrapper_t, not a SIZE_MAX sentinel, so a missing column is refused, not asserted.
    inline uint64_t column_of(const components::cursor::cursor_t_ptr& c, std::string_view name) {
        auto idx = c->column_index(name);
        REQUIRE_FALSE(idx.has_error());
        return idx.value();
    }
} // namespace


// INSERT and UPDATE wire each constraint in separately, so every constraint here is tested on both paths.

namespace {

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    void
    both_accept(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& insert, const std::string& update) {
        INFO("must be accepted: " << insert);
        CHECK(run(dispatcher, insert)->is_success());

        INFO("must be accepted: " << update);
        CHECK(run(dispatcher, update)->is_success());
    }

    void
    both_reject(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& insert, const std::string& update) {
        INFO("must be rejected: " << insert);
        CHECK(run(dispatcher, insert)->is_error());

        INFO("must be rejected: " << update);
        CHECK(run(dispatcher, update)->is_error());
    }

    configuration::config config_for(const std::string& name) {
        auto config = test_create_config(integration_fixture_path("test_constraint_write_paths/" + name));
        test_clear_directory(config);
        config.wal.on = false;
        config.log.level = log_t::level::off;
        return config;
    }

    // A write spanning several chunks; the mid-pump gate flushes each full chunk to storage as it fills.
    constexpr uint64_t kFlushThreshold = components::vector::DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t kSpanningRows = 3 * components::vector::DEFAULT_VECTOR_CAPACITY;

    configuration::config spanning_config_for(const std::string& name) {
        auto config = config_for(name);
        config.execution.dml_flush_row_threshold = kFlushThreshold;
        return config;
    }

} // namespace

TEST_CASE("integration::cpp::test_constraint_write_paths::not_null", "[writepaths]") {
    auto config = config_for("not_null");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, val text NOT NULL);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, val) VALUES (1, 'seed');")->is_success());

    both_accept(d, "INSERT INTO c.t (id, val) VALUES (2, 'ok');", "UPDATE c.t SET val = 'ok' WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, val) VALUES (3, NULL);", "UPDATE c.t SET val = NULL WHERE id = 1;");
}

TEST_CASE("integration::cpp::test_constraint_write_paths::check_against_a_literal", "[writepaths]") {
    auto config = config_for("check_literal");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, age bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT age_pos CHECK (age > 0);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, age) VALUES (1, 5);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, age) VALUES (2, 42);", "UPDATE c.t SET age = 42 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, age) VALUES (3, -1);", "UPDATE c.t SET age = -1 WHERE id = 1;");

    auto cur = run(d, "SELECT id FROM c.t WHERE age < 0;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);

    CHECK(run(d, "UPDATE c.t SET id = 7 WHERE id = 1;")->is_success());
}

// The two sides of a CHECK are resolved independently, so a literal-on-the-left spelling needs its own case.
TEST_CASE("integration::cpp::test_constraint_write_paths::check_with_the_literal_on_the_left", "[writepaths]") {
    auto config = config_for("check_literal_left");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT npos CHECK (0 < n);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (1, 5);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, n) VALUES (2, 50);", "UPDATE c.t SET n = 50 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, n) VALUES (3, -5);", "UPDATE c.t SET n = -5 WHERE id = 1;");
}

// CHECK operands that aren't simple column-vs-literal; naive compilation can silently pass every row.
TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_computed_operands", "[writepaths]") {
    auto config = config_for("computed_operands");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT neg CHECK (n * -1 > 0);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (1, -5);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, n) VALUES (2, -20);", "UPDATE c.t SET n = -20 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, n) VALUES (3, 20);", "UPDATE c.t SET n = 20 WHERE id = 1;");
}

TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_a_function", "[writepaths]") {
    auto config = config_for("function");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT big CHECK (abs(n) > 10);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (1, 50);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, n) VALUES (2, -50);", "UPDATE c.t SET n = -50 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, n) VALUES (3, 5);", "UPDATE c.t SET n = 5 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, n) VALUES (4, -5);", "UPDATE c.t SET n = -5 WHERE id = 1;");
}

TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_a_case", "[writepaths]") {
    auto config = config_for("case");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(
        run(d, "ALTER TABLE c.t ADD CONSTRAINT pos CHECK ((CASE WHEN n > 0 THEN 1 ELSE 0 END) = 1);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (1, 5);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, n) VALUES (2, 7);", "UPDATE c.t SET n = 7 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, n) VALUES (3, -7);", "UPDATE c.t SET n = -7 WHERE id = 1;");
}

TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_a_set_and_a_pattern", "[writepaths]") {
    auto config = config_for("set_and_pattern");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint, s text);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT known CHECK (n IN (1, 2, 3));")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT named CHECK (s LIKE 'a%');")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n, s) VALUES (1, 1, 'alpha');")->is_success());

    both_accept(d,
                "INSERT INTO c.t (id, n, s) VALUES (2, 3, 'apple');",
                "UPDATE c.t SET n = 3, s = 'apple' WHERE id = 1;");
    both_reject(d,
                "INSERT INTO c.t (id, n, s) VALUES (3, 9, 'apple');",
                "UPDATE c.t SET n = 9, s = 'apple' WHERE id = 1;");
    both_reject(d,
                "INSERT INTO c.t (id, n, s) VALUES (4, 1, 'beta');",
                "UPDATE c.t SET n = 1, s = 'beta' WHERE id = 1;");
}

// Check text is stored as SQL and recompiled, so a bare identifier resolves as a column, never a value.
TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_two_columns", "[writepaths]") {
    auto config = config_for("check_two_columns");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, lo bigint, hi bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT ord CHECK (lo < hi);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, lo, hi) VALUES (1, 1, 10);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, lo, hi) VALUES (2, 2, 9);", "UPDATE c.t SET lo = 2, hi = 9 WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, lo, hi) VALUES (3, 10, 1);", "UPDATE c.t SET lo = 10, hi = 1 WHERE id = 1;");

    {
        INFO("writing `hi` is judged against the stored `lo`");
        CHECK(run(d, "UPDATE c.t SET hi = 0 WHERE id = 1;")->is_error());
    }
    {
        INFO("writing `lo` is judged against the stored `hi`");
        CHECK(run(d, "UPDATE c.t SET lo = 100 WHERE id = 1;")->is_error());
    }
    {
        auto cur = run(d, "SELECT lo, hi FROM c.t WHERE id = 1;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        INFO("a refused UPDATE leaves the stored row alone");
        CHECK(cur->value(column_of(cur, "lo"), 0).value<int64_t>() == 2);
        CHECK(cur->value(column_of(cur, "hi"), 0).value<int64_t>() == 9);
    }
}

// A value shorter than the array's declared size is padded with NULL, which the NOT NULL column can't hold.
TEST_CASE("integration::cpp::test_constraint_write_paths::not_null_fixed_array", "[writepaths]") {
    auto config = config_for("fixed_array");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, a bigint[3] NOT NULL);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, a) VALUES (1, ARRAY[1,2,3]);")->is_success());

    both_accept(d,
                "INSERT INTO c.t (id, a) VALUES (2, ARRAY[4,5,6]);",
                "UPDATE c.t SET a = ARRAY[4,5,6] WHERE id = 1;");
    both_reject(d, "INSERT INTO c.t (id, a) VALUES (3, ARRAY[8,8]);", "UPDATE c.t SET a = ARRAY[8,8] WHERE id = 1;");
}

// UNIQUE/PRIMARY KEY can't accept the same value on both paths -- writing one key twice is what they forbid.
TEST_CASE("integration::cpp::test_constraint_write_paths::unique", "[writepaths]") {
    auto config = config_for("unique");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, code bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT uq_code UNIQUE (code);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, code) VALUES (1, 100), (2, 200);")->is_success());

    both_accept(d, "INSERT INTO c.t (id, code) VALUES (3, 300);", "UPDATE c.t SET code = 400 WHERE id = 2;");
    both_reject(d, "INSERT INTO c.t (id, code) VALUES (4, 100);", "UPDATE c.t SET code = 100 WHERE id = 2;");
}

TEST_CASE("integration::cpp::test_constraint_write_paths::primary_key", "[writepaths]") {
    auto config = config_for("primary_key");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, label text);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT pk_id PRIMARY KEY (id);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, label) VALUES (100, 'gear'), (200, 'bolt');")->is_success());

    both_accept(d, "INSERT INTO c.t (id, label) VALUES (300, 'nut');", "UPDATE c.t SET id = 400 WHERE id = 200;");
    both_reject(d, "INSERT INTO c.t (id, label) VALUES (100, 'washer');", "UPDATE c.t SET id = 100 WHERE id = 400;");

    both_reject(d, "INSERT INTO c.t (id, label) VALUES (NULL, 'pin');", "UPDATE c.t SET id = NULL WHERE id = 400;");
}

// An unresolved FK addresses no column; a check reading no row reports success, indistinguishable from satisfied.
TEST_CASE("integration::cpp::test_constraint_write_paths::foreign_key", "[writepaths]") {
    auto config = config_for("foreign_key");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.parent (id bigint, name text);")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.child (id bigint, parent_id bigint);")->is_success());
    // Inline REFERENCES is silently dropped by the planner, so the constraint is added via ALTER TABLE instead.
    REQUIRE(run(d, "ALTER TABLE c.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
    REQUIRE(run(d,
                "ALTER TABLE c.child ADD CONSTRAINT child_fk FOREIGN KEY (parent_id) "
                "REFERENCES c.parent (id);")
                ->is_success());
    REQUIRE(run(d, "INSERT INTO c.parent (id, name) VALUES (1, 'one'), (2, 'two');")->is_success());
    REQUIRE(run(d, "INSERT INTO c.child (id, parent_id) VALUES (10, 1);")->is_success());

    both_accept(d,
                "INSERT INTO c.child (id, parent_id) VALUES (11, 2);",
                "UPDATE c.child SET parent_id = 2 WHERE id = 10;");
    both_reject(d,
                "INSERT INTO c.child (id, parent_id) VALUES (12, 999);",
                "UPDATE c.child SET parent_id = 999 WHERE id = 10;");

    auto cur = run(d, "SELECT id FROM c.child WHERE parent_id = 999;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);
}

// A violation in the last chunk is found after earlier chunks are already on disk and must be undone too.

TEST_CASE("integration::cpp::test_constraint_write_paths::insert_spanning_chunks_leaves_nothing", "[writepaths]") {
    auto config = spanning_config_for("insert_spanning_chunks");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT npos CHECK (n > 0);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (-1, 7);")->is_success());

    std::stringstream insert;
    insert << "INSERT INTO c.t (id, n) VALUES ";
    for (uint64_t i = 0; i < kSpanningRows; ++i) {
        insert << (i ? ", (" : "(") << i << ", " << (i + 1 == kSpanningRows ? "-1" : "5") << ")";
    }
    insert << ";";

    const auto flushes_before = services::collection::executor::dml_flush_count();
    CHECK(run(d, insert.str())->is_error());
    INFO("the violating row must land after at least one chunk has already been flushed");
    CHECK(services::collection::executor::dml_flush_count() - flushes_before >= 1);

    auto cur = run(d, "SELECT COUNT(id) AS c FROM c.t;");
    REQUIRE(cur->is_success());
    CHECK(cur->value(0, 0).value<uint64_t>() == 1u);
}

TEST_CASE("integration::cpp::test_constraint_write_paths::update_spanning_chunks_leaves_nothing", "[writepaths]") {
    auto config = spanning_config_for("update_spanning_chunks");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT npos CHECK (n > 0);")->is_success());

    std::stringstream seed;
    seed << "INSERT INTO c.t (id, n) VALUES ";
    for (uint64_t i = 0; i < kSpanningRows; ++i) {
        seed << (i ? ", (" : "(") << i << ", " << (i + 1 == kSpanningRows ? "1" : "10") << ")";
    }
    seed << ";";
    REQUIRE(run(d, seed.str())->is_success());

    const auto flushes_before = services::collection::executor::dml_flush_count();
    CHECK(run(d, "UPDATE c.t SET n = n - 1;")->is_error());
    INFO("the violating row must land after at least one chunk has already been flushed");
    CHECK(services::collection::executor::dml_flush_count() - flushes_before >= 1);

    {
        auto cur = run(d, "SELECT COUNT(id) AS c FROM c.t;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<uint64_t>() == kSpanningRows);
    }
    {
        auto cur = run(d, "SELECT COUNT(id) AS c FROM c.t WHERE n = 10;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<uint64_t>() == kSpanningRows - 1);
    }
    {
        auto cur = run(d, "SELECT n FROM c.t WHERE id = 0;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 10);
    }
}
