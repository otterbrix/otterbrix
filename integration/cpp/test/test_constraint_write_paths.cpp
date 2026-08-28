#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/vector/indexing_vector.hpp>
#include <services/collection/executor.hpp>
#include <sstream>
#include <string>

// Every constraint must give the SAME answer whichever statement presents the row.
//
// INSERT and UPDATE reach storage down separate plans, and a constraint is wired into each of
// those plans separately, so one path can enforce a constraint while the other lets it through.
// A case that exercises a single path cannot tell "this constraint is enforced" apart from
// "nothing is checked anywhere".
//
// So every constraint the write path enforces has a case here, and every case asks both paths the
// same question: a value they must both accept, and a value they must both reject. The INSERT and
// the UPDATE are written out side by side so the pair is visible. A newly supported constraint
// belongs here, not in a case covering whichever path it was implemented on first.
//
// Only WHETHER the write was refused is asserted, never the error text or code. A constraint may
// legitimately be caught anywhere between validation and the table itself, and each of those sites
// words its refusal differently.

namespace {

    components::cursor::cursor_t_ptr run(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    // Both write paths must ACCEPT the row they write.
    void
    both_accept(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& insert, const std::string& update) {
        INFO("must be accepted: " << insert);
        CHECK(run(dispatcher, insert)->is_success());

        INFO("must be accepted: " << update);
        CHECK(run(dispatcher, update)->is_success());
    }

    // Both write paths must REJECT the row they write.
    void
    both_reject(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& insert, const std::string& update) {
        INFO("must be rejected: " << insert);
        CHECK(run(dispatcher, insert)->is_error());

        INFO("must be rejected: " << update);
        CHECK(run(dispatcher, update)->is_error());
    }

    configuration::config config_for(const std::string& name) {
        auto config = test_create_config("/tmp/otterbrix/integration/test_constraint_write_paths/" + name);
        test_clear_directory(config);
        config.disk.on = true;
        config.wal.on = false;
        config.log.level = log_t::level::off;
        return config;
    }

    // A write large enough that the sink cannot hold it in one piece: the row set spans several
    // chunks, and the mid-pump gate flushes each one to storage as it fills. Rows are addressed by
    // an `id` running 0..kSpanningRows-1, so "the last chunk" is the high end of that range.
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

    // A refused write leaves nothing behind, on either path.
    auto cur = run(d, "SELECT id FROM c.t WHERE age < 0;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);

    // A constraint must not reject writes it has nothing to say about.
    CHECK(run(d, "UPDATE c.t SET id = 7 WHERE id = 1;")->is_success());
}

// The literal may sit on either side of the operator. The two sides are resolved independently, so
// the constant-on-the-left spelling has to hold on both paths as well.
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

// A CHECK whose operands are computed rather than named. Every one of these was admitted into the
// catalog and then passed every row, because the compiler that read them back understood only
// column-against-literal and quietly answered "true" for anything else.
TEST_CASE("integration::cpp::test_constraint_write_paths::check_over_computed_operands", "[writepaths]") {
    auto config = config_for("computed_operands");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT neg CHECK (n * -1 > 0);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (1, -5);")->is_success());

    // -20 * -1 = 20, which is positive; 20 * -1 = -20, which is not.
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

    // The magnitude is what the constraint reads, so either sign passes when it is large enough.
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

// A CHECK whose two sides are both columns. The check text is stored as SQL and recompiled against
// the write-set, so each side has to be resolved as a column in its own right — a bare identifier
// is a column reference, never a value.
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

    // Only UPDATE can write ONE of the two columns and leave the other alone, so only UPDATE can be
    // asked whether the constraint reads the STORED value of the column it did not write.
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
        CHECK(cur->value(cur->column_index("lo"), 0).value<int64_t>() == 2);
        CHECK(cur->value(cur->column_index("hi"), 0).value<int64_t>() == 9);
    }
}

// A fixed-size ARRAY column declared NOT NULL. A value shorter than the declared size is reconciled
// by padding NULL, which the column cannot hold, so it has to be refused before the append. Both
// paths reconcile a short value the same way, so both owe the same refusal.
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

// UNIQUE and PRIMARY KEY are the constraints whose accepted value cannot be the same on both paths
// — writing one key twice is precisely what they forbid. The accepted pair therefore writes two
// different fresh keys; the rejected pair aims both paths at the one key already taken.
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

    // A PRIMARY KEY is NOT NULL as well, on both paths.
    both_reject(d, "INSERT INTO c.t (id, label) VALUES (NULL, 'pin');", "UPDATE c.t SET id = NULL WHERE id = 400;");
}

// A foreign key is resolved from child column names to positions in the write-set chunk before it
// can be checked, and that resolution is per-path. An unresolved key addresses no column, and a
// check that reads no row reports success — so an unresolved key is indistinguishable from a
// satisfied one, which is what makes asking both paths worth the lines.
TEST_CASE("integration::cpp::test_constraint_write_paths::foreign_key", "[writepaths]") {
    auto config = config_for("foreign_key");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.parent (id bigint, name text);")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.child (id bigint, parent_id bigint);")->is_success());
    // Inline REFERENCES is silently dropped by the planner, so the constraint is added the long way
    // round.
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

    // A refused write leaves no child pointing at a parent that does not exist.
    auto cur = run(d, "SELECT id FROM c.child WHERE parent_id = 999;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);
}

// A write whose rows do not fit in one chunk reaches storage in pieces: the sink flushes each
// full chunk as the pump fills it, and the constraint is only judged once the whole row set has
// arrived. So a violation in the LAST chunk is found after earlier chunks are already on disk,
// and every one of those appends has to be lifted again — a statement either applies whole or
// leaves nothing at all. Both write paths flush the same way, and UPDATE has more to undo: it
// writes new rows AND marks the old ones deleted, so both halves must come back.

TEST_CASE("integration::cpp::test_constraint_write_paths::insert_spanning_chunks_leaves_nothing", "[writepaths]") {
    auto config = spanning_config_for("insert_spanning_chunks");
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(run(d, "CREATE DATABASE c;")->is_success());
    REQUIRE(run(d, "CREATE TABLE c.t (id bigint, n bigint);")->is_success());
    REQUIRE(run(d, "ALTER TABLE c.t ADD CONSTRAINT npos CHECK (n > 0);")->is_success());
    REQUIRE(run(d, "INSERT INTO c.t (id, n) VALUES (-1, 7);")->is_success());

    // One statement, kSpanningRows rows, with the only violating row last.
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

    // Nothing of the failed statement survives: only the row that was there beforehand.
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

    // Every row holds n = 10 except the last, which holds 1. Decrementing every row therefore
    // takes only that last row out of the constraint, once the rest have already been written.
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

    // Every row still holds what it did: no row was decremented, and none was left deleted by the
    // half of the UPDATE that had already run.
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
