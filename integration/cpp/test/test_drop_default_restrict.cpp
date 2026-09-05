// ============================================================================
// THE UNWRITTEN DROP DEFAULTS TO RESTRICT — PostgreSQL parity (queue #209/#638).
//
// gram.y's opt_drop_behavior yields DROP_RESTRICT for both the written word and
// the empty alternative — in PostgreSQL they are the same thing, so the two are
// deliberately one token. drop_behavior_of reads that token as restrict_, and a
// bare DROP is refused when a 'n' (normal) pg_depend edge lands on the seed.
//
// The one statement form that stays implicitly CASCADE is DROP DATABASE: its
// grammar takes no behavior at all ("This is implicitly CASCADE", gram.y
// ~:11047), so transform_drop_database stamps cascade_ explicitly — the control
// case below is what fails if that stamp is ever lost.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_default_restrict/") + leaf).string();
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    void seed_fk_pair(otterbrix::wrapper_dispatcher_t* d) {
        run_ok(d, "CREATE DATABASE ddr;");
        run_ok(d, "CREATE TABLE ddr.parent (id bigint, name text);");
        run_ok(d, "CREATE TABLE ddr.child (id bigint, pid bigint);");
        run_ok(d, "ALTER TABLE ddr.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);");
        run_ok(d,
               "ALTER TABLE ddr.child ADD CONSTRAINT fk_pid FOREIGN KEY (pid) "
               "REFERENCES ddr.parent (id);");
        run_ok(d, "INSERT INTO ddr.parent (id, name) VALUES (1, 'one');");
        run_ok(d, "INSERT INTO ddr.child (id, pid) VALUES (10, 1);");
    }

} // namespace

// A bare DROP TABLE on a table a live FOREIGN KEY references is refused, and the
// refusal leaves the parent fully alive; the same statement WITH CASCADE drops it.
TEST_CASE("integration::cpp::drop_default_restrict::bare_drop_table_refuses_fk_parent", "[dropdefault]") {
    auto config = make_test_config(fixture_path("fk_parent"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_fk_pair(d);

    {
        auto cur = exec(d, "DROP TABLE ddr.parent;");
        INFO("a bare DROP TABLE must be read as RESTRICT and refuse the FK child's 'n' edge");
        REQUIRE(cur->is_error());
    }

    // The refusal touched nothing: the parent answers, and the FK still enforces.
    REQUIRE(run_ok(d, "SELECT id FROM ddr.parent;")->size() == 1);
    {
        auto bad = exec(d, "INSERT INTO ddr.child (id, pid) VALUES (11, 999);");
        INFO("the FK the refusal protected must still be in force");
        REQUIRE(bad->is_error());
    }

    // The written word still cascades.
    run_ok(d, "DROP TABLE ddr.parent CASCADE;");
    auto gone = exec(d, "SELECT id FROM ddr.parent;");
    CHECK_FALSE(gone->is_success());
}

// Control: a table with only its own dependents — PK constraint ('i' edges) and
// an index ('a' edge) — is droppable bare: auto/internal children never block.
TEST_CASE("integration::cpp::drop_default_restrict::own_dependents_do_not_block", "[dropdefault]") {
    auto config = make_test_config(fixture_path("own_deps"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE ddr;");
    run_ok(d, "CREATE TABLE ddr.solo (id bigint, v text);");
    run_ok(d, "ALTER TABLE ddr.solo ADD CONSTRAINT solo_pk PRIMARY KEY (id);");
    run_ok(d, "CREATE INDEX solo_v_idx ON ddr.solo (v);");
    run_ok(d, "INSERT INTO ddr.solo (id, v) VALUES (1, 'x');");

    run_ok(d, "DROP TABLE ddr.solo;");
    auto gone = exec(d, "SELECT id FROM ddr.solo;");
    CHECK_FALSE(gone->is_success());
}

// Control: bare ALTER TABLE ... DROP COLUMN keeps working when the column's only
// dependents are an index ('a') and its own PK membership ('i') — the sole 'n'
// edge a column can carry is an FK parent reference, and THAT refuses under
// every behavior (test_fk_parent_column_drop.cpp pins it).
TEST_CASE("integration::cpp::drop_default_restrict::bare_drop_column_with_index_still_drops", "[dropdefault]") {
    auto config = make_test_config(fixture_path("drop_column"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE ddr;");
    run_ok(d, "CREATE TABLE ddr.t (a bigint, b bigint);");
    run_ok(d, "CREATE INDEX t_b_idx ON ddr.t (b);");
    run_ok(d, "INSERT INTO ddr.t (a, b) VALUES (1, 2);");

    run_ok(d, "ALTER TABLE ddr.t DROP COLUMN b;");
    auto after = exec(d, "SELECT b FROM ddr.t;");
    CHECK_FALSE(after->is_success());
    REQUIRE(run_ok(d, "SELECT a FROM ddr.t;")->size() == 1);
}

// Control: DROP DATABASE takes no behavior word in the grammar and is implicitly
// CASCADE — a populated database still drops bare. Loses red the day
// transform_drop_database stops stamping cascade_.
TEST_CASE("integration::cpp::drop_default_restrict::drop_database_stays_implicit_cascade", "[dropdefault]") {
    auto config = make_test_config(fixture_path("drop_db"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE ddr;");
    run_ok(d, "CREATE TABLE ddr.t (a bigint);");
    run_ok(d, "INSERT INTO ddr.t (a) VALUES (1);");

    run_ok(d, "DROP DATABASE ddr;");
    auto gone = exec(d, "SELECT a FROM ddr.t;");
    CHECK_FALSE(gone->is_success());
}
