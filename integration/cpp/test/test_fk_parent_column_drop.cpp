// ============================================================================
// DROPPING THE PARENT COLUMN OF A LIVE FOREIGN KEY MUST NOT BRICK THE CHILD.
//
// build_create_constraint_writes emitted per-column pg_depend edges for conkey
// only — the REFERENCING (child) columns. The REFERENCED (parent) columns named
// by confkey got no per-column edge at all, just the table-level
// constraint -> ref_table 'n' row. operator_alter_column_drop_t discovers what
// depends on a column by reading pg_depend keyed on
// (refclassid = pg_attribute, refobjid = attoid), so for a parent column that
// read came back EMPTY and the drop was accepted.
//
// What that produced: with `fk_pid FOREIGN KEY (pid) REFERENCES parent (id)`
// alive, `ALTER TABLE parent DROP COLUMN id` succeeded, and from then on EVERY
// insert into the child — including perfectly valid ones — died in
// operator_fk_check's parent probe with "keyed read: table has no column id".
// The table was bricked, and the message named the symptom (a read of a column
// that is not there) in a table the user had not touched, never the cause (a
// column dropped yesterday in the OTHER table).
//
// The fix mirrors the child side exactly: confkey now gets the same per-column
// pg_depend edges conkey always had, written with deptype 'n' (a NORMAL,
// cross-table dependency) rather than the 'i' used for the constraint's own
// columns. operator_alter_column_drop_t refuses a drop blocked by such an edge
// and names the constraint and the table that owns it.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {

    // parent(id, name) + child(id, pid) with a PRIMARY KEY on the parent and a
    // FOREIGN KEY on the child. The PK matters: it puts a SAME-TABLE dependent
    // ('i' edge, conkey) on parent.id next to the cross-table FK dependent, so
    // the refusal below has to be selective rather than "any dependent blocks".
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE fkdrop;")->is_success());
        REQUIRE(exec("CREATE TABLE fkdrop.parent (id bigint, name text);")->is_success());
        REQUIRE(exec("CREATE TABLE fkdrop.child (id bigint, pid bigint);")->is_success());
        REQUIRE(exec("ALTER TABLE fkdrop.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec("ALTER TABLE fkdrop.child ADD CONSTRAINT fk_pid FOREIGN KEY (pid) "
                     "REFERENCES fkdrop.parent (id);")
                    ->is_success());
        REQUIRE(exec("INSERT INTO fkdrop.parent (id, name) VALUES (1, 'one'), (2, 'two');")->is_success());
        REQUIRE(exec("INSERT INTO fkdrop.child (id, pid) VALUES (10, 1);")->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::test_fk_parent_column_drop::referenced_parent_column_cannot_be_dropped", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/referenced"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN id;");
        // get_error() on a successful cursor throws, so read it only when there is
        // one — otherwise the assertion that matters is drowned by the exception.
        const std::string what = cur->is_error()
                                     ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                     : std::string{"none"};
        INFO("error: " << what);
        INFO("a column a live FOREIGN KEY references may not be dropped out from under it");
        REQUIRE(cur->is_error());
        // Rule 6: the refusal has to name the real cause. "column has dependent
        // objects" would be true and useless — the user needs the constraint and
        // the table that owns it, neither of which is the table being altered.
        CHECK(what.find("fk_pid") != std::string::npos);
        CHECK(what.find("child") != std::string::npos);
        CHECK(what.find("id") != std::string::npos);
    }

    // The refusal must be total: a rejected DROP writes nothing, so the column,
    // the constraint and the data all survive intact.
    {
        auto cur = exec("SELECT id, name FROM fkdrop.parent WHERE id = 1;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
    {
        // THE BRICK. Before the fix this insert — a valid row pointing at a live
        // parent — failed with "keyed read: table has no column id".
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (11, 2);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        INFO("a valid child insert must still be accepted after the refused DROP");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT id FROM fkdrop.child;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        // And the constraint is still ENFORCED, not merely still listed.
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (12, 999);");
        INFO("a child row pointing at a missing parent must still be rejected");
        CHECK(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::unreferenced_parent_column_still_drops", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/unreferenced"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // The other half of the guard: the new edge must block ONLY the referenced
    // column. parent.name carries no dependent at all and must still drop.
    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN name;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (11, 2);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::child_key_column_drop_takes_the_constraint_with_it",
          "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/child_side"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // Neighbouring case: the CHILD side already had its per-column 'i' edges, and
    // dropping the column an FK is DECLARED on cascades the constraint away — the
    // constraint cannot outlive its own key column. That behaviour is the model
    // the parent side was measured against, so pin it.
    {
        auto cur = exec("ALTER TABLE fkdrop.child DROP COLUMN pid;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id) VALUES (13);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    // With the FK gone, the parent column it referenced is droppable again.
    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN id;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::dropping_the_parent_table_clears_the_constraint",
          "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/drop_parent_table"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // Neighbouring case: DROP TABLE parent. The cascade walk seeds at
    // (pg_class, parent_oid) and reaches the constraint through the table-level
    // 'n' edge, and deletes_for_classid(pg_class) also clears pg_constraint by
    // confrelid — so the FK goes with the table and the child stays usable.
    {
        auto cur = exec("DROP TABLE fkdrop.parent;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (14, 999);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        INFO("the constraint went with the parent table; the child must not be left bricked");
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::drop_constraint_does_not_claim_success", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/drop_constraint"));
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // Neighbouring case: ALTER TABLE ... DROP CONSTRAINT. The grammar parses it
    // (AT_DropConstraint), but the transformer had no case for it, so it fell
    // through to `default:` and the statement reported SUCCESS having removed
    // nothing — the constraint, and every pg_depend edge under it, stayed. A
    // statement that removes nothing must not report that it removed something.
    {
        auto cur = exec("ALTER TABLE fkdrop.child DROP CONSTRAINT fk_pid;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    // Proof the constraint really is untouched: it still enforces.
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (15, 999);");
        CHECK(cur->is_error());
    }
}
