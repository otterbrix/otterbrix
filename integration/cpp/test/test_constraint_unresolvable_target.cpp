// ============================================================================
// A CONSTRAINT THAT NAMES SOMETHING THAT IS NOT THERE MUST BE REFUSED, NOT
// ACCEPTED AND THEN QUIETLY NOT ENFORCED.
//
// Same class as `REFERENCES parent` with the column list omitted (see
// test_fk_omitted_ref_columns.cpp): the user declares integrity, the engine
// answers "ok", and nothing is guarded. Four ways to reach it, all through
// plain SQL:
//
//   (1) `REFERENCES nosuchtable` — the referenced table never resolved, so
//       enrich skipped the WHOLE FK branch and the planner still wrote a
//       pg_constraint row with an INVALID confrelid and an EMPTY confkey.
//       operator_resolve_constraint needs BOTH name lists, so it drops that
//       row: orphans go in, ON DELETE RESTRICT lets the parent go.
//
//   (2) `FOREIGN KEY (nosuchcol)` / `REFERENCES parent (nosuchcol)` — the
//       column-name → attoid loops in enrich appended nothing for a name that
//       matched nothing, leaving conkey / confkey SHORTER than declared. At
//       length 0 the constraint enforces nothing; shorter than declared it
//       enforces a DIFFERENT constraint, because both lists are read
//       positionally from there on.
//
//   (3) `UNIQUE (nosuchcol)` / `PRIMARY KEY (nosuchcol)` — the same enrich loop,
//       the same silence: an empty conkey is never even decoded by
//       operator_resolve_constraint (it skips empty groups).
//
//   (4) UNIQUE / PRIMARY KEY on a dynamic-schema (relkind='g') table: resolves at
//       DDL time and dies at DML time. A schemaless table has NO pg_attribute
//       rows — its columns live in pg_computed_column, with attoids from a
//       different sequence — so conkey holds attoids the resolve step's
//       pg_attribute read can never match. The group was dropped from the
//       constraint set without a word, and duplicates went straight in under a
//       declared UNIQUE. FOREIGN KEY and CHECK are already refused on such tables
//       for exactly this reason (stable attoids); the key constraints were not.
//
// The last case is also the one reachable route into the UNIQUE/PK group-drop in
// operator_resolve_constraint. With it refused at DDL that drop becomes a last
// line of defence for a catalog already holding such a row — so the loud guard
// there is watched by the success-path sentinel at the bottom of this file.
// ============================================================================

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

using namespace test_helpers;

namespace {

    std::vector<int64_t> column_i64(const components::cursor::cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    void require_ids(otterbrix::wrapper_dispatcher_t* d, const std::string& table, const std::vector<int64_t>& ids) {
        auto cur = exec(d, "SELECT id FROM " + table + " ORDER BY id;");
        INFO(table << " read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == ids);
    }

    // parent(id, val) + child(id, pid), both real, both empty of constraints.
    void seed_pair(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.parent (id bigint, val text);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.child (id bigint, pid bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE cur.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.parent (id, val) VALUES (1, 'p1');")->is_success());
    }

} // namespace

// (1) REFERENCES a table that does not exist. PostgreSQL answers
// `relation "nosuchtable" does not exist` and writes nothing. Otterbrix wrote a
// dead pg_constraint row and reported success.
TEST_CASE("integration::cpp::constraint_unresolvable_target::references_missing_table_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/missing_table"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed_pair(d);

    auto ddl = exec(d,
                    "ALTER TABLE cur.child ADD CONSTRAINT fk_ghost "
                    "FOREIGN KEY (pid) REFERENCES cur.nosuchtable (id);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_error());
    const std::string what{ddl->get_error().what};
    INFO("the message must name the relation the user misspelled");
    REQUIRE(what.find("nosuchtable") != std::string::npos);

    INFO("nothing half-landed: the child table is still usable");
    REQUIRE(exec(d, "INSERT INTO cur.child (id, pid) VALUES (10, 999);")->is_success());
    require_ids(d, "cur.child", {10});

    INFO("and the same constraint spelled against the real parent still works, and enforces");
    REQUIRE(exec(d,
                 "ALTER TABLE cur.child ADD CONSTRAINT fk_real "
                 "FOREIGN KEY (pid) REFERENCES cur.parent (id);")
                ->is_success());
    CHECK(exec(d, "INSERT INTO cur.child (id, pid) VALUES (11, 998);")->is_error());
    REQUIRE(exec(d, "INSERT INTO cur.child (id, pid) VALUES (12, 1);")->is_success());
    require_ids(d, "cur.child", {10, 12});
}

// (2a) The REFERENCING column list names a column the child does not have. The
// list is read positionally from conkey onwards, so dropping the unmatched name
// silently produces a constraint on a different (here: an empty) column set.
TEST_CASE("integration::cpp::constraint_unresolvable_target::fk_referencing_missing_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/fk_child_col"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed_pair(d);

    auto ddl = exec(d,
                    "ALTER TABLE cur.child ADD CONSTRAINT fk_badchild "
                    "FOREIGN KEY (nosuchcol) REFERENCES cur.parent (id);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_error());
    const std::string what{ddl->get_error().what};
    REQUIRE(what.find("nosuchcol") != std::string::npos);
}

// (2b) The REFERENCED column list names a column the parent does not have.
TEST_CASE("integration::cpp::constraint_unresolvable_target::fk_referenced_missing_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/fk_parent_col"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed_pair(d);

    auto ddl = exec(d,
                    "ALTER TABLE cur.child ADD CONSTRAINT fk_badparent "
                    "FOREIGN KEY (pid) REFERENCES cur.parent (nosuchcol);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_error());
    const std::string what{ddl->get_error().what};
    REQUIRE(what.find("nosuchcol") != std::string::npos);
}

// (3) UNIQUE over a column that does not exist. Accepted today, and the key it
// declares does not exist: two identical rows go in under it.
TEST_CASE("integration::cpp::constraint_unresolvable_target::unique_on_missing_column_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/unique_col"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, v bigint);")->is_success());

    auto ddl = exec(d, "ALTER TABLE cur.t ADD CONSTRAINT uq_ghost UNIQUE (nosuchcol);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_error());
    const std::string what{ddl->get_error().what};
    REQUIRE(what.find("nosuchcol") != std::string::npos);

    INFO("a PRIMARY KEY spelled the same way is refused the same way");
    auto pk = exec(d, "ALTER TABLE cur.t ADD CONSTRAINT pk_ghost PRIMARY KEY (alsonotacolumn);");
    INFO("ADD CONSTRAINT result: " << (pk->is_error() ? pk->get_error().what : "accepted"));
    REQUIRE(pk->is_error());

    INFO("the table is untouched by either refusal");
    REQUIRE(exec(d, "INSERT INTO cur.t (id, v) VALUES (1, 10);")->is_success());
    REQUIRE(exec(d, "INSERT INTO cur.t (id, v) VALUES (2, 20);")->is_success());
}

// (3b) A partially resolvable list is the sharper half of the same defect: the
// declared key is (id, nosuchcol), the row that lands in pg_constraint says
// (id). Silently narrowing a key changes which rows the table will accept.
TEST_CASE("integration::cpp::constraint_unresolvable_target::unique_partially_resolvable_list_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/unique_partial"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, v bigint);")->is_success());

    auto ddl = exec(d, "ALTER TABLE cur.t ADD CONSTRAINT uq_partial UNIQUE (id, nosuchcol);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));
    REQUIRE(ddl->is_error());
    const std::string what{ddl->get_error().what};
    REQUIRE(what.find("nosuchcol") != std::string::npos);

    INFO("and the narrowed key must not be in force: two rows sharing id are still legal");
    REQUIRE(exec(d, "INSERT INTO cur.t (id, v) VALUES (1, 10);")->is_success());
    REQUIRE(exec(d, "INSERT INTO cur.t (id, v) VALUES (1, 20);")->is_success());
}

// (4) UNIQUE on a dynamic-schema (relkind='g') table. Its columns live in
// pg_computed_column, so the attoids enrich writes into conkey are from a
// different sequence than the pg_attribute rows the resolve step reads — the
// group can never be matched, so it must not be dropped in silence.
//
// The assertion is deliberately on the PAIR, not on which half gives: what must
// never happen is "the DDL is accepted AND the duplicate goes in". Refusing the
// DDL is one legal answer, enforcing the key is the other.
TEST_CASE("integration::cpp::constraint_unresolvable_target::unique_on_dynamic_schema_is_never_a_no_op") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/unique_dynamic"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    // No column list: relkind='g', schema inferred from the rows.
    REQUIRE(exec(d, "CREATE TABLE cur.docs();")->is_success());
    REQUIRE(exec(d, "INSERT INTO cur.docs (id, v) VALUES (1, 10);")->is_success());

    auto ddl = exec(d, "ALTER TABLE cur.docs ADD CONSTRAINT uq_docs_id UNIQUE (id);");
    INFO("ADD CONSTRAINT result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto dup = exec(d, "INSERT INTO cur.docs (id, v) VALUES (1, 20);");
    INFO("duplicate INSERT result: " << (dup->is_error() ? dup->get_error().what : "accepted"));
    INFO("a UNIQUE that was accepted must be enforced; one that cannot be must be refused");
    const bool accepted_and_ignored = ddl->is_success() && dup->is_success();
    CHECK_FALSE(accepted_and_ignored);

    INFO("and refusing it must not brick the table: unrelated rows still go in");
    auto other = exec(d, "INSERT INTO cur.docs (id, v) VALUES (2, 30);");
    INFO("follow-up INSERT result: " << (other->is_error() ? other->get_error().what : "accepted"));
    REQUIRE(other->is_success());
}

// SENTINEL for the UNIQUE / PRIMARY KEY group resolution in
// operator_resolve_constraint. Its length guard ("every conkey attoid must
// resolve to a live column name") sits on the path every keyed INSERT takes, so
// a defect that makes the resolution fail turns this case red instead of
// letting the key quietly stop existing.
//
// Sensitivity was proven by injection. Mis-keying the attoid comparison in the
// UNIQUE/PK loop so nothing can match it (`row_attoid == wanted_oid` →
// `row_attoid == wanted_oid + 1000000`) turns the first INSERT below red with
// the guard's own words:
//
//   primary key constraint "t_pk": key column list cannot be resolved — a
//   column it is declared on has no live pg_attribute row
//
// Before the guard, the same injection left this test GREEN on that line and
// let the duplicate id through instead. A near-miss injection (`+ 1`) is a
// different defect — it binds the NEXT column's name, so the lists still agree
// in length — and this case catches that one too, at the duplicate-id check.
TEST_CASE("integration::cpp::constraint_unresolvable_target::resolvable_key_constraints_stay_enforced") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/sentinel"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint, v bigint);")->is_success());
    REQUIRE(exec(d, "ALTER TABLE cur.t ADD CONSTRAINT t_pk PRIMARY KEY (id);")->is_success());
    REQUIRE(exec(d, "ALTER TABLE cur.t ADD CONSTRAINT t_uq UNIQUE (code);")->is_success());

    INFO("the first row goes in through both constraints");
    {
        auto cur = exec(d, "INSERT INTO cur.t (id, code, v) VALUES (1, 100, 7);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    INFO("a distinct row goes in too");
    {
        auto cur = exec(d, "INSERT INTO cur.t (id, code, v) VALUES (2, 200, 8);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    INFO("the PRIMARY KEY still rejects a duplicate id");
    CHECK(exec(d, "INSERT INTO cur.t (id, code, v) VALUES (1, 300, 9);")->is_error());
    INFO("the UNIQUE still rejects a duplicate code");
    CHECK(exec(d, "INSERT INTO cur.t (id, code, v) VALUES (3, 100, 9);")->is_error());

    require_ids(d, "cur.t", {1, 2});
}
