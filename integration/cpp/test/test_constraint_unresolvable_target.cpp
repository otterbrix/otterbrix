// A constraint naming something that isn't there must be refused, not silently accepted and left unenforced --
// same class as `REFERENCES parent` with the column list omitted (see test_fk_omitted_ref_columns.cpp).

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

    void seed_pair(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.parent (id bigint, val text);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.child (id bigint, pid bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE cur.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.parent (id, val) VALUES (1, 'p1');")->is_success());
    }

} // namespace

// PostgreSQL refuses this outright; Otterbrix wrote a dead pg_constraint row and reported success.
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

// conkey is read positionally, so dropping the unmatched name would enforce a different (empty) column set.
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

// Accepted today; the declared key doesn't exist, so two identical rows go in under it unenforced.
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

// The declared key (id, nosuchcol) narrows to (id) alone in pg_constraint, silently changing what's accepted.
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

// relkind='g' columns use a different attoid sequence than pg_attribute, so the group can never match by resolve.
// The assertion is on the PAIR of outcomes: DDL-accepted AND duplicate-admitted together is the one illegal answer.
TEST_CASE("integration::cpp::constraint_unresolvable_target::unique_on_dynamic_schema_is_never_a_no_op") {
    auto config = make_test_config(integration_fixture_path("test_constraint_unresolvable_target/unique_dynamic"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
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

// Sentinel for the UNIQUE/PK group resolution's length guard in operator_resolve_constraint, on the path every
// keyed INSERT takes; sensitivity was proven by injecting a mis-keyed attoid comparison, which turned this case red.
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
