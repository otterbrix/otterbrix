#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// A foreign key must be checked on UPDATE, not only on INSERT.
//
// enrich_logical_plan resolves each foreign key's child column NAMES into positions in the chunk.
// It used to do that only on the INSERT branch, leaving child_col_indices empty on UPDATE — and
// operator_fk_check computes `has_absent = indices.empty()` and skips every row it cannot address,
// which leaves qcount at zero, and a zero qualifying count is the operator's success path. The check
// ran, read nothing, and reported that everything was fine, so the most ordinary way a user breaks
// referential integrity — pointing an existing row at a parent that does not exist — was accepted
// silently while INSERT of the same value was rejected.

namespace {
    void make_parent_and_child(otterbrix::wrapper_dispatcher_t* d) {
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE fk;")->is_success());
        REQUIRE(exec("CREATE TABLE fk.parent (id bigint, name text);")->is_success());
        REQUIRE(exec("CREATE TABLE fk.child (id bigint, parent_id bigint);")->is_success());
        // Inline REFERENCES is silently dropped by the planner, so the constraint has to be added
        // the long way round.
        REQUIRE(exec("ALTER TABLE fk.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec("ALTER TABLE fk.child ADD CONSTRAINT child_fk FOREIGN KEY (parent_id) "
                     "REFERENCES fk.parent (id);")
                    ->is_success());
        REQUIRE(exec("INSERT INTO fk.parent (id, name) VALUES (1, 'one'), (2, 'two');")->is_success());
        REQUIRE(exec("INSERT INTO fk.child (id, parent_id) VALUES (10, 1), (11, 2);")->is_success());
    }
} // namespace

TEST_CASE("integration::cpp::test_fk_check_on_update::update_to_a_missing_parent_is_rejected", "[fkupdate]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_fk_update/missing_parent");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    make_parent_and_child(d);

    // Control: the same value through INSERT must be rejected too — the two paths have to agree, or
    // the UPDATE result below is a design choice about this constraint rather than a defect.
    {
        auto cur = exec("INSERT INTO fk.child (id, parent_id) VALUES (12, 999);");
        INFO("INSERT with a missing parent must fail");
        REQUIRE(cur->is_error());
    }

    {
        auto cur = exec("UPDATE fk.child SET parent_id = 999 WHERE id = 10;");
        INFO("UPDATE to a missing parent must fail the same way");
        CHECK(cur->is_error());
    }

    // And the row must not have been changed by a statement that should not have been allowed.
    {
        auto cur = exec("SELECT id FROM fk.child WHERE parent_id = 999;");
        REQUIRE(cur->is_success());
        INFO("no child row may point at a parent that does not exist");
        CHECK(cur->size() == 0);
    }
}

TEST_CASE("integration::cpp::test_fk_check_on_update::update_to_an_existing_parent_still_works", "[fkupdate]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_fk_update/existing_parent");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    make_parent_and_child(d);

    // The other half of the guard: turning the check on must not start rejecting valid updates.
    {
        auto cur = exec("UPDATE fk.child SET parent_id = 2 WHERE id = 10;");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT id FROM fk.child WHERE parent_id = 2;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    // An update that does not touch the key column must stay unaffected too.
    {
        auto cur = exec("UPDATE fk.child SET id = 20 WHERE id = 11;");
        REQUIRE(cur->is_success());
    }
}
