// The transformer copied an absent pk_attrs (omitted REFERENCES column list) as an EMPTY ref list; enrich
// resolved that to an empty confkey, and operator_resolve_constraint silently drops any row where either
// name list is empty -- so the declared constraint simply never existed. PostgreSQL instead resolves the
// omitted list to the referenced table's primary key, or refuses the DDL if it has none.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

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

    // Seeded before the constraint exists, so these inserts never run through the FK check themselves.
    void seed(otterbrix::wrapper_dispatcher_t* d, const std::string& del_action) {
        REQUIRE(exec(d, "CREATE DATABASE fkr;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE fkr.parent (id bigint, val text);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE fkr.child (id bigint, pid bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE fkr.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec(d, "INSERT INTO fkr.parent (id, val) VALUES (1, 'p1');")->is_success());
        REQUIRE(exec(d, "INSERT INTO fkr.child (id, pid) VALUES (10, 1);")->is_success());
        auto cur = exec(d,
                        "ALTER TABLE fkr.child ADD CONSTRAINT fk_no_list "
                        "FOREIGN KEY (pid) REFERENCES fkr.parent ON DELETE " +
                            del_action + ";");
        INFO("ADD CONSTRAINT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::fk_omitted_ref_columns::orphan_insert_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_fk_omitted_ref_columns/orphan_insert"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d, "NO ACTION");

    INFO("pid=999 exists in no parent row: the INSERT must fail");
    {
        auto cur = exec(d, "INSERT INTO fkr.child (id, pid) VALUES (11, 999);");
        CHECK(cur->is_error());
    }

    INFO("the orphan must not be in the table either way");
    require_ids(d, "fkr.child", {10});

    INFO("a row that DOES reference a live parent still goes in");
    {
        auto cur = exec(d, "INSERT INTO fkr.child (id, pid) VALUES (12, 1);");
        INFO("valid insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    require_ids(d, "fkr.child", {10, 12});
}

TEST_CASE("integration::cpp::fk_omitted_ref_columns::restrict_blocks_parent_delete") {
    auto config = make_test_config(integration_fixture_path("test_fk_omitted_ref_columns/restrict"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d, "RESTRICT");

    INFO("child 10 still references parent 1: the DELETE must fail");
    {
        auto cur = exec(d, "DELETE FROM fkr.parent WHERE id = 1;");
        CHECK(cur->is_error());
    }

    INFO("nothing moved on either side");
    require_ids(d, "fkr.parent", {1});
    require_ids(d, "fkr.child", {10});
}

// An unenforced constraint here shows up as a surviving orphan, not an error.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::cascade_removes_children") {
    auto config = make_test_config(integration_fixture_path("test_fk_omitted_ref_columns/cascade"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d, "CASCADE");

    INFO("second parent + a child of it, to prove the cascade is per-key");
    REQUIRE(exec(d, "INSERT INTO fkr.parent (id, val) VALUES (2, 'p2');")->is_success());
    REQUIRE(exec(d, "INSERT INTO fkr.child (id, pid) VALUES (20, 2);")->is_success());

    INFO("deleting parent 1 must succeed and remove child 10 only");
    {
        auto cur = exec(d, "DELETE FROM fkr.parent WHERE id = 1;");
        INFO("delete error: " << (cur->is_error() ? cur->get_error().what : "none"));
        CHECK(cur->is_success());
    }

    require_ids(d, "fkr.parent", {2});
    require_ids(d, "fkr.child", {20});
}

TEST_CASE("integration::cpp::fk_omitted_ref_columns::no_primary_key_refuses_the_ddl") {
    auto config = make_test_config(integration_fixture_path("test_fk_omitted_ref_columns/no_pk"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE fkr;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE fkr.parent (id bigint, val text);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE fkr.child (id bigint, pid bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO fkr.parent (id, val) VALUES (1, 'p1');")->is_success());

    auto cur = exec(d, "ALTER TABLE fkr.child ADD CONSTRAINT fk_no_pk FOREIGN KEY (pid) REFERENCES fkr.parent;");
    INFO("ADD CONSTRAINT result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    REQUIRE(what.find("primary key") != std::string::npos);

    INFO("and the constraint must not have half-landed: an orphan insert is unaffected by it");
    REQUIRE(exec(d, "INSERT INTO fkr.child (id, pid) VALUES (11, 999);")->is_success());
}

// No pairing exists for the extra column, so the DDL must refuse instead of binding positionally and
// losing the tail.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::arity_against_primary_key_is_checked") {
    auto config = make_test_config(integration_fixture_path("test_fk_omitted_ref_columns/arity"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE fkr;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE fkr.parent (id bigint, val text);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE fkr.child (id bigint, pid bigint, pid2 bigint);")->is_success());
    REQUIRE(exec(d, "ALTER TABLE fkr.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());

    auto cur = exec(d,
                    "ALTER TABLE fkr.child ADD CONSTRAINT fk_wide "
                    "FOREIGN KEY (pid, pid2) REFERENCES fkr.parent;");
    INFO("ADD CONSTRAINT result: " << (cur->is_error() ? cur->get_error().what : "accepted"));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what};
    REQUIRE(what.find("column count") != std::string::npos);
}
