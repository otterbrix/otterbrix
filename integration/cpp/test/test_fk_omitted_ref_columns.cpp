// ============================================================================
// `REFERENCES parent` WITHOUT A COLUMN LIST MUST BIND TO THE PARENT'S PRIMARY KEY.
//
// SQL lets the referenced column list be omitted:
//
//     FOREIGN KEY (pid) REFERENCES D.parent
//
// PostgreSQL then resolves it to the primary key of the referenced table, and
// refuses the DDL outright when that table has no primary key ("there is no
// primary key for referenced table"). Otterbrix did neither: the transformer
// copied an absent pk_attrs as an EMPTY ref list, enrich resolved that empty
// list to an empty confkey, and pg_constraint got a row whose referenced side
// names nothing. operator_resolve_constraint drops such a row on the floor
// (it requires BOTH name lists to be non-empty), so the constraint the user
// declared simply did not exist:
//
//   * an orphan INSERT — a value with no matching parent row — succeeded;
//   * ON DELETE RESTRICT did not block deleting a referenced parent;
//   * ON DELETE CASCADE left the children behind, pointing at nothing.
//
// A declared constraint that the engine accepts and then does not enforce is
// worse than one it refuses: the user believes the data is guarded.
// ============================================================================

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

    void require_ids(otterbrix::wrapper_dispatcher_t* d,
                     const std::string& table,
                     const std::vector<int64_t>& ids) {
        auto cur = exec(d, "SELECT id FROM " + table + " ORDER BY id;");
        INFO(table << " read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == ids);
    }

    // parent(id PRIMARY KEY, val), child(id, pid) with `REFERENCES fkr.parent`
    // spelled WITHOUT a column list. Rows are seeded BEFORE the constraint so the
    // seeding INSERTs never run through the FK check themselves.
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

// (1) The INSERT side. A child row whose pid is in no parent row must be refused
// by the omitted-list FK exactly as it would be by `REFERENCES fkr.parent (id)`.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::orphan_insert_is_refused") {
    auto config = make_test_config("/tmp/test_fk_omitted_ref_columns/orphan_insert");
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

// (2) ON DELETE RESTRICT over the same omitted-list FK: deleting a parent that
// still has children must be refused, and both tables must be untouched.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::restrict_blocks_parent_delete") {
    auto config = make_test_config("/tmp/test_fk_omitted_ref_columns/restrict");
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

// (3) ON DELETE CASCADE over the same omitted-list FK: the parent delete must
// succeed AND take the referencing child rows with it. An unenforced constraint
// shows up here as a surviving orphan, not as an error.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::cascade_removes_children") {
    auto config = make_test_config("/tmp/test_fk_omitted_ref_columns/cascade");
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

// (4) The referenced table has NO primary key. PostgreSQL refuses the DDL; the
// one thing that must NOT happen is accepting the statement and enforcing
// nothing. The constraint has no referenced side to bind to, so the ALTER fails.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::no_primary_key_refuses_the_ddl") {
    auto config = make_test_config("/tmp/test_fk_omitted_ref_columns/no_pk");
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

// (5) Arity: the referencing list is longer than the parent's primary key. There
// is no pairing to make, so the DDL must say so instead of binding the columns
// positionally and losing the tail.
TEST_CASE("integration::cpp::fk_omitted_ref_columns::arity_against_primary_key_is_checked") {
    auto config = make_test_config("/tmp/test_fk_omitted_ref_columns/arity");
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

