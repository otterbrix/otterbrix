// operator_resolve_constraint only queued a pg_constraint row into pending_uniques when
// parse_oid_csv's decoded attoid list was non-empty, so a conkey that decoded to nothing (which
// parse_oid_csv also returns for an unreadable one) silently left the constraint set unenforced.
// No live SQL route reaches this shape anymore; what's left is a floor for a catalog written
// before those gates existed.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>

#include <filesystem>
#include <string>
#include <utility>
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

    // The node carries the user's column NAMES and an EMPTY attoid list, so conkey encodes as "".
    components::cursor::cursor_t_ptr
    add_constraint_with_attoids(otterbrix::wrapper_dispatcher_t* d,
                                const std::string& db,
                                const std::string& rel,
                                const std::string& con_name,
                                components::logical_plan::constraint_kind kind,
                                std::vector<std::string> cols,
                                std::vector<components::catalog::oid_t> attoids) {
        auto* resource = d->resource();
        auto node = components::logical_plan::make_node_create_constraint(resource,
                                                                          db,
                                                                          rel,
                                                                          core::constraint_name_t{con_name},
                                                                          kind);
        node->set_local_col_names(std::move(cols));
        // Suppresses the attoid stamping in enrich, so the list handed in here reaches the catalog write.
        node->set_inline_with_table(true);
        node->set_fk_col_attoids(std::move(attoids));
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, db, rel);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    components::cursor::cursor_t_ptr add_constraint_with_lost_conkey(otterbrix::wrapper_dispatcher_t* d,
                                                                     const std::string& db,
                                                                     const std::string& rel,
                                                                     const std::string& con_name,
                                                                     components::logical_plan::constraint_kind kind,
                                                                     std::vector<std::string> cols) {
        return add_constraint_with_attoids(d, db, rel, con_name, kind, std::move(cols), {});
    }

} // namespace

// Either the write is refused, or the key is enforced — never "accepted, and the duplicate is in the table".
TEST_CASE("integration::cpp::declared_key_conkey_loss::unreadable_conkey_does_not_repeal_a_unique") {
    auto config = make_test_config(integration_fixture_path("test_declared_key_conkey_loss/unique"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint);")->is_success());

    auto ddl = add_constraint_with_lost_conkey(d,
                                               "cur",
                                               "t",
                                               "uq_code",
                                               components::logical_plan::constraint_kind::unique,
                                               {"code"});
    INFO("ADD CONSTRAINT UNIQUE (code) result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto first = exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 100);");
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    auto dup = exec(d, "INSERT INTO cur.t (id, code) VALUES (2, 100);");
    INFO("duplicate-code INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));

    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    const auto ids = column_i64(stored, 0);
    INFO("rows carrying code = 100: " << ids.size());
    INFO("a UNIQUE that was accepted must be enforced; one that cannot be read must be refused");
    const bool accepted_and_duplicated = ddl->is_success() && ids.size() > 1;
    REQUIRE_FALSE(accepted_and_duplicated);
}

// A silently dropped PK repeals TWO promises at once — uniqueness and the NOT NULL it implies.
TEST_CASE("integration::cpp::declared_key_conkey_loss::unreadable_conkey_does_not_repeal_a_primary_key") {
    auto config = make_test_config(integration_fixture_path("test_declared_key_conkey_loss/pk"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, v bigint);")->is_success());

    auto ddl = add_constraint_with_lost_conkey(d,
                                               "cur",
                                               "t",
                                               "t_pk",
                                               components::logical_plan::constraint_kind::primary_key,
                                               {"id"});
    INFO("ADD CONSTRAINT PRIMARY KEY (id) result: " << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto first = exec(d, "INSERT INTO cur.t (id, v) VALUES (1, 10);");
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    auto dup = exec(d, "INSERT INTO cur.t (id, v) VALUES (1, 20);");
    INFO("duplicate-id INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));

    auto stored = exec(d, "SELECT v FROM cur.t WHERE id = 1 ORDER BY v;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    const auto vs = column_i64(stored, 0);
    INFO("rows carrying id = 1: " << vs.size());
    const bool accepted_and_duplicated = ddl->is_success() && vs.size() > 1;
    REQUIRE_FALSE(accepted_and_duplicated);
}

// Loud is not fatal: a refusal that bricked the database would be worse than the silence it replaces.
TEST_CASE("integration::cpp::declared_key_conkey_loss::an_unreadable_key_row_does_not_brick_the_database") {
    auto config = make_test_config(integration_fixture_path("test_declared_key_conkey_loss/not_bricked"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.other (id bigint);")->is_success());

    add_constraint_with_lost_conkey(d,
                                    "cur",
                                    "t",
                                    "uq_code",
                                    components::logical_plan::constraint_kind::unique,
                                    {"code"});

    INFO("the table still reads, and reads what was there");
    {
        auto cur = exec(d, "SELECT id FROM cur.t ORDER BY id;");
        INFO("read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == std::vector<int64_t>{1, 2});
    }
    INFO("a table that does NOT carry the bad row is untouched by it");
    {
        auto ins = exec(d, "INSERT INTO cur.other (id) VALUES (7);");
        INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
        REQUIRE(ins->is_success());
    }
    INFO("and the affected table can still be dropped — the way out is open");
    {
        auto drop = exec(d, "DROP TABLE cur.t;");
        INFO("error: " << (drop->is_error() ? drop->get_error().what : "none"));
        REQUIRE(drop->is_success());
    }
}

// A dynamic-schema (relkind='g') table's columns live in pg_computed_column, a different oid
// sequence than pg_attribute's, so a UNIQUE/PRIMARY KEY on one has a conkey that resolve can
// never match; refused at DDL now, so what's reproduced here is the same catalog shape left by a
// catalog written before that gate existed. The relkind='g' end itself is covered by
// test_constraint_unresolvable_target::unique_on_dynamic_schema_is_never_a_no_op.
TEST_CASE("integration::cpp::declared_key_conkey_loss::an_unenforceable_key_row_survives_a_restart_without_bricking") {
    const std::filesystem::path dir = integration_fixture_path("test_declared_key_conkey_loss/restart");
    auto config = make_test_config(dir);

    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.lost (id bigint, code bigint);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.stale (id bigint, code bigint);")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.plain (id bigint);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.lost (id, code) VALUES (1, 100), (2, 200);")->is_success());
        REQUIRE(exec(d, "INSERT INTO cur.stale (id, code) VALUES (1, 100), (2, 200);")->is_success());

        INFO("shape 1 — the conkey-loss row: the column name is there, the attoid list is not");
        auto lost = add_constraint_with_lost_conkey(d,
                                                    "cur",
                                                    "lost",
                                                    "uq_lost_code",
                                                    components::logical_plan::constraint_kind::unique,
                                                    {"code"});
        INFO("planting result: " << (lost->is_error() ? lost->get_error().what : "accepted"));
        REQUIRE(lost->is_success());

        INFO("shape 2 — the pre-gate document-table row: readable attoids from the wrong sequence");
        auto stale = add_constraint_with_attoids(d,
                                                 "cur",
                                                 "stale",
                                                 "uq_stale_code",
                                                 components::logical_plan::constraint_kind::unique,
                                                 {"code"},
                                                 {900001});
        INFO("planting result: " << (stale->is_error() ? stale->get_error().what : "accepted"));
        REQUIRE(stale->is_success());
    }

    INFO("THE ENGINE OPENS over that catalog — this is the half that would be fatal");
    {
        auto reopened = test_create_config(dir);
        test_spaces space(reopened);
        auto* d = space.dispatcher();

        for (const std::string& table : {std::string{"cur.lost"}, std::string{"cur.stale"}}) {
            INFO("table under test: " << table);
            INFO("it still reads, and reads what was there");
            auto cur = exec(d, "SELECT id FROM " + table + " ORDER BY id;");
            INFO("read error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(column_i64(cur, 0) == std::vector<int64_t>{1, 2});
        }

        INFO("every other table in the database still writes");
        {
            auto ins = exec(d, "INSERT INTO cur.plain (id) VALUES (7);");
            INFO("error: " << (ins->is_error() ? ins->get_error().what : "none"));
            REQUIRE(ins->is_success());
        }

        INFO("a write onto an unenforceable key is refused WITH WORDS, never silently taken");
        for (const auto& [table, con_name] : std::vector<std::pair<std::string, std::string>>{
                 {"cur.lost", "uq_lost_code"},
                 {"cur.stale", "uq_stale_code"}}) {
            INFO("table under test: " << table);
            auto ins = exec(d, "INSERT INTO " + table + " (id, code) VALUES (3, 100);");
            INFO("result: " << (ins->is_error() ? ins->get_error().what : "accepted"));
            REQUIRE(ins->is_error());
            const std::string what{ins->get_error().what};
            INFO("the message has to name the constraint the user can act on");
            CHECK(what.find(con_name) != std::string::npos);

            INFO("and the refused row is not in the table");
            auto stored = exec(d, "SELECT id FROM " + table + " ORDER BY id;");
            REQUIRE(stored->is_success());
            REQUIRE(column_i64(stored, 0) == std::vector<int64_t>{1, 2});
        }

        INFO("and the way out is open: each affected table can still be dropped");
        for (const std::string& table : {std::string{"cur.lost"}, std::string{"cur.stale"}}) {
            INFO("table under test: " << table);
            auto drop = exec(d, "DROP TABLE " + table + ";");
            INFO("error: " << (drop->is_error() ? drop->get_error().what : "none"));
            REQUIRE(drop->is_success());
        }
    }
}

// The same silence on the foreign-key side: the FK leg had no gate on empty decoded lists at all,
// so both length guards compared resolved names against the attoid list they came FROM and, at
// length zero, agreed and passed. The FK fell out of `fks` without a word — no outgoing_fks, no
// fk_check node spliced in — so the referencing table could take orphans while ON DELETE RESTRICT
// let the parent go.

namespace {

    components::cursor::cursor_t_ptr add_fk_with_lost_key_lists(otterbrix::wrapper_dispatcher_t* d,
                                                                const std::string& db,
                                                                const std::string& child_rel,
                                                                const std::string& parent_rel,
                                                                const std::string& con_name,
                                                                std::vector<std::string> child_cols,
                                                                std::vector<std::string> parent_cols) {
        auto* resource = d->resource();
        auto node =
            components::logical_plan::make_node_create_constraint(resource,
                                                                  db,
                                                                  child_rel,
                                                                  core::constraint_name_t{con_name},
                                                                  components::logical_plan::constraint_kind::foreign_key,
                                                                  db);
        node->set_ref_relname(parent_rel);
        node->set_local_col_names(std::move(child_cols));
        node->set_ref_col_names(std::move(parent_cols));
        node->set_inline_with_table(true);
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_tables(resource,
                                                                    &plan.catalog_resolves,
                                                                    {{db, child_rel}, {db, parent_rel}});
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

} // namespace

TEST_CASE("integration::cpp::declared_key_conkey_loss::an_unreadable_fk_column_list_does_not_repeal_the_key") {
    auto config = make_test_config(integration_fixture_path("test_declared_key_conkey_loss/fk"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.parent (id bigint);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.child (id bigint, parent_id bigint);")->is_success());
    REQUIRE(exec(d, "INSERT INTO cur.parent (id) VALUES (1);")->is_success());

    auto ddl = add_fk_with_lost_key_lists(d, "cur", "child", "parent", "fk_child_parent", {"parent_id"}, {"id"});
    INFO("ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent (id): "
         << (ddl->is_error() ? ddl->get_error().what : "accepted"));

    auto orphan = exec(d, "INSERT INTO cur.child (id, parent_id) VALUES (1, 999);");
    INFO("orphan INSERT: " << (orphan->is_error() ? orphan->get_error().what : "accepted"));

    auto stored = exec(d, "SELECT id FROM cur.child WHERE parent_id = 999;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    INFO("rows pointing at a parent that does not exist: " << stored->size());
    INFO("a FOREIGN KEY that was accepted must be enforced; one that cannot be read must be refused");
    const bool accepted_and_orphaned = ddl->is_success() && stored->size() > 0;
    REQUIRE_FALSE(accepted_and_orphaned);
}
