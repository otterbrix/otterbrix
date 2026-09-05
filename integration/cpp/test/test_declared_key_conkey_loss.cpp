// ============================================================================
// A UNIQUE / PRIMARY KEY WHOSE conkey CANNOT BE READ MUST NOT QUIETLY STOP
// EXISTING.
//
// operator_resolve_constraint decoded a 'u' / 'p' pg_constraint row like this:
//
//     auto attoids = catalog::parse_oid_csv(...conkey...);
//     if (!attoids.empty()) { ... pending_uniques.push_back(...); }
//
// so a conkey that decoded to NOTHING never became a pending group, and the
// length guard that refuses an unresolvable key list sits BELOW, inside the loop
// over pending_uniques, where it could never see it. The declared key left the
// constraint set without a word and the table went back to accepting every row.
// parse_oid_csv made that easier: it SWALLOWED any token it could not read and
// answered with a bare vector, so "the CSV was empty" and "the CSV was
// unreadable" arrived as the same value.
//
// HOW THE ROW IS PRODUCED HERE, and why this is not a code-level probe. The
// pg_constraint row is written by the ENGINE, through the same
// node_create_constraint_t → rewrite_create_constraint →
// build_create_constraint_writes path every ALTER TABLE ... ADD CONSTRAINT
// takes. The test only hands that node over with its ATTOID list unstamped — the
// state an inline (CREATE TABLE) constraint node is in before
// rewrite_create_table mints the attoids, and the state any writer that lost the
// column list leaves behind. conkey is then encoded from an empty oid list, and
// everything after is ordinary engine behaviour. The assertion is on the CONTENT
// OF THE TABLE, not on a return code: the user declared UNIQUE (code), the
// engine said yes, and the question is whether two rows carrying code = 100 are
// in the table afterwards.
//
// PATH NOT NAMED FROM SQL. Every live route that used to write such a row is
// closed one floor up: `UNIQUE (nosuchcol)` is refused by enrich, and UNIQUE /
// PRIMARY KEY on a dynamic-schema (relkind='g') table is refused at DDL (see
// test_constraint_unresolvable_target.cpp). What is left is a catalog written
// before those gates — which is what a floor is for. The third case below proves
// the floor is not fatal: the same database still reads and still drops.
// ============================================================================

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

    // ADD CONSTRAINT <name> <kind> (<cols>) whose attoid list never got stamped:
    // the node carries the column NAMES the user wrote, and an EMPTY attoid list,
    // so build_create_constraint_writes encodes conkey as "". Everything else is
    // the production path.
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
        // Suppresses the attoid stamping in enrich (that is what an inline node
        // does — its parent mints the attoids instead), so the list handed in here
        // is the one that reaches the catalog write.
        node->set_inline_with_table(true);
        node->set_fk_col_attoids(std::move(attoids));
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, db, rel);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    // The conkey-loss shape: the column NAMES the user wrote, and no attoids at all,
    // so build_create_constraint_writes encodes conkey as the empty string.
    components::cursor::cursor_t_ptr add_constraint_with_lost_conkey(otterbrix::wrapper_dispatcher_t* d,
                                                                     const std::string& db,
                                                                     const std::string& rel,
                                                                     const std::string& con_name,
                                                                     components::logical_plan::constraint_kind kind,
                                                                     std::vector<std::string> cols) {
        return add_constraint_with_attoids(d, db, rel, con_name, kind, std::move(cols), {});
    }

} // namespace

// A UNIQUE the engine accepted, with a conkey it cannot read. Either the write
// is refused, or the key is enforced — what must never happen is the third
// answer, "accepted, and the duplicate is in the table".
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

    // THE USER CONSEQUENCE, read off the table: how many rows carry code = 100.
    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    const auto ids = column_i64(stored, 0);
    INFO("rows carrying code = 100: " << ids.size());
    INFO("a UNIQUE that was accepted must be enforced; one that cannot be read must be refused");
    const bool accepted_and_duplicated = ddl->is_success() && ids.size() > 1;
    REQUIRE_FALSE(accepted_and_duplicated);
}

// The same row shaped as a PRIMARY KEY. A silently dropped PK repeals TWO
// promises at once — uniqueness and the NOT NULL it implies — so the key column
// takes both a duplicate and a NULL.
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

// LOUD IS NOT FATAL. A refusal that made the database unopenable would be a
// worse defect than the silence it replaces, so the same catalog that carries
// the unreadable key row must still be readable and still be droppable — only
// the writes that would ride on the unenforced key are refused.
TEST_CASE("integration::cpp::declared_key_conkey_loss::an_unreadable_key_row_does_not_brick_the_database") {
    auto config = make_test_config(integration_fixture_path("test_declared_key_conkey_loss/not_bricked"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint);")->is_success());
    // Rows that predate the bad constraint row: they are the ones a bricked
    // database would take with it.
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

// ============================================================================
// THE DOCUMENT-TABLE POPULATION — "LOUD" MUST NOT MEAN "UNOPENABLE".
//
// A dynamic-schema (relkind='g') table keeps its columns in pg_computed_column,
// whose attoids come from a different sequence than pg_attribute's, so a UNIQUE /
// PRIMARY KEY declared on one carries a conkey the resolve step's pg_attribute read
// can NEVER match. Declaring one is refused at DDL now
// (executor_t::execute_plan_full), leaving exactly one population that can still
// hold such a row: a catalog written BEFORE that gate existed. That is the
// population a loud refusal could strand, and this case stands for it.
//
// WHY THE ROW IS PLANTED ON A NORMAL TABLE. The DDL gate refuses the planting itself
// on a relkind='g' table — there is no way left to write one through the engine,
// which is the point of the gate. What CAN be reproduced faithfully is the catalog
// SHAPE such a row has: a conkey of perfectly readable integers matching no live
// pg_attribute row (`stale` below), which takes the same path through the resolve.
// The relkind='g' end is held by
// test_constraint_unresolvable_target::unique_on_dynamic_schema_is_never_a_no_op.
//
// NOTE WHICH GUARD EACH SHAPE LANDS ON. `stale` is non-empty and readable, so it
// never reaches the empty/unreadable-conkey refusals added for the conkey-loss
// defect — it lands on the pre-existing length guard. `lost` is the new one. Both
// are checked here because the property being pinned is about the DATABASE, not
// about which guard spoke.
// ============================================================================
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
        // Rows that predate both bad constraint rows: they are what a bricked
        // database would take with it.
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
        // Reopen the SAME directory: no clear, so both planted rows are read back.
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

// ============================================================================
// THE SAME SILENCE ON THE FOREIGN-KEY SIDE.
//
// The UNIQUE / PRIMARY KEY leg above was gated on `if (!attoids.empty())`. The FK leg
// has no such gate at the DECODE — it was believed that empty lists ride all the way
// down to a refusal. They do not. Pass 2 ends with
//
//     if (!fk.child_col_names.empty() && !fk.parent_col_names.empty()) {
//         fks.push_back(std::move(fk));
//     }
//
// and both length guards above it compare the resolved names against the attoid list
// they were resolved FROM — so at length zero they compare 0 with 0, agree, and pass.
// The FK then falls out of `fks` without a word: enrich stamps no outgoing_fks, the
// planner splices no fk_check node, and the referencing table takes orphans while ON
// DELETE RESTRICT lets the parent go.
//
// The row is produced the same way as the UNIQUE ones above — the engine's own ADD
// CONSTRAINT path with the attoid lists unstamped — and the assertion is on the CONTENT
// of the table: whether a row pointing at a parent that does not exist is sitting in the
// child afterwards.
// ============================================================================

namespace {

    // ADD CONSTRAINT <name> FOREIGN KEY (<cols>) REFERENCES <parent> (<ref_cols>)
    // with neither attoid list stamped, so build_create_constraint_writes encodes
    // BOTH conkey and confkey as the empty string. Everything else — the
    // referenced table's oid, the pg_constraint row, the pg_depend rows — is the
    // production path.
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
        // As above: suppresses the attoid stamping in enrich, so the EMPTY lists
        // the node carries are the ones that reach the catalog write.
        node->set_inline_with_table(true);
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        // Both tables: the child is the constraint's own target, the parent is what
        // bind_catalog_data resolves confrelid from.
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

    // 999 is in no parent row. Under the declared FK this row cannot exist.
    auto orphan = exec(d, "INSERT INTO cur.child (id, parent_id) VALUES (1, 999);");
    INFO("orphan INSERT: " << (orphan->is_error() ? orphan->get_error().what : "accepted"));

    // THE USER CONSEQUENCE, read off the table.
    auto stored = exec(d, "SELECT id FROM cur.child WHERE parent_id = 999;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    INFO("rows pointing at a parent that does not exist: " << stored->size());
    INFO("a FOREIGN KEY that was accepted must be enforced; one that cannot be read must be refused");
    const bool accepted_and_orphaned = ddl->is_success() && stored->size() > 0;
    REQUIRE_FALSE(accepted_and_orphaned);
}
