// ============================================================================
// A CONSTRAINT ENTRY THAT DOES NOT NAME ITS TABLE REPEALS EVERY CONSTRAINT ON
// THAT TABLE, IN SILENCE.
//
// operator_resolve_constraint_t opened its per-entry loop with three facts
// sharing ONE `continue`:
//
//     if (ctx->disk_address == empty_address() || tables_node_ == nullptr ||
//         entry.target >= tables_node_->entries().size()) {
//         continue;
//     }
//
// The first two are TOPOLOGY — no disk to ask, no tables node to read out of —
// and a gather that has nowhere to look has legitimately nothing to gather. The
// third is not topology at all. `target` is a POSITION in the tables node, and
// an out-of-range one (resolve_entry_t::no_target, size_t(-1), is the default)
// is a plan that was assembled without naming the table its constraints belong
// to. Skipping it leaves fks, check_exprs, unique_constraints AND pk_columns all
// empty at once — exactly what "this table declares no constraints" looks like —
// so enrich stamps nothing on the DML node, the planner splices no constraint
// operator, and every declared key on the table stops existing while the
// statement reports success.
//
// HOW THE PLAN IS PRODUCED HERE. Everything is the engine's own: the database,
// the table and its UNIQUE come from plain SQL; the INSERT is an ordinary
// hand-built plan of the shape the C++/C API produces (make_node_insert +
// name_catalog_target, the same two calls test_arithmetic and
// test_batch_execution use), and its catalog lookups are registered through the
// transformer's own register_catalog_resolve_table. Exactly ONE thing differs
// between the two INSERTs below — the `target` field of the constraint entry —
// and the assertion is on the CONTENT of the table: how many rows carrying the
// same `code` are in it afterwards.
//
// The control INSERT is what proves the harness has teeth: with `target` naming
// the table, the very same plan is refused by the UNIQUE.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <string>
#include <vector>

using namespace test_helpers;
using namespace components;

namespace {

    std::vector<int64_t> column_i64(const components::cursor::cursor_t_ptr& cur, uint64_t col) {
        std::vector<int64_t> out;
        out.reserve(cur->size());
        for (std::size_t row = 0; row < cur->size(); ++row) {
            out.push_back(cur->value(col, row).value<int64_t>());
        }
        return out;
    }

    std::pmr::vector<types::complex_logical_type> two_col_types(std::pmr::memory_resource* resource) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::BIGINT);
        types.back().set_alias("id");
        types.emplace_back(types::logical_type::BIGINT);
        types.back().set_alias("code");
        return types;
    }

    // One row, two BIGINT columns aliased "id" and "code" — the shape the table
    // below is created with.
    vector::data_chunk_t one_row(std::pmr::memory_resource* resource, int64_t id, int64_t code) {
        vector::data_chunk_t chunk{resource, two_col_types(resource), 1};
        chunk.set_value(0, 0, id);
        chunk.set_value(1, 0, code);
        chunk.set_cardinality(1);
        return chunk;
    }

    // An INSERT plan carrying ONE constraint entry for (db, rel), whose `target`
    // is whatever the caller says. Everything else — the node, the naming, the
    // table/namespace lookups — is the production path.
    components::cursor::cursor_t_ptr insert_with_constraint_target(otterbrix::wrapper_dispatcher_t* d,
                                                                   const std::string& db,
                                                                   const std::string& rel,
                                                                   int64_t id,
                                                                   int64_t code,
                                                                   std::size_t target) {
        auto* resource = d->resource();
        auto node = components::sql::transform::name_catalog_target(
            db,
            rel,
            logical_plan::make_node_insert(resource, one_row(resource, id, code)));
        logical_plan::execution_plan_t plan{resource, node, logical_plan::make_parameter_node(resource)};
        // The table + its namespace, registered exactly as the transformer does.
        // This is what puts the table entry at index 0 of the tables node.
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, db, rel);

        logical_plan::resolve_entry_t constraint_entry;
        constraint_entry.direction = logical_plan::resolve_direction::outgoing;
        constraint_entry.target = target;
        plan.catalog_resolves.ensure(resource, logical_plan::resolve_kind::constraint)
            .add(std::move(constraint_entry));

        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE cur.t ADD CONSTRAINT uq_code UNIQUE (code);")->is_success());
    }

} // namespace

// THE CONTROL. The constraint entry names the table (target 0, the index
// register_catalog_resolve_table just minted for it), so the declared UNIQUE is
// gathered and enforced: the second row carrying code = 100 does not go in.
TEST_CASE("integration::cpp::constraint_entry_lost_target::a_named_target_enforces_the_declared_key") {
    auto config = make_test_config("/tmp/test_constraint_entry_lost_target/control");
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = insert_with_constraint_target(d, "cur", "t", 1, 100, 0);
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    REQUIRE(first->is_success());

    auto dup = insert_with_constraint_target(d, "cur", "t", 2, 100, 0);
    INFO("duplicate-code INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));
    REQUIRE(dup->is_error());

    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    REQUIRE(stored->is_success());
    REQUIRE(column_i64(stored, 0) == std::vector<int64_t>{1});
}

// THE DEFECT. The same plan with the entry's `target` left at its default,
// resolve_entry_t::no_target — an entry that never named its table. The resolve
// used to skip it, hand on an EMPTY constraint set, and let both rows in under a
// UNIQUE the user declared and the engine had accepted.
TEST_CASE("integration::cpp::constraint_entry_lost_target::an_unnamed_target_does_not_repeal_the_key") {
    auto config = make_test_config("/tmp/test_constraint_entry_lost_target/no_target");
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = insert_with_constraint_target(d,
                                               "cur",
                                               "t",
                                               1,
                                               100,
                                               logical_plan::resolve_entry_t::no_target);
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    auto dup = insert_with_constraint_target(d,
                                             "cur",
                                             "t",
                                             2,
                                             100,
                                             logical_plan::resolve_entry_t::no_target);
    INFO("duplicate-code INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));

    // THE USER CONSEQUENCE, read off the table.
    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    const auto ids = column_i64(stored, 0);
    INFO("rows carrying code = 100: " << ids.size());
    INFO("a declared UNIQUE must be enforced, or the write refused — never quietly ignored");
    REQUIRE(ids.size() <= 1);
}

// The same shape one step further out: a target that IS a number but points past
// the end of the tables node. Same silence, same consequence.
TEST_CASE("integration::cpp::constraint_entry_lost_target::an_out_of_range_target_does_not_repeal_the_key") {
    auto config = make_test_config("/tmp/test_constraint_entry_lost_target/out_of_range");
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = insert_with_constraint_target(d, "cur", "t", 1, 100, 7);
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    auto dup = insert_with_constraint_target(d, "cur", "t", 2, 100, 7);
    INFO("duplicate-code INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));

    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    REQUIRE(stored->is_success());
    const auto ids = column_i64(stored, 0);
    INFO("rows carrying code = 100: " << ids.size());
    REQUIRE(ids.size() <= 1);
}

// LOUD IS NOT FATAL. The refusal is per-STATEMENT: the plan that carries the
// broken entry is refused, and everything else about the database — reading the
// table, writing it through ordinary SQL, dropping it — still works.
TEST_CASE("integration::cpp::constraint_entry_lost_target::the_refusal_does_not_brick_the_database") {
    auto config = make_test_config("/tmp/test_constraint_entry_lost_target/not_bricked");
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);
    REQUIRE(exec(d, "INSERT INTO cur.t (id, code) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE cur.other (id bigint);")->is_success());

    insert_with_constraint_target(d, "cur", "t", 3, 300, logical_plan::resolve_entry_t::no_target);

    INFO("the table still reads, and reads what was there");
    {
        auto cur = exec(d, "SELECT id FROM cur.t ORDER BY id;");
        INFO("read error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(column_i64(cur, 0) == std::vector<int64_t>{1, 2});
    }
    INFO("ordinary SQL against the same table still writes, and the key still holds");
    {
        auto ok = exec(d, "INSERT INTO cur.t (id, code) VALUES (4, 400);");
        INFO("error: " << (ok->is_error() ? ok->get_error().what : "none"));
        REQUIRE(ok->is_success());
        CHECK(exec(d, "INSERT INTO cur.t (id, code) VALUES (5, 400);")->is_error());
    }
    INFO("a table that never saw the broken plan is untouched by it");
    REQUIRE(exec(d, "INSERT INTO cur.other (id) VALUES (7);")->is_success());
    INFO("and the way out is open");
    {
        auto drop = exec(d, "DROP TABLE cur.t;");
        INFO("error: " << (drop->is_error() ? drop->get_error().what : "none"));
        REQUIRE(drop->is_success());
    }
}
