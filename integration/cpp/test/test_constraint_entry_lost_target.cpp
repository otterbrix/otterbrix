// `target` is a position in the tables node, not a topology flag like a missing disk or tables
// node; an out-of-range value (resolve_entry_t::no_target, the default) means the plan never
// named its table, so treating it like the topology cases empties fks/check_exprs/
// unique_constraints/pk_columns at once -- exactly what "no constraints declared" looks like --
// and silently drops a declared UNIQUE.

#include "integration_fixture_path.hpp"
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

    vector::data_chunk_t one_row(std::pmr::memory_resource* resource, int64_t id, int64_t code) {
        vector::data_chunk_t chunk{resource, two_col_types(resource), 1};
        chunk.set_value(0, 0, id);
        chunk.set_value(1, 0, code);
        chunk.set_cardinality(1);
        return chunk;
    }

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
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, db, rel);

        logical_plan::resolve_entry_t constraint_entry;
        constraint_entry.direction = logical_plan::resolve_direction::outgoing;
        constraint_entry.target = target;
        plan.catalog_resolves.ensure(resource, logical_plan::resolve_kind::constraint).add(std::move(constraint_entry));

        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE cur;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE cur.t (id bigint, code bigint);")->is_success());
        REQUIRE(exec(d, "ALTER TABLE cur.t ADD CONSTRAINT uq_code UNIQUE (code);")->is_success());
    }

} // namespace

// Control: target 0 is the index register_catalog_resolve_table minted for this table, so the
// declared UNIQUE is enforced.
TEST_CASE("integration::cpp::constraint_entry_lost_target::a_named_target_enforces_the_declared_key") {
    auto config = make_test_config(integration_fixture_path("test_constraint_entry_lost_target/control"));
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

// Defect: an unnamed target (resolve_entry_t::no_target) makes the resolve skip the entry, so
// the constraint set comes back empty and the UNIQUE goes silently unenforced.
TEST_CASE("integration::cpp::constraint_entry_lost_target::an_unnamed_target_does_not_repeal_the_key") {
    auto config = make_test_config(integration_fixture_path("test_constraint_entry_lost_target/no_target"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = insert_with_constraint_target(d, "cur", "t", 1, 100, logical_plan::resolve_entry_t::no_target);
    INFO("first INSERT: " << (first->is_error() ? first->get_error().what : "accepted"));
    auto dup = insert_with_constraint_target(d, "cur", "t", 2, 100, logical_plan::resolve_entry_t::no_target);
    INFO("duplicate-code INSERT: " << (dup->is_error() ? dup->get_error().what : "accepted"));

    auto stored = exec(d, "SELECT id FROM cur.t WHERE code = 100 ORDER BY id;");
    INFO("read error: " << (stored->is_error() ? stored->get_error().what : "none"));
    REQUIRE(stored->is_success());
    const auto ids = column_i64(stored, 0);
    INFO("rows carrying code = 100: " << ids.size());
    INFO("a declared UNIQUE must be enforced, or the write refused — never quietly ignored");
    REQUIRE(ids.size() <= 1);
}

TEST_CASE("integration::cpp::constraint_entry_lost_target::an_out_of_range_target_does_not_repeal_the_key") {
    auto config = make_test_config(integration_fixture_path("test_constraint_entry_lost_target/out_of_range"));
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

// The refusal is per-statement: everything else about the database keeps working.
TEST_CASE("integration::cpp::constraint_entry_lost_target::the_refusal_does_not_brick_the_database") {
    auto config = make_test_config(integration_fixture_path("test_constraint_entry_lost_target/not_bricked"));
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
