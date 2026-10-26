// A plan built from (node, params) carries no catalog_resolves of its own; the executor registers its targets.
// Every declared CHECK, FOREIGN KEY, UNIQUE and PRIMARY KEY must hold for it exactly as for the same SQL.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/test/parse_ok.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/pmr.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

using namespace test_helpers;
using namespace components;
using expressions::compare_type;
using expressions::side_t;
using key = components::expressions::key_t;
using id_par = core::parameter_id_t;

namespace {

    const core::schema_t database{"hb"};

    configuration::config config_for(const std::string& leaf) {
        auto config = make_test_config(integration_fixture_path("test_hand_built_constraints/" + leaf));
        config.log.level = log_t::level::off;
        return config;
    }

    void run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
    }

    std::size_t rows_of(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        REQUIRE(cur->is_success());
        return cur->size();
    }

    std::string what_of(const components::cursor::cursor_t_ptr& cur) {
        return cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                               : std::string{"accepted"};
    }

    // (id, code) BIGINT rows; an empty optional is a NULL cell.
    vector::data_chunk_t rows(std::pmr::memory_resource* resource,
                              const std::vector<std::pair<std::optional<int64_t>, std::optional<int64_t>>>& values) {
        std::pmr::vector<types::complex_logical_type> types{resource};
        types.emplace_back(types::logical_type::BIGINT);
        types.back().set_alias("id");
        types.emplace_back(types::logical_type::BIGINT);
        types.back().set_alias("code");
        vector::data_chunk_t chunk{resource, types, values.size()};
        for (std::size_t row = 0; row < values.size(); ++row) {
            const auto& [id, code] = values[row];
            if (id) {
                chunk.set_value(0, row, *id);
            } else {
                chunk.data[0].set_null(row, true);
            }
            if (code) {
                chunk.set_value(1, row, *code);
            } else {
                chunk.data[1].set_null(row, true);
            }
        }
        chunk.set_cardinality(values.size());
        return chunk;
    }

    components::cursor::cursor_t_ptr
    insert_rows(otterbrix::wrapper_dispatcher_t* d,
                const std::string& rel,
                const std::vector<std::pair<std::optional<int64_t>, std::optional<int64_t>>>& values) {
        auto* resource = d->resource();
        auto node = test_dml_target(logical_plan::make_node_insert(resource, rows(resource, values)),
                                    database,
                                    core::relname_t{rel});
        return d->execute_plan(
            otterbrix::session_id_t(),
            logical_plan::execution_plan_t{resource, node, logical_plan::make_parameter_node(resource)});
    }

    // UPDATE rel SET column = value WHERE id = where_id
    components::cursor::cursor_t_ptr update_where_id(otterbrix::wrapper_dispatcher_t* d,
                                                     const std::string& rel,
                                                     int64_t where_id,
                                                     const std::string& column,
                                                     int64_t value) {
        auto* resource = d->resource();
        const qualified_name_t target{database, core::relname_t{rel}};
        auto match = logical_plan::make_node_match(
            resource,
            target,
            make_compare_expression(resource, compare_type::eq, key{resource, "id", side_t::left}, id_par{0}));
        auto set =
            expressions::make_scalar_expression(resource, expressions::scalar_type::constant, key{resource, column});
        set->append_param(id_par{1});
        auto node = test_dml_target(
            logical_plan::make_node_update(resource,
                                           match,
                                           logical_plan::make_node_limit(resource, logical_plan::limit_t::all()),
                                           {set}),
            database,
            core::relname_t{rel});
        auto params = logical_plan::make_parameter_node(resource);
        REQUIRE(params->add_parameter(vector::make_scalar(resource, where_id), vector::parameter_kind::literal) ==
                id_par{0});
        REQUIRE(params->add_parameter(vector::make_scalar(resource, value), vector::parameter_kind::literal) ==
                id_par{1});
        return d->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{resource, node, params});
    }

    // DELETE FROM rel WHERE id = where_id
    components::cursor::cursor_t_ptr
    delete_where_id(otterbrix::wrapper_dispatcher_t* d, const std::string& rel, int64_t where_id) {
        auto* resource = d->resource();
        const qualified_name_t target{database, core::relname_t{rel}};
        auto node = test_dml_target(
            logical_plan::make_node_delete(
                resource,
                logical_plan::make_node_match(
                    resource,
                    target,
                    make_compare_expression(resource, compare_type::eq, key{resource, "id", side_t::left}, id_par{0})),
                logical_plan::make_node_limit(resource, logical_plan::limit_t::all())),
            database,
            core::relname_t{rel});
        auto params = logical_plan::make_parameter_node(resource);
        REQUIRE(params->add_parameter(vector::make_scalar(resource, where_id), vector::parameter_kind::literal) ==
                id_par{0});
        return d->execute_plan(otterbrix::session_id_t(), logical_plan::execution_plan_t{resource, node, params});
    }

    // The same statement written in SQL is refused with the same error.
    void require_refused_like(otterbrix::wrapper_dispatcher_t* d,
                              const components::cursor::cursor_t_ptr& hand_built,
                              const std::string& sql) {
        INFO("hand-built write: " << what_of(hand_built));
        REQUIRE(hand_built->is_error());
        auto via_sql = exec(d, sql);
        INFO("the same in SQL: " << sql << " -> " << what_of(via_sql));
        REQUIRE(via_sql->is_error());
        CHECK(hand_built->get_error().type == via_sql->get_error().type);
        CHECK(what_of(hand_built) == what_of(via_sql));
    }

    void seed_parent_and_child(otterbrix::wrapper_dispatcher_t* d, const std::string& on_delete) {
        run_ok(d, "CREATE SCHEMA hb;");
        run_ok(d, "CREATE TABLE hb.p (id bigint, code bigint);");
        run_ok(d, "CREATE TABLE hb.c (id bigint, code bigint);");
        run_ok(d, "ALTER TABLE hb.p ADD CONSTRAINT p_pk PRIMARY KEY (id);");
        run_ok(d, "ALTER TABLE hb.c ADD CONSTRAINT c_fk FOREIGN KEY (code) REFERENCES hb.p (id)" + on_delete + ";");
        run_ok(d, "INSERT INTO hb.p (id, code) VALUES (1, 10), (2, 20);");
        run_ok(d, "INSERT INTO hb.c (id, code) VALUES (100, 1);");
    }

} // namespace

TEST_CASE("integration::cpp::hand_built_constraints::insert_honours_check") {
    auto config = config_for("insert_check");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA hb;");
    run_ok(d, "CREATE TABLE hb.t (id bigint, code bigint);");
    run_ok(d, "ALTER TABLE hb.t ADD CONSTRAINT id_positive CHECK (id > 0);");

    auto accepted = insert_rows(d, "t", {{1, 10}});
    INFO("a row the CHECK admits: " << what_of(accepted));
    REQUIRE(accepted->is_success());

    auto refused = insert_rows(d, "t", {{-1, 20}});
    CHECK(what_of(refused).find("id_positive") != std::string::npos);
    require_refused_like(d, refused, "INSERT INTO hb.t (id, code) VALUES (-1, 20);");
    CHECK(rows_of(d, "SELECT id FROM hb.t;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::update_honours_check") {
    auto config = config_for("update_check");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA hb;");
    run_ok(d, "CREATE TABLE hb.t (id bigint, code bigint);");
    run_ok(d, "ALTER TABLE hb.t ADD CONSTRAINT code_positive CHECK (code > 0);");
    run_ok(d, "INSERT INTO hb.t (id, code) VALUES (1, 10);");

    auto refused = update_where_id(d, "t", 1, "code", -5);
    CHECK(what_of(refused).find("code_positive") != std::string::npos);
    require_refused_like(d, refused, "UPDATE hb.t SET code = -5 WHERE id = 1;");
    CHECK(rows_of(d, "SELECT id FROM hb.t WHERE code = 10;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::insert_honours_unique") {
    auto config = config_for("insert_unique");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA hb;");
    run_ok(d, "CREATE TABLE hb.t (id bigint, code bigint);");
    run_ok(d, "ALTER TABLE hb.t ADD CONSTRAINT uq_code UNIQUE (code);");
    run_ok(d, "INSERT INTO hb.t (id, code) VALUES (1, 10);");

    require_refused_like(d, insert_rows(d, "t", {{2, 10}}), "INSERT INTO hb.t (id, code) VALUES (2, 10);");
    CHECK(rows_of(d, "SELECT id FROM hb.t WHERE code = 10;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::update_honours_unique") {
    auto config = config_for("update_unique");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA hb;");
    run_ok(d, "CREATE TABLE hb.t (id bigint, code bigint);");
    run_ok(d, "ALTER TABLE hb.t ADD CONSTRAINT uq_code UNIQUE (code);");
    run_ok(d, "INSERT INTO hb.t (id, code) VALUES (1, 10), (2, 20);");

    require_refused_like(d, update_where_id(d, "t", 2, "code", 10), "UPDATE hb.t SET code = 10 WHERE id = 2;");
    CHECK(rows_of(d, "SELECT id FROM hb.t WHERE code = 10;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::insert_honours_primary_key") {
    auto config = config_for("insert_primary_key");
    test_spaces space(config);
    auto* d = space.dispatcher();
    run_ok(d, "CREATE SCHEMA hb;");
    run_ok(d, "CREATE TABLE hb.t (id bigint, code bigint);");
    run_ok(d, "ALTER TABLE hb.t ADD CONSTRAINT t_pk PRIMARY KEY (id);");
    run_ok(d, "INSERT INTO hb.t (id, code) VALUES (1, 10);");

    require_refused_like(d, insert_rows(d, "t", {{1, 20}}), "INSERT INTO hb.t (id, code) VALUES (1, 20);");
    require_refused_like(d,
                         insert_rows(d, "t", {{std::nullopt, 30}}),
                         "INSERT INTO hb.t (id, code) VALUES (NULL, 30);");
    CHECK(rows_of(d, "SELECT id FROM hb.t;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::insert_honours_foreign_key") {
    auto config = config_for("insert_fk");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_parent_and_child(d, "");

    auto accepted = insert_rows(d, "c", {{101, 2}});
    INFO("a child of an existing parent: " << what_of(accepted));
    REQUIRE(accepted->is_success());

    require_refused_like(d, insert_rows(d, "c", {{102, 42}}), "INSERT INTO hb.c (id, code) VALUES (102, 42);");
    CHECK(rows_of(d, "SELECT id FROM hb.c WHERE code = 42;") == 0);
}

TEST_CASE("integration::cpp::hand_built_constraints::update_honours_foreign_key") {
    auto config = config_for("update_fk");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_parent_and_child(d, "");

    require_refused_like(d, update_where_id(d, "c", 100, "code", 42), "UPDATE hb.c SET code = 42 WHERE id = 100;");
    CHECK(rows_of(d, "SELECT id FROM hb.c WHERE code = 1;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::delete_honours_a_referencing_foreign_key") {
    auto config = config_for("delete_restrict");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_parent_and_child(d, "");

    auto unreferenced = delete_where_id(d, "p", 2);
    INFO("a parent nothing references: " << what_of(unreferenced));
    REQUIRE(unreferenced->is_success());

    require_refused_like(d, delete_where_id(d, "p", 1), "DELETE FROM hb.p WHERE id = 1;");
    CHECK(rows_of(d, "SELECT id FROM hb.p WHERE id = 1;") == 1);
    CHECK(rows_of(d, "SELECT id FROM hb.c;") == 1);
}

TEST_CASE("integration::cpp::hand_built_constraints::delete_cascades_like_sql") {
    auto config = config_for("delete_cascade");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed_parent_and_child(d, " ON DELETE CASCADE");

    auto removed = delete_where_id(d, "p", 1);
    INFO("a parent with a cascading child: " << what_of(removed));
    REQUIRE(removed->is_success());
    CHECK(rows_of(d, "SELECT id FROM hb.p;") == 1);
    CHECK(rows_of(d, "SELECT id FROM hb.c;") == 0);
}

// The SQL path reaches its constraints through the same registration: one entry per write target, the direction its
// statement needs, and none for the tables a statement only reads.
TEST_CASE("integration::cpp::hand_built_constraints::sql_registers_one_constraint_entry_per_write_target") {
    struct case_t {
        std::string sql;
        std::size_t entries;
        logical_plan::resolve_direction direction;
    };
    using logical_plan::resolve_direction;
    const std::vector<case_t> cases{
        {"INSERT INTO hb.t (id, code) VALUES (1, 2);", 1, resolve_direction::outgoing},
        {"INSERT INTO hb.t (id, code) VALUES (1, 2) RETURNING id;", 1, resolve_direction::outgoing},
        {"INSERT INTO hb.t (id, code) SELECT id, code FROM hb.s;", 1, resolve_direction::outgoing},
        {"INSERT INTO hb.t (id, code) VALUES (1, 2) ON CONFLICT (id) DO NOTHING;", 1, resolve_direction::outgoing},
        {"INSERT INTO hb.t (id, code) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET code = excluded.code;",
         1,
         resolve_direction::outgoing},
        {"INSERT INTO hb.t (id, code) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET code = excluded.code RETURNING id;",
         1,
         resolve_direction::outgoing},
        {"WITH w AS (SELECT id, code FROM hb.s) INSERT INTO hb.t (id, code) SELECT id, code FROM w;",
         1,
         resolve_direction::outgoing},
        {"UPDATE hb.t SET code = 3 WHERE id = 1;", 1, resolve_direction::outgoing},
        {"UPDATE hb.t SET code = 3 WHERE id = 1 RETURNING code;", 1, resolve_direction::outgoing},
        {"UPDATE hb.t SET code = s.code FROM hb.s WHERE hb.t.id = s.id;", 1, resolve_direction::outgoing},
        {"WITH w AS (SELECT id, code FROM hb.s) UPDATE hb.t SET code = w.code FROM w WHERE hb.t.id = w.id;",
         1,
         resolve_direction::outgoing},
        {"DELETE FROM hb.t;", 1, resolve_direction::referencing},
        {"DELETE FROM hb.t WHERE id = 1 RETURNING id;", 1, resolve_direction::referencing},
        {"DELETE FROM hb.t USING hb.s WHERE hb.t.id = s.id;", 1, resolve_direction::referencing},
        {"WITH w AS (SELECT id FROM hb.s) DELETE FROM hb.t USING w WHERE hb.t.id = w.id;",
         1,
         resolve_direction::referencing},
        {"SELECT id FROM hb.t;", 0, resolve_direction::outgoing},
    };
    auto resource = core::pmr::otterbrix_resource();
    for (const auto& c : cases) {
        INFO("statement: " << c.sql);
        std::pmr::monotonic_buffer_resource arena(&resource);
        auto* raw = parse_ok(&arena, c.sql.c_str());
        REQUIRE(raw != nullptr);
        sql::transform::transformer transformer(&resource, c.sql.c_str());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw)));
        REQUIRE_FALSE(result.has_error());
        auto plan = result.finalize();
        REQUIRE_FALSE(plan.has_error());
        auto& resolves = plan.value().catalog_resolves;
        services::dispatcher::register_plan_targets(&resource, plan.value().sub_queries.back().get(), &resolves);

        const std::size_t found = resolves.constraints ? resolves.constraints->entries().size() : 0;
        REQUIRE(found == c.entries);
        if (found == 0) {
            continue;
        }
        const auto& entry = resolves.constraints->entries().front();
        CHECK(entry.direction == c.direction);
        CHECK_FALSE(entry.names_only);
        REQUIRE(resolves.tables != nullptr);
        REQUIRE(entry.target < resolves.tables->entries().size());
        CHECK(resolves.tables->entries()[entry.target].schema == "hb");
        CHECK(resolves.tables->entries()[entry.target].relname == "t");
    }
}

// What register_plan_targets now adds for every plan, the DDL path keeps gathering for itself: a REFERENCES target's
// key, DROP CONSTRAINT's names. No second entry joins them.
TEST_CASE("integration::cpp::hand_built_constraints::ddl_keeps_its_own_constraint_entries") {
    const std::vector<std::string> statements{
        "CREATE TABLE hb.c (id bigint, code bigint REFERENCES hb.p);",
        "ALTER TABLE hb.c ADD CONSTRAINT c_fk FOREIGN KEY (code) REFERENCES hb.p;",
        "ALTER TABLE hb.c DROP CONSTRAINT c_fk;",
    };
    auto resource = core::pmr::otterbrix_resource();
    for (const auto& sql : statements) {
        INFO("statement: " << sql);
        std::pmr::monotonic_buffer_resource arena(&resource);
        auto* raw = parse_ok(&arena, sql.c_str());
        REQUIRE(raw != nullptr);
        sql::transform::transformer transformer(&resource, sql.c_str());
        auto result = transformer.transform(sql::transform::pg_cell_to_node_cast(linitial(raw)));
        REQUIRE_FALSE(result.has_error());
        auto plan = result.finalize();
        REQUIRE_FALSE(plan.has_error());
        auto& resolves = plan.value().catalog_resolves;
        const std::size_t transformed = resolves.constraints ? resolves.constraints->entries().size() : 0;
        CHECK(transformed == 1);
        services::dispatcher::register_plan_targets(&resource, plan.value().sub_queries.back().get(), &resolves);
        CHECK((resolves.constraints ? resolves.constraints->entries().size() : 0) == transformed);
    }
}

// The plan register_plan_targets sees for a hand-built write: its table and the constraints it must honour.
TEST_CASE("integration::cpp::hand_built_constraints::a_hand_built_write_registers_its_constraints") {
    auto resource = core::pmr::otterbrix_resource();
    const core::relname_t rel{"t"};
    struct case_t {
        logical_plan::node_ptr node;
        logical_plan::resolve_direction direction;
    };
    auto match = [&] {
        return logical_plan::make_node_match(&resource,
                                             qualified_name_t{database, rel},
                                             make_compare_expression(&resource, compare_type::all_true));
    };
    auto limit = [&] { return logical_plan::make_node_limit(&resource, logical_plan::limit_t::all()); };
    std::vector<case_t> cases;
    cases.push_back(
        {test_dml_target(logical_plan::make_node_insert(&resource, rows(&resource, {{1, 2}})), database, rel),
         logical_plan::resolve_direction::outgoing});
    const std::pmr::vector<expressions::expression_ptr> no_assignments{&resource};
    cases.push_back(
        {test_dml_target(logical_plan::make_node_update(&resource, match(), limit(), no_assignments), database, rel),
         logical_plan::resolve_direction::outgoing});
    cases.push_back({test_dml_target(logical_plan::make_node_delete(&resource, match(), limit()), database, rel),
                     logical_plan::resolve_direction::referencing});
    for (const auto& c : cases) {
        INFO("node: " << c.node->to_string());
        logical_plan::catalog_resolves_t resolves;
        services::dispatcher::register_plan_targets(&resource, c.node.get(), &resolves);
        REQUIRE(resolves.constraints != nullptr);
        REQUIRE(resolves.constraints->entries().size() == 1);
        const auto& entry = resolves.constraints->entries().front();
        CHECK(entry.direction == c.direction);
        REQUIRE(resolves.tables != nullptr);
        REQUIRE(entry.target < resolves.tables->entries().size());
        CHECK(resolves.tables->entries()[entry.target].relname == "t");
    }
}
