#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <services/disk/manager_disk.hpp>

#include <unistd.h>

#include <core/tests/wait_ready.hpp>
#include <limits>
#include <string>
#include <thread>

// A DROP CASCADE step's own-row delete ({classid, col 0, objid}) must count nonzero: a zero means
// the catalog never held the planned object, so proceeding would push storage/index drops over a
// catalog inconsistency.

using namespace test_helpers;

namespace {

    namespace catalog = components::catalog;

    std::size_t rows_where(otterbrix::wrapper_dispatcher_t* d,
                           const std::string& table,
                           const std::string& column,
                           catalog::oid_t oid) {
        auto cur = exec(d,
                        "SELECT " + column + " FROM pg_catalog." + table + " WHERE " + column + " = " +
                            std::to_string(oid) + ";");
        REQUIRE(cur->is_success());
        return cur->size();
    }

    catalog::oid_t first_oid(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        REQUIRE(cur->is_success());
        for (const auto& chunk : cur->chunks()) {
            if (chunk.size() != 0) {
                return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, 0));
            }
        }
        return catalog::INVALID_OID;
    }

    catalog::oid_t table_oid_named(otterbrix::wrapper_dispatcher_t* d, const std::string& name) {
        return first_oid(d, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + name + "';");
    }

    // The PK row carries no confrelid, so this key selects the FK alone.
    catalog::oid_t fk_oid_referencing(otterbrix::wrapper_dispatcher_t* d, catalog::oid_t parent_oid) {
        return first_oid(d,
                         "SELECT oid FROM pg_catalog.pg_constraint WHERE confrelid = " + std::to_string(parent_oid) +
                             " AND contype = 'f';");
    }

    // Forges the edge instead of deleting a real row: a td{0,0} delete would leave a ghost the
    // DROP's own scan still marks, failing through the commit-drain replay instead of the path under test.
    void forge_depend_edge(catalog_forging_spaces_t& space,
                           catalog::oid_t classid,
                           catalog::oid_t objid,
                           catalog::oid_t refclassid,
                           catalog::oid_t refobjid) {
        auto* resource = space.dispatcher()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto row = catalog::build_pg_depend_row(resource, classid, objid, refclassid, refobjid, /*deptype=*/'n');
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk_address(),
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    catalog::well_known_oid::pg_depend_table,
                                                    std::move(row));
        REQUIRE(test_helpers::wait_ready(fut));
        auto appended = std::move(fut).take_ready();
        REQUIRE_FALSE(appended.has_error());
    }

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_cascade_lost_row/") + leaf).string();
    }

} // namespace

TEST_CASE("integration::cpp::drop_cascade_lost_row::planned_step_without_a_catalog_row_refuses") {
    auto config = make_test_config(fixture_path("lost"));
    catalog_forging_spaces_t space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE lost;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE lost.parent (id bigint PRIMARY KEY);")->is_success());

    const auto parent_oid = table_oid_named(d, "parent");
    REQUIRE(parent_oid != catalog::INVALID_OID);

    const catalog::oid_t ghost_oid = catalog::FIRST_USER_OID + 777777;
    REQUIRE(rows_where(d, "pg_constraint", "oid", ghost_oid) == 0);
    forge_depend_edge(space,
                      catalog::well_known_oid::pg_constraint_table,
                      ghost_oid,
                      catalog::well_known_oid::pg_class_table,
                      parent_oid);
    REQUIRE(rows_where(d, "pg_depend", "objid", ghost_oid) == 1);

    // CASCADE is required since #638: bare DROP = RESTRICT, whose gate would refuse on the
    // forged edge before the walk could reach the ghost step this case is about.
    auto cur = exec(d, "DROP TABLE lost.parent CASCADE;");
    INFO("DROP over the lost constraint row: "
         << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                             : std::string{"reported success"}));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what.begin(), cur->get_error().what.end()};
    REQUIRE(what.find("has no catalog row") != std::string::npos);

    REQUIRE(rows_where(d, "pg_class", "oid", parent_oid) == 1);
    REQUIRE(exec(d, "SELECT * FROM lost.parent;")->is_success());
}

TEST_CASE("integration::cpp::drop_cascade_lost_row::diamond_dependent_is_judged_once") {
    auto config = make_test_config(fixture_path("diamond"));
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE dia;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE dia.parent (id bigint PRIMARY KEY);")->is_success());
    REQUIRE(
        exec(d, "CREATE TABLE dia.child (pid bigint, FOREIGN KEY (pid) REFERENCES dia.parent (id));")->is_success());

    const auto parent_oid = table_oid_named(d, "parent");
    REQUIRE(parent_oid != catalog::INVALID_OID);
    const auto fk_oid = fk_oid_referencing(d, parent_oid);
    REQUIRE(fk_oid != catalog::INVALID_OID);

    // The walker emits each object once per FINISHED node, not once per edge reaching it, so an
    // FK reachable from both its table and its referenced table cannot appear twice with a
    // falsely-zero duplicate own-row count; this DROP guards that invariant from the consumer side.
    auto cur = exec(d, "DROP DATABASE dia;");
    INFO("DROP DATABASE over the FK diamond: "
         << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                             : std::string{"success"}));
    REQUIRE(cur->is_success());

    REQUIRE(rows_where(d, "pg_constraint", "oid", fk_oid) == 0);
    REQUIRE(rows_where(d, "pg_class", "oid", parent_oid) == 0);
}
