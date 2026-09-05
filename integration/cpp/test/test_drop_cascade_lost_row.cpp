#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <services/disk/manager_disk.hpp>

#include <unistd.h>

#include <limits>
#include <string>
#include <thread>

// A DROP CASCADE MUST NOT REPORT SUCCESS OVER A PLANNED OBJECT THE CATALOG DOES NOT HOLD.
//
// operator_dynamic_cascade_delete_t plans its steps from pg_depend edges and then executes a
// per-classid template of catalog-row deletes per step. The template is deliberately
// over-generated (it re-issues e.g. the pg_sequence and pg_rewrite deletes for a plain table),
// so most zero counts carry no information — but ONE spec of every template is the step's OWN
// row ({classid, col 0, objid}). A zero there means the catalog never held (or no longer
// holds) the object the plan named: proceeding takes the storage/index drop marks over a
// catalog inconsistency — the half-applied DROP the operator's own comments promise to avoid.
//
// The first case builds exactly that inconsistency: a pg_depend edge is forged through the
// disk manager's own funnel, claiming a constraint that has NO pg_constraint row — the state
// any half-applied earlier scrub leaves behind. (Forging the edge, rather than deleting a real
// constraint's row, keeps the fixture honest: a td{0,0} funnel delete leaves a ghost the DROP's
// statement-time scan still marks, and the failure then comes from the commit drain's replay —
// a different, later channel.) The cascade walks the edge, plans the constraint step, and its
// own-row delete counts 0. The statement must refuse — and the refused DROP must leave the
// parent table intact (the autocommit abort puts the already-deleted rows back).
//
// The second case pins the reason a blanket zero-refusal was NOT the fix: the dependency walker
// pushes a dependent once per edge that reaches it (topological_drop_order's order.push_back
// sits outside the black-set check), so an FK constraint reachable from BOTH its table and its
// referenced table appears TWICE in the plan. The second occurrence's own-row delete
// legitimately counts 0 — steps have to be deduplicated by (classid, objid) before any count is
// judged, and this DROP DATABASE must keep succeeding.

using namespace test_helpers;

namespace {

    namespace catalog = components::catalog;

    class lost_row_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit lost_row_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            components::compute::function_registry_t::reset_default();
        }

        services::disk::manager_disk_t* disk() noexcept { return manager_disk_.get(); }
    };

    // Committed rows of `table_oid` whose column `key_col` equals `key`, read through the
    // disk manager's own funnel (snapshot_horizon = max: every committed row).
    template<typename Key>
    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    catalog_chunks_with(lost_row_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto* resource = space.disk()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        std::pmr::vector<std::uint64_t> key_cols(resource);
        key_cols.emplace_back(key_col);
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    table_oid,
                                                    std::move(key_cols),
                                                    components::operators::make_key_chunk(resource, key),
                                                    std::pmr::vector<std::uint64_t>{resource});
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        return std::move(fut).take_ready();
    }

    template<typename Key>
    std::size_t
    catalog_rows_with(lost_row_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto batches = catalog_chunks_with(space, table_oid, key_col, key);
        REQUIRE_FALSE(batches.has_error());
        std::size_t rows = 0;
        for (const auto& chunk : batches.value()) {
            rows += static_cast<std::size_t>(chunk.size());
        }
        return rows;
    }

    // The relation's oid from the pg_class row that names it.
    catalog::oid_t table_oid_named(lost_row_spaces_t& space, const std::string& name) {
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_class_table,
                                           catalog::pg_class_col::relname,
                                           std::string_view{name});
        REQUIRE_FALSE(batches.has_error());
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!chunk.is_null(0, i)) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        return catalog::INVALID_OID;
    }

    // The FK constraint whose confrelid names `parent_oid` (the PK row carries no
    // confrelid, so this key selects the FK alone; contype is asserted anyway).
    catalog::oid_t fk_oid_referencing(lost_row_spaces_t& space, catalog::oid_t parent_oid) {
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_constraint_table,
                                           catalog::pg_constraint_col::confrelid,
                                           parent_oid);
        REQUIRE_FALSE(batches.has_error());
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(catalog::pg_constraint_col::contype, i)) {
                    continue;
                }
                const auto contype_cell = chunk.get_value<std::string_view>(catalog::pg_constraint_col::contype, i);
                if (!contype_cell.empty() && contype_cell.front() == 'f') {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        return catalog::INVALID_OID;
    }

    // Forge one pg_depend edge through the manager's own funnel: (classid, objid) names a
    // dependent whose own catalog row does not exist. This is the smallest honest replica
    // of the half-applied state the case is about — the object row gone, the edge alive.
    void forge_depend_edge(lost_row_spaces_t& space,
                           catalog::oid_t classid,
                           catalog::oid_t objid,
                           catalog::oid_t refclassid,
                           catalog::oid_t refobjid) {
        auto* resource = space.disk()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto row = catalog::build_pg_depend_row(resource, classid, objid, refclassid, refobjid, /*deptype=*/'n');
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    catalog::well_known_oid::pg_depend_table,
                                                    std::move(row));
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        auto appended = std::move(fut).take_ready();
        REQUIRE_FALSE(appended.has_error());
    }

    std::string fixture_path(const char* leaf) {
        std::string p = "/tmp/test_drop_cascade_lost_row_";
        p += std::to_string(::getpid());
        p += '_';
        p += leaf;
        return p;
    }

} // namespace

TEST_CASE("integration::cpp::drop_cascade_lost_row::planned_step_without_a_catalog_row_refuses") {
    auto config = make_test_config(fixture_path("lost"));
    lost_row_spaces_t space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE lost;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE lost.parent (id bigint PRIMARY KEY);")->is_success());

    const auto parent_oid = table_oid_named(space, "parent");
    REQUIRE(parent_oid != catalog::INVALID_OID);

    // Build the inconsistency: an edge names a constraint the catalog holds no row for.
    const catalog::oid_t ghost_oid = catalog::FIRST_USER_OID + 777777;
    REQUIRE(catalog_rows_with(space, catalog::well_known_oid::pg_constraint_table, catalog::pg_constraint_col::oid, ghost_oid) ==
            0);
    forge_depend_edge(space,
                      catalog::well_known_oid::pg_constraint_table,
                      ghost_oid,
                      catalog::well_known_oid::pg_class_table,
                      parent_oid);
    REQUIRE(catalog_rows_with(space, catalog::well_known_oid::pg_depend_table, catalog::pg_depend_col::objid, ghost_oid) ==
            1);

    // The cascade walks the surviving edge, plans the constraint step, and the step's own
    // row deletes 0 rows. That zero is the statement's answer.
    auto cur = exec(d, "DROP TABLE lost.parent;");
    INFO("DROP over the lost constraint row: "
         << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                             : std::string{"reported success"}));
    REQUIRE(cur->is_error());
    const std::string what{cur->get_error().what.begin(), cur->get_error().what.end()};
    REQUIRE(what.find("has no catalog row") != std::string::npos);

    // Nothing was half-applied: the refused DROP left the parent readable, its pg_class
    // row in place, and the edge that exposed the inconsistency still there to report.
    REQUIRE(catalog_rows_with(space, catalog::well_known_oid::pg_class_table, catalog::pg_class_col::oid, parent_oid) ==
            1);
    REQUIRE(exec(d, "SELECT * FROM lost.parent;")->is_success());
}

TEST_CASE("integration::cpp::drop_cascade_lost_row::diamond_dependent_is_judged_once") {
    auto config = make_test_config(fixture_path("diamond"));
    lost_row_spaces_t space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE dia;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE dia.parent (id bigint PRIMARY KEY);")->is_success());
    REQUIRE(
        exec(d, "CREATE TABLE dia.child (pid bigint, FOREIGN KEY (pid) REFERENCES dia.parent (id));")->is_success());

    const auto parent_oid = table_oid_named(space, "parent");
    REQUIRE(parent_oid != catalog::INVALID_OID);
    const auto fk_oid = fk_oid_referencing(space, parent_oid);
    REQUIRE(fk_oid != catalog::INVALID_OID);

    // The constraint is reachable from BOTH tables, so the walker emits it twice; only a
    // deduplicated plan may judge own-row counts. This DROP has to keep succeeding — a
    // refusal here would be the zero policy misreading its own duplicate.
    auto cur = exec(d, "DROP DATABASE dia;");
    INFO("DROP DATABASE over the FK diamond: "
         << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                             : std::string{"success"}));
    REQUIRE(cur->is_success());

    // The cascade actually happened: constraint row, both tables' pg_class rows, gone.
    REQUIRE(catalog_rows_with(space, catalog::well_known_oid::pg_constraint_table, catalog::pg_constraint_col::oid, fk_oid) ==
            0);
    REQUIRE(catalog_rows_with(space, catalog::well_known_oid::pg_class_table, catalog::pg_class_col::oid, parent_oid) ==
            0);
}
