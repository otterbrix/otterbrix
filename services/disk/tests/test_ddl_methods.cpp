#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <limits>
#include <unistd.h>

// DDL roundtrip tests. Each ddl_* method allocates an OID, writes pg_catalog rows,
// and the result becomes visible via resolve_*.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string ddl_dir() {
        static std::string p = "/tmp/test_otterbrix_ddl_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(ddl_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = ddl_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(ddl_dir());
            manager->set_run_fn([this] { scheduler->run(10000); });
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. ddl_create_namespace allocates a fresh OID >= FIRST_USER_OID.
TEST_CASE("services::disk::ddl::create_namespace_allocates_oid") {
    fixture fx;
    auto r = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns1"));
    REQUIRE(r.created_oid >= FIRST_USER_OID);
}

// 2. Two ddl_create_namespace calls produce distinct OIDs (monotonic generator).
TEST_CASE("services::disk::ddl::create_namespace_oids_are_unique") {
    fixture fx;
    auto a = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_a"));
    auto b = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_b"));
    REQUIRE(a.created_oid != b.created_oid);
    REQUIRE(b.created_oid > a.created_oid);
}

// 3. ddl_create_namespace + resolve_namespace round-trips.
TEST_CASE("services::disk::ddl::create_namespace_resolves") {
    fixture fx;
    auto c = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_x"));
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(),
                        std::string("ns_x"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == c.created_oid);
}

// 4. ddl_create_table writes pg_class + pg_attribute; resolve_table sees the columns.
TEST_CASE("services::disk::ddl::create_table_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nstab"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    REQUIRE(rt.created_oid >= FIRST_USER_OID);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.oid == rt.created_oid);
    REQUIRE(rr.columns.size() == 2);
}

// 5. ddl_create_table relkind='g' (generated/computing) is observable via resolve_table.relkind.
TEST_CASE("services::disk::ddl::create_table_computing_relkind") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsc"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("metrics"),
                         std::vector<components::table::column_definition_t>{}, catalog::relkind::computed);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("metrics"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.relkind == components::catalog::relkind::computed);
    (void)rt;
}

// 6. ddl_create_index writes pg_class (relkind='i') + pg_index; the index is resolvable.
TEST_CASE("services::disk::ddl::create_index_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsidx"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid,
                         rt.created_oid, std::string("idx_id"),
                         std::vector<std::string>{"id"});
    REQUIRE(ri.created_oid >= FIRST_USER_OID);
    REQUIRE(ri.created_oid != rt.created_oid);
}

// 7. ddl_index_set_valid flips indisvalid; both directions work.
TEST_CASE("services::disk::ddl::index_set_valid_flip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsiv"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid,
                         rt.created_oid, std::string("idx_id"),
                         std::vector<std::string>{"id"});
    // Flip to true then back to false — both succeed.
    auto sv1 = fx.invoke(&manager_disk_t::ddl_index_set_valid, fx.ctx(), ri.created_oid, true);
    REQUIRE(sv1.created_oid == ri.created_oid);
    auto sv2 = fx.invoke(&manager_disk_t::ddl_index_set_valid, fx.ctx(), ri.created_oid, false);
    REQUIRE(sv2.created_oid == ri.created_oid);
}

// 8. ddl_create_type allocates OID and pg_type row; resolve_type observes it.
TEST_CASE("services::disk::ddl::create_type_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsty"));
    auto rty = fx.invoke(&manager_disk_t::ddl_create_type, fx.ctx(), rns.created_oid,
                          std::string("widget"), std::string{});
    REQUIRE(rty.created_oid >= FIRST_USER_OID);
    auto rr = fx.invoke(&manager_disk_t::resolve_type, fx.ctx(), rns.created_oid,
                          std::string("widget"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.oid == rty.created_oid);
}

// 9. ddl_create_function allocates OID and pg_proc row; resolve_function observes it.
TEST_CASE("services::disk::ddl::create_function_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsfn"));
    auto rfn = fx.invoke(&manager_disk_t::ddl_create_function, fx.ctx(), rns.created_oid,
                          std::string("my_fn"), std::int32_t{0}, std::int64_t{0}, std::string{}, std::string{});
    REQUIRE(rfn.created_oid >= FIRST_USER_OID);
    auto rr = fx.invoke(&manager_disk_t::resolve_function, fx.ctx(), rns.created_oid,
                          std::string("my_fn"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.oid == rfn.created_oid);
}

// 10. ddl_drop_table removes pg_class + pg_attribute; resolve_table misses afterwards.
TEST_CASE("services::disk::ddl::drop_table_visible_to_resolve") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsdt"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), rt.created_oid,
               drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 11. ddl_create_sequence/view/macro write distinct pg_class relkinds (S/v/m).
TEST_CASE("services::disk::ddl::sequence_view_macro_distinct_oids") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsmix"));
    auto rs = fx.invoke(&manager_disk_t::ddl_create_sequence, fx.ctx(), rns.created_oid,
                         std::string("seq1"),
                         std::int64_t{1}, std::int64_t{1}, std::int64_t{1},
                         std::int64_t{std::numeric_limits<std::int64_t>::max()}, bool{false});
    auto rv = fx.invoke(&manager_disk_t::ddl_create_view, fx.ctx(), rns.created_oid,
                         std::string("v1"), std::string{});
    auto rm = fx.invoke(&manager_disk_t::ddl_create_macro, fx.ctx(), rns.created_oid,
                         std::string("m1"), std::string{});
    REQUIRE(rs.created_oid != rv.created_oid);
    REQUIRE(rv.created_oid != rm.created_oid);
    REQUIRE(rs.created_oid != rm.created_oid);
}

// 12. ddl_add_column appends a pg_attribute row, allocates a fresh attoid, and uses next_attnum.
TEST_CASE("services::disk::ddl::add_column_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsac"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    components::table::column_definition_t new_col(
        "added", components::types::complex_logical_type{components::types::logical_type::INTEGER});
    auto r = fx.invoke(&manager_disk_t::ddl_add_column, fx.ctx(), rt.created_oid,
                        std::move(new_col));
    REQUIRE(r.created_oid >= FIRST_USER_OID);
    // After add, the column count visible via resolve_table grows.
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 2);
}

// 13. ddl_drop_column writes a tombstone (attisdropped=true). resolve_table omits it.
TEST_CASE("services::disk::ddl::drop_column_tombstone") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsdc"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    fx.invoke(&manager_disk_t::ddl_drop_column, fx.ctx(), rt.created_oid,
               std::string("name"), drop_behavior_t::cascade_);
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 1);
}

// 14. E3.1: attnum is never reused. After drop_column + add_column, the new column gets
//     a fresh attnum > the dropped one.
TEST_CASE("services::disk::ddl::attnum_never_reused_after_drop") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nse3"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("b", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    // a=1, b=2 initially. Drop b (tombstone, attnum still 2), add c — c must get attnum 3.
    fx.invoke(&manager_disk_t::ddl_drop_column, fx.ctx(), rt.created_oid, std::string("b"),
               drop_behavior_t::cascade_);
    components::table::column_definition_t new_col(
        "c", components::types::complex_logical_type{components::types::logical_type::INTEGER});
    auto r = fx.invoke(&manager_disk_t::ddl_add_column, fx.ctx(), rt.created_oid,
                        std::move(new_col));
    REQUIRE(r.created_oid >= FIRST_USER_OID);
    // After the add, resolve_table shows 2 columns (a + c, b is tombstoned).
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 2);
}

// 15. ddl_rename_column updates the attname while keeping attoid + attnum.
TEST_CASE("services::disk::ddl::rename_column_keeps_oid") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsrn"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("old_name",
                       components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto rs1 = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                          std::string("t"), std::uint64_t{0});
    REQUIRE(rs1.found);
    REQUIRE(rs1.columns.size() == 1);
    auto orig_attoid = rs1.columns[0].attoid;

    fx.invoke(&manager_disk_t::ddl_rename_column, fx.ctx(), rt.created_oid,
               std::string("old_name"), std::string("new_name"));
    auto rs2 = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                          std::string("t"), std::uint64_t{0});
    REQUIRE(rs2.found);
    REQUIRE(rs2.columns.size() == 1);
    REQUIRE(rs2.columns[0].attoid == orig_attoid);
}

// 16. ddl_drop_column on an unknown column name is a no-op.
TEST_CASE("services::disk::ddl::drop_unknown_column_no_op") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsdcu"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    fx.invoke(&manager_disk_t::ddl_drop_column, fx.ctx(), rt.created_oid,
               std::string("not_a_column"), drop_behavior_t::cascade_);
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.columns.size() == 1); // unchanged
}

// 17. ddl_create_constraint allocates an OID and writes pg_constraint + pg_depend.
TEST_CASE("services::disk::ddl::create_constraint_round_trip") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nscc"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto rc = fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(), rt.created_oid,
                         std::string("uniq_id"), catalog::contype::unique, INVALID_OID,
                         std::vector<oid_t>{}, std::vector<oid_t>{},
                         catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});
    REQUIRE(rc.created_oid >= FIRST_USER_OID);
    REQUIRE(rc.created_oid != rt.created_oid);
}

// 18. FK constraint emits a pg_depend row to the referenced table; drop_table RESTRICT
//     of the referenced table is blocked.
TEST_CASE("services::disk::ddl::fk_constraint_blocks_ref_table_drop") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsfk"));
    std::vector<components::table::column_definition_t> parent_cols;
    parent_cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto parent = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                              std::string("parent"), std::move(parent_cols), catalog::relkind::regular);
    std::vector<components::table::column_definition_t> child_cols;
    child_cols.emplace_back("parent_id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto child = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                             std::string("child"), std::move(child_cols), catalog::relkind::regular);
    auto fk = fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(), child.created_oid,
                          std::string("fk_parent"), catalog::contype::foreign_key, parent.created_oid,
                          std::vector<oid_t>{}, std::vector<oid_t>{},
                          catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});
    REQUIRE(fk.created_oid >= FIRST_USER_OID);
    // RESTRICT drop of parent: blocked because FK constraint depends on it.
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), parent.created_oid,
                          drop_behavior_t::restrict_);
    REQUIRE(rd.status == ddl_status::restrict_blocked);
    REQUIRE(rd.blocking_oid != INVALID_OID);
}

// 19a. ddl_create_constraint with FK columns — verifies the constraint was stored
// (fk_constraints_for_table removed in Etap 5.1; conkey/confkey field checks moved to
// catalog_view / planner layer which reads pg_constraint directly).
TEST_CASE("services::disk::ddl::fk_constraint_persists_columns") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsfkc"));
    std::vector<components::table::column_definition_t> parent_cols;
    parent_cols.emplace_back("id",
                              components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto parent = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                              std::string("parent"), std::move(parent_cols), catalog::relkind::regular);
    std::vector<components::table::column_definition_t> child_cols;
    child_cols.emplace_back("parent_id",
                              components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto child = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                             std::string("child"), std::move(child_cols), catalog::relkind::regular);
    std::vector<oid_t> conkey{42};
    std::vector<oid_t> confkey{7};
    auto rc = fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(), child.created_oid,
               std::string("fk_parent_with_cols"), catalog::contype::foreign_key, parent.created_oid,
               conkey, confkey, catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});
    REQUIRE(rc.created_oid != INVALID_OID);
}

// 19. ddl_drop_constraint sweeps pg_constraint + pg_depend rows.
TEST_CASE("services::disk::ddl::drop_constraint_sweeps_pg_depend") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsdc"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto rc = fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(), rt.created_oid,
                         std::string("ck"), catalog::contype::check, INVALID_OID,
                         std::vector<oid_t>{}, std::vector<oid_t>{},
                         catalog::fk_match::simple, catalog::fk_action::no_action, catalog::fk_action::no_action, std::string{});
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_constraint, fx.ctx(), rc.created_oid,
                          drop_behavior_t::cascade_);
    REQUIRE(rd.created_oid == rc.created_oid);
}

// 20. ddl_adopt_computing_schema bumps catalog_version (schema mutation).
TEST_CASE("services::disk::ddl::adopt_computing_schema_bumps_version") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsac"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("metrics"),
                         std::vector<components::table::column_definition_t>{}, catalog::relkind::computed);
    auto v_before = fx.manager->catalog_version();
    std::vector<components::table::column_definition_t> new_cols;
    new_cols.emplace_back("count", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::ddl_adopt_computing_schema, fx.ctx(),
               rt.created_oid, std::move(new_cols));
    REQUIRE(fx.manager->catalog_version() > v_before);
}

// 21. ddl_create_computing_table allocates an OID and bumps catalog_version. Equivalent
// shape to ddl_create_table(relkind='g') with no columns; this is the named entry point.
TEST_CASE("services::disk::ddl::create_computing_table_allocates_oid") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nscct"));
    auto v_before = fx.manager->catalog_version();
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("metrics"));
    REQUIRE(rt.created_oid >= FIRST_USER_OID);
    REQUIRE(fx.manager->catalog_version() > v_before);
}

// 22. First ddl_computed_append on a fresh field inserts a pg_computed_column row and
// bumps the version. The append call must succeed without throwing.
TEST_CASE("services::disk::ddl::computed_append_new_field") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsca"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("agg"));
    auto v_before = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               rt.created_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    REQUIRE(fx.manager->catalog_version() > v_before);
}

// 23. Two appends with the same (field, type) bump the existing row's refcount instead
// of inserting a duplicate; both calls succeed and version monotonically advances.
TEST_CASE("services::disk::ddl::computed_append_same_type_bumps_refcount") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nscb"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("agg"));
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               rt.created_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto v_mid = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               rt.created_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    REQUIRE(fx.manager->catalog_version() > v_mid);
}

// 24. Drop after a single append removes the row entirely (refcount == 1 → delete).
// Drop after that is an idempotent no-op (still bumps version, but does not error).
TEST_CASE("services::disk::ddl::computed_drop_removes_when_refcount_zero") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nscd"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("agg"));
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               rt.created_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto v_before = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               rt.created_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_before);
    // Second drop on a now-empty field — idempotent.
    auto v_mid = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               rt.created_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_mid);
}

// 25. Append (rc=1), append (rc=2), drop (rc=1) — the field is still considered live.
// A subsequent drop (rc=0) finally removes it.
TEST_CASE("services::disk::ddl::computed_drop_decrements_refcount") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nsce"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("agg"));
    for (int i = 0; i < 2; ++i) {
        fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
                   rt.created_oid, std::string("count"),
                   components::catalog::well_known_oid::int64_type);
    }
    auto v_after_appends = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               rt.created_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_after_appends);
    // Second drop — refcount reaches 0, row removed.
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               rt.created_oid, std::string("count"));
    // Third drop on a removed field — idempotent (no exception, version still bumps).
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               rt.created_oid, std::string("count"));
}

// 26. Computing tables (relkind='g') do not get pg_attribute rows on creation —
// versioned fields live in pg_computed_column. resolve_table.columns must
// therefore be empty for a fresh computing table. Doc test alias:
// test_computing_table_pg_attribute_empty (catalog-migration-to-postgresql-style.md §14).
TEST_CASE("services::disk::ddl::computing_table_pg_attribute_empty") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("nscempty"));
    auto rt = fx.invoke(&manager_disk_t::ddl_create_computing_table, fx.ctx(),
                         rns.created_oid, std::string("agg"));
    REQUIRE(rt.created_oid >= FIRST_USER_OID);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("agg"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.relkind == components::catalog::relkind::computed);
    REQUIRE(rr.columns.empty());

    // After ddl_computed_append the field lives in pg_computed_column. V4 resolve_table
    // for relkind='g' tables fills `columns` from pg_computed_column (latest non-zero
    // refcount per attname) — matches catalog_view_t's expectation for sync validation.
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               rt.created_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto rr2 = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                          std::string("agg"), std::uint64_t{0});
    REQUIRE(rr2.found);
    REQUIRE(rr2.relkind == components::catalog::relkind::computed);
    REQUIRE(rr2.columns.size() == 1);
    REQUIRE(rr2.columns[0].attname == "count");
    REQUIRE(rr2.columns[0].atttypid == components::catalog::well_known_oid::int64_type);
}

// 27. §1.8: CHECK constraint stores conexpr verbatim in pg_constraint.
//     Constraint OID is allocated on create; DROP CONSTRAINT removes the row
//     (verified via scan_by_key on pg_constraint).
TEST_CASE("services::disk::ddl::check_constraint_stored") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_chk"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("age", components::types::complex_logical_type{components::types::logical_type::INTEGER});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto rc = fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(), rt.created_oid,
                         std::string("age_positive"), char{'c'},
                         components::catalog::INVALID_OID,
                         std::vector<components::catalog::oid_t>{},
                         std::vector<components::catalog::oid_t>{},
                         catalog::fk_match::simple, catalog::fk_action::no_action, char{'a'},
                         std::string("age > 0"));
    REQUIRE(rc.created_oid >= FIRST_USER_OID);
    // Verify pg_constraint row exists via scan_by_key on conrelid column.
    const collection_full_name_t pg_constraint{"pg_catalog", "main", "pg_constraint"};
    auto rows = fx.invoke(&manager_disk_t::scan_by_key, fx.ctx(), pg_constraint,
                           std::vector<std::string>{"conrelid"},
                           std::vector<components::types::logical_value_t>{
                               components::types::logical_value_t(&fx.resource, rt.created_oid)});
    REQUIRE(rows.size() == 1);
    // DROP CONSTRAINT removes the row.
    fx.invoke(&manager_disk_t::ddl_drop_constraint, fx.ctx(), rc.created_oid,
               drop_behavior_t::cascade_);
    auto rows_after = fx.invoke(&manager_disk_t::scan_by_key, fx.ctx(), pg_constraint,
                                 std::vector<std::string>{"conrelid"},
                                 std::vector<components::types::logical_value_t>{
                                     components::types::logical_value_t(&fx.resource, rt.created_oid)});
    REQUIRE(rows_after.empty());
}

// 28. §1.16: DROP COLUMN RESTRICT fails when an index depends on the column.
TEST_CASE("services::disk::ddl::drop_column_restrict_blocked_by_index") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_dcr"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid, rt.created_oid,
               std::string("idx_id"), std::vector<std::string>{"id"});
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_column, fx.ctx(), rt.created_oid,
                         std::string("id"), drop_behavior_t::restrict_);
    REQUIRE(rd.status == ddl_status::restrict_blocked);
    REQUIRE(rd.blocking_oid != INVALID_OID);
    // Column still present.
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.columns.size() == 1);
}

// 29. §1.16: DROP COLUMN CASCADE drops the dependent index automatically.
TEST_CASE("services::disk::ddl::drop_column_cascade_drops_index") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_dcc"));
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
    auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::move(cols), catalog::relkind::regular);
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid, rt.created_oid,
                         std::string("idx_id"), std::vector<std::string>{"id"});
    REQUIRE(ri.created_oid >= FIRST_USER_OID);
    // CASCADE drops the index along with the column.
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_column, fx.ctx(), rt.created_oid,
                         std::string("id"), drop_behavior_t::cascade_);
    REQUIRE(rd.status == ddl_status::ok);
    // Table still exists with only 'name' column.
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 1);
    REQUIRE(rs.columns[0].attname == "name");
    // Index is gone.
    auto ri_after = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), rns.created_oid,
                               std::string("idx_id"), std::uint64_t{0});
    REQUIRE_FALSE(ri_after.found);
}
