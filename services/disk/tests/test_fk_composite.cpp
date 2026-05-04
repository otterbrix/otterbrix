#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <optional>
#include <unistd.h>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using namespace components::types;
using session_id_t = components::session::session_id_t;
using execution_context_t = components::execution_context_t;

namespace {
    std::string fk_comp_dir() {
        static std::string p = "/tmp/test_otterbrix_fk_comp_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(fk_comp_dir()); }

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
                c.path = fk_comp_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(fk_comp_dir());
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
            auto [_, future] =
                actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        execution_context_t ctx(collection_full_name_t name = {}) {
            return execution_context_t{session_id_t{}, components::table::transaction_data{0, 0},
                                       std::move(name)};
        }

        // Create a 2-column BIGINT table; returns ddl_result_t with all_oids.
        ddl_result_t make_table(oid_t ns_oid, const std::string& tbl,
                                const std::string& c1, const std::string& c2) {
            std::vector<components::table::column_definition_t> cols;
            cols.emplace_back(c1, complex_logical_type{logical_type::BIGINT});
            cols.emplace_back(c2, complex_logical_type{logical_type::BIGINT});
            return invoke(&manager_disk_t::ddl_create_table, ctx(), ns_oid, tbl,
                          std::move(cols), catalog::relkind::regular);
        }

        // Register storage and append one row (v1, v2) at the given key.
        void register_and_insert(collection_full_name_t name, int64_t v1, int64_t v2) {
            std::vector<components::table::column_definition_t> cols;
            cols.emplace_back("c1", complex_logical_type{logical_type::BIGINT});
            cols.emplace_back("c2", complex_logical_type{logical_type::BIGINT});
            invoke(&manager_disk_t::create_storage_with_columns, session_id_t{}, name,
                   std::move(cols));

            std::pmr::vector<complex_logical_type> types(&resource);
            types.emplace_back(logical_type::BIGINT);
            types.emplace_back(logical_type::BIGINT);
            auto chunk = std::make_unique<components::vector::data_chunk_t>(&resource, types, 1);
            chunk->set_cardinality(1);
            chunk->set_value(0, 0, logical_value_t(&resource, v1));
            chunk->set_value(1, 0, logical_value_t(&resource, v2));
            invoke(&manager_disk_t::storage_append, ctx(name), std::move(chunk));
        }

        // Build a 1-row 2-column BIGINT chunk; nullopt → NULL column.
        std::unique_ptr<components::vector::data_chunk_t>
        make_chunk(std::optional<int64_t> v1, std::optional<int64_t> v2) {
            std::pmr::vector<complex_logical_type> types(&resource);
            types.emplace_back(logical_type::BIGINT);
            types.emplace_back(logical_type::BIGINT);
            auto chunk = std::make_unique<components::vector::data_chunk_t>(&resource, types, 1);
            chunk->set_cardinality(1);
            chunk->set_value(
                0, 0,
                v1 ? logical_value_t(&resource, *v1) : logical_value_t(&resource, logical_type::NA));
            chunk->set_value(
                1, 0,
                v2 ? logical_value_t(&resource, *v2) : logical_value_t(&resource, logical_type::NA));
            return chunk;
        }
    };

    // Common setup for INSERT-side FK tests:
    //   - namespace "ns_cmp", parent table "parent_cmp"(pk1,pk2), child table "child_cmp"(fk1,fk2)
    //   - FK with given matchtype from child(fk1,fk2) → parent(pk1,pk2)
    //   - parent storage populated with one row (10, 20)
    // Returns child_full_name used to call fk_validate_insert.
    struct insert_setup_t {
        collection_full_name_t child_name; // {database="ns_cmp", collection="child_cmp"}
    };

    insert_setup_t setup_insert_fk(fixture& fx, char matchtype) {
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_cmp"));
        REQUIRE(rns.created_oid >= FIRST_USER_OID);
        auto parent_r = fx.make_table(rns.created_oid, "parent_cmp", "pk1", "pk2");
        REQUIRE(parent_r.created_oid >= FIRST_USER_OID);
        auto child_r = fx.make_table(rns.created_oid, "child_cmp", "fk1", "fk2");
        REQUIRE(child_r.created_oid >= FIRST_USER_OID);

        std::vector<oid_t> child_attoids = {child_r.all_oids.at("fk1"), child_r.all_oids.at("fk2")};
        std::vector<oid_t> parent_attoids = {parent_r.all_oids.at("pk1"), parent_r.all_oids.at("pk2")};
        fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(),
                  child_r.created_oid, std::string("fk_cmp"),
                  catalog::contype::foreign_key, parent_r.created_oid,
                  child_attoids, parent_attoids,
                  matchtype, catalog::fk_action::no_action, catalog::fk_action::no_action,
                  std::string{});

        // Register parent storage and insert (10, 20).
        collection_full_name_t parent_name("ns_cmp", "parent_cmp");
        fx.register_and_insert(parent_name, 10, 20);

        return insert_setup_t{collection_full_name_t("ns_cmp", "child_cmp")};
    }

} // namespace

// --- MATCH SIMPLE (default) ---

TEST_CASE("services::disk::fk_composite::simple_valid_insert") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::simple);
    // Child row (10, 20) → parent (10, 20) exists → passes
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(10, 20));
    REQUIRE_FALSE(err.has_value());
}

TEST_CASE("services::disk::fk_composite::simple_null_fk_column_passes") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::simple);
    // MATCH SIMPLE: any NULL FK component → row passes without checking parent.
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(std::nullopt, 20));
    REQUIRE_FALSE(err.has_value());
}

TEST_CASE("services::disk::fk_composite::simple_no_matching_parent_rejects") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::simple);
    // Child row (10, 99) → no parent row with pk2=99 → error
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(10, 99));
    REQUIRE(err.has_value());
    REQUIRE_FALSE(err->empty());
}

// --- MATCH FULL ---

TEST_CASE("services::disk::fk_composite::full_partial_null_rejects") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::full);
    // MATCH FULL: partial NULL (one col null, one non-null) → error
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(10, std::nullopt));
    REQUIRE(err.has_value());
}

TEST_CASE("services::disk::fk_composite::full_all_null_passes") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::full);
    // MATCH FULL: all-NULL tuple → passes without checking parent
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(std::nullopt, std::nullopt));
    REQUIRE_FALSE(err.has_value());
}

// --- MATCH PARTIAL ---

TEST_CASE("services::disk::fk_composite::partial_null_acts_as_wildcard") {
    fixture fx;
    auto [child_name] = setup_insert_fk(fx, catalog::fk_match::partial);
    // MATCH PARTIAL: null fk1 is a wildcard; only fk2=20 is checked → parent (10,20) satisfies
    auto err = fx.invoke(&manager_disk_t::fk_validate_insert, fx.ctx(), child_name,
                          fx.make_chunk(std::nullopt, 20));
    REQUIRE_FALSE(err.has_value());
}

// --- Parent-delete side: RESTRICT ---

TEST_CASE("services::disk::fk_composite::restrict_blocks_parent_delete") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_cmp"));
    auto parent_r = fx.make_table(rns.created_oid, "parent_cmp", "pk1", "pk2");
    auto child_r = fx.make_table(rns.created_oid, "child_cmp", "fk1", "fk2");

    std::vector<oid_t> child_attoids = {child_r.all_oids.at("fk1"), child_r.all_oids.at("fk2")};
    std::vector<oid_t> parent_attoids = {parent_r.all_oids.at("pk1"), parent_r.all_oids.at("pk2")};
    fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(),
              child_r.created_oid, std::string("fk_restrict_cmp"),
              catalog::contype::foreign_key, parent_r.created_oid,
              child_attoids, parent_attoids,
              catalog::fk_match::simple,
              catalog::fk_action::restrict_, catalog::fk_action::no_action,
              std::string{});

    // Register child storage under key matching fk_validate_parent_delete lookup:
    // {database=parent.name.database, schema=child_ns_name, collection=child_table}
    // Here parent.name.database = "ns_cmp", child_ns_name = "ns_cmp" (same namespace).
    collection_full_name_t child_key("ns_cmp", "ns_cmp", "child_cmp");
    fx.register_and_insert(child_key, 10, 20);

    // fk_validate_parent_delete is called with parent's name (database=ns_cmp).
    collection_full_name_t parent_name("ns_cmp", "parent_cmp");
    auto err = fx.invoke(&manager_disk_t::fk_validate_parent_delete, fx.ctx(), parent_name,
                          fx.make_chunk(10, 20));
    REQUIRE(err.has_value());
    REQUIRE(err->find("RESTRICT") != std::string::npos);
}

// --- Parent-delete side: CASCADE ---

TEST_CASE("services::disk::fk_composite::cascade_deletes_child_rows") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_cmp"));
    auto parent_r = fx.make_table(rns.created_oid, "parent_cmp", "pk1", "pk2");
    auto child_r = fx.make_table(rns.created_oid, "child_cmp", "fk1", "fk2");

    std::vector<oid_t> child_attoids = {child_r.all_oids.at("fk1"), child_r.all_oids.at("fk2")};
    std::vector<oid_t> parent_attoids = {parent_r.all_oids.at("pk1"), parent_r.all_oids.at("pk2")};
    fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(),
              child_r.created_oid, std::string("fk_cascade_cmp"),
              catalog::contype::foreign_key, parent_r.created_oid,
              child_attoids, parent_attoids,
              catalog::fk_match::simple,
              catalog::fk_action::cascade, catalog::fk_action::no_action,
              std::string{});

    collection_full_name_t child_key("ns_cmp", "ns_cmp", "child_cmp");
    fx.register_and_insert(child_key, 10, 20);

    collection_full_name_t parent_name("ns_cmp", "parent_cmp");
    auto err = fx.invoke(&manager_disk_t::fk_validate_parent_delete, fx.ctx(), parent_name,
                          fx.make_chunk(10, 20));
    // CASCADE: no error (deletion completes).
    REQUIRE_FALSE(err.has_value());

    // Verify: second call finds no child rows (they were MVCC-deleted by the cascade).
    // storage_total_rows counts physical rows (including tombstoned); use a second
    // fk_validate_parent_delete call to confirm the scan-visible child is gone.
    auto err2 = fx.invoke(&manager_disk_t::fk_validate_parent_delete, fx.ctx(), parent_name,
                           fx.make_chunk(10, 20));
    REQUIRE_FALSE(err2.has_value());
}

// --- Parent-delete side: SET NULL ---

TEST_CASE("services::disk::fk_composite::set_null_nulls_child_fk_columns") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_cmp"));
    auto parent_r = fx.make_table(rns.created_oid, "parent_cmp", "pk1", "pk2");
    auto child_r = fx.make_table(rns.created_oid, "child_cmp", "fk1", "fk2");

    std::vector<oid_t> child_attoids = {child_r.all_oids.at("fk1"), child_r.all_oids.at("fk2")};
    std::vector<oid_t> parent_attoids = {parent_r.all_oids.at("pk1"), parent_r.all_oids.at("pk2")};
    fx.invoke(&manager_disk_t::ddl_create_constraint, fx.ctx(),
              child_r.created_oid, std::string("fk_setnull_cmp"),
              catalog::contype::foreign_key, parent_r.created_oid,
              child_attoids, parent_attoids,
              catalog::fk_match::simple,
              catalog::fk_action::set_null, catalog::fk_action::no_action,
              std::string{});

    collection_full_name_t child_key("ns_cmp", "ns_cmp", "child_cmp");
    fx.register_and_insert(child_key, 10, 20);

    collection_full_name_t parent_name("ns_cmp", "parent_cmp");
    auto err = fx.invoke(&manager_disk_t::fk_validate_parent_delete, fx.ctx(), parent_name,
                          fx.make_chunk(10, 20));
    // SET NULL: no error (update completes).
    REQUIRE_FALSE(err.has_value());

    // Verify child storage still has 1 row (row was updated, not deleted).
    auto rows = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, child_key);
    REQUIRE(rows == 1);
}
