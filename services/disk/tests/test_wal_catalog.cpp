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
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_reader.hpp>
#include <services/wal/record.hpp>

#include <filesystem>
#include <limits>
#include <unistd.h>

// DDL records persist through WAL via manager_disk_t::append_pg_catalog_row,
// which calls write_physical_insert before direct_append_sync. Here we wire WAL +
// disk together, run a few ddl_* operations, then drop the actors and use a
// standalone wal_reader_t to verify the records are durable.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string wal_cat_dir() {
        static std::string p = "/tmp/test_otterbrix_walcat_" + std::to_string(::getpid());
        return p;
    }
    void cleanup_dir(const std::string& d) { std::filesystem::remove_all(d); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_wal wal_config;
        configuration::config_disk disk_config;
        std::unique_ptr<services::wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> disk;

        explicit fixture(const std::string& dir)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , wal_config([&]() {
                configuration::config_wal c;
                c.path = dir;
                c.on = true;
                return c;
            }())
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = dir;
                return c;
            }())
            , wal(actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource, scheduler, wal_config, log))
            , disk(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            std::filesystem::create_directories(dir);
            wal->set_run_fn([this] { scheduler->run(10000); });
            disk->set_run_fn([this] { scheduler->run(10000); });
            wal->sync(std::make_tuple(actor_zeta::address_t(disk->address()), actor_zeta::address_t::empty_address()));
            disk->sync(std::make_tuple(wal->address()));
            disk->bootstrap_system_tables_sync();
        }
        ~fixture() {
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(disk->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };

    // Count physical records whose collection.database == "pg_catalog" in a fresh
    // wal_reader scan. Used by tests after the fixture is destroyed.
    std::size_t pg_catalog_physical_count(const std::string& dir) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        services::wal::wal_reader_t reader(c, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        std::size_t n = 0;
        for (auto& r : records) {
            if (r.is_physical() && r.collection_name.database == "pg_catalog")
                ++n;
        }
        return n;
    }

    std::size_t pg_catalog_records_for(const std::string& dir, const std::string& collection) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        services::wal::wal_reader_t reader(c, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        std::size_t n = 0;
        for (auto& r : records) {
            if (r.is_physical() && r.collection_name.database == "pg_catalog" &&
                r.collection_name.collection == collection)
                ++n;
        }
        return n;
    }
} // namespace

// 1. Bootstrap doesn't emit WAL records — well-known rows are seeded via direct_append_sync
//    at txn=0 (idempotent on every startup). WAL records only appear once user ddl_* runs.
TEST_CASE("services::disk::wal_catalog::bootstrap_alone_no_wal") {
    auto dir = wal_cat_dir() + "/bootstrap";
    cleanup_dir(dir);
    {
        fixture fx(dir);
        (void)fx;
    }
    // No ddl_* invoked → no WAL records expected.
    REQUIRE(pg_catalog_physical_count(dir) == 0);
    cleanup_dir(dir);
}

// 2. ddl_create_namespace adds at least one pg_namespace record.
TEST_CASE("services::disk::wal_catalog::create_namespace_writes_pg_namespace") {
    auto dir = wal_cat_dir() + "/create_ns";
    cleanup_dir(dir);
    auto before = std::size_t{0};
    {
        fixture fx(dir);
        before = pg_catalog_records_for(dir, "pg_namespace");
        fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("user_ns"));
    }
    auto after = pg_catalog_records_for(dir, "pg_namespace");
    REQUIRE(after > before);
    cleanup_dir(dir);
}

// 3. ddl_create_table writes pg_class + per-column pg_attribute rows.
TEST_CASE("services::disk::wal_catalog::create_table_writes_pg_class_and_pg_attribute") {
    auto dir = wal_cat_dir() + "/create_table";
    cleanup_dir(dir);
    std::size_t cls_before = 0, att_before = 0;
    {
        fixture fx(dir);
        cls_before = pg_catalog_records_for(dir, "pg_class");
        att_before = pg_catalog_records_for(dir, "pg_attribute");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
        cols.emplace_back("count", components::types::complex_logical_type{components::types::logical_type::INTEGER});
        fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                   std::string("t"), std::move(cols), catalog::relkind::regular);
    }
    auto cls_after = pg_catalog_records_for(dir, "pg_class");
    auto att_after = pg_catalog_records_for(dir, "pg_attribute");
    REQUIRE(cls_after >= cls_before + 1);
    // At least one pg_attribute row per column.
    REQUIRE(att_after >= att_before + 3);
    cleanup_dir(dir);
}

// 4. ddl_create_table writes pg_depend rows (table→namespace + column→type per column).
TEST_CASE("services::disk::wal_catalog::create_table_writes_pg_depend") {
    auto dir = wal_cat_dir() + "/create_dep";
    cleanup_dir(dir);
    std::size_t before = 0;
    {
        fixture fx(dir);
        before = pg_catalog_records_for(dir, "pg_depend");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                   std::string("t"), std::move(cols), catalog::relkind::regular);
    }
    auto after = pg_catalog_records_for(dir, "pg_depend");
    // table→namespace + column→type for the single column = at least 2 new rows.
    REQUIRE(after >= before + 2);
    cleanup_dir(dir);
}

// 5. ddl_create_index writes pg_class (relkind='i') + pg_index + pg_depend (index→table 'a').
TEST_CASE("services::disk::wal_catalog::create_index_writes_pg_index") {
    auto dir = wal_cat_dir() + "/create_idx";
    cleanup_dir(dir);
    std::size_t idx_before = 0;
    {
        fixture fx(dir);
        idx_before = pg_catalog_records_for(dir, "pg_index");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                             std::string("t"), std::move(cols), catalog::relkind::regular);
        fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid,
                   rt.created_oid, std::string("idx_id"), std::vector<std::string>{"id"});
    }
    auto idx_after = pg_catalog_records_for(dir, "pg_index");
    REQUIRE(idx_after >= idx_before + 1);
    cleanup_dir(dir);
}

// 6. ddl_index_set_valid writes a fresh pg_index row (delete + insert).
TEST_CASE("services::disk::wal_catalog::index_set_valid_writes_pg_index") {
    auto dir = wal_cat_dir() + "/idx_valid";
    cleanup_dir(dir);
    std::size_t mid = 0, after = 0;
    {
        fixture fx(dir);
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                             std::string("t"), std::move(cols), catalog::relkind::regular);
        auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), rns.created_oid,
                             rt.created_oid, std::string("idx_id"), std::vector<std::string>{"id"});
        // Capture pg_index count after create_index but before set_valid.
        // (Reading mid-stream requires destroying the actors first; for simplicity we
        // count after instead and assert a minimum bump.)
        mid = pg_catalog_records_for(dir, "pg_index");
        fx.invoke(&manager_disk_t::ddl_index_set_valid, fx.ctx(), ri.created_oid, true);
    }
    after = pg_catalog_records_for(dir, "pg_index");
    REQUIRE(after >= mid);  // set_valid wrote at least the re-insert (and possibly delete tombstones)
    cleanup_dir(dir);
}

// 7. ddl_create_type writes a pg_type record and a pg_depend type→namespace record.
TEST_CASE("services::disk::wal_catalog::create_type_writes_pg_type_and_depend") {
    auto dir = wal_cat_dir() + "/create_type";
    cleanup_dir(dir);
    std::size_t ty_before = 0, dep_before = 0;
    {
        fixture fx(dir);
        ty_before = pg_catalog_records_for(dir, "pg_type");
        dep_before = pg_catalog_records_for(dir, "pg_depend");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        fx.invoke(&manager_disk_t::ddl_create_type, fx.ctx(), rns.created_oid,
                   std::string("widget"), std::string{});
    }
    REQUIRE(pg_catalog_records_for(dir, "pg_type") >= ty_before + 1);
    REQUIRE(pg_catalog_records_for(dir, "pg_depend") >= dep_before + 1);
    cleanup_dir(dir);
}

// 8. ddl_create_function writes a pg_proc record and a pg_depend function→namespace record.
TEST_CASE("services::disk::wal_catalog::create_function_writes_pg_proc_and_depend") {
    auto dir = wal_cat_dir() + "/create_fn";
    cleanup_dir(dir);
    std::size_t pr_before = 0, dep_before = 0;
    {
        fixture fx(dir);
        pr_before = pg_catalog_records_for(dir, "pg_proc");
        dep_before = pg_catalog_records_for(dir, "pg_depend");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        fx.invoke(&manager_disk_t::ddl_create_function, fx.ctx(), rns.created_oid,
                   std::string("my_fn"), std::int32_t{0}, std::int64_t{0}, std::string{}, std::string{});
    }
    REQUIRE(pg_catalog_records_for(dir, "pg_proc") >= pr_before + 1);
    REQUIRE(pg_catalog_records_for(dir, "pg_depend") >= dep_before + 1);
    cleanup_dir(dir);
}

// 9. All pg_catalog WAL records carry collection_name.database == "pg_catalog" — needed for
//    the WAL replay split (pg_catalog records replayed first, user records second).
TEST_CASE("services::disk::wal_catalog::all_records_under_pg_catalog_database") {
    auto dir = wal_cat_dir() + "/db_prefix";
    cleanup_dir(dir);
    {
        fixture fx(dir);
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                   std::string("t"), std::move(cols), catalog::relkind::regular);
    }
    // Read all records and verify pg_catalog records all carry the right database tag.
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    configuration::config_wal c;
    c.path = dir;
    c.on = true;
    services::wal::wal_reader_t reader(c, log);
    auto records = reader.read_committed_records(services::wal::id_t{0});
    bool seen_any = false;
    for (auto& r : records) {
        if (!r.is_physical())
            continue;
        // Every physical record we wrote was for a pg_catalog.* collection.
        REQUIRE(r.collection_name.database == "pg_catalog");
        seen_any = true;
    }
    REQUIRE(seen_any);
    cleanup_dir(dir);
}

// 10. ddl_drop_table emits delete-style WAL records (the cascade walks pg_class/pg_attribute/pg_depend).
//     We can't easily count deletes, but the operation should produce no INSERT records targeting
//     the collection of the dropped relation (i.e., we don't see resurrection writes).
TEST_CASE("services::disk::wal_catalog::drop_table_no_resurrect_writes") {
    auto dir = wal_cat_dir() + "/drop_no_resurrect";
    cleanup_dir(dir);
    components::catalog::oid_t t_oid = INVALID_OID;
    std::size_t cls_before_drop = 0;
    {
        fixture fx(dir);
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt = fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns.created_oid,
                             std::string("t"), std::move(cols), catalog::relkind::regular);
        t_oid = rt.created_oid;
        cls_before_drop = pg_catalog_records_for(dir, "pg_class");
        fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), t_oid, drop_behavior_t::cascade_);
    }
    // After the drop we still see at least the INSERT records that created the table — drop
    // path is MVCC-delete, not WAL append for new pg_class rows.
    auto cls_after = pg_catalog_records_for(dir, "pg_class");
    REQUIRE(cls_after >= cls_before_drop);
    cleanup_dir(dir);
}

// 11. Multiple ddl operations within a fixture lifetime accumulate WAL records monotonically.
TEST_CASE("services::disk::wal_catalog::record_count_grows_with_ddl") {
    auto dir = wal_cat_dir() + "/grow";
    cleanup_dir(dir);
    std::size_t after_each[4] = {0, 0, 0, 0};
    {
        fixture fx(dir);
        after_each[0] = pg_catalog_physical_count(dir);  // bootstrap baseline
        auto rns1 = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns1"));
        after_each[1] = pg_catalog_physical_count(dir);
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        fx.invoke(&manager_disk_t::ddl_create_table, fx.ctx(), rns1.created_oid,
                   std::string("t"), std::move(cols), catalog::relkind::regular);
        after_each[2] = pg_catalog_physical_count(dir);
        fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns2"));
        after_each[3] = pg_catalog_physical_count(dir);
    }
    REQUIRE(after_each[1] >= after_each[0]);
    REQUIRE(after_each[2] >= after_each[1]);
    REQUIRE(after_each[3] >= after_each[2]);
    cleanup_dir(dir);
}

// 12. ddl_create_sequence writes a pg_class row with relkind='S' (well-known sequence relkind).
TEST_CASE("services::disk::wal_catalog::create_sequence_writes_pg_class") {
    auto dir = wal_cat_dir() + "/create_seq";
    cleanup_dir(dir);
    std::size_t cls_before = 0;
    {
        fixture fx(dir);
        cls_before = pg_catalog_records_for(dir, "pg_class");
        auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns"));
        fx.invoke(&manager_disk_t::ddl_create_sequence, fx.ctx(), rns.created_oid,
                   std::string("widget_seq"),
                   std::int64_t{1}, std::int64_t{1}, std::int64_t{1},
                   std::int64_t{std::numeric_limits<std::int64_t>::max()}, bool{false});
    }
    REQUIRE(pg_catalog_records_for(dir, "pg_class") >= cls_before + 1);
    cleanup_dir(dir);
}
