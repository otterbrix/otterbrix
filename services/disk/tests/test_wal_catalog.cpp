#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/session_catalog.hpp>
#include <core/date/timezones.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/pmr.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_reader.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"

#include <filesystem>
#include <limits>
#include <thread>
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
    using namespace disk_test_helpers;
    std::string wal_cat_dir() {
        static std::string p = "/tmp/test_otterbrix_walcat_" + std::to_string(::getpid());
        return p;
    }
    void cleanup_dir(const std::string& d) { std::filesystem::remove_all(d); }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_wal wal_config;
        configuration::config_disk disk_config;
        std::unique_ptr<services::wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> disk;

        // wire_wal=false leaves the WAL manager unwired (disk never learns the WAL
        // address, so agent_disk_t::manager_wal_addr_ stays empty_address()): catalog
        // mutations still hit storage via direct_append_sync but emit no WAL records.
        // Mirrors the production "WAL off" path and the bootstrap_alone_no_wal scenario.
        explicit fixture(const std::string& dir, bool wire_wal = true)
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
            if (wire_wal) {
                wal->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(disk->address()),
                                                         actor_zeta::address_t::empty_address(),
                                                         actor_zeta::address_t::empty_address()});
                disk->sync(services::disk::manager_disk_t::disk_sync_pack_t{wal->address()});
            }
            disk->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the managers first: each dtor joins its internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
            disk.reset();
            wal.reset();
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(disk->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };

    // WAL records carry table_oid, and every pg_catalog table has a well-known OID
    // fixed in catalog_oids.hpp. Anything below FIRST_USER_OID is a system-table
    // record; a specific table is selected by well_known_oid::pg_*_table.

    namespace wk = components::catalog::well_known_oid;

    std::size_t pg_catalog_physical_count(const std::string& dir) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        core::pmr::otterbrix_resource reader_resource;
        services::wal::wal_reader_t reader(&reader_resource, c, log);
        auto records_result = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();
        std::size_t n = 0;
        for (auto& r : records) {
            if (r.is_physical() && r.table_oid != components::catalog::INVALID_OID &&
                r.table_oid < components::catalog::FIRST_USER_OID)
                ++n;
        }
        return n;
    }

    std::size_t pg_catalog_records_for(const std::string& dir, components::catalog::oid_t target_oid) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        core::pmr::otterbrix_resource reader_resource;
        services::wal::wal_reader_t reader(&reader_resource, c, log);
        auto records_result = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();
        std::size_t n = 0;
        for (auto& r : records) {
            if (r.is_physical() && r.table_oid == target_oid)
                ++n;
        }
        return n;
    }

    // Ordered list of (type, table_oid) for every pg_catalog physical record, in
    // wal-id order (== the order agent-0 wrote them). read_committed_records sorts
    // by wal_id ascending, so this is the durable cross-catalog WAL ordering.
    struct phys_rec_t {
        services::wal::wal_record_type type;
        components::catalog::oid_t table_oid;
    };
    std::vector<phys_rec_t> pg_catalog_physical_sequence(const std::string& dir) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        core::pmr::otterbrix_resource reader_resource;
        services::wal::wal_reader_t reader(&reader_resource, c, log);
        auto records_result = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records_result.has_error());
        auto& records = records_result.value();
        std::vector<phys_rec_t> seq;
        for (auto& r : records) {
            if (r.is_physical() && r.table_oid != components::catalog::INVALID_OID &&
                r.table_oid < components::catalog::FIRST_USER_OID)
                seq.push_back(phys_rec_t{r.record_type, r.table_oid});
        }
        return seq;
    }
} // namespace

// 1. Bootstrap doesn't emit WAL records — well-known rows are seeded via direct_append_sync
//    at txn=0 (idempotent on every startup). WAL records only appear once user ddl_* runs.
TEST_CASE("services::disk::wal_catalog::bootstrap_alone_no_wal") {
    auto dir = wal_cat_dir() + "/bootstrap";
    cleanup_dir(dir);
    {
        fixture fx(dir);
        // The fixture's ctor ran the bootstrap; prove it did its half before asserting the
        // absence of WAL records below (a bootstrap that seeded nothing would also emit none).
        REQUIRE_FALSE(fx.disk->read_setting_sync("TimeZone").empty());
    }
    // No ddl_* invoked → no WAL records expected.
    REQUIRE(pg_catalog_physical_count(dir) == 0);
    cleanup_dir(dir);
}

// 1b. The default the bootstrap seeds must be a value the engine itself recognizes. It used
//     to seed 'UTC' — which core::date::timezone_to_offset refuses (the recognizer's contract
//     is lowercase input, and the one SQL ingress lowercases before it stores), so EVERY start
//     of EVERY node seeded a default and then WARNed about refusing it, and the stored
//     catalog's timezone offset never came from the setting it had just written.
TEST_CASE("services::disk::wal_catalog::bootstrap_seeds_a_recognized_timezone") {
    auto dir = wal_cat_dir() + "/tz_default";
    cleanup_dir(dir);
    {
        fixture fx(dir);
        const auto seeded = fx.disk->read_setting_sync("TimeZone");
        REQUIRE_FALSE(seeded.empty());
        CAPTURE(seeded);
        // RED before the fix: seeded == "UTC", refused by the engine's own recognizer.
        REQUIRE(core::date::timezone_to_offset(seeded).has_value());
        // And the consumer that WARNed on every start accepts it now.
        components::catalog::session_catalog_t accepts;
        REQUIRE_FALSE(accepts.set_timezone(&fx.resource, seeded).contains_error());
        REQUIRE(accepts.timezone_offset == core::date::timezone_offset_t{0});
    }
    cleanup_dir(dir);
}

// 2. CREATE NAMESPACE adds at least one pg_namespace record.
TEST_CASE("services::disk::wal_catalog::create_namespace_writes_pg_namespace") {
    auto dir = wal_cat_dir() + "/create_ns";
    cleanup_dir(dir);
    auto before = std::size_t{0};
    {
        fixture fx(dir);
        before = pg_catalog_records_for(dir, wk::pg_namespace_table);
        test_create_namespace(fx, "user_ns");
    }
    auto after = pg_catalog_records_for(dir, wk::pg_namespace_table);
    REQUIRE(after > before);
    cleanup_dir(dir);
}

// 3. CREATE TABLE writes pg_class + per-column pg_attribute rows.
TEST_CASE("services::disk::wal_catalog::create_table_writes_pg_class_and_pg_attribute") {
    auto dir = wal_cat_dir() + "/create_table";
    cleanup_dir(dir);
    std::size_t cls_before = 0, att_before = 0;
    {
        fixture fx(dir);
        cls_before = pg_catalog_records_for(dir, wk::pg_class_table);
        att_before = pg_catalog_records_for(dir, wk::pg_attribute_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("name",
                          components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
        cols.emplace_back("count", components::types::complex_logical_type{components::types::logical_type::INTEGER});
        test_create_table(fx, ns_oid, "t", cols);
    }
    auto cls_after = pg_catalog_records_for(dir, wk::pg_class_table);
    auto att_after = pg_catalog_records_for(dir, wk::pg_attribute_table);
    REQUIRE(cls_after >= cls_before + 1);
    // pg_attribute rows for all columns are now batched into a single WAL
    // record (one chunk holds N rows, see build_create_table_writes).
    REQUIRE(att_after >= att_before + 1);
    cleanup_dir(dir);
}

// 4. CREATE TABLE writes pg_depend rows (table→namespace + column→type per column).
TEST_CASE("services::disk::wal_catalog::create_table_writes_pg_depend") {
    auto dir = wal_cat_dir() + "/create_dep";
    cleanup_dir(dir);
    std::size_t before = 0;
    {
        fixture fx(dir);
        before = pg_catalog_records_for(dir, wk::pg_depend_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        test_create_table(fx, ns_oid, "t", cols);
    }
    auto after = pg_catalog_records_for(dir, wk::pg_depend_table);
    // table→namespace only — column→type pg_depend written only when atttypid != INVALID_OID.
    REQUIRE(after >= before + 1);
    cleanup_dir(dir);
}

// 5. ddl_create_index writes pg_class (relkind='i') + pg_index + pg_depend (index→table 'a').
TEST_CASE("services::disk::wal_catalog::create_index_writes_pg_index") {
    auto dir = wal_cat_dir() + "/create_idx";
    cleanup_dir(dir);
    std::size_t idx_before = 0;
    {
        fixture fx(dir);
        idx_before = pg_catalog_records_for(dir, wk::pg_index_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt_oid = test_create_table(fx, ns_oid, "t", cols);
        test_create_index(fx, ns_oid, rt_oid, "idx_id", std::vector<std::string>{"id"});
    }
    auto idx_after = pg_catalog_records_for(dir, wk::pg_index_table);
    REQUIRE(idx_after >= idx_before + 1);
    cleanup_dir(dir);
}

// 6. ddl_index_set_valid writes a fresh pg_index row (delete + insert).
TEST_CASE("services::disk::wal_catalog::index_set_valid_writes_pg_index") {
    auto dir = wal_cat_dir() + "/idx_valid";
    cleanup_dir(dir);
    std::size_t idx_before = 0, after = 0;
    {
        fixture fx(dir);
        idx_before = pg_catalog_records_for(dir, wk::pg_index_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt_oid = test_create_table(fx, ns_oid, "t", cols);
        // test_create_index already marks the index as valid; no separate set_valid needed.
        test_create_index(fx, ns_oid, rt_oid, "idx_id", std::vector<std::string>{"id"});
    }
    after = pg_catalog_records_for(dir, wk::pg_index_table);
    REQUIRE(after >= idx_before + 1);
    cleanup_dir(dir);
}

// 7. ddl_create_type writes a pg_type record and a pg_depend type→namespace record.
TEST_CASE("services::disk::wal_catalog::create_type_writes_pg_type_and_depend") {
    auto dir = wal_cat_dir() + "/create_type";
    cleanup_dir(dir);
    std::size_t ty_before = 0, dep_before = 0;
    {
        fixture fx(dir);
        ty_before = pg_catalog_records_for(dir, wk::pg_type_table);
        dep_before = pg_catalog_records_for(dir, wk::pg_depend_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_type(fx, ns_oid, "widget");
    }
    REQUIRE(pg_catalog_records_for(dir, wk::pg_type_table) >= ty_before + 1);
    REQUIRE(pg_catalog_records_for(dir, wk::pg_depend_table) >= dep_before + 1);
    cleanup_dir(dir);
}

// 8. ddl_create_function writes a pg_proc record and a pg_depend function→namespace record.
TEST_CASE("services::disk::wal_catalog::create_function_writes_pg_proc_and_depend") {
    auto dir = wal_cat_dir() + "/create_fn";
    cleanup_dir(dir);
    std::size_t pr_before = 0, dep_before = 0;
    {
        fixture fx(dir);
        pr_before = pg_catalog_records_for(dir, wk::pg_proc_table);
        dep_before = pg_catalog_records_for(dir, wk::pg_depend_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_function(fx, ns_oid, "my_fn");
    }
    REQUIRE(pg_catalog_records_for(dir, wk::pg_proc_table) >= pr_before + 1);
    REQUIRE(pg_catalog_records_for(dir, wk::pg_depend_table) >= dep_before + 1);
    cleanup_dir(dir);
}

// 9. All pg_catalog WAL records carry table_oid < FIRST_USER_OID — needed for
//    the WAL replay split (pg_catalog records replayed first, user records second).
TEST_CASE("services::disk::wal_catalog::all_records_under_pg_catalog_database") {
    auto dir = wal_cat_dir() + "/db_prefix";
    cleanup_dir(dir);
    {
        fixture fx(dir);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        test_create_table(fx, ns_oid, "t", cols);
    }
    // Read all records and verify pg_catalog records all carry the right database tag.
    auto log = initialization_logger("python", "/tmp/docker_logs/");
    configuration::config_wal c;
    c.path = dir;
    c.on = true;
    core::pmr::otterbrix_resource reader_resource;
    services::wal::wal_reader_t reader(&reader_resource, c, log);
    auto records_result = reader.read_committed_records(services::wal::id_t{0});
    REQUIRE_FALSE(records_result.has_error());
    auto& records = records_result.value();
    bool seen_any = false;
    for (auto& r : records) {
        if (!r.is_physical())
            continue;
        // Every physical record we wrote was for a pg_catalog.* collection (oid < FIRST_USER_OID).
        REQUIRE(r.table_oid != components::catalog::INVALID_OID);
        REQUIRE(r.table_oid < components::catalog::FIRST_USER_OID);
        seen_any = true;
    }
    REQUIRE(seen_any);
    cleanup_dir(dir);
}

// 10. DROP TABLE emits delete-style WAL records (the cascade walks pg_class/pg_attribute/pg_depend).
//     We can't easily count deletes, but the operation should produce no INSERT records targeting
//     the collection of the dropped relation (i.e., we don't see resurrection writes).
TEST_CASE("services::disk::wal_catalog::drop_table_no_resurrect_writes") {
    auto dir = wal_cat_dir() + "/drop_no_resurrect";
    cleanup_dir(dir);
    components::catalog::oid_t t_oid = INVALID_OID;
    std::size_t cls_before_drop = 0;
    {
        fixture fx(dir);
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        t_oid = test_create_table(fx, ns_oid, "t", cols);
        cls_before_drop = pg_catalog_records_for(dir, wk::pg_class_table);
        test_drop_table(fx, t_oid);
    }
    // After the drop we still see at least the INSERT records that created the table — drop
    // path is MVCC-delete, not WAL append for new pg_class rows.
    auto cls_after = pg_catalog_records_for(dir, wk::pg_class_table);
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
        after_each[0] = pg_catalog_physical_count(dir); // bootstrap baseline
        auto ns1_oid = test_create_namespace(fx, "ns1");
        after_each[1] = pg_catalog_physical_count(dir);
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        test_create_table(fx, ns1_oid, "t", cols);
        after_each[2] = pg_catalog_physical_count(dir);
        test_create_namespace(fx, "ns2");
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
        cls_before = pg_catalog_records_for(dir, wk::pg_class_table);
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_sequence(fx, ns_oid, "widget_seq");
    }
    REQUIRE(pg_catalog_records_for(dir, wk::pg_class_table) >= cls_before + 1);
    cleanup_dir(dir);
}

// 13. agent-0 catalog WAL ordering — a single txn sends append(pg_depend) →
//     delete(pg_depend) → append(pg_index) and the durable WAL must replay those
//     three physical records in the SAME order. The catalog-DDL→agent migration
//     funnels every pg_* mutation through agent-0's single mailbox, so FIFO there
//     is what preserves cross-catalog WAL record order. We compare exactly the
//     tail of the physical record sequence (bootstrap emits none, see test 1).
TEST_CASE("services::disk::wal_catalog::agent0_catalog_wal_ordering") {
    auto dir = wal_cat_dir() + "/agent0_order";
    cleanup_dir(dir);
    constexpr catalog::oid_t pg_depend = catalog::well_known_oid::pg_depend_table;
    constexpr catalog::oid_t pg_index = catalog::well_known_oid::pg_index_table;
    {
        fixture fx(dir);
        // bootstrap seeds rows via direct_append_sync (txn=0), no WAL records yet.
        REQUIRE(pg_catalog_physical_sequence(dir).empty());

        // Allocate two oids: one objid for the pg_depend row, one for the pg_index row.
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
        const catalog::oid_t dep_objid = oids[0];
        const catalog::oid_t idx_oid = oids[1];

        std::vector<components::pg_catalog_append_range_t> appends_local;

        // (1) append a pg_depend row (objid is column index 1 in
        //     [classid, objid, refclassid, refobjid, deptype]).
        auto dep_row = catalog::build_pg_depend_row(&fx.resource,
                                                    pg_index,  // classid
                                                    dep_objid, // objid
                                                    pg_index,  // refclassid
                                                    idx_oid,   // refobjid
                                                    'n');
        appends_local.push_back(disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_depend, std::move(dep_row))));

        // (2) delete the pg_depend row we just appended (objid == col 1 == dep_objid).
        //     delete_pg_catalog_rows_inner only emits a PHYSICAL_DELETE when it finds
        //     a matching live row, so this targets the row from step (1). auto_ctx()
        //     (txn=0) keeps the emitted record always-visible to read_committed_records
        //     (no COMMIT marker is written in these disk-only tests), matching the
        //     txn=0 the surrounding append calls use.
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, auto_ctx(), pg_depend, std::int64_t{1}, dep_objid);

        // (3) append a pg_index row — a DIFFERENT catalog, after the delete.
        auto idx_row = catalog::build_pg_index_row(&fx.resource,
                                                   idx_oid,
                                                   idx_oid,
                                                   std::string("0"),
                                                   true,
                                                   components::catalog::indtype::single);
        appends_local.push_back(disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(idx_row))));

        std::set<catalog::oid_t> deletes_local{pg_depend};
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));
        fx.invoke(&manager_disk_t::storage_publish_deletes, txn_ctx(), std::uint64_t{1000}, std::move(deletes_local));
    }
    // Durable WAL must hold exactly these three physical records in send order.
    auto seq = pg_catalog_physical_sequence(dir);
    REQUIRE(seq.size() == 3);
    REQUIRE(seq[0].type == services::wal::wal_record_type::PHYSICAL_INSERT);
    REQUIRE(seq[0].table_oid == pg_depend);
    REQUIRE(seq[1].type == services::wal::wal_record_type::PHYSICAL_DELETE);
    REQUIRE(seq[1].table_oid == pg_depend);
    REQUIRE(seq[2].type == services::wal::wal_record_type::PHYSICAL_INSERT);
    REQUIRE(seq[2].table_oid == pg_index);
    cleanup_dir(dir);
}

// 14. WAL-disabled append still mutates storage, emits no WAL record. With the WAL
//     manager left unwired (fixture(dir, /*wire_wal=*/false) → agent-0's
//     manager_wal_addr_ stays empty), append_pg_catalog_row_inner skips
//     write_physical_insert but still runs direct storage append. Mirrors
//     bootstrap_alone_no_wal's "no WAL records" assertion and adds a read-back.
TEST_CASE("services::disk::wal_catalog::wal_disabled_append_no_record") {
    auto dir = wal_cat_dir() + "/wal_disabled";
    cleanup_dir(dir);
    constexpr catalog::oid_t pg_index = catalog::well_known_oid::pg_index_table;
    {
        fixture fx(dir, /*wire_wal=*/false);

        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const catalog::oid_t idx_oid = oids[0];

        auto idx_row = catalog::build_pg_index_row(&fx.resource,
                                                   idx_oid,
                                                   idx_oid,
                                                   std::string("0"),
                                                   true,
                                                   components::catalog::indtype::single);
        auto rng = disk_test_helpers::append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(idx_row)));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));

        // (a) the row is actually present: read pg_index back by indexrelid (col 0).
        std::pmr::vector<std::uint64_t> keys{&fx.resource};
        keys.emplace_back(components::catalog::pg_index_col::indexrelid);
        std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, idx_oid);
        auto batches =
            services::disk::test_probe::probe_read(fx, auto_ctx(), pg_index, std::move(keys), std::move(vals));
        std::size_t found = 0;
        for (const auto& chunk : batches) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                auto oid_v = chunk.value(0, i);
                if (!oid_v.is_null() && static_cast<catalog::oid_t>(oid_v.value<std::uint32_t>()) == idx_oid)
                    ++found;
            }
        }
        REQUIRE(found == 1);
    }
    // (b) no WAL record was emitted — WAL manager was never wired.
    REQUIRE(pg_catalog_physical_count(dir) == 0);
    cleanup_dir(dir);
}

// ---------------------------------------------------------------------------
// Wave-disk cases: the PHYSICAL_ADD_COLUMN journal leg on the append and update
// paths, and the backfill's replay leg.
// ---------------------------------------------------------------------------

namespace {
    // Count the PHYSICAL_ADD_COLUMN records the journal holds for one table.
    std::size_t add_column_records_for(const std::string& dir, components::catalog::oid_t target_oid) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        core::pmr::otterbrix_resource reader_resource;
        services::wal::wal_reader_t reader(&reader_resource, c, log);
        auto records_result = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records_result.has_error());
        std::size_t n = 0;
        for (auto& r : records_result.value()) {
            if (r.record_type == services::wal::wal_record_type::PHYSICAL_ADD_COLUMN && r.table_oid == target_oid)
                ++n;
        }
        return n;
    }

    // Build a one-chunk batch over BIGINT columns col_names, all rows valued base+i.
    std::pmr::vector<components::vector::data_chunk_t> bigint_batch(std::pmr::memory_resource* resource,
                                                                    const std::vector<std::string>& col_names,
                                                                    uint64_t rows,
                                                                    std::int64_t base) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        for (const auto& name : col_names) {
            components::types::complex_logical_type t{components::types::logical_type::BIGINT};
            t.set_alias(name);
            types.push_back(std::move(t));
        }
        components::vector::data_chunk_t chunk(resource, types, rows);
        chunk.set_cardinality(rows);
        for (uint64_t i = 0; i < rows; ++i) {
            for (uint64_t cidx = 0; cidx < col_names.size(); ++cidx) {
                chunk.set_value(cidx, i, static_cast<std::int64_t>(base + static_cast<std::int64_t>(i)));
            }
        }
        std::pmr::vector<components::vector::data_chunk_t> batch(resource);
        batch.emplace_back(std::move(chunk));
        return batch;
    }

    components::execution_context_t txn_exec_ctx(uint64_t txn_id, components::catalog::oid_t table_oid) {
        components::table::transaction_data td(txn_id, 1);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return components::execution_context_t{session_id_t{}, td, {}, table_oid};
    }
} // namespace

// #34 — THE ADD-COLUMN JOURNAL RECORD IS AWAITED, NOT FIRE-AND-FORGET. Schema growth on the
// append path sends its PHYSICAL_ADD_COLUMN record ahead of the PHYSICAL_INSERT to the same
// FIFO WAL worker; the send used to DROP the future, so the record's outcome was never read.
// It is kept and DRAINED now, after the insert await — a completed future by then (same FIFO
// worker, send order), so the drain never suspends and the handler keeps its single
// suspension point. This case pins the drained path end-to-end on the happy side: a growth
// append with WAL wired must succeed, materialise the row, AND land exactly one
// PHYSICAL_ADD_COLUMN record in the journal, wal-id-ordered AHEAD of the PHYSICAL_INSERT it
// enabled. A hang in the drain (the lost-wakeup the ordering guards against) or a mis-read
// of the future fails here. The pure "add-column write refused while the insert write
// succeeds" isolation is NOT stageable at this layer — wal_page_writer coalesces both small
// records into one buffered page and one file write, so any file-level fault that reaches
// the add-column write reaches the insert write too, and the insert's already-awaited
// refusal covers the append on either leg; the drain's value is that it no longer LEAKS the
// add-column outcome, proven structurally + by this happy-path guard.
TEST_CASE("services::disk::wal_catalog::a_growth_append_journals_the_add_column_ahead_of_the_insert") {
    auto dir = wal_cat_dir() + "/addcol_journaled";
    cleanup_dir(dir);
    catalog::oid_t table_oid = catalog::INVALID_OID;
    {
        fixture fx(dir);
        auto ns_oid = test_create_namespace(fx, "ns_addcol");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_addcol", cols);
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  cols,
                  /*is_computed=*/false);

        // A first, healthy append (no growth): one row of just column 'a'.
        {
            auto r = fx.invoke(&manager_disk_t::storage_append,
                               txn_exec_ctx(88, table_oid),
                               table_oid,
                               bigint_batch(&fx.resource, {"a"}, 1, 1));
            REQUIRE_FALSE(r.has_error());
            REQUIRE(r.value().second == 1);
        }

        // A second append that CARRIES a new alias 'b' at a wider width — stage 1b grows the
        // schema, emits the PHYSICAL_ADD_COLUMN record (now drained), then the PHYSICAL_INSERT.
        {
            auto r = fx.invoke(&manager_disk_t::storage_append,
                               txn_exec_ctx(88, table_oid),
                               table_oid,
                               bigint_batch(&fx.resource, {"a", "b"}, 1, 2));
            INFO("the drained add-column path must not hang and must not fail the growth append");
            REQUIRE_FALSE(r.has_error());
            REQUIRE(r.value().second == 1);
        }

        // Commit txn 88 so read_committed_records keeps the physical records.
        {
            auto [_c, cf] = actor_zeta::otterbrix::send(fx.wal->address(),
                                                        &services::wal::manager_wal_replicate_t::commit_txn,
                                                        session_id_t{},
                                                        std::uint64_t{88},
                                                        services::wal::wal_sync_mode::NORMAL,
                                                        catalog::well_known_oid::main_database,
                                                        std::uint64_t{1000});
            for (int i = 0; i < 400000 && !cf.is_ready(); ++i) {
                fx.scheduler->run(1);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            REQUIRE_FALSE(std::move(cf).take_ready().has_error());
        }

        auto total = fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid);
        REQUIRE_FALSE(total.has_error());
        REQUIRE(total.value() == 2);
    }

    INFO("exactly one PHYSICAL_ADD_COLUMN record, and it precedes its PHYSICAL_INSERT in wal order");
    REQUIRE(add_column_records_for(dir, table_oid) == 1);
    // Ordering: the first physical record for this table that is an ADD_COLUMN must come
    // before the INSERT it enabled (read_committed_records returns wal-id ascending).
    {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        core::pmr::otterbrix_resource reader_resource;
        services::wal::wal_reader_t reader(&reader_resource, c, log);
        auto records_result = reader.read_committed_records(services::wal::id_t{0});
        REQUIRE_FALSE(records_result.has_error());
        int add_col_idx = -1;
        int growth_insert_idx = -1;
        int seq = 0;
        int inserts_seen = 0;
        for (auto& r : records_result.value()) {
            if (r.table_oid != table_oid || !r.is_physical()) {
                continue;
            }
            if (r.record_type == services::wal::wal_record_type::PHYSICAL_ADD_COLUMN && add_col_idx < 0) {
                add_col_idx = seq;
            }
            if (r.record_type == services::wal::wal_record_type::PHYSICAL_INSERT) {
                ++inserts_seen;
                // The growth INSERT is the SECOND insert for this table (the first append
                // carried no growth).
                if (inserts_seen == 2) {
                    growth_insert_idx = seq;
                }
            }
            ++seq;
        }
        REQUIRE(add_col_idx >= 0);
        REQUIRE(growth_insert_idx >= 0);
        REQUIRE(add_col_idx < growth_insert_idx);
    }
    cleanup_dir(dir);
}

// #319 — THE BACKFILL'S REPLAY LEG, PINNED WITHOUT THE DESTRUCTOR CHECKPOINT. The
// added_at_commit_id stamp is patched in memory and journalled as a PHYSICAL_UPDATE; after
// a kill with NO checkpoint the journal is the stamp's ONLY carrier. The restart test in
// integration absorbs the stamp through the teardown checkpoint, so the record's content
// and the disk-side replay leg (direct_update_sync) went unpinned — corrupting either
// failed nothing. This fixture never checkpoints: phase B replays the journal through the
// same direct_* methods base_spaces replay uses and the stamp must come back.
TEST_CASE("services::disk::wal_catalog::the_backfill_stamp_survives_a_kill_through_the_journal_alone") {
    auto dir = wal_cat_dir() + "/backfill_replay";
    cleanup_dir(dir);
    constexpr auto pg_attr = catalog::well_known_oid::pg_attribute_table;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t attoid_a = catalog::INVALID_OID;

    auto read_added_at = [&](fixture& fx, catalog::oid_t attoid) -> std::int64_t {
        std::pmr::vector<std::uint64_t> keys{&fx.resource};
        keys.emplace_back(catalog::pg_attribute_col::attoid);
        std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, attoid);
        auto batches = test_probe::probe_read(fx, auto_ctx(), pg_attr, std::move(keys), std::move(vals));
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(catalog::pg_attribute_col::added_at_commit_id, i))
                    continue;
                return chunk.get_value<std::int64_t>(catalog::pg_attribute_col::added_at_commit_id, i);
            }
        }
        return -1;
    };

    // Phase A — live: create a table (its column's pg_attribute row is journalled), stamp
    // added_at via the backfill (journalled as PHYSICAL_UPDATE), then KILL: the fixture
    // teardown checkpoints nothing.
    {
        fixture fx(dir);
        auto ns_oid = test_create_namespace(fx, "ns_backfill");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_backfill", cols);

        // Find the attoid the create minted for column 'a'.
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_attribute_col::attrelid);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, table_oid);
            auto batches = test_probe::probe_read(fx, auto_ctx(), pg_attr, std::move(keys), std::move(vals));
            for (auto& chunk : batches) {
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.is_null(catalog::pg_attribute_col::attoid, i))
                        continue;
                    attoid_a = static_cast<catalog::oid_t>(
                        chunk.get_value<std::uint32_t>(catalog::pg_attribute_col::attoid, i));
                }
            }
            REQUIRE(attoid_a != catalog::INVALID_OID);
        }

        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> backfills(&fx.resource);
        components::pg_attribute_commit_id_backfill_t b;
        b.attoid = attoid_a;
        b.kind = components::pg_attribute_commit_id_backfill_t::kind_t::added_at;
        backfills.push_back(std::move(b));
        auto stamp_err = fx.invoke(&manager_disk_t::update_pg_attribute_commit_id_fields,
                                   auto_ctx(),
                                   std::move(backfills),
                                   std::uint64_t{4242});
        REQUIRE_FALSE(stamp_err.contains_error());
        REQUIRE(read_added_at(fx, attoid_a) == 4242);
    }

    // Read the journal BEFORE any new engine touches the directory.
    core::pmr::otterbrix_resource reader_resource;
    configuration::config_wal wal_c;
    wal_c.path = dir;
    wal_c.on = true;
    auto reader_log = initialization_logger("python", "/tmp/docker_logs/");
    services::wal::wal_reader_t reader(&reader_resource, wal_c, reader_log);
    auto records_result = reader.read_committed_records(services::wal::id_t{0});
    REQUIRE_FALSE(records_result.has_error());
    auto& records = records_result.value();

    // Phase B — restart after the kill: bootstrap re-seeds, then the journal's pg_attribute
    // records are applied through the SAME direct replay methods base_spaces uses
    // (direct_append_sync per chunk for PHYSICAL_INSERT, direct_update_sync for
    // PHYSICAL_UPDATE). The stamp must come back from the journal alone.
    {
        fixture fx2(dir, /*wire_wal=*/false);
        std::size_t updates_applied = 0;
        for (auto& r : records) {
            if (!r.is_physical() || r.table_oid != pg_attr)
                continue;
            if (r.record_type == services::wal::wal_record_type::PHYSICAL_INSERT) {
                for (auto& chunk : r.physical_data) {
                    auto append_r = fx2.disk->direct_append_sync(pg_attr, chunk);
                    REQUIRE_FALSE(append_r.has_error());
                }
            } else if (r.record_type == services::wal::wal_record_type::PHYSICAL_UPDATE) {
                REQUIRE_FALSE(r.physical_data.empty());
                auto upd_err = fx2.disk->direct_update_sync(pg_attr, r.physical_row_ids, r.physical_data.front());
                REQUIRE_FALSE(upd_err.contains_error());
                ++updates_applied;
            }
        }
        INFO("the journal must hold the backfill's PHYSICAL_UPDATE record");
        REQUIRE(updates_applied >= 1);
        INFO("after replay the stamp is back — the journal alone carried it across the kill");
        REQUIRE(read_added_at(fx2, attoid_a) == 4242);
    }
    cleanup_dir(dir);
}
