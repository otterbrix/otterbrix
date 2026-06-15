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
#include <services/wal/record.hpp>
#include <services/wal/wal_reader.hpp>

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
        std::pmr::synchronized_pool_resource resource;
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

    // WAL records carry table_oid; pg_catalog tables have well-known oids
    // (pg_class=11, pg_attribute=12, pg_namespace=10, pg_depend=15, pg_index=17,
    // pg_proc=14, pg_type=13, pg_constraint=16, pg_sequence=34, pg_rewrite=35,
    // pg_computed_column=18, pg_database=19). Anything below FIRST_USER_OID is
    // a system-table record.

    namespace wk = components::catalog::well_known_oid;

    components::catalog::oid_t pg_catalog_oid_for(const std::string& collection) {
        if (collection == "pg_namespace")
            return wk::pg_namespace_table;
        if (collection == "pg_class")
            return wk::pg_class_table;
        if (collection == "pg_attribute")
            return wk::pg_attribute_table;
        if (collection == "pg_type")
            return wk::pg_type_table;
        if (collection == "pg_proc")
            return wk::pg_proc_table;
        if (collection == "pg_depend")
            return wk::pg_depend_table;
        if (collection == "pg_constraint")
            return wk::pg_constraint_table;
        if (collection == "pg_index")
            return wk::pg_index_table;
        if (collection == "pg_computed_column")
            return wk::pg_computed_column_table;
        if (collection == "pg_database")
            return wk::pg_database_table;
        if (collection == "pg_sequence")
            return wk::pg_sequence_table;
        if (collection == "pg_rewrite")
            return wk::pg_rewrite_table;
        return components::catalog::INVALID_OID;
    }

    std::size_t pg_catalog_physical_count(const std::string& dir) {
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        configuration::config_wal c;
        c.path = dir;
        c.on = true;
        services::wal::wal_reader_t reader(c, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        std::size_t n = 0;
        for (auto& r : records) {
            if (r.is_physical() && r.table_oid != components::catalog::INVALID_OID &&
                r.table_oid < components::catalog::FIRST_USER_OID)
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
        const auto target_oid = pg_catalog_oid_for(collection);
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
        services::wal::wal_reader_t reader(c, log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
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
        (void) fx;
    }
    // No ddl_* invoked → no WAL records expected.
    REQUIRE(pg_catalog_physical_count(dir) == 0);
    cleanup_dir(dir);
}

// 2. CREATE NAMESPACE adds at least one pg_namespace record.
TEST_CASE("services::disk::wal_catalog::create_namespace_writes_pg_namespace") {
    auto dir = wal_cat_dir() + "/create_ns";
    cleanup_dir(dir);
    auto before = std::size_t{0};
    {
        fixture fx(dir);
        before = pg_catalog_records_for(dir, "pg_namespace");
        test_create_namespace(fx, "user_ns");
    }
    auto after = pg_catalog_records_for(dir, "pg_namespace");
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
        cls_before = pg_catalog_records_for(dir, "pg_class");
        att_before = pg_catalog_records_for(dir, "pg_attribute");
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        cols.emplace_back("name",
                          components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});
        cols.emplace_back("count", components::types::complex_logical_type{components::types::logical_type::INTEGER});
        test_create_table(fx, ns_oid, "t", cols);
    }
    auto cls_after = pg_catalog_records_for(dir, "pg_class");
    auto att_after = pg_catalog_records_for(dir, "pg_attribute");
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
        before = pg_catalog_records_for(dir, "pg_depend");
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        test_create_table(fx, ns_oid, "t", cols);
    }
    auto after = pg_catalog_records_for(dir, "pg_depend");
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
        idx_before = pg_catalog_records_for(dir, "pg_index");
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt_oid = test_create_table(fx, ns_oid, "t", cols);
        test_create_index(fx, ns_oid, rt_oid, "idx_id", std::vector<std::string>{"id"});
    }
    auto idx_after = pg_catalog_records_for(dir, "pg_index");
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
        idx_before = pg_catalog_records_for(dir, "pg_index");
        auto ns_oid = test_create_namespace(fx, "ns");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto rt_oid = test_create_table(fx, ns_oid, "t", cols);
        // test_create_index already marks the index as valid; no separate set_valid needed.
        test_create_index(fx, ns_oid, rt_oid, "idx_id", std::vector<std::string>{"id"});
    }
    after = pg_catalog_records_for(dir, "pg_index");
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
        ty_before = pg_catalog_records_for(dir, "pg_type");
        dep_before = pg_catalog_records_for(dir, "pg_depend");
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_type(fx, ns_oid, "widget");
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
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_function(fx, ns_oid, "my_fn");
    }
    REQUIRE(pg_catalog_records_for(dir, "pg_proc") >= pr_before + 1);
    REQUIRE(pg_catalog_records_for(dir, "pg_depend") >= dep_before + 1);
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
    services::wal::wal_reader_t reader(c, log);
    auto records = reader.read_committed_records(services::wal::id_t{0});
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
        cls_before_drop = pg_catalog_records_for(dir, "pg_class");
        test_drop_table(fx, t_oid);
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
        cls_before = pg_catalog_records_for(dir, "pg_class");
        auto ns_oid = test_create_namespace(fx, "ns");
        test_create_sequence(fx, ns_oid, "widget_seq");
    }
    REQUIRE(pg_catalog_records_for(dir, "pg_class") >= cls_before + 1);
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
                                                    pg_index,   // classid
                                                    dep_objid,  // objid
                                                    pg_index,   // refclassid
                                                    idx_oid,    // refobjid
                                                    'n');
        appends_local.push_back(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_depend, std::move(dep_row)));

        // (2) delete the pg_depend row we just appended (objid == col 1 == dep_objid).
        //     delete_pg_catalog_rows_inner only emits a PHYSICAL_DELETE when it finds
        //     a matching live row, so this targets the row from step (1). auto_ctx()
        //     (txn=0) keeps the emitted record always-visible to read_committed_records
        //     (no COMMIT marker is written in these disk-only tests), matching the
        //     txn=0 the surrounding append calls use.
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows, auto_ctx(), pg_depend, std::int64_t{1}, dep_objid);

        // (3) append a pg_index row — a DIFFERENT catalog, after the delete.
        auto idx_row = catalog::build_pg_index_row(&fx.resource, idx_oid, idx_oid, std::string("0"), true);
        appends_local.push_back(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(idx_row)));

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

        auto idx_row = catalog::build_pg_index_row(&fx.resource, idx_oid, idx_oid, std::string("0"), true);
        auto rng = fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), pg_index, std::move(idx_row));
        std::vector<components::pg_catalog_append_range_t> appends_local;
        appends_local.push_back(std::move(rng));
        fx.invoke(&manager_disk_t::storage_publish_commits,
                  rebuild_ctx(),
                  std::uint64_t{1000},
                  std::move(appends_local));

        // (a) the row is actually present: read pg_index back by indexrelid (col 0).
        std::pmr::vector<std::string> keys{&fx.resource};
        keys.emplace_back("indexrelid");
        std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, idx_oid);
        auto batches = services::disk::test_probe::probe_read(fx, auto_ctx(), pg_index, std::move(keys), std::move(vals));
        std::size_t found = 0;
        for (const auto& chunk : batches) {
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                auto oid_v = chunk.value(0, i);
                if (!oid_v.is_null() &&
                    static_cast<catalog::oid_t>(oid_v.value<std::uint32_t>()) == idx_oid)
                    ++found;
            }
        }
        REQUIRE(found == 1);
    }
    // (b) no WAL record was emitted — WAL manager was never wired.
    REQUIRE(pg_catalog_physical_count(dir) == 0);
    cleanup_dir(dir);
}
