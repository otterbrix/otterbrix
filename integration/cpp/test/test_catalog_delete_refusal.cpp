#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/wal_page.hpp>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>

// A DDL STATEMENT MUST NOT REPORT SUCCESS OVER A CATALOG ROW IT DID NOT DELETE.
//
// manager_disk_t::delete_pg_catalog_rows_many was declared unique_future<void>, so all six
// operators that scrub catalog rows through it (DROP FUNCTION, DROP CAST, ALTER TABLE DROP
// COLUMN, the DROP cascade, VACUUM, DROP INDEX) ended their catalog work with a bare
// `co_await std::move(df);` — there was no answer to read. Underneath, the agent body had the
// mirror-image hole: when the journal REFUSED the PHYSICAL_DELETE record it logged the refusal
// at error level and then DELETED THE ROWS ANYWAY, leaving storage one state ahead of a journal
// that has no record of the delete to replay. Both halves show in one statement: DROP INDEX
// reported success while its scrub had no journal record behind it.
//
// THE INJECTION, and why it lands where it does. Inside a statement the catalog delete's only
// device write is the WAL PHYSICAL_DELETE record — the .otbx is not touched until a checkpoint —
// so this case uses the WAL's own DEV_MODE seam (services/wal/wal_page.hpp) rather than the
// .otbx one, exactly as integration/cpp/test/test_wal_write_refusal.cpp does. The plan is armed
// AFTER the setup DDL, so the only writes it can refuse belong to the DROP.
//
// AND WHY THE TABLE IS 700 COLUMNS WIDE. wal_page_writer_t::append only touches the device when
// a record fills a 4 KiB page: a short record is buffered and the refusal would not be seen
// until some later flush, in a different statement. A PHYSICAL_DELETE carries eight bytes per
// row id, so the pg_attribute scrub of a 700-column table is a ~5.6 KiB record — it has to spill
// a page mid-record, and that flush is the write this plan refuses. It is also the FIFTH of the
// eleven specs the DROP cascade issues, ahead of the pg_class one, which is what makes the
// content assertion below possible at all.
//
// AND WHY THE DROP RUNS INSIDE AN EXPLICIT TRANSACTION. In autocommit the same statement ends
// with a commit record of its own, and THAT write — refused by the same armed plan — fails the
// statement for a reason that has nothing to do with the scrub, then the abort puts the rows
// back. The defect would be invisible behind an accident. Inside BEGIN there is no commit record
// yet, so the refused PHYSICAL_DELETE is the only thing that can decide the statement's answer.
//
// WHAT IS ASSERTED IS CONTENT. The transaction is COMMITTED (with the fault cleared, so the
// commit itself is honest) and pg_class is then read back through the disk manager's own funnel:
// the table's row has to still be there. Committing rather than rolling back is deliberate — a
// ROLLBACK would put the rows back in BOTH worlds and the case would pass without the fix.

using namespace components;

namespace {

    const std::string kTableName = "wide_refusal_t";
    // Eight bytes of row id per column, against a 4064-byte WAL page: the pg_attribute scrub of
    // this table cannot fit in one page, which is what makes its refusal land inside the
    // statement instead of in some later flush.
    constexpr int kColumns = 700;

    // Process-wide seam, so it is scoped by this object and narrowed to WAL segment files by
    // path. Same shape as the scope in integration/cpp/test/test_wal_write_refusal.cpp.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string faulty_marker;
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            const auto name = path.string();
            if (inner != nullptr && !faulty_marker.empty() && name.find(faulty_marker) != std::string::npos) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

    // The engine plus the one thing test_spaces does not expose: the disk manager, so catalog
    // CONTENT can be read back directly rather than inferred from a status code.
    class delete_refusal_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit delete_refusal_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            components::compute::function_registry_t::reset_default();
        }

        services::disk::manager_disk_t* disk() noexcept { return manager_disk_.get(); }
    };

    // "the read refused" — distinct from every honest row count, including zero.
    constexpr std::size_t kReadRefused = static_cast<std::size_t>(-1);

    // Committed rows of `table_oid` whose column `key_col` equals `key`, read back through the
    // disk manager's own funnel. The manager pumps its own inbox on an internal loop thread, so
    // the send only has to be waited on. `snapshot_horizon = max` is "see every COMMITTED row":
    // a bare transaction_data{0, 0} carries horizon 0 and would hide every row whose insert id
    // is a commit id (i.e. everything written inside an explicit transaction).
    template<typename Key>
    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    catalog_chunks_with(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto* resource = space.disk()->resource();
        table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
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
    catalog_rows_with(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto batches = catalog_chunks_with(space, table_oid, key_col, key);
        if (batches.has_error()) {
            return kReadRefused;
        }
        std::size_t rows = 0;
        for (const auto& chunk : batches.value()) {
            rows += static_cast<std::size_t>(chunk.size());
        }
        return rows;
    }

    // The table's pg_class row, addressed by the name the statement names.
    std::size_t pg_class_rows_named(delete_refusal_spaces_t& space, const std::string& name) {
        return catalog_rows_with(space,
                                 catalog::well_known_oid::pg_class_table,
                                 catalog::pg_class_col::relname,
                                 std::string_view{name});
    }

    // The relation's own oid, from the pg_class row that names it. INVALID_OID when the name
    // has no row — asserted by the callers, which all seed the table first.
    catalog::oid_t table_oid_named(delete_refusal_spaces_t& space, const std::string& name) {
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

    // What pg_attribute says about ONE column of ONE relation. `live` counts the rows that
    // still describe an existing column (attisdropped false); `tombstones` the ones that say
    // the column was dropped; `added_at_commit_id` is column 10 of the LIVE row, 0 when there
    // is none.
    struct column_rows_t {
        std::size_t live = 0;
        std::size_t tombstones = 0;
        std::int64_t added_at_commit_id = 0;
    };

    column_rows_t
    pg_attribute_rows_for(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::string_view attname) {
        // Keyed on attrelid (column 1): every attribute row of the relation, then matched by
        // name here — the same (attrelid, attname) identity operator_alter_column_drop_t uses.
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_attribute_table,
                                           catalog::pg_attribute_col::attrelid,
                                           table_oid);
        REQUIRE_FALSE(batches.has_error());
        column_rows_t out{};
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(2, i)) {
                    continue;
                }
                // Bind the cell before comparing: get_value<string_view> points into the
                // chunk's own buffer, which outlives the comparison.
                const auto name_cell = chunk.get_value<std::string_view>(2, i);
                if (name_cell != attname) {
                    continue;
                }
                const bool dropped = !chunk.is_null(7, i) && chunk.get_value<bool>(7, i);
                if (dropped) {
                    ++out.tombstones;
                    continue;
                }
                ++out.live;
                if (chunk.column_count() > 10 && !chunk.is_null(10, i)) {
                    out.added_at_commit_id = chunk.get_value<std::int64_t>(10, i);
                }
            }
        }
        return out;
    }

    // A plain two-column table, seeded outside any explicit transaction so its own catalog
    // rows are direct writes and every case below starts from the same committed state.
    void seed_plain_table(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE del;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE del." + name + " (a bigint, b bigint);")->is_success());
    }

    void seed_wide_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE del;")->is_success());
        std::string sql = "CREATE TABLE del." + kTableName + " (";
        for (int i = 0; i < kColumns; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "c" + std::to_string(i) + " bigint";
        }
        sql += ");";
        REQUIRE(test_helpers::exec(dispatcher, sql)->is_success());
    }

} // namespace

// ===========================================================================
// A DROP WHOSE CATALOG SCRUB WAS REFUSED MUST FAIL, AND LEAVE THE ROWS.
//
// BEFORE: delete_pg_catalog_rows_many answered with nothing at all, so operator_dynamic_cascade_
// delete_t could not tell a completed scrub from a refused one. It walked on through the rest of
// the spec list, marked the storage and the index entry dropped, and reported success — and the
// commit published a catalog delete the journal never recorded.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::drop_table_fails_when_the_catalog_delete_is_refused") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/drop_table";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_"; // WAL segment files only; the .otbx files stay untouched

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    seed_wide_table(dispatcher);

    // The row this case is about is there before it starts.
    REQUIRE(pg_class_rows_named(space, kTableName) == 1);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());

    // Arm only now: the DDL above had to reach the journal, so the refusal below can only be
    // about the DROP's own catalog scrub.
    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    const auto writes_before = fault.plan.writes_seen;

    auto cur = dispatcher->execute_sql(txn, "DROP TABLE del." + kTableName + ";");

    INFO("a DROP whose catalog scrub the journal refused must FAIL, not report success");
    CHECK(cur->is_error());
    // The refusal really travelled through a refused write, i.e. the record did reach the device
    // inside this statement rather than sitting in the page buffer.
    CHECK(fault.plan.writes_seen > writes_before);

    // The commit itself must be honest, so the fault is gone before it runs. Its own answer is
    // not the subject here (a statement that refused may or may not leave the transaction
    // committable); the catalog CONTENT after it is.
    fault.plan.fail_writes_from = 0;
    auto committed = dispatcher->execute_sql(txn, "COMMIT;");
    INFO("COMMIT after the refused DROP TABLE: "
         << (committed->is_error() ? std::string(committed->get_error().what.c_str()) : std::string("success")));

    // THE POINT OF THE CASE, asserted on CONTENT. The scrub was refused, so the row it would
    // have removed on the way to succeeding is exactly where it was — and a COMMIT rather than a
    // ROLLBACK is what makes that a real assertion: a rollback would restore the row even in the
    // world where the delete silently went through.
    const auto rows = pg_class_rows_named(space, kTableName);
    INFO("pg_class rows named '" << kTableName << "' after the refused DROP TABLE + COMMIT: " << rows);
    CHECK(rows == 1);
}

// The collapse guard: with nothing injected the SAME statements must SUCCEED and the table's
// pg_class row must disappear exactly when the DROP does. Without it the case above could go
// green by collapse — any change that made every DROP TABLE fail would satisfy every assertion
// it makes.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_healthy_drop_table_scrubs_the_catalog") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/healthy";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    seed_wide_table(dispatcher);
    CHECK(pg_class_rows_named(space, kTableName) == 1);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "DROP TABLE del." + kTableName + ";")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    CHECK(pg_class_rows_named(space, kTableName) == 0);
}

// ===========================================================================
// A COLUMN ADDED AND DROPPED INSIDE ONE TRANSACTION MUST DROP.
//
// THE DEFECT, and it was introduced by the very wave the cases above belong to. Once
// delete_pg_catalog_rows_many could report a per-spec count, operator_alter_column_drop_t
// started reading a count of 0 on the live pg_attribute row as a refusal — "the column is
// still live in the catalog" — on the stated ground that the operator had just READ that row.
//
// THE GROUND WAS WRONG, because the read and the delete did not see the same catalog:
//   * the read goes through manager_disk_t::read_chunks_by_key with
//     execution_context_t{session, ctx->txn, {}}, and the whole route down to
//     agent_disk_t::read_chunks_by_key_inner carries that transaction_data;
//   * the delete's scan, in agent_disk_t::delete_pg_catalog_rows_inner, ran
//     detail::inline_scan with NO transaction at all, so collection_scan_state::txn stayed
//     {0, 0} — which row_version_manager_t reads as "insert id 0 only", i.e. rows written
//     outside any explicit transaction.
// A pg_attribute row appended INSIDE a transaction carries insert_id == transaction_id until
// the commit publishes it, so the read saw it and the delete could not. Zero deleted, and the
// statement refused a sequence that is entirely legal.
//
// WHAT IS ASSERTED IS BOTH HALVES: the DROP COLUMN must SUCCEED, and after the COMMIT
// pg_attribute must hold no LIVE row for the column — a tombstone, and nothing else. The
// second half is the one a "make every ALTER fail" change cannot satisfy.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_one_transaction_is_dropped") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/add_drop_in_txn";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);
    // The column is not there before the transaction opens — neither live nor as a tombstone.
    REQUIRE(pg_attribute_rows_for(space, table_oid, "c").live == 0);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());

    auto dropped = dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " DROP COLUMN c;");
    INFO("DROP COLUMN of a column ADDed in the same transaction: "
         << (dropped->is_error() ? std::string(dropped->get_error().what.c_str()) : std::string("success")));
    CHECK(dropped->is_success());

    auto committed = dispatcher->execute_sql(txn, "COMMIT;");
    INFO("COMMIT after the in-transaction ADD + DROP COLUMN: "
         << (committed->is_error() ? std::string(committed->get_error().what.c_str()) : std::string("success")));
    CHECK(committed->is_success());

    // CONTENT. The live row the ADD wrote has to be gone; the DROP's tombstone is what
    // legitimately stays behind (attnum is never reused, so the slot keeps describing itself).
    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    INFO("pg_attribute rows for column 'c' after COMMIT: live=" << rows.live << " tombstones=" << rows.tombstones);
    CHECK(rows.live == 0);
}

// The collapse guard for the case above: the SAME add-then-drop, split across two autocommit
// statements, must behave identically. Without it, a change that made DROP COLUMN delete
// nothing at all — or one that made ADD COLUMN write nothing — would satisfy every assertion
// the transactional case makes.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_autocommit_is_dropped") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/add_drop_autocommit";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_ac_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    // The ADD really wrote the row this case is about, so the DROP below has something to do.
    REQUIRE(pg_attribute_rows_for(space, table_oid, "c").live == 1);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " DROP COLUMN c;")->is_success());
    CHECK(pg_attribute_rows_for(space, table_oid, "c").live == 0);
}

// ===========================================================================
// THE SAME BLINDNESS, ONE METHOD OVER — DIAGNOSED, NOT FIXED, AND HERE IS WHY.
//
// STEP 4 of operator_commit_transaction_t asks the disk to backfill
// pg_attribute.added_at_commit_id on the rows an in-transaction ALTER ... ADD COLUMN wrote, and
// hands it the transaction's own transaction_data. agent_disk_t::
// update_pg_attribute_commit_id_field_inner then looks the row up with a detail::inline_scan
// that is given components::table::transaction_data{} — and, as that step's own comment states,
// "the rows still carry insert_id == transaction_id" at that moment. transaction_data{} is
// horizon 0 with no owning transaction, so it cannot see a single one of them: every backfill of
// an in-transaction ALTER logs "attoid not found (skipping)" and the column keeps its
// placeholder 0. That reads as "added before every snapshot" (the rule is
// added_at_commit_id <= snapshot horizon), i.e. the column shows up in snapshots older than the
// ALTER that created it.
//
// WHY THE ONE-LINE FIX IS NOT IN THIS TREE. Passing ctx.txn there — the same change made to
// delete_pg_catalog_rows_inner, in the same file — makes the scan find the row, and the
// direct_update_sync that follows then CRASHES:
//
//   EXC_BAD_ACCESS in components::table::update_segment_t::merge_update_loop_internal
//   <unsigned int, unsigned int>, components/table/update_segment.hpp:830, on a base_info
//   pointer that is not a valid address. Reproduced on the autocommit ALTER of the case above,
//   which is how a change confined to the transactional path was caught at all.
//
// So the backfill has never once run against a real row, and the update-merge path it would
// drive does not survive being handed one. That is a defect of its own, below this file, and it
// needs its own red test at the components/table floor before the scan above may be widened.
// This case pins what is verifiable today — the row is written and survives the commit — and
// carries the finding rather than a false green.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_add_column_row_survives_the_commit") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/added_at_backfill";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "added_at_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    CHECK(rows.live == 1);
    if (rows.added_at_commit_id == 0) {
        WARN("KNOWN, UNFIXED: pg_attribute.added_at_commit_id is still the placeholder 0 after an "
             "in-transaction ALTER ... ADD COLUMN. agent_disk_t::update_pg_attribute_commit_id_field_inner "
             "scans with transaction_data{} and cannot see the row it was asked to patch; passing ctx.txn "
             "there crashes in update_segment_t::merge_update_loop_internal "
             "(components/table/update_segment.hpp:830).");
    }
}

// ===========================================================================
// THE TWO CALLERS OF THE SINGULAR delete_pg_catalog_rows THAT DELETE-THEN-APPEND.
//
// Both of these read a row, delete it, and append a replacement carrying the same identity —
// operator_alter_column_rename_t (pg_attribute, new attname) and
// operator_create_index_backfill_t (pg_index, indisvalid=true). Their read carries the
// transaction and their delete, before this change, did not: inside a BEGIN the row they were
// replacing was invisible to the delete, so nothing was removed and the replacement was appended
// ON TOP. One identity, two live rows, and a success reported over it — the singular route is
// unique_future<void>, so neither operator could have been told otherwise.
//
// These two cases are the reason the visibility fix is the fix and not a workaround: the
// singular route still has no error channel (see the debt note in
// services/disk/manager_disk_ddl.cpp), but with the delete looking at the same catalog as the
// read, the delete no longer silently misses.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_rename_leaves_one_attribute_row") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/rename_in_txn";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "rename_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    auto renamed = dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " RENAME COLUMN c TO d;");
    INFO("RENAME of a column ADDed in the same transaction: "
         << (renamed->is_error() ? std::string(renamed->get_error().what.c_str()) : std::string("success")));
    CHECK(renamed->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    // The old name must be gone and the new one must be there ONCE. The old-name count is the
    // half that was broken: the delete missed and the append landed anyway.
    const auto old_rows = pg_attribute_rows_for(space, table_oid, "c");
    const auto new_rows = pg_attribute_rows_for(space, table_oid, "d");
    INFO("pg_attribute after the in-transaction RENAME: live 'c'=" << old_rows.live << " live 'd'=" << new_rows.live);
    CHECK(old_rows.live == 0);
    CHECK(new_rows.live == 1);
}

TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_create_index_leaves_one_pg_index_row") {
    const std::filesystem::path dir = "/tmp/otterbrix/integration/test_catalog_delete_refusal/create_index_in_txn";
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "indexed_t";
    seed_plain_table(dispatcher, table);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(
        dispatcher->execute_sql(txn, "CREATE INDEX one_row_idx ON del." + table + " (a);")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    // The index relation's own oid, i.e. the indexrelid every pg_index row for it carries.
    const auto index_oid = table_oid_named(space, "one_row_idx");
    REQUIRE(index_oid != catalog::INVALID_OID);

    // operator_create_index_backfill_t deletes the indisvalid=false row the metadata operator
    // wrote and appends the indisvalid=true one. Exactly ONE must survive.
    const auto rows =
        catalog_rows_with(space, catalog::well_known_oid::pg_index_table, catalog::pg_index_col::indexrelid, index_oid);
    INFO("pg_index rows for indexrelid " << static_cast<unsigned>(index_oid) << " after the in-transaction CREATE INDEX: "
                                         << rows);
    CHECK(rows == 1);
}
