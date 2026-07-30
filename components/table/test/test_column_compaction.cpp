// data_table_t::compact_dropped_columns — the storage half of ALTER TABLE ... DROP COLUMN.
//
// The catalog half runs at DDL time and the storage keeps the slot, because removing it
// rewrites every row group. Until the rewrite happens the relation is DISPLACED (it holds a
// storage slot the logical schema no longer names) and the layers above refuse column
// pruning, filter pushdown, aggregate pushdown and index probes on it, all four of which
// address a storage column by its logical ordinal. This is the rewrite, and these are the
// properties it has to have to be allowed to run at all.
//
// FOUR columns with the SECOND dropped, and every column holding a value range that names it
// (col0 = 1000+i, col1 = 2000+i, col2 = 3000+i, col3 = 4000+i). Two columns cannot tell a
// working compaction from one that shifted every value one slot left — "correct" and
// "swapped twice" are the same observation — and dropping the LAST column would not move
// anything at all. Middle-of-four is the shape where a shift is visible in the VALUE.

#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    // The four columns' catalog identities. Real attoids come from pg_attribute; all that
    // matters here is that they are distinct and non-zero (0 is INVALID_OID).
    constexpr uint32_t kAttoid0 = 101;
    constexpr uint32_t kAttoid1 = 102;
    constexpr uint32_t kAttoid2 = 103;
    constexpr uint32_t kAttoid3 = 104;

    constexpr uint64_t kRows = 64;

    struct test_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::in_memory_block_manager_t block_manager;

        test_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE) {}
    };

    // Four BIGINT columns, each carrying a distinct catalog identity. `identified` off
    // leaves every column identity-less, which is the pre-versioning-.otbx / WAL-synthesised
    // shape that must never be compacted.
    std::unique_ptr<data_table_t> make_table(test_env& env, bool identified = true) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("c0", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c1", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c2", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("c3", complex_logical_type(logical_type::BIGINT));
        if (identified) {
            columns[0].set_attoid(kAttoid0);
            columns[1].set_attoid(kAttoid1);
            columns[2].set_attoid(kAttoid2);
            columns[3].set_attoid(kAttoid3);
        }
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
    }

    void append_rows(data_table_t& table, test_env& env, uint64_t count, transaction_data txn) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            for (uint64_t c = 0; c < chunk.column_count(); c++) {
                chunk.data[c].set_value(i, static_cast<int64_t>((c + 1) * 1000 + i));
            }
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, txn);
    }

    // One full-width scan of everything the txn-less "see all committed" view returns.
    data_chunk_t scan_all(data_table_t& table, test_env& env) {
        std::vector<storage_index_t> column_ids;
        for (uint64_t i = 0; i < table.column_count(); i++) {
            column_ids.emplace_back(i);
        }
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        auto types = table.copy_types();
        data_chunk_t result(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table.scan(result, scan_state);
        return result;
    }

    // Two chunks under ONE append state — the multi-chunk idiom the table layer supports
    // (test_table.cpp appends the same way): lock + initialize once, append per chunk,
    // finalize once. Both chunks carry the same value pattern, so row j of the second
    // repeats row j of the first.
    void append_two_chunks(data_table_t& table, test_env& env, uint64_t per_chunk) {
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        for (uint64_t batch = 0; batch < 2; batch++) {
            auto types = table.copy_types();
            // A fresh chunk per batch: collection_t::append SLICES the chunk in place when
            // an append spills into a new row group.
            auto chunk = data_chunk_t(&env.resource, types, per_chunk);
            for (uint64_t i = 0; i < per_chunk; i++) {
                for (uint64_t c = 0; c < chunk.column_count(); c++) {
                    chunk.data[c].set_value(i, static_cast<int64_t>((c + 1) * 1000 + i));
                }
            }
            chunk.set_cardinality(per_chunk);
            REQUIRE_FALSE(table.append(chunk, state).has_error());
        }
        table.finalize_append(state, transaction_data{0, 0});
    }

    // Every committed row, delivered as the ≤DEFAULT_VECTOR_CAPACITY batches the scan
    // actually produces. `fn(absolute_row, batch, index_in_batch)`; returns the row count.
    template<typename Fn>
    uint64_t for_each_scanned_row(data_table_t& table, test_env& env, Fn&& fn) {
        std::vector<storage_index_t> column_ids;
        for (uint64_t i = 0; i < table.column_count(); i++) {
            column_ids.emplace_back(i);
        }
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        auto types = table.copy_types();
        data_chunk_t batch(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t total = 0;
        while (true) {
            batch.reset();
            table.scan(batch, scan_state);
            if (batch.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < batch.size(); i++) {
                fn(total + i, batch, i);
            }
            total += batch.size();
        }
        return total;
    }

    std::pmr::vector<uint32_t> dead_set(test_env& env, std::initializer_list<uint32_t> attoids) {
        std::pmr::vector<uint32_t> out(&env.resource);
        for (auto a : attoids) {
            out.push_back(a);
        }
        return out;
    }

} // namespace

TEST_CASE("components::table::column_compaction::drops_the_middle_column_without_moving_values") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    REQUIRE(table->column_count() == 4);

    const auto outcome = table->compact_dropped_columns(dead_set(env, {kAttoid1}), /*compact_watermark=*/0);
    REQUIRE(outcome.removed == 1);
    REQUIRE_FALSE(outcome.mvcc_refused);

    INFO("the column list narrowed, and the survivors kept their identities in order");
    REQUIRE(table->column_count() == 3);
    const auto& cols = table->columns();
    REQUIRE(cols[0].attoid() == kAttoid0);
    REQUIRE(cols[1].attoid() == kAttoid2);
    REQUIRE(cols[2].attoid() == kAttoid3);
    REQUIRE(cols[0].name() == "c0");
    REQUIRE(cols[1].name() == "c2");
    REQUIRE(cols[2].name() == "c3");

    INFO("the POSITIONAL handles were renumbered onto the new width");
    REQUIRE(cols[0].storage_oid() == 0);
    REQUIRE(cols[1].storage_oid() == 1);
    REQUIRE(cols[2].storage_oid() == 2);

    INFO("VALUES DID NOT MOVE: c2 answers with 3000+i and not with c1's 2000+i");
    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.column_count() == 3);
    REQUIRE(scanned.size() == kRows);
    for (uint64_t i = 0; i < kRows; i++) {
        REQUIRE(scanned.data[0].value(i).value<int64_t>() == static_cast<int64_t>(1000 + i));
        REQUIRE(scanned.data[1].value(i).value<int64_t>() == static_cast<int64_t>(3000 + i));
        REQUIRE(scanned.data[2].value(i).value<int64_t>() == static_cast<int64_t>(4000 + i));
    }
}

TEST_CASE("components::table::column_compaction::drops_several_columns_at_once") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    // Two holes, one of them interior and one of them at the end: the survivors are c0 and
    // c2, which are NOT adjacent in the original list.
    const auto outcome = table->compact_dropped_columns(dead_set(env, {kAttoid1, kAttoid3}), 0);
    REQUIRE(outcome.removed == 2);
    REQUIRE(table->column_count() == 2);
    REQUIRE(table->columns()[0].attoid() == kAttoid0);
    REQUIRE(table->columns()[1].attoid() == kAttoid2);

    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.column_count() == 2);
    REQUIRE(scanned.size() == kRows);
    for (uint64_t i = 0; i < kRows; i++) {
        REQUIRE(scanned.data[0].value(i).value<int64_t>() == static_cast<int64_t>(1000 + i));
        REQUIRE(scanned.data[1].value(i).value<int64_t>() == static_cast<int64_t>(3000 + i));
    }
}

TEST_CASE("components::table::column_compaction::takes_more_rows_than_one_chunk") {
    test_env env;
    auto table = make_table(env);
    // Two chunks totalling MORE than one vector, so the rebuild's scan loop runs several
    // times, spills into a second row group, and each batch has to land AFTER the previous
    // one instead of over it. Row j of the second chunk repeats row j of the first, which
    // is what makes a mis-ordered rebuild visible.
    constexpr uint64_t kBatch = 700;
    append_two_chunks(*table, env, kBatch);

    const auto outcome = table->compact_dropped_columns(dead_set(env, {kAttoid1}), 0);
    REQUIRE(outcome.removed == 1);
    REQUIRE(table->row_group()->total_rows() == 2 * kBatch);

    const auto seen = for_each_scanned_row(*table, env, [](uint64_t row, const data_chunk_t& batch, uint64_t i) {
        const auto j = row < kBatch ? row : row - kBatch;
        REQUIRE(batch.data[0].value(i).value<int64_t>() == static_cast<int64_t>(1000 + j));
        REQUIRE(batch.data[1].value(i).value<int64_t>() == static_cast<int64_t>(3000 + j));
        REQUIRE(batch.data[2].value(i).value<int64_t>() == static_cast<int64_t>(4000 + j));
    });
    REQUIRE(seen == 2 * kBatch);
}

TEST_CASE("components::table::column_compaction::keeps_a_column_that_has_no_identity") {
    test_env env;
    auto table = make_table(env, /*identified=*/false);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    // Every column's attoid is 0 (INVALID_OID). Nothing in a dead-attoid set can be ABOUT
    // such a column, and matching it by name instead is exactly what ALTER TABLE ... RENAME
    // COLUMN makes wrong — a rename never touches storage, so the storage still says the
    // old name. Refusing costs an optimisation; guessing costs a column.
    const auto outcome = table->compact_dropped_columns(dead_set(env, {kAttoid1, 0}), 0);
    REQUIRE(outcome.removed == 0);
    REQUIRE_FALSE(outcome.mvcc_refused);
    REQUIRE(table->column_count() == 4);
}

TEST_CASE("components::table::column_compaction::is_a_no_op_when_nothing_is_dead") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    REQUIRE(table->compact_dropped_columns(dead_set(env, {}), 0).removed == 0);
    // An attoid that belongs to no column of this relation (another table's) removes
    // nothing rather than removing something positionally.
    REQUIRE(table->compact_dropped_columns(dead_set(env, {9999}), 0).removed == 0);
    REQUIRE(table->column_count() == 4);
}

TEST_CASE("components::table::column_compaction::never_empties_the_relation") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    // A zero-column table is not a narrower table, it is a different object. DROP COLUMN
    // already refuses to remove a relation's last column, so this cannot arrive from SQL —
    // it stays a refusal rather than an assert so that it also cannot arrive from a caller.
    const auto outcome =
        table->compact_dropped_columns(dead_set(env, {kAttoid0, kAttoid1, kAttoid2, kAttoid3}), 0);
    REQUIRE(outcome.removed == 0);
    REQUIRE(table->column_count() == 4);
}

TEST_CASE("components::table::column_compaction::refused_while_a_version_is_above_the_watermark") {
    test_env env;
    auto table = make_table(env);

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    append_rows(*table, env, kRows, txn.data());

    // The append is stamped with a PENDING txn id, which is above any watermark. The
    // rebuild re-stamps every surviving row transaction_data{0,0} — it collapses the
    // version history — so running it now would publish an uncommitted write. The
    // compaction has to say no and leave the table exactly as it was.
    const auto refused = table->compact_dropped_columns(dead_set(env, {kAttoid1}), mgr.compact_watermark());
    REQUIRE(refused.removed == 0);
    REQUIRE(refused.mvcc_refused);
    REQUIRE(table->column_count() == 4);

    // Once the write is committed AND published, the same call goes through.
    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_append(commit_id, 0, kRows);

    const auto allowed = table->compact_dropped_columns(dead_set(env, {kAttoid1}), mgr.compact_watermark());
    REQUIRE(allowed.removed == 1);
    REQUIRE_FALSE(allowed.mvcc_refused);
    REQUIRE(table->column_count() == 3);

    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.size() == kRows);
    for (uint64_t i = 0; i < kRows; i++) {
        REQUIRE(scanned.data[1].value(i).value<int64_t>() == static_cast<int64_t>(3000 + i));
    }
}

TEST_CASE("components::table::column_compaction::the_table_still_takes_writes_afterwards") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    REQUIRE(table->compact_dropped_columns(dead_set(env, {kAttoid1}), 0).removed == 1);

    // An append after compaction must land in row groups the NEW column list describes.
    // A compaction that rebuilt a separate data_table_t sharing the old one's row groups
    // would fail here with a column-count mismatch inside row_group::append; narrowing in
    // place is what keeps this working.
    {
        auto types = table->copy_types();
        REQUIRE(types.size() == 3);
        auto chunk = data_chunk_t(&env.resource, types, 4);
        for (uint64_t i = 0; i < 4; i++) {
            chunk.data[0].set_value(i, static_cast<int64_t>(7000 + i));
            chunk.data[1].set_value(i, static_cast<int64_t>(8000 + i));
            chunk.data[2].set_value(i, static_cast<int64_t>(9000 + i));
        }
        chunk.set_cardinality(4);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    REQUIRE(table->row_group()->total_rows() == kRows + 4);
    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.column_count() == 3);
    REQUIRE(scanned.size() == kRows + 4);
    REQUIRE(scanned.data[0].value(kRows).value<int64_t>() == 7000);
    REQUIRE(scanned.data[1].value(kRows).value<int64_t>() == 8000);
    REQUIRE(scanned.data[2].value(kRows).value<int64_t>() == 9000);
}

TEST_CASE("components::table::column_compaction::a_second_pass_removes_nothing_more") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, kRows, transaction_data{0, 0});

    REQUIRE(table->compact_dropped_columns(dead_set(env, {kAttoid1}), 0).removed == 1);
    // VACUUM runs repeatedly against the same dead-attoid set (the pg_attribute tombstone
    // outlives the compaction — attnum is never reused, so the tombstone stays). The second
    // pass must find nothing and rewrite nothing.
    REQUIRE(table->compact_dropped_columns(dead_set(env, {kAttoid1}), 0).removed == 0);
    REQUIRE(table->column_count() == 3);

    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.size() == kRows);
    REQUIRE(scanned.data[1].value(0).value<int64_t>() == 3000);
}

// ---------------------------------------------------------------------------
// Not compaction, but the bug that stood between DROP COLUMN and it: the second
// IN-PLACE update of a column segment corrupted its own merge loop.
//
// data_table_t::update writes through update_segment_t, which keeps a per-vector chain
// of updated tuples. The FIRST update on a vector has nothing to merge with; the second
// merges its tuples into the existing chain, and the drain loop that copies the leftover
// new tuples incremented `count` — its own loop bound — instead of the merged-output
// position. The loop therefore never terminated and walked `aidx` off the end of the
// indexing vector and the id array.
//
// Reachable from SQL as two ALTER TABLE ... DROP COLUMN statements on one relation: each
// COMMIT patches one pg_attribute row in place (the dropped_at_commit_id backfill), and
// the second patch is the one with a chain to merge into. It is pinned HERE, at the layer
// that owns the loop, with two updates to DIFFERENT rows of one column — the shape that
// makes both the merge branch and the drain branch run.
// ---------------------------------------------------------------------------
TEST_CASE("components::table::column_compaction::two_in_place_updates_merge_without_running_off") {
    test_env env;
    auto table = make_table(env);
    append_rows(*table, env, 16, transaction_data{0, 0});

    // In-place update of ONE row, twice over, at two different row ids. Each call is the
    // "see all committed" write direct_update_sync performs: full-width chunk, one row.
    auto update_row = [&](int64_t row_id, int64_t base) {
        auto types = table->copy_types();
        data_chunk_t patch(&env.resource, types, 1);
        for (uint64_t c = 0; c < patch.column_count(); c++) {
            patch.data[c].set_value(0, static_cast<int64_t>(base + static_cast<int64_t>(c)));
        }
        patch.set_cardinality(1);

        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        data_chunk_t ids(&env.resource, id_type, 1);
        ids.data[0].set_value(0, row_id);
        ids.set_cardinality(1);

        auto state = table->initialize_update({});
        REQUIRE_FALSE(table->update(*state, ids.data[0], patch).has_error());
    };

    update_row(3, 30000);
    // Pre-fix: this second update never returns — the drain loop raises its own bound on
    // every iteration and reads past the end of the id array (EXC_BAD_ACCESS).
    update_row(7, 70000);

    auto scanned = scan_all(*table, env);
    REQUIRE(scanned.size() == 16);
    INFO("both updates are visible, each on its own row");
    REQUIRE(scanned.data[0].value(3).value<int64_t>() == 30000);
    REQUIRE(scanned.data[1].value(3).value<int64_t>() == 30001);
    REQUIRE(scanned.data[0].value(7).value<int64_t>() == 70000);
    REQUIRE(scanned.data[1].value(7).value<int64_t>() == 70001);
    INFO("and no row either update did not name moved");
    REQUIRE(scanned.data[0].value(0).value<int64_t>() == 1000);
    REQUIRE(scanned.data[1].value(0).value<int64_t>() == 2000);
    REQUIRE(scanned.data[0].value(15).value<int64_t>() == 1015);
    REQUIRE(scanned.data[3].value(15).value<int64_t>() == 4015);
}
