#include "operator_vacuum.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/table/column_state.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_vacuum_t::operator_vacuum_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::vacuum) {}

    void operator_vacuum_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_vacuum_t::await_async_and_resume(pipeline::context_t* ctx) {
        const std::uint64_t lowest = ctx->lowest_active_start_time;

        // Compact watermark for vacuum_inner's MVCC-gated compact: the
        // dispatcher's visible-to-all horizon. lowest_active_start_time is NOT a
        // substitute — it lives in start-time space and ignores in-flight
        // (committed-but-unpublished) commits whose versions a compact would
        // drop. 0 when no dispatcher is wired: compacts are then skipped.
        std::uint64_t compact_watermark = 0;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_wm, wmf] = actor_zeta::send(ctx->current_message_sender,
                                               &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wmf);
        }

        // cleanup_versions + compact across every user storage. The disk manager
        // iterates its own storages_ map, so one global call suffices.
        {
            auto [_v, vf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::vacuum_all,
                                             ctx->session,
                                             lowest,
                                             compact_watermark);
            co_await std::move(vf);
        }

        // Without an index actor the rest of the work is moot.
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        {
            auto [_cv, cvf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::cleanup_all_versions,
                                               ctx->session,
                                               lowest);
            co_await std::move(cvf);
        }

        // Enumerate user relations via pg_class (relkind 'r'/'g') and rebuild +
        // repopulate their indexes: the compact pass above invalidated row positions.
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;

        std::unique_ptr<components::vector::data_chunk_t> pg_class_rows;
        {
            auto [_sc, scf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_scan,
                                               ctx->session,
                                               kPgClass,
                                               std::unique_ptr<components::table::table_filter_t>{},
                                               /*limit=*/-1,
                                               ctx->txn);
            pg_class_rows = co_await std::move(scf);
        }
        if (!pg_class_rows) {
            mark_executed();
            co_return;
        }

        // Per-table rebuild loop. We collect oids first so we don't
        // hold the data_chunk across more co_awaits than necessary.
        struct user_table_t {
            catalog::oid_t table_oid;
        };
        std::vector<user_table_t> user_tables;
        // Collect computing-table OIDs in the same pass so the later
        // pg_computed_column GC doesn't have to re-scan pg_class.
        std::vector<catalog::oid_t> computing_table_oids;

        for (std::uint64_t i = 0; i < pg_class_rows->size(); ++i) {
            // pg_class columns: 0=oid, 1=relname, 2=relnamespace, 3=relkind, 4=relstoragemode
            auto rk_v = pg_class_rows->value(3, i);
            const auto rkv = rk_v.is_null() ? std::string_view{"r"} : rk_v.value<std::string_view>();
            const char relkind = rkv.empty() ? catalog::relkind::regular : rkv[0];
            if (relkind != catalog::relkind::regular && relkind != catalog::relkind::computed) {
                continue;
            }

            auto oid_v = pg_class_rows->value(0, i);
            if (oid_v.is_null())
                continue;
            const auto this_oid = static_cast<catalog::oid_t>(oid_v.value<std::uint32_t>());
            if (this_oid == catalog::INVALID_OID)
                continue;

            if (relkind == catalog::relkind::computed) {
                computing_table_oids.push_back(this_oid);
            }
            user_tables.push_back({this_oid});
        }

        // For each user table, re-populate its index from the just-compacted
        // storage via repopulate_table (clears the on-disk index backing + the
        // in-memory engine internally, then re-inserts at post-compact ids).
        //
        // repopulate_table re-inserts with txn_id=0 (committed-for-everyone), the
        // path that needs no commit. Entries inserted under a real txn id stay
        // PENDING-invisible unless that txn index-commits, and VACUUM never
        // index-commits — so the txn_id=0 path is required here.
        for (auto& tbl : user_tables) {
            std::uint64_t total = 0;
            {
                auto [_tr, trf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_total_rows,
                                                   ctx->session,
                                                   tbl.table_oid);
                total = co_await std::move(trf);
            }

            // total==0 (table emptied by compact) still repopulates: the clear
            // step inside repopulate_table wipes stale index entries.
            // storage_scan_segment returns an empty chunk for count==0.
            std::unique_ptr<components::vector::data_chunk_t> scan_data;
            {
                auto [_ss, ssf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_scan_segment,
                                                   ctx->session,
                                                   tbl.table_oid,
                                                   std::int64_t{0},
                                                   total);
                scan_data = co_await std::move(ssf);
            }

            auto [_rp, rpf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::repopulate_table,
                                               ctx->session,
                                               tbl.table_oid,
                                               std::move(scan_data),
                                               total,
                                               ctx->session_tz);
            co_await std::move(rpf);
        }

        // GC pg_computed_column rows for relkind='g' tables.
        //
        // Safety vs. concurrent VACUUM + INSERT: VACUUM uses
        // ctx->lowest_active_start_time as the snapshot horizon. The
        // pg_computed_column GC below reads via read_chunks_by_key and deletes
        // via delete_pg_catalog_rows — both go through ctx->txn, so rows
        // newer than the horizon are NOT GC-eligible. Concurrent INSERT's
        // writes (under txn_id >= TRANSACTION_ID_START) are invisible to
        // VACUUM until commit; MVCC tag flipping is atomic per row.
        //
        // Two-pass strategy:
        //  (a) drop tombstones (attrefcount<=0) produced by
        //      operator_computed_field_unregister;
        //  (b) version-GC — for each (relid, attname) group keep only the
        //      row with max(attversion); delete older versions even if their
        //      refcount is still positive. The resolver picks max version
        //      per attname, so older rows are invisible to readers but
        //      accumulate over ALTER COLUMN cycles and bloat
        //      pg_computed_column.
        //
        // Physical column compaction for relkind='g' IN_MEMORY tables.
        // After (a) tombstone GC and (b) version GC above, columns whose
        // every pg_computed_column row was deleted are physically dead in
        // table_storage_t.table().column_definitions_ but invisible to
        // readers (resolve_table reads from pg_computed_column). We reclaim
        // them by calling compact_relkind_g_storage with the post-GC live
        // attname set.
        //
        // Implementation: data_table_t has an existing rebuild constructor
        // (parent, removed_column) backed by collection_t::remove_column —
        // this drops the column from every row_group segment in IN_MEMORY
        // mode (compact_relkind_g_storage is a no-op for DISK-backed
        // tables).
        //
        // FIXME: storage_append auto-extends the IN_MEMORY schema when an
        // INSERT brings a new attname. After we drop a physical column
        // here, a subsequent INSERT with that attname will trigger schema
        // re-extension. That's correct behavior but does waste work if the
        // column is being immediately re-added (e.g. drop+readd cycles).
        if (!computing_table_oids.empty()) {
            constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
            components::execution_context_t cc_ctx{ctx->session, ctx->txn, {}};

            for (const auto table_oid : computing_table_oids) {
                // pg_computed_column layout: 0=relid 1=attoid 2=attname
                // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
                types::logical_value_t toid_lv(resource_, table_oid);
                std::pmr::vector<std::string> cc_keys(resource_);
                cc_keys.emplace_back("relid");
                std::pmr::vector<types::logical_value_t> cc_vals(resource_);
                cc_vals.emplace_back(toid_lv);
                auto [_cc, ccf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   cc_ctx,
                                                   kPgComputedColumn,
                                                   std::move(cc_keys),
                                                   components::operators::make_key_chunk(resource_, std::move(cc_vals)));
                auto cc_batches = co_await std::move(ccf);

                // Both GC passes below delete by (kPgComputedColumn, col 1=attoid)
                // and derive their target attoids purely from the already-awaited
                // cc_batches (no intervening await), so collect every delete into one
                // batched call issued before the post-GC re-read.
                std::pmr::vector<services::disk::pg_catalog_delete_spec_t> cc_specs(resource_);

                std::vector<catalog::oid_t> dead_attoids;
                for (auto& chunk : cc_batches) {
                    if (chunk.column_count() < 7)
                        continue;
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        auto attoid_v = chunk.value(1, i);
                        auto refcount_v = chunk.value(6, i);
                        if (attoid_v.is_null() || refcount_v.is_null())
                            continue;
                        const auto rc = refcount_v.value<std::int64_t>();
                        if (rc > 0)
                            continue;
                        dead_attoids.push_back(static_cast<catalog::oid_t>(attoid_v.value<std::uint32_t>()));
                    }
                }

                for (const auto attoid : dead_attoids) {
                    // attoid is column index 1 in pg_computed_column.
                    cc_specs.push_back({kPgComputedColumn, std::int64_t{1}, attoid});
                }

                // version-GC: for each (relid, attname) group, keep only
                // max(attversion). Older versions with refcount>0 are
                // invisible to readers (resolver picks max version) but
                // accumulate over time; delete them to save space.
                struct version_row_t {
                    catalog::oid_t attoid;
                    std::int64_t attversion;
                };
                std::map<std::string, std::vector<version_row_t>> grouped;
                for (auto& chunk : cc_batches) {
                    if (chunk.column_count() < 7)
                        continue;
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        auto attoid_v = chunk.value(1, i);
                        auto attname_v = chunk.value(2, i);
                        auto attversion_v = chunk.value(5, i);
                        auto refcount_v = chunk.value(6, i);
                        if (attoid_v.is_null() || attname_v.is_null() || attversion_v.is_null() ||
                            refcount_v.is_null()) {
                            continue;
                        }
                        // Skip rows already queued for deletion as tombstones.
                        if (refcount_v.value<std::int64_t>() <= 0)
                            continue;
                        auto attname = attname_v.value<std::string_view>();
                        auto attversion = attversion_v.value<std::int64_t>();
                        auto attoid = static_cast<catalog::oid_t>(attoid_v.value<std::uint32_t>());
                        grouped[std::string(attname)].push_back({attoid, attversion});
                    }
                }
                for (auto& [_key, rows] : grouped) {
                    if (rows.size() <= 1)
                        continue;
                    // Sort by version descending; keep first (max), delete rest.
                    std::sort(rows.begin(), rows.end(), [](const version_row_t& a, const version_row_t& b) {
                        return a.attversion > b.attversion;
                    });
                    for (std::size_t i = 1; i < rows.size(); ++i) {
                        cc_specs.push_back({kPgComputedColumn, std::int64_t{1}, rows[i].attoid});
                    }
                }

                if (!cc_specs.empty()) {
                    auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                     cc_ctx,
                                                     std::move(cc_specs));
                    co_await std::move(df);
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(kPgComputedColumn);
                    }
                }

                // Physical column compaction step. Re-read
                // pg_computed_column post-GC for this table_oid (the
                // tombstone + version-GC deletes above ran under ctx->txn so
                // they're visible here), build the live attname set, and ask
                // the disk actor to drop every storage column that's NOT in
                // that set. The disk actor skips DISK-backed storages and
                // missing/already-compact columns silently.
                //
                // We re-read instead of reusing cc_rows because cc_rows was
                // taken BEFORE the deletes; row[5]>0 there can include rows
                // whose live counterparts were just version-GC'd.
                {
                    types::logical_value_t toid_lv2(resource_, table_oid);
                    std::pmr::vector<std::string> cc2_keys(resource_);
                    cc2_keys.emplace_back("relid");
                    std::pmr::vector<types::logical_value_t> cc2_vals(resource_);
                    cc2_vals.emplace_back(toid_lv2);
                    auto [_cc2, ccf2] = actor_zeta::send(ctx->disk_address,
                                                         &services::disk::manager_disk_t::read_chunks_by_key,
                                                         cc_ctx,
                                                         kPgComputedColumn,
                                                         std::move(cc2_keys),
                                                         components::operators::make_key_chunk(resource_, std::move(cc2_vals)));
                    auto live_cc = co_await std::move(ccf2);

                    std::set<std::string> live_attnames;
                    for (auto& chunk : live_cc) {
                        if (chunk.column_count() < 7)
                            continue;
                        for (uint64_t i = 0; i < chunk.size(); ++i) {
                            auto attname_v = chunk.value(2, i);
                            auto refcount_v = chunk.value(6, i);
                            if (attname_v.is_null() || refcount_v.is_null())
                                continue;
                            if (refcount_v.value<std::int64_t>() <= 0)
                                continue;
                            live_attnames.emplace(std::string(attname_v.value<std::string_view>()));
                        }
                    }

                    auto [_dc, dcf] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::compact_relkind_g_storage,
                                                       cc_ctx,
                                                       table_oid,
                                                       std::move(live_attnames));
                    (void) co_await std::move(dcf);
                }
            }
        }

        mark_executed();
    }

} // namespace components::operators
