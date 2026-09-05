#include "operator_vacuum.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/table/column_state.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/index_rebuild_driver.hpp>
#include <services/index/manager_index.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_vacuum_t::operator_vacuum_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::vacuum) {}

    actor_zeta::unique_future<void> operator_vacuum_t::await_async_and_resume(pipeline::context_t* ctx) {
        const std::uint64_t lowest = ctx->lowest_active_start_time;

        // vacuum_all answers how many storages it renumbered — the fact that decides whether the rebuild below
        // runs. No compact watermark is fetched: vacuum_inner does not compact, so nothing here would read it.
        std::uint64_t renumbered_storages = 0;
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_v, vf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::vacuum_all,
                                                        ctx->session,
                                                        lowest);
            renumbered_storages = co_await std::move(vf);
        }

        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_cv, cvf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::cleanup_all_versions,
                                                          ctx->session,
                                                          lowest);
            co_await std::move(cvf);
        }

        // A full index rebuild is owed only for a renumbering; only data_table_t::compact() renumbers, and
        // VACUUM never reaches it, so renumbered_storages > 0 only guards a future change on that route.
        // Does NOT arm manager_index_t::rebuild_marker_path_ (flush_all_indexes does, for the compacting
        // orchestrations) — safe only because VACUUM opens no compact window today; if it ever does, the arm
        // must move above the vacuum_all call, not here.
        if (renumbered_storages > 0) {
            auto rebuild_error =
                co_await services::index::repopulate_indexes_after_compaction(resource_,
                                                                              ctx->disk_address,
                                                                              ctx->index_address,
                                                                              ctx->session,
                                                                              ctx->txn,
                                                                              ctx->execution_context.timezone_offset);
            if (rebuild_error.contains_error()) {
                // A producer defect in the rebuild feed (scan chunks without physical row_ids)
                // or a refused scan: fail the VACUUM statement loudly rather than leave behind
                // an index that lies.
                set_error(rebuild_error);
                mark_failed();
                co_return;
            }
        }

        // The rest of this operator is the pg_computed_column GC, which needs the DISK actor
        // and not the index one — so it is gated on the disk address, not on the index one.
        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        // Enumerate relations via pg_class to find the COMPUTING tables (relkind 'g'), whose
        // pg_computed_column rows the two GC passes further down reclaim.
        constexpr catalog::oid_t kPgClass = catalog::well_known_oid::pg_class_table;

        // Draining the cursor to completion is MANDATORY: a live cursor gates compact() on its oid, so an
        // abandoned one would wedge the very table VACUUM is here to reclaim — hence the explicit release below.
        std::pmr::vector<components::vector::data_chunk_t> pg_class_batches(resource_);
        {
            uint64_t cursor_id = 0; // 0 == OPEN on the first fetch
            bool scan_failed = false;
            while (true) {
                auto [_sc, scf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_fetch_next_batch,
                                                ctx->session,
                                                kPgClass,
                                                cursor_id,
                                                std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                /*limit=*/int64_t{-1},
                                                std::vector<size_t>{},
                                                ctx->txn);
                auto scan_r = co_await std::move(scf);
                if (scan_r.has_error()) {
                    set_error(scan_r.error());
                    scan_failed = true;
                    break;
                }
                auto reply = std::move(scan_r.value());
                cursor_id = reply.cursor_id;
                if (!reply.batch || reply.batch->size() == 0) {
                    break; // drained: the agent replied an empty batch and erased the cursor
                }
                pg_class_batches.emplace_back(std::move(*reply.batch));
            }
            if (scan_failed) {
                if (cursor_id != 0) {
                    auto [_cc, ccf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                                  &services::disk::manager_disk_t::storage_close_cursor,
                                                                  ctx->session,
                                                                  kPgClass,
                                                                  cursor_id);
                    co_await std::move(ccf);
                }
                mark_failed();
                co_return;
            }
        }
        if (pg_class_batches.empty()) {
            mark_executed();
            co_return;
        }

        // Collect the COMPUTING-table OIDs — the only thing the pg_class scan is for.
        std::vector<catalog::oid_t> computing_table_oids;

        for (const auto& pg_class_rows : pg_class_batches) {
            for (std::uint64_t i = 0; i < pg_class_rows.size(); ++i) {
                // pg_class columns: 0=oid, 1=relname, 2=relnamespace, 3=relkind, 4=relstoragemode
                // Every writer stamps both oid and relkind (ddl_metadata_builder.cpp), so a NULL/empty/invalid
                // value in either is corrupt, not a variant to default — defaulting relkind to 'r' would quietly
                // drop a 'g' table's GC forever.
                if (pg_class_rows.is_null(0, i) || pg_class_rows.is_null(3, i)) {
                    set_error(core::error_t{
                        core::error_code_t::data_corruption,
                        std::pmr::string{"VACUUM: a pg_class row carries a NULL oid or relkind — corrupt "
                                         "catalog row; refusing to decide table maintenance over it",
                                         resource_}});
                    mark_failed();
                    co_return;
                }
                const auto rkv = pg_class_rows.get_value<std::string_view>(3, i);
                const auto this_oid = static_cast<catalog::oid_t>(pg_class_rows.get_value<std::uint32_t>(0, i));
                if (rkv.empty() || this_oid == catalog::INVALID_OID) {
                    set_error(core::error_t{
                        core::error_code_t::data_corruption,
                        std::pmr::string{"VACUUM: a pg_class row carries an empty relkind or oid 0 — corrupt "
                                         "catalog row; refusing to decide table maintenance over it",
                                         resource_}});
                    mark_failed();
                    co_return;
                }
                const char relkind = rkv[0];
                if (relkind == catalog::relkind::computed) {
                    computing_table_oids.push_back(this_oid);
                }
            }
        }

        // GC pg_computed_column rows for relkind='g' tables, through ctx->txn so rows newer than the VACUUM
        // horizon are not eligible. Two passes: (a) drop tombstones (attrefcount<=0); (b) version-GC — per
        // (relid, attname) keep only max(attversion). compact_relkind_g_storage then drops every physical
        // column NOT in the post-GC live attname set, so a gap in this read would physically drop a surviving
        // column.
        if (!computing_table_oids.empty()) {
            constexpr catalog::oid_t kPgComputedColumn = catalog::well_known_oid::pg_computed_column_table;
            components::execution_context_t cc_ctx{ctx->session, ctx->txn, {}};

            for (const auto table_oid : computing_table_oids) {
                // pg_computed_column layout: 0=relid 1=attoid 2=attname
                // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
                std::pmr::vector<std::uint64_t> cc_keys(resource_);
                cc_keys.emplace_back(catalog::pg_computed_column_col::relid);
                auto [_cc, ccf] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::read_chunks_by_key,
                                                cc_ctx,
                                                kPgComputedColumn,
                                                std::move(cc_keys),
                                                components::operators::make_key_chunk(resource_, table_oid),
                                                std::pmr::vector<std::uint64_t>{resource_});
                auto cc_batches_r = co_await std::move(ccf);
                if (cc_batches_r.has_error()) {
                    // A failed pg_computed_column read is not a miss; treating it as one lets the
                    // operation proceed on data that was never read.
                    set_error(cc_batches_r.error());
                    co_return;
                }
                auto& cc_batches = cc_batches_r.value();

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
                        if (chunk.is_null(1, i) || chunk.is_null(6, i))
                            continue;
                        const auto rc = chunk.get_value<std::int64_t>(6, i);
                        if (rc > 0)
                            continue;
                        dead_attoids.push_back(static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i)));
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
                        if (chunk.is_null(1, i) || chunk.is_null(2, i) || chunk.is_null(5, i) || chunk.is_null(6, i)) {
                            continue;
                        }
                        // Skip rows already queued for deletion as tombstones.
                        if (chunk.get_value<std::int64_t>(6, i) <= 0)
                            continue;
                        auto attname = chunk.get_value<std::string_view>(2, i);
                        auto attversion = chunk.get_value<std::int64_t>(5, i);
                        auto attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
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
                    auto [_d, df] =
                        actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                    cc_ctx,
                                                    std::move(cc_specs));
                    auto deleted_r = co_await std::move(df);
                    // A zero-match delete is fine (GC pass, already-gone is the goal state), but an error must be
                    // read here, before the SUBTRACTIVE and irreversible compaction below runs on an unknown GC outcome.
                    if (deleted_r.has_error()) {
                        set_error(deleted_r.error());
                        co_return;
                    }
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(kPgComputedColumn);
                    }
                }

                // Re-read post-GC rather than reuse cc_rows (taken before the deletes above), then drop every
                // storage column not in the live attname set.
                {
                    std::pmr::vector<std::uint64_t> cc2_keys(resource_);
                    cc2_keys.emplace_back(catalog::pg_computed_column_col::relid);
                    auto [_cc2, ccf2] =
                        actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    cc_ctx,
                                                    kPgComputedColumn,
                                                    std::move(cc2_keys),
                                                    components::operators::make_key_chunk(resource_, table_oid),
                                                    std::pmr::vector<std::uint64_t>{resource_});
                    auto live_cc_r = co_await std::move(ccf2);
                    if (live_cc_r.has_error()) {
                        // This list drives which physical columns survive compaction. A failed
                        // read would look like "no live computed columns" and drop all of them.
                        set_error(live_cc_r.error());
                        co_return;
                    }
                    auto& live_cc = live_cc_r.value();

                    std::set<std::string> live_attnames;
                    for (auto& chunk : live_cc) {
                        if (chunk.column_count() < 7)
                            continue;
                        for (uint64_t i = 0; i < chunk.size(); ++i) {
                            if (chunk.is_null(2, i) || chunk.is_null(6, i))
                                continue;
                            if (chunk.get_value<std::int64_t>(6, i) <= 0)
                                continue;
                            live_attnames.emplace(std::string(chunk.get_value<std::string_view>(2, i)));
                        }
                    }

                    auto [_dc, dcf] =
                        actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::compact_relkind_g_storage,
                                                    cc_ctx,
                                                    table_oid,
                                                    std::move(live_attnames));
                    // Answer is the count of physical columns actually dropped (skipped silently for DISK-backed
                    // or already-compact storages) — logged so the subtractive leg leaves a record.
                    const std::uint64_t dropped_columns = co_await std::move(dcf);
                    if (dropped_columns > 0) {
                        trace(log_,
                              "operator_vacuum: relkind='g' storage oid {} — column compaction dropped {} "
                              "physical column(s)",
                              static_cast<unsigned>(table_oid),
                              dropped_columns);
                    }
                }
            }
        }

        mark_executed();
    }

} // namespace components::operators
