#include "index_rebuild_driver.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace services::index {

    committed_rows_snapshot_t committed_rows_snapshot() noexcept {
        components::table::transaction_data txn;
        txn.transaction_id = 0;
        // Below every PENDING txn id and at or above every commit id: transaction_version_operator's
        // own visibility check already refuses anything >= TRANSACTION_ID_START, so this horizon admits
        // exactly the committed half. start_time isn't read by the visibility filter; set to the
        // same value so the record can't be mistaken for a partially-filled one.
        constexpr uint64_t all_commit_ids = components::table::TRANSACTION_ID_START - 1;
        txn.start_time = all_commit_ids;
        txn.snapshot_horizon = all_commit_ids;
        return committed_rows_snapshot_t{std::move(txn)};
    }

    actor_zeta::unique_future<core::error_t>
    repopulate_indexes_after_compaction(std::pmr::memory_resource* resource,
                                        actor_zeta::actor::address_t disk_address,
                                        actor_zeta::actor::address_t index_address,
                                        components::session::session_id_t session,
                                        committed_rows_snapshot_t snapshot,
                                        core::date::timezone_offset_t session_tz) {
        // Both addresses or nothing: a topology missing either has no rebuild to do, and a loop
        // that sent scans to an empty disk address would believe it rebuilt the index while
        // reaching nowhere.
        if (index_address == actor_zeta::actor::address_t::empty_address() ||
            disk_address == actor_zeta::actor::address_t::empty_address()) {
            co_return core::error_t::no_error();
        }

        std::pmr::vector<components::catalog::oid_t> indexed_oids{resource};
        {
            auto [_io, iof] = actor_zeta::otterbrix::send(index_address,
                                                          &services::index::manager_index_t::all_indexed_oids,
                                                          session);
            indexed_oids = co_await std::move(iof);
        }

        for (const auto table_oid : indexed_oids) {
            // The table's compact epoch, read STRICTLY BEFORE the scan below. A compact
            // interleaving capture and scan (a second checkpoint round racing this one) leaves
            // the stamp too LOW — every read on the oid then refuses until that round's own
            // rebuild re-stamps, which is the safe side; captured after the scan the same race
            // would stamp fresh over stale ids, the exact lie the stamp exists to refuse.
            std::uint64_t built_epoch = 0;
            {
                auto [_ce, cef] = actor_zeta::otterbrix::send(disk_address,
                                                              &services::disk::manager_disk_t::storage_compact_epoch,
                                                              session,
                                                              table_oid);
                auto epoch_r = co_await std::move(cef);
                if (epoch_r.has_error()) {
                    // Same reasoning as the total-rows refusal below: an INDEXED oid names a
                    // table that must have a storage; a rebuild stamped with a guess would
                    // either lie or permanently refuse.
                    co_return epoch_r.error();
                }
                built_epoch = epoch_r.value();
            }
            std::uint64_t total = 0;
            {
                auto [_tr, trf] = actor_zeta::otterbrix::send(disk_address,
                                                              &services::disk::manager_disk_t::storage_total_rows,
                                                              session,
                                                              table_oid);
                auto total_r = co_await std::move(trf);
                if (total_r.has_error()) {
                    // The same refusal the scan below would hit, one round-trip earlier: an
                    // INDEXED oid names a table that must have a storage. Reported the way
                    // this loop reports every other failure — stop, do not average it into a
                    // rebuild sized by a count nobody read.
                    co_return total_r.error();
                }
                total = total_r.value();
            }

            // total==0 (table emptied by compact) still repopulates: repopulate_table's clear
            // step wipes stale entries over the empty vector a drained scan yields.
            //
            // A cursor left open here would permanently gate compact() on this oid, so the loop
            // exits only drained-or-released, never with the cursor still open.
            std::pmr::vector<components::vector::data_chunk_t> scan_data{resource};
            {
                uint64_t cursor_id = 0; // 0 == OPEN on the first fetch
                core::error_t scan_error = core::error_t::no_error();
                while (true) {
                    auto [_ss, ssf] =
                        actor_zeta::otterbrix::send(disk_address,
                                                    &services::disk::manager_disk_t::storage_fetch_next_batch,
                                                    session,
                                                    table_oid,
                                                    cursor_id,
                                                    std::unique_ptr<components::table::table_filter_t>(nullptr),
                                                    /*limit=*/int64_t{-1},
                                                    std::vector<size_t>{},
                                                    snapshot.txn());
                    auto scan_r = co_await std::move(ssf);
                    if (scan_r.has_error()) {
                        scan_error = scan_r.error();
                        break;
                    }
                    auto reply = std::move(scan_r.value());
                    cursor_id = reply.cursor_id;
                    if (!reply.batch || reply.batch->size() == 0) {
                        break; // drained: the agent replied an empty batch and erased the cursor
                    }
                    scan_data.emplace_back(std::move(*reply.batch));
                }
                if (scan_error.contains_error()) {
                    if (cursor_id != 0) {
                        auto [_cc, ccf] =
                            actor_zeta::otterbrix::send(disk_address,
                                                        &services::disk::manager_disk_t::storage_close_cursor,
                                                        session,
                                                        table_oid,
                                                        cursor_id);
                        co_await std::move(ccf);
                    }
                    co_return scan_error;
                }
            }

            auto [_rp, rpf] = actor_zeta::otterbrix::send(index_address,
                                                          &services::index::manager_index_t::repopulate_table,
                                                          session,
                                                          table_oid,
                                                          std::move(scan_data),
                                                          total,
                                                          session_tz,
                                                          built_epoch);
            auto repopulate_error = co_await std::move(rpf);
            if (repopulate_error.contains_error()) {
                // A producer defect (scan chunks without physical row_ids) or a refused write.
                // Stops rather than rebuilding the rest: the caller must decide what a
                // half-rebuilt set of indexes means, which requires the failure not be averaged away.
                co_return repopulate_error;
            }
        }

        co_return core::error_t::no_error();
    }

} // namespace services::index
