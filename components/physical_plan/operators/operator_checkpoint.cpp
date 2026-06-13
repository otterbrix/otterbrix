#include "operator_checkpoint.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_checkpoint_t::operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::checkpoint) {}

    void operator_checkpoint_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_checkpoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Flush dirty index btrees so a post-recovery rebuild starts from a
        // consistent on-disk index state.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_fi, fif] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::flush_all_indexes,
                                               ctx->session);
            co_await std::move(fif);
        }

        // snapshot the current WAL id BEFORE the checkpoint so the per-table
        // W-TORN (prev/current) snapshot pins a known recovery boundary.
        services::wal::id_t wal_max_id{0};
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wi, wif] = actor_zeta::send(ctx->wal_address,
                                               &services::wal::manager_wal_replicate_t::current_wal_id,
                                               ctx->session);
            wal_max_id = co_await std::move(wif);
        }

        // Compact watermark for checkpoint_inner's MVCC-gated compact: the
        // dispatcher's visible-to-all horizon (current_message_sender is the
        // dispatcher — the executor wires parent_address_ into the context).
        // 0 when no dispatcher is wired (test topologies): compacts and the
        // affected per-table checkpoints are then skipped, never unsafe.
        std::uint64_t compact_watermark = 0;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_wm, wmf] = actor_zeta::send(ctx->current_message_sender,
                                               &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wmf);
        }

        // checkpoint_all. No-op when disk is off.
        services::wal::id_t checkpoint_wal_id{0};
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_cp, cpf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::checkpoint_all,
                                               ctx->session,
                                               wal_max_id,
                                               compact_watermark);
            checkpoint_wal_id = co_await std::move(cpf);
        }

        if (checkpoint_wal_id > services::wal::id_t{0} && ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wt, wtf] = actor_zeta::send(ctx->wal_address,
                                               &services::wal::manager_wal_replicate_t::truncate_before,
                                               ctx->session,
                                               checkpoint_wal_id);
            co_await std::move(wtf);
        }

        // Index rebuild. This MUST run AFTER checkpoint_all: checkpoint_inner
        // compact()s each table's on-disk storage, which renumbers row ids
        // (0-based, gap-free post-compact). The in-memory index engines hold
        // POSITIONAL row refs into the pre-compact layout, so leaving them as-is
        // would make every post-checkpoint index_scan return stale/wrong rows.
        // repopulate_table clears the on-disk index backing AND the in-memory
        // engine before re-inserting, so both btree duplicate-growth and
        // disk_hash wrong-row drift are wiped in one pass. Sequential per-oid is
        // fine: checkpoint is a cold, exclusive operation.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<components::catalog::oid_t> indexed_oids{resource_};
            {
                auto [_io, iof] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::all_indexed_oids,
                                                   ctx->session);
                indexed_oids = co_await std::move(iof);
            }

            for (const auto table_oid : indexed_oids) {
                std::uint64_t total = 0;
                {
                    auto [_tr, trf] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::storage_total_rows,
                                                       ctx->session,
                                                       table_oid);
                    total = co_await std::move(trf);
                }

                // total==0 (table emptied by compact) still repopulates: the
                // clear step inside repopulate_table wipes stale index entries.
                // storage_scan_segment returns an empty chunk for count==0, which
                // is exactly what repopulate_table expects.
                std::unique_ptr<components::vector::data_chunk_t> scan_data;
                {
                    auto [_ss, ssf] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::storage_scan_segment,
                                                       ctx->session,
                                                       table_oid,
                                                       std::int64_t{0},
                                                       total);
                    scan_data = co_await std::move(ssf);
                }

                auto [_rp, rpf] = actor_zeta::send(ctx->index_address,
                                                   &services::index::manager_index_t::repopulate_table,
                                                   ctx->session,
                                                   table_oid,
                                                   std::move(scan_data),
                                                   total,
                                                   ctx->session_tz);
                co_await std::move(rpf);
            }
        }

        mark_executed();
    }

} // namespace components::operators
