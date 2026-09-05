#include "operator_checkpoint.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/index_rebuild_driver.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_checkpoint_t::operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::checkpoint) {}

    actor_zeta::unique_future<void> operator_checkpoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Flush dirty index btrees so a post-recovery rebuild starts from a
        // consistent on-disk index state.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_fi, fif] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::flush_all_indexes,
                                               ctx->session);
            // THE STATEMENT IS THE CHANNEL. Step 4 below truncates the WAL behind whatever
            // this step made durable, so an index flush that did not reach the device must
            // stop the round here rather than be logged inside the agent and forgotten.
            if (auto flush_error = co_await std::move(fif); flush_error.contains_error()) {
                set_error(flush_error);
                mark_failed();
                co_return;
            }
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
        // (0-based, gap-free post-compact). The index stores those PHYSICAL ids, so
        // leaving it as-is would make every post-checkpoint index_scan name a row that
        // moved or vanished. repopulate_table clears the on-disk index backing AND the
        // agents' stores before re-inserting, so both btree duplicate-growth and
        // disk_hash wrong-row drift are wiped in one pass. Sequential per-oid is
        // fine: checkpoint is a cold, exclusive operation.
        //
        // THE LOOP ITSELF NOW LIVES IN services::index::repopulate_indexes_after_compaction,
        // and moving it there is the point rather than tidiness. compact() is reached from
        // two orchestrations — this operator and manager_wal_replicate_t's auto-checkpoint —
        // and only this one had the rebuild written out longhand, so every auto-checkpoint
        // renumbered indexed tables and left their indexes naming pre-compact rows. One
        // shared driver is what stops a third caller from repeating that.
        {
            auto rebuild_error =
                co_await services::index::repopulate_indexes_after_compaction(resource_,
                                                                              ctx->disk_address,
                                                                              ctx->index_address,
                                                                              ctx->session,
                                                                              ctx->txn,
                                                                              ctx->execution_context.timezone_offset);
            if (rebuild_error.contains_error()) {
                // A producer defect in the rebuild feed (scan chunks without physical
                // row_ids) or a refused scan: fail the CHECKPOINT statement loudly rather
                // than leave behind an index that lies.
                set_error(rebuild_error);
                mark_failed();
                co_return;
            }
        }

        mark_executed();
    }

} // namespace components::operators
