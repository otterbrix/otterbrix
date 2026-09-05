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
        // STEP 1 NOW DOES TWO THINGS, AND THE SECOND IS THE ONE THE ROUND CANNOT START
        // WITHOUT. flush_all_indexes ARMS the durable "these indexes are about to be
        // renumbered and are not yet rebuilt" guard before it flushes anything
        // (manager_index_t::rebuild_marker_path_), because this handler is the first step of
        // both compacting orchestrations and is sent from nowhere else in the tree. A guard
        // that could not be written comes back as a refusal here and stops the round below,
        // ahead of the compaction it was meant to cover.
        //
        // THE FLUSH ITSELF IS HERE FOR ITS ANSWER, NOT FOR ITS BYTES, and the distinction was
        // measured rather than argued.
        //
        // What it writes is genuinely dead. force_flush persists each store's dirty state;
        // the rebuild at the end of this operator then calls repopulate_table, whose first
        // act per index is index_agent_contract::clear -- btree_index_disk_t::clear removes
        // the whole tree DIRECTORY (core::filesystem::remove_directory is recursive) and
        // re-creates an empty one, bitcask_index_disk_t::clear unlinks every segment, CURRENT,
        // the txn log and its applied-offset sidecar. So every byte this flush put on the
        // device for a rebuilt index is unlinked a few steps later, and what makes the RESULT
        // durable is the rebuild's own force_flush inside publish_buckets (both agent
        // families end commit_inserts with it). The old justification standing here -- "so a
        // post-recovery rebuild starts from a consistent on-disk index state" -- named a
        // bootstrap rebuild that was removed from this branch as a proven no-op.
        //
        // WHAT IS NOT DEAD IS THE REFUSAL, and it is the ONLY report on the health of the
        // index's EXISTING durable state, taken before the rebuild below destroys and
        // re-creates the store. Measured by removing this block and running
        // test_index_flush_refusal: with the tree's `metadata` path replaced by a directory,
        // the CHECKPOINT stopped failing and started SUCCEEDING -- clear()'s recursive
        // remove_directory erases the injected fault together with the tree, so the rebuild's
        // flush then lands on a clean path and the round reports success over a device the
        // index could not write a moment earlier. The last step truncates the WAL, so that
        // success is not harmless.
        //
        // Replacing it with a probe that reports the same health WITHOUT writing bytes that
        // are about to be unlinked needs a new door on index_agent_contract, which is not this
        // change's surface.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_fi, fif] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::flush_all_indexes,
                                               ctx->session);
            // THE STATEMENT IS THE CHANNEL. The last step below truncates the WAL, so an index
            // that cannot reach the device must stop the round here rather than be logged
            // inside the agent and forgotten.
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

        // Index rebuild. It MUST run AFTER checkpoint_all and BEFORE the truncate below, and
        // the two halves of that sandwich are owed to different facts.
        //
        // AFTER checkpoint_all, because checkpoint_inner compact()s each table's on-disk
        // storage, which renumbers row ids (0-based, gap-free post-compact). The index stores
        // those PHYSICAL ids, so rebuilding earlier would re-stage the ids that are about to
        // be replaced. repopulate_table clears the on-disk index backing AND the agents'
        // stores before re-inserting, so both btree duplicate-growth and disk_hash wrong-row
        // drift are wiped in one pass. Sequential per-oid is fine: checkpoint is a cold,
        // exclusive operation.
        //
        // BEFORE truncate_before, because THE TRUNCATION IS THIS ROUND'S POINT OF NO RETURN
        // and the rebuild is the round's last chance to fail recoverably. Between the compact
        // committing its header and the rebuild's force_flush landing (btree and bitcask both
        // end commit_inserts with one), the durable state is a post-compact table under
        // pre-compact indexes. Whatever ends the round inside that window — a refused
        // truncate returning through the branch below, or a kill -9 — must not ALSO have
        // destroyed journal segments. Ordering it this way is what makes the refusal path
        // safe; it does not shorten the window itself, and it never could: a call order
        // cannot reach past the round's own death. What does reach past it is the durable
        // guard step 1 armed (manager_index_t::rebuild_marker_path_, armed inside
        // flush_all_indexes and cleared per table inside repopulate_table). A start that
        // finds it still armed declines to wire the indexes it names, so the window now
        // costs full scans and an error line rather than silent wrong answers.
        //
        // THE LOOP ITSELF LIVES IN services::index::repopulate_indexes_after_compaction, and
        // it being there is the point rather than tidiness. compact() is reached from three
        // orchestrations — this operator, manager_wal_replicate_t's auto-checkpoint and
        // operator_vacuum_t — and only this one once had the rebuild written out longhand, so
        // every auto-checkpoint renumbered indexed tables and left their indexes naming
        // pre-compact rows. One shared driver is what stops a fourth caller from repeating
        // that; the ORDER around it is stated on the driver, because that is the half a
        // shared function cannot enforce for its callers and the half these two diverged on.
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
                // than leave behind an index that lies — and leave the journal alone, since
                // the step that would have trimmed it is below this return.
                set_error(rebuild_error);
                mark_failed();
                co_return;
            }
        }

        if (checkpoint_wal_id > services::wal::id_t{0} && ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wt, wtf] = actor_zeta::send(ctx->wal_address,
                                               &services::wal::manager_wal_replicate_t::truncate_before,
                                               ctx->session,
                                               checkpoint_wal_id);
            // THE STATEMENT IS THE CHANNEL, same as the index flush in step 1. A truncate that
            // refused means a segment could not be read — the WAL is not in the state this
            // CHECKPOINT reports, so say so instead of returning success over it.
            if (auto truncate_error = co_await std::move(wtf); truncate_error.contains_error()) {
                set_error(truncate_error);
                mark_failed();
                co_return;
            }
        }

        mark_executed();
    }

} // namespace components::operators
