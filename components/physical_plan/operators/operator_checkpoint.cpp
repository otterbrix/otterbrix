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

#ifdef DEV_MODE
    namespace {
        checkpoint_repopulate_gate_t* g_checkpoint_repopulate_gate = nullptr;
    } // namespace

    void dev_set_checkpoint_repopulate_gate(checkpoint_repopulate_gate_t* gate) {
        g_checkpoint_repopulate_gate = gate;
    }
    checkpoint_repopulate_gate_t* dev_checkpoint_repopulate_gate() { return g_checkpoint_repopulate_gate; }
#endif

    operator_checkpoint_t::operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::checkpoint) {}

    actor_zeta::unique_future<void> operator_checkpoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // flush_all_indexes first arms the durable rebuild_marker_path_ guard; a refusal here stops the round
        // before compaction. Per test_index_flush_refusal, skipping this lets clear()'s recursive
        // remove_directory erase an injected fault (a `metadata` path replaced by a directory) before
        // anything reads it — turning a real fault into a false success.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_fi, fif] = actor_zeta::otterbrix::send(ctx->index_address,
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
            auto [_wi, wif] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                          &services::wal::manager_wal_replicate_t::current_wal_id,
                                                          ctx->session);
            wal_max_id = co_await std::move(wif);
        }

        // Compact watermark = dispatcher's visible-to-all horizon; 0 when no dispatcher is wired (test
        // topologies), which just skips the affected per-table compacts safely.
        std::uint64_t compact_watermark = 0;
        if (ctx->current_message_sender != actor_zeta::address_t::empty_address()) {
            auto [_wm, wmf] =
                actor_zeta::otterbrix::send(ctx->current_message_sender,
                                            &services::dispatcher::manager_dispatcher_t::txn_compact_watermark_msg);
            compact_watermark = co_await std::move(wmf);
        }

        // checkpoint_all. No-op when disk is off.
        services::wal::id_t checkpoint_wal_id{0};
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_cp, cpf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::checkpoint_all,
                                                          ctx->session,
                                                          wal_max_id,
                                                          compact_watermark);
            checkpoint_wal_id = co_await std::move(cpf);
        }

#ifdef DEV_MODE
        // Measurement seam (see the header): park the round between the compaction above and the
        // index rebuild below. Non-blocking — one no-op cross-actor round-trip per poll parks this
        // coroutine without pinning an actor thread, so a reader from another session can land
        // inside the window.
        while (auto* gate = dev_checkpoint_repopulate_gate()) {
            if (!gate->hold()) {
                break;
            }
            if (ctx->wal_address == actor_zeta::address_t::empty_address()) {
                break;
            }
            auto [_gp, gpf] = actor_zeta::otterbrix::send(ctx->wal_address,
                                                          &services::wal::manager_wal_replicate_t::current_wal_id,
                                                          ctx->session);
            [[maybe_unused]] const services::wal::id_t ping = co_await std::move(gpf);
        }
#endif

        // Must run after checkpoint_all (compact() renumbers row ids the indexes still hold pre-compact) and
        // before the truncate below, its point of no return. The rebuild_marker_path_ guard armed in step 1
        // covers a mid-rebuild crash: a restart that finds it still armed declines to wire those indexes.
        // repopulate_indexes_after_compaction is the ONE shared driver — also used by auto-checkpoint and
        // VACUUM. It scans under the all-committed snapshot, NOT ctx->txn: this statement's snapshot can
        // predate a neighbour's commit, and the clear-then-refill rebuild would silently drop that row
        // from the index (test_checkpoint_rebuild_snapshot.cpp). The driver's parameter type accepts only
        // committed_rows_snapshot(), so no caller can hand it a statement snapshot and compile.
        {
            auto rebuild_error =
                co_await services::index::repopulate_indexes_after_compaction(resource_,
                                                                              ctx->disk_address,
                                                                              ctx->index_address,
                                                                              ctx->session,
                                                                              services::index::committed_rows_snapshot(),
                                                                              ctx->execution_context.timezone_offset);
            if (rebuild_error.contains_error()) {
                // Fail the CHECKPOINT loudly rather than leave behind a lying index, and leave the
                // journal alone — the truncate that would trim it is below this return.
                set_error(rebuild_error);
                mark_failed();
                co_return;
            }
        }

        if (checkpoint_wal_id > services::wal::id_t{0} && ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wt, wtf] = actor_zeta::otterbrix::send(ctx->wal_address,
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
