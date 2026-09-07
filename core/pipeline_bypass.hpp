#pragma once

#include <cstdint>

namespace core::maintenance {

    // Every disk/index mutation goes through the pipeline; a bypass is enumerated here and marked via pipeline_bypass,
    // and there are exactly three of them today.
    enum class bypass_site : std::uint8_t
    {
        // WAL replay in base_otterbrix_t's constructor runs before any scheduler exists, so there is no pipeline yet to
        // route through.
        wal_replay_storage_synthesis,

        // Horizon GC sweep in manager_dispatcher_t reclaims an already-committed DROP's deferred artefacts; the DROP
        // itself already went through the pipeline.
        horizon_gc_sweep,

        // WAL auto-checkpoint (manager_wal_replicate_t::commit_txn) is the only site where the pipeline IS available;
        // legality is unsettled between routing through node_checkpoint like shutdown, or keeping the self-send.
        wal_auto_checkpoint,
    };

    // Identity wrapper: zero cost, only tags `work` with its bypass_site for the grep above.
    template<bypass_site site, class callable_t>
    [[nodiscard]] callable_t pipeline_bypass(callable_t work) {
        return work;
    }

} // namespace core::maintenance
