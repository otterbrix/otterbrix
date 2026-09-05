#pragma once

#include <cstdint>

namespace core::maintenance {

    // Every disk/index mutation goes through one pipeline (plan -> planner -> optimizer
    // -> physical plan -> executor). A path that mutates without a statement behind it is a
    // bypass. Enumerated below (not just commented) so grep finds the complete list and a new
    // one can't be added silently; the marker means "known and named", not "safe to call".
    //
    // Sanity check: grep for qualified `pipeline_bypass` call sites; count must match the
    // enumerators here (today three).
    enum class bypass_site : std::uint8_t
    {
        // (1) WAL replay in base_otterbrix_t's constructor: synthesises a lost table's .otbx from
        //     the journal. Runs before any scheduler exists, so there is no pipeline to route
        //     through yet (base_spaces is the one place allowed direct sync calls).
        wal_replay_storage_synthesis,

        // (2) Horizon GC sweep broadcast in manager_dispatcher_t: reclaims artefacts of an
        //     already-committed DROP once the oldest live snapshot moves. Carries no rows/query
        //     -- the DROP itself went through the pipeline; this is only the deferred reclaim.
        horizon_gc_sweep,

        // (3) WAL auto-checkpoint from manager_wal_replicate_t::commit_txn, fired when WAL
        //     growth trips a threshold. Unlike (1)/(2) the pipeline IS available here --
        //     operator_checkpoint does the same work via a node_checkpoint plan on shutdown.
        //     Legality unsettled; open question is whether to route through node_checkpoint
        //     like shutdown does, or keep the self-send (a failed index rebuild here is logged
        //     and dropped, since nothing above this frame can carry the error).
        wal_auto_checkpoint,
    };

    // Identity wrapper: zero cost, only tags `work` with its bypass_site for the grep above.
    // [[nodiscard]] so a declared-but-unused bypass can't become dead code silently.
    template<bypass_site site, class callable_t>
    [[nodiscard]] callable_t pipeline_bypass(callable_t work) {
        return work;
    }

} // namespace core::maintenance
