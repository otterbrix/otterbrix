#pragma once

#include <cstdint>

namespace core::maintenance {

    // RULE 3 OF THE PROJECT, AND ITS ONLY SANCTIONED EXCEPTIONS.
    //
    // Everything reaches storage through ONE pipeline:
    //   logical plan -> planner -> optimizer -> physical plan generator -> executor -> disk or index.
    // A path that mutates disk or index state without a statement behind it is a BYPASS. Three exist
    // in this tree, each enumerated below with the reason it is there. Two are legal for a reason
    // that can be checked: they run where the pipeline does not exist yet (the pre-scheduler
    // bootstrap window) or where there is nothing for a plan to describe (a service message that
    // carries no rows). The third — the WAL auto-checkpoint — is declared because it EXISTS, not
    // because it was judged unavoidable. Every other write MUST be an operator.
    //
    // WHY THIS IS CODE AND NOT A COMMENT: a comment does not survive a rename, does not answer a
    // grep, and does not stand between the bypass and its next caller. The callable that performs a
    // bypass is passed through `pipeline_bypass<site>` below, so a reviewer greps the marker and
    // gets the complete list, and a FOURTH one cannot be introduced quietly — it has to add an
    // enumerator to `bypass_site` here. THE MARKER IS NOT A LICENCE: it says "this bypass is known
    // and named", not "this code is safe to call", and for (3) not even "this was judged
    // unavoidable". Each site below documents what breaks if it is called from somewhere new.
    //
    // HOW TO CHECK THE COUNT. Grep the tree for the fully-qualified form of the function below
    // (namespace, then `pipeline_bypass`): it must return exactly as many hits as there are
    // enumerators here — today three. This header never spells that qualified form, so the grep
    // answers with call sites only, and a mismatch means a bypass was added, moved or lost its
    // declaration.

    // One enumerator per bypass that exists in the tree. Adding one is the review gate; this list IS
    // the permission set, and it is deliberately short.
    enum class bypass_site : std::uint8_t
    {
        // (1) WAL replay in base_otterbrix_t's constructor: synthesises the .otbx of a table whose
        //     file was lost, then applies the journalled chunks straight to storage. Runs in the
        //     single-threaded window BEFORE any scheduler starts — there is no planner, no
        //     executor and no transaction to route through yet, and rule 11 names base_spaces as
        //     the one place allowed direct synchronous calls.
        wal_replay_storage_synthesis,

        // (2) The horizon GC sweep broadcast in manager_dispatcher_t: tells the disk and index
        //     managers that the oldest live snapshot has moved, so artefacts of an ALREADY
        //     COMMITTED drop can be reclaimed. It carries no rows and describes no query — the
        //     DROP itself went through the pipeline; this is only the deferred reclaim of what
        //     that committed statement left behind.
        horizon_gc_sweep,

        // (3) The WAL auto-checkpoint, fired from manager_wal_replicate_t::commit_txn once WAL
        //     growth since the last checkpoint trips the configured threshold. THIS ONE IS NOT LIKE
        //     THE OTHER TWO: it runs with the engine fully up, so the pipeline it goes around is
        //     RIGHT THERE, and a statement-shaped twin already does the same work through it —
        //     operator_checkpoint, which ~base_otterbrix_t reaches by building a node_checkpoint
        //     plan. The work is a full checkpoint: flush every index, compact and rewrite the .otbx
        //     files (renumbering physical row ids), clear and rebuild every index of every compacted
        //     table, then unlink WAL segments. It is declared here because a bypass that no grep can
        //     find is the thing this header exists to prevent — NOT because its legality was
        //     settled. The open question, for whoever settles it: route the trigger through a
        //     node_checkpoint plan the way shutdown does, or keep the self-send and accept a
        //     maintenance round that no statement can be blamed for (nothing above that frame is a
        //     statement that could carry an error, so a failed index rebuild is logged and dropped).
        wal_auto_checkpoint,
    };

    // Declares `work` to be the callable of the named bypass and hands it back UNCHANGED:
    // zero runtime cost, zero behavioural difference. Its only job is to put the site's name into
    // the code that performs the bypass.
    //
    // [[nodiscard]]: a declared bypass that is neither bound nor invoked is a marker over dead
    // code, which is exactly the state this is meant to make impossible.
    template<bypass_site site, class callable_t>
    [[nodiscard]] callable_t pipeline_bypass(callable_t work) {
        return work;
    }

} // namespace core::maintenance
