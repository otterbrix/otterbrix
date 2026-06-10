#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/table/transaction.hpp>
#include <core/date/date_types.hpp>

#include <set>
#include <vector>

namespace services::dispatcher {

    // Value-only payloads crossing the executor <-> dispatcher mailbox for the
    // txn_*_msg handler family. The dispatcher is the SOLE owner of
    // transaction_manager_t / transaction_t; executors and the txn operators
    // never dereference them — every txn-state read or mutation rides one of
    // these structs through a mailbox message.
    //
    // Plain std containers on purpose (NOT std::pmr): same convention as
    // transaction_data (row_version_manager.hpp) — a pmr container anchored to
    // an actor-local arena must not cross the actor boundary.

    // Session context bundle returned by txn_begin_session_msg. One round-trip
    // at plan start gives the executor everything it needs:
    //   txn      — the (idempotently begun) active txn snapshot for the session;
    //              shared MVCC scope for resolve + the operator pipeline.
    //   session_tz — dispatcher-owned session timezone (feeds context_storage_t).
    //   is_explicit — whether a prior SQL BEGIN marked this txn explicit; the
    //              executor's DML tail uses it to pick accumulate-vs-publish.
    //   lowest_active_start_time — VACUUM/MVCC GC gate value for pipeline ctx.
    struct txn_session_context_t {
        components::table::transaction_data txn{0, 0};
        core::date::timezone_offset_t session_tz{};
        bool is_explicit{false};
        uint64_t lowest_active_start_time{0};
    };

    // Result of txn_commit_drain_msg: the dispatcher snapshots txn_data, drains
    // every range parked on transaction_t, allocates the commit_id via
    // txn_manager.commit() — and returns it ALL by value, because after
    // commit() purges the active map the caller can never read txn_t again.
    //
    // INVARIANT: the drain handler must NOT call txn_manager.publish().
    // publish() is the ProcArray barrier and runs ONLY via txn_publish_msg,
    // sent by the commit operator AFTER storage_publish_* / WAL completed —
    // otherwise concurrent snapshots observe not-yet-flipped pg_catalog rows.
    //
    // Field shapes mirror operator_commit_transaction_t's post-drain locals
    // (operator_commit_transaction.cpp): base ranges arrive already remapped
    // to pg_catalog_append_range_t / a table-oid set, so the operator's
    // storage_publish_* block consumes them unchanged.
    struct txn_commit_drain_t {
        uint64_t commit_id{0};
        components::table::transaction_data txn{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends{};
        std::set<components::catalog::oid_t> swap_deletes{};
        std::vector<components::pg_attribute_commit_id_backfill_t> swap_backfills{};
        std::vector<components::pg_catalog_append_range_t> base_appends{};
        std::set<components::catalog::oid_t> base_delete_tables{};
        // Storage oids retired by DROP in this txn, drained out so the commit
        // operator's GC-remap can stamp them with commit_id. Non-empty here is
        // the trigger for that remap block (NOT the is_ddl_commit_ flag).
        std::vector<components::catalog::oid_t> dropped_storage_oids{};
        // Storage oids / indexes a CREATE in this txn brought into being, drained
        // out so the commit operator can publish them. Symmetric with
        // dropped_storage_oids above (the COMMIT counterpart of the DROP side).
        std::vector<components::catalog::oid_t> created_storage_oids{};
        std::vector<components::table::created_index_t> created_indexes{};
    };

    // Result of txn_abort_drain_msg: txn_data snapshot + the pg_catalog appends
    // that need storage_revert_appends, plus the UNIQUE base-table oids the txn
    // accumulated appends for so the abort operator can fan out
    // manager_index_t::revert_insert per oid (parity with the failed-DML path in
    // executor.cpp, which reverts PENDING in-memory index entries per touched
    // base table). Mirrors operator_abort_transaction_t's drain (backfill markers
    // are discarded on abort — their targets ride in swap_appends; the pg_catalog
    // delete-tables are KEPT so the abort operator can un-stamp the catalog heap
    // delete marks a DROP left behind). The handler calls txn_manager.abort()
    // after draining.
    //
    // base_append_tables collapses the drained base-append ranges to a table-oid
    // set (loss-free: every range carries the same explicit txn id). pg_catalog
    // tables are deliberately absent — they have no index engines, so a
    // revert_insert on a pg_catalog oid is a no-op by manager_index_t's engines_
    // lookup; reverting only base oids is both correct and minimal.
    //
    // base_delete_tables mirrors base_append_tables for the DELETE side: the
    // UNIQUE base-table oids the txn accumulated DELETE ranges for. The abort
    // operator fans out manager_index_t::revert_delete per oid to clear the
    // PENDING in-memory index DELETE markers an uncommitted DELETE staged
    // (parity with executor.cpp's failed-DML revert, which an explicit SQL
    // ROLLBACK must match). The delete RANGES themselves are still discarded on
    // abort — uncommitted tombstones (delete_id == txn_id) are invisible to every
    // reader and VACUUM reclaims them; only the index markers need an explicit
    // revert because they are not gated by the MVCC visibility filter.
    //
    // pg_catalog_delete_tables surfaces the CATALOG tables a DROP inside this txn
    // stamped delete marks on (pg_class, pg_attribute, pg_depend, ...). Unlike
    // base-table tombstones, these on-heap marks must be UN-STAMPED on abort:
    // the mark carries the aborted txn_id, so it is invisible to readers, BUT it
    // persists and blocks a future re-DELETE of the same catalog row (delete_rows
    // skips an already-marked slot). The abort operator routes these through the
    // SAME storage_revert_deletes as base_delete_tables. Drained (not discarded)
    // precisely so the heap mark can be reverted — mirrors the base side.
    struct txn_abort_drain_t {
        components::table::transaction_data txn{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends{};
        std::set<components::catalog::oid_t> base_append_tables{};
        std::set<components::catalog::oid_t> base_delete_tables{};
        std::set<components::catalog::oid_t> pg_catalog_delete_tables{};
        // Storage oids retired by DROP in this (now aborting) txn. Informational
        // today: the abort operator does not yet un-stamp them, so they ride out
        // for symmetry with the commit drain.
        std::vector<components::catalog::oid_t> dropped_storage_oids{};
        // Storage oids / indexes a CREATE in this (now aborting) txn brought into
        // being. Drained so the abort operator can drop the still-uncommitted
        // storage / index — symmetric with the commit drain's created_* fields.
        std::vector<components::catalog::oid_t> created_storage_oids{};
        std::vector<components::table::created_index_t> created_indexes{};
    };

    // Payload of txn_accumulate_msg: every range an executor statement parks on
    // the session's transaction_t. ONE message serves both producers:
    //   explicit-DML statements — all five fields populated as needed;
    //   DDL statements          — base_* empty, pg_catalog_* / backfills carry
    //                             the catalog swap-info.
    // The handler replays accumulate_base_append / accumulate_base_delete /
    // accumulate_pg_catalog_pending / accumulate_pg_attribute_commit_id_backfills
    // on the dispatcher loop thread — the single-owner-thread invariant of
    // transaction_t (transaction.hpp) is enforced by the mailbox.
    // Implicit (auto-commit) DML NEVER sends this message: it publishes its
    // ranges inline and per-range (including index commit mirrors).
    struct txn_accumulate_payload_t {
        std::vector<components::table::dml_append_range_t> base_appends{};
        std::vector<components::table::dml_delete_range_t> base_deletes{};
        std::vector<components::pg_catalog_append_range_t> pg_catalog_appends{};
        std::set<components::catalog::oid_t> pg_catalog_delete_tables{};
        std::vector<components::pg_attribute_commit_id_backfill_t> backfills{};
        // Storage oids a DROP TABLE / DROP INDEX statement in this txn retired;
        // the handler forwards them to transaction_t::accumulate_dropped_storage
        // so COMMIT can stamp them with the commit_id for the GC-remap.
        std::vector<components::catalog::oid_t> dropped_storage_oids{};
        // Storage oids / indexes a CREATE TABLE / CREATE INDEX statement in this
        // txn brought into being; the handler forwards them to
        // transaction_t::accumulate_created_storage / accumulate_created_index so
        // COMMIT publishes them and ABORT drops the still-uncommitted artifacts.
        std::vector<components::catalog::oid_t> created_storage_oids{};
        std::vector<components::table::created_index_t> created_indexes{};

        bool empty() const noexcept {
            return base_appends.empty() && base_deletes.empty() && pg_catalog_appends.empty() &&
                   pg_catalog_delete_tables.empty() && backfills.empty() && dropped_storage_oids.empty() &&
                   created_storage_oids.empty() && created_indexes.empty();
        }
    };

} // namespace services::dispatcher
