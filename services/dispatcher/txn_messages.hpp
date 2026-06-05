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
    };

    // Result of txn_abort_drain_msg: txn_data snapshot + the pg_catalog appends
    // that need storage_revert_appends. Mirrors operator_abort_transaction_t's
    // drain (delete-tables and backfill markers are discarded on abort: rows
    // written under an uncommitted txn_id stay invisible and VACUUM reclaims
    // them). The handler calls txn_manager.abort() after draining.
    struct txn_abort_drain_t {
        components::table::transaction_data txn{0, 0};
        std::vector<components::pg_catalog_append_range_t> swap_appends{};
    };

    // Payload of txn_accumulate_msg: every range an executor statement parks on
    // the session's transaction_t. ONE message serves both producers:
    //   explicit-DML statements — all five fields populated as needed;
    //   DDL statements          — base_* empty, pg_catalog_* / backfills carry
    //                             the catalog swap-info (the former dispatcher
    //                             merge block).
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

        bool empty() const noexcept {
            return base_appends.empty() && base_deletes.empty() && pg_catalog_appends.empty() &&
                   pg_catalog_delete_tables.empty() && backfills.empty();
        }
    };

} // namespace services::dispatcher
