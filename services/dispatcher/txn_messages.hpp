#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/context/pg_catalog_swap.hpp>
#include <components/table/transaction.hpp>
#include <core/date/date_types.hpp>
#include <core/result_wrapper.hpp>

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

    // What the dispatcher resolved for a statement before sending it to an executor:
    //   txn      — snapshot of the transaction the statement runs in; shared MVCC
    //              scope for resolve + the operator pipeline.
    //   settings — dispatcher-owned settings cache (feeds context_storage_t).
    //   lowest_active_start_time — VACUUM/MVCC GC gate value for pipeline ctx.
    struct txn_session_context_t {
        components::table::transaction_data txn{0, 0};
        components::catalog::session_catalog_t settings{};
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
