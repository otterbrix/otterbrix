#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

#ifdef DEV_MODE
    // Test-observable counter of the scan_by_keys sends the EXISTING-ROW layer
    // issues. Each send is one FULL pass over the target table, so a statement that
    // cannot change any unique key must leave this counter untouched.
    uint64_t unique_constraint_scan_sends() noexcept;
#endif

    // Enforces UNIQUE / PRIMARY KEY constraints on an INSERT or UPDATE chunk.
    //
    // One instance carries the column groups of every UNIQUE/PK constraint on the
    // target table (unique_groups_[g] = the ordered column list of constraint g).
    // Shaped EXACTLY like operator_fk_check_t: a sourceless streaming-constraint
    // SINK whose push()/finalize() are no-ops and whose validation runs in the
    // executor's bottom-up async-finalize drive — it reads the child DML's
    // constraint_input() snapshot of the just-written rows (the DML committed
    // first, so those rows are already txn-visible in the table too).
    //
    // Duplicate detection has two independent layers, per constraint group:
    //   (1) WITHIN-BATCH: two rows in the SAME write-set sharing a key. Detected by
    //       a typed hash + verify (R1: no logical_value_t round-trip; the verify
    //       mirrors operator_group.cpp's vector::cells_equal — NULL-aware).
    //   (2) EXISTING-ROW: an already-committed table row with the same key that is
    //       NOT the row being written. The DML ran first (bottom-up), so its rows
    //       are visible to scan_by_keys; a key whose scan returns MORE than the one
    //       just-written row therefore collides with a pre-existing distinct row.
    //
    // NULL handling: a key with any NULL column is SKIPPED (SQL UNIQUE treats NULLs
    // as distinct; PRIMARY KEY columns are NOT NULL, enforced upstream by
    // operator_check_constraint). Every table column is IN the rows this reads — an
    // INSERT that omitted one had it expanded to its DEFAULT (or to NULL) before the
    // append — so the key is extracted from what was STORED. It used to be assembled
    // from the plan's own copy of the default, which is how the uniqueness verdict came
    // to be about a value the write path had not written. On the first duplicate it
    // sets a core::error_t — no silent dedup, no throw across the mailbox (R2/R9).
    class operator_unique_constraint_t final : public read_write_operator_t {
    public:
        operator_unique_constraint_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     catalog::oid_t table_oid,
                                     std::vector<std::vector<std::string>> unique_groups);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No streaming input of its own: the child DML sink drains the pumped
        // stream, so push() is never reached with rows. Explicit no-ops keep the
        // operator off the base "not a pipeline operator" error path.
        [[nodiscard]] core::error_t push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t table_oid_;
        std::vector<std::vector<std::string>> unique_groups_;
    };

} // namespace components::operators
