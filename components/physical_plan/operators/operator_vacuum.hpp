#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // VACUUM — global no-arg operation.
    //
    // Steps (in await_async_and_resume):
    //   1. manager_disk_t::vacuum_all — for every user storage: cleanup_versions
    //      (drop tuple versions older than lowest_active_start_time) + compact
    //      (consolidate live segments). Implemented globally on the disk side;
    //      called once.
    //   2. manager_index_t::cleanup_all_versions — drop index entries whose
    //      tuple versions were just reclaimed. Called once if index_address
    //      is set.
    //   3. Iterate pg_class via storage_scan and repopulate indexes for every
    //      user relation (relkind 'r' = regular, 'g' = computing). Compact in
    //      step 1 changes row positions, so we must, per oid:
    //        a. storage_total_rows(oid)            — post-compact row count
    //        b. storage_scan_segment(oid, 0, total) — read the consolidated rows
    //        c. repopulate_table(oid, chunk, total) — clear on-disk index backing
    //           + in-memory engine, then re-insert at post-compact ids with
    //           txn_id=0 (committed-for-everyone).
    //      pg_class is the source of truth for the set of user relations.
    //      The txn_id=0 re-insert needs no index-commit; entries inserted under a
    //      real txn id would stay PENDING-invisible (VACUUM never index-commits).
    //
    // Reads pipeline_context.lowest_active_start_time (set by executor from
    // txn_manager_t) — same value the legacy inline path used.
    class operator_vacuum_t final : public read_write_operator_t {
    public:
        operator_vacuum_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
