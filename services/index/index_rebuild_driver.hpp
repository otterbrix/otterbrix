#pragma once

#include <actor-zeta/actor/address.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/date/timezones.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>

namespace services::index {

    class committed_rows_snapshot_t;

    [[nodiscard]] committed_rows_snapshot_t committed_rows_snapshot() noexcept;

    // Must be all-committed, not transactional: repopulate_table clears every store before refill,
    // so a STATEMENT snapshot would silently drop rows a neighbour committed after its horizon
    // (test_checkpoint_rebuild_snapshot.cpp).
    class committed_rows_snapshot_t {
    public:
        [[nodiscard]] const components::table::transaction_data& txn() const noexcept { return txn_; }

    private:
        explicit committed_rows_snapshot_t(components::table::transaction_data txn) noexcept
            : txn_(std::move(txn)) {}
        friend committed_rows_snapshot_t committed_rows_snapshot() noexcept;

        components::table::transaction_data txn_;
    };

    // Must run after compaction (which renumbers rows an index still keys by the old id) and before
    // WAL truncation (test_checkpoint_rebuild_before_truncate), so a refused rebuild still stops the
    // round before the journal it would need is destroyed.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    repopulate_indexes_after_compaction(std::pmr::memory_resource* resource,
                                        actor_zeta::actor::address_t disk_address,
                                        actor_zeta::actor::address_t index_address,
                                        components::session::session_id_t session,
                                        committed_rows_snapshot_t snapshot,
                                        core::date::timezone_offset_t session_tz);

} // namespace services::index
