#pragma once

namespace services::disk {
    class agent_disk_t;
} // namespace services::disk

namespace components::table {

    // Passkey of the NON-transactional in-place update (data_table_t::update and the txn-less
    // storage_t::update): the overlay it writes publishes immediately — no version chain, no
    // undo, no conflict detection (test_nontransactional_update_contract.cpp pins two writers
    // over one row both succeeding, last writer wins, no diagnostic). The only legs entitled to
    // that are WAL replay (pre-scheduler) and the pg_attribute commit-id stamp (below the
    // durable commit marker), and both flow through agent_disk_t::direct_update_sync — so
    // agent_disk_t is the whole friend list. A third caller does not compile without editing it.
    class nontransactional_update_access_t {
#ifdef DEV_MODE
    public:
        // Test mint. Compiled out of production builds, where the friend list is the gate.
        static nontransactional_update_access_t for_test() noexcept { return {}; }
#endif
    private:
        nontransactional_update_access_t() noexcept = default;
        friend class ::services::disk::agent_disk_t;
    };

} // namespace components::table
