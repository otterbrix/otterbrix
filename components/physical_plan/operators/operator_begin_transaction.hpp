#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // BEGIN / START TRANSACTION operator. Synchronous, no disk/WAL I/O: its only
    // side effect is marking the session's transaction explicit (idempotent
    // begin_transaction reuses an existing active txn). The executor commit phase
    // then reads is_explicit() to choose per-statement publish (implicit) vs
    // accumulation for COMMIT-time drain (see node_begin_transaction.hpp).
    class operator_begin_transaction_t final : public read_write_operator_t {
    public:
        operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
