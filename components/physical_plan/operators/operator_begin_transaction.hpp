#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // BEGIN / START TRANSACTION operator.
    //
    // Synchronous: looks up or starts the session's transaction via
    // ctx->txn_manager->begin_transaction (idempotent — returns the existing
    // active txn if one exists), then calls transaction_t::mark_explicit().
    // No disk / WAL I/O — the explicit flag is the only side effect.
    //
    // Downstream effect: the executor commit phase consults
    // transaction_t::is_explicit() to decide between per-statement publish
    // (implicit auto-commit) and accumulation into pending_base_appends_ /
    // pending_base_deletes_ (explicit BEGIN..COMMIT, drained by
    // operator_commit_transaction_t).
    class operator_begin_transaction_t final : public read_write_operator_t {
    public:
        operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
