#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // BEGIN / START TRANSACTION operator. No disk/WAL I/O: its only side effect
    // is marking the session's transaction explicit. The work is delegated to
    // the dispatcher via txn_mark_explicit_msg (the dispatcher does an idempotent
    // begin_transaction THEN mark_explicit, so a stray BEGIN inside an open txn
    // reuses it — Postgres semantics). The mailbox round-trip means this operator
    // joins the pipeline's await chain like commit/abort. The executor commit
    // phase then reads is_explicit() to choose per-statement publish (implicit)
    // vs accumulation for COMMIT-time drain (see node_begin_transaction.hpp).
    class operator_begin_transaction_t final : public read_write_operator_t {
    public:
        operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
