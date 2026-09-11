#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // BEGIN / START TRANSACTION operator. No disk/WAL I/O: its only side effect
    // is keeping the session's transaction open until COMMIT/ROLLBACK. The work
    // is delegated to the dispatcher via txn_mark_explicit_msg (a stray BEGIN
    // inside an open txn reuses it — Postgres semantics). The mailbox round-trip
    // means this operator joins the pipeline's await chain like commit/abort.
    class operator_begin_transaction_t final : public read_write_operator_t {
    public:
        operator_begin_transaction_t(std::pmr::memory_resource* resource, log_t log);

        // Sourceless SINK leaf (no data pipeline, no children): the executor
        // admits it as a streaming sink-root and drives await_async_and_resume via
        // the bottom-up needs_async_finalize pass. push()/finalize() inherit the
        // no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;
    };

} // namespace components::operators
