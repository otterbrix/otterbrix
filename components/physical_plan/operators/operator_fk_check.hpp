#pragma once

#include <components/catalog/fk_info.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Enforces one outgoing FK constraint on an INSERT or UPDATE chunk.
    // Extracts FK child-col values per row into one key batch, calls
    // disk.scan_by_keys on the parent table, and errors on the first row
    // whose key has no matching parent row.
    class operator_fk_check_t final : public read_write_operator_t {
    public:
        operator_fk_check_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk);

        // STREAMING CONSTRAINT SINK. fk_check is the PARENT of a DML sink in the
        // plan chain (fk_check -> insert/update -> scan). Marking it a sink with
        // needs_async_finalize lets the whole chain stream: the executor pumps the
        // scan into the DML's push(), then drives await_async_and_resume BOTTOM-UP —
        // the DML commits first (snapshotting the written rows into
        // constraint_input_), then this fk_check validates them. push()/finalize()
        // are no-ops: this operator holds no streaming input of its own; it reads its
        // child DML's snapshot in the async step (the same rows the legacy materialize
        // path read from the DML output). on_execute_impl stays the materialized
        // entry point for any non-streaming caller — both share the SAME validation.
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No streaming input of its own: the child DML sink drains the pumped
        // stream, so push() is never reached with rows. Explicit no-ops keep the
        // operator off the base "not a pipeline operator" error path.
        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::fk_info_t fk_;
    };

} // namespace components::operators