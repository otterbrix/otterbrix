#pragma once

#include <components/catalog/fk_info.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // Enforces one referencing FK constraint after a DELETE.
    // Scans the child table for rows referencing the deleted parent rows,
    // then applies the configured ON DELETE action:
    //   'a'/'r' NO ACTION / RESTRICT  — error if any child rows reference deleted rows
    //   'c'     CASCADE               — delete the referencing child rows
    //   'n'/'d' SET NULL / SET DEFAULT — update referencing rows in-place
    class operator_fk_cascade_t final : public read_write_operator_t {
    public:
        operator_fk_cascade_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk);

        // STREAMING CONSTRAINT SINK. fk_cascade is the PARENT of a DELETE sink in
        // the plan chain (fk_cascade -> delete -> scan). Marking it a sink with
        // needs_async_finalize lets the chain stream: the executor pumps the scan
        // into delete's push(), then drives await_async_and_resume BOTTOM-UP — the
        // delete commits first (snapshotting the matched OLD rows into
        // constraint_input_), then this fk_cascade reads them to find / mutate the
        // referencing child rows. push()/finalize() are no-ops (no streaming input of
        // its own).
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No streaming input of its own: the child DELETE sink drains the pumped
        // stream, so push() is never reached with rows. Explicit no-ops.
        [[nodiscard]] core::error_t
        push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::fk_info_t fk_;
    };

} // namespace components::operators