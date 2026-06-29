#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/predicates/predicate.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // Checks NOT NULL constraints and CHECK expressions over the incoming chunk.
    // CHECK expressions are compiled to predicate_ptr once in the constructor;
    // evaluated per-row without re-parsing.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::vector<std::string> not_null_columns,
                                    std::vector<std::pair<std::string, std::string>> check_exprs = {},
                                    std::vector<std::pair<std::string, uint64_t>> array_size_reqs = {});

        // STREAMING CONSTRAINT SINK. check_constraint is the PARENT of a DML sink in
        // the plan chain (check_constraint -> insert/update -> scan). Its validation
        // is SYNCHRONOUS (no cross-actor I/O), but it must run AFTER the DML's await —
        // the DML snapshots the just-written rows into constraint_input() there, and
        // on the streaming path the scan SOURCE's output_ is empty. needs_async_finalize
        // routes the validation into the executor's bottom-up async-finalize drive (the
        // FLUSH phase runs BEFORE the DML await, which would be too early). push()/
        // finalize() are no-ops; the validation runs in await_async_and_resume
        // via the validate_() core, completing synchronously (co_return).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No streaming input of its own: the child DML sink drains the pumped
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

        // Synchronous validation core. Resolves the rows to validate (the DML's
        // constraint_input() snapshot, or the fallbacks) and runs NOT NULL +
        // fixed-ARRAY length + CHECK-expression checks over them; on the first
        // violation it sets the error and returns. Called by await_async_and_resume.
        void validate_();

        std::vector<std::string> not_null_columns_;
        std::vector<std::pair<std::string, predicates::predicate_ptr>> check_predicates_; // (name, compiled)
        // Fixed-ARRAY columns (NOT NULL, no DEFAULT) and their declared sizes: a value
        // shorter than the size cannot be padded and is rejected with an error.
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;
    };

} // namespace components::operators