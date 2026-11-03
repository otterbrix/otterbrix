#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // FETCH / MOVE: the source of the statement's rows is a portal of the same executor, which it runs as far as the
    // count asks (ctx->runner); MOVE answers no rows and reports how many it passed as written().
    class operator_portal_fetch_t final : public read_only_operator_t {
    public:
        operator_portal_fetch_t(std::pmr::memory_resource* resource,
                                log_t log,
                                uint64_t portal_id,
                                std::string name,
                                uint64_t count,
                                bool is_move);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }

        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t* ctx) override;

    private:
        uint64_t portal_id_;
        std::string name_;
        uint64_t count_;
        bool is_move_;
        bool fetched_{false};
        chunks_vector_t rows_;
        std::size_t next_{0};
    };

    // CLOSE: the portal's run ends (its sources' cursors close, the storages it read join the transaction's end).
    class operator_portal_close_t final : public read_only_operator_t {
    public:
        operator_portal_close_t(std::pmr::memory_resource* resource, log_t log, uint64_t portal_id, std::string name);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        uint64_t portal_id_;
        std::string name_;
    };

} // namespace components::operators
