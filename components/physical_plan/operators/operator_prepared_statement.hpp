#pragma once

#include <components/logical_plan/prepared_statement.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // PREPARE and DEALLOCATE, as SET is run: the change to the session's store goes back with the statement's result,
    // and the dispatcher applies it once the statement succeeded. `exists`: the session has a statement of the
    // change's name (PostgreSQL 18 StorePreparedStatement, DropPreparedStatement).
    class operator_prepared_statement_t final : public read_write_operator_t {
    public:
        operator_prepared_statement_t(std::pmr::memory_resource* resource,
                                      log_t log,
                                      logical_plan::prepared_change_t change,
                                      bool exists);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        logical_plan::prepared_change_t change_;
        bool exists_;
    };

} // namespace components::operators
