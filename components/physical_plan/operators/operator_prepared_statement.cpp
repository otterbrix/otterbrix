#include "operator_prepared_statement.hpp"

#include <components/context/context.hpp>

namespace components::operators {

    operator_prepared_statement_t::operator_prepared_statement_t(std::pmr::memory_resource* resource,
                                                                 log_t log,
                                                                 logical_plan::prepared_change_t change,
                                                                 bool exists)
        : read_write_operator_t(resource, std::move(log), operator_type::prepared_statement)
        , change_(std::move(change))
        , exists_(exists) {}

    actor_zeta::unique_future<void> operator_prepared_statement_t::await_async_and_resume(pipeline::context_t* ctx) {
        using kind_t = logical_plan::prepared_change_t::kind_t;
        if (change_.kind == kind_t::store && exists_) {
            set_error(core::error_t{
                core::error_code_t::duplicate_prepared_statement,
                std::pmr::string{"prepared statement \"" + change_.name + "\" already exists", this->resource()}});
            mark_failed();
            co_return;
        }
        if (change_.kind == kind_t::drop && !exists_) {
            set_error(core::error_t{
                core::error_code_t::undefined_prepared_statement,
                std::pmr::string{"prepared statement \"" + change_.name + "\" does not exist", this->resource()}});
            mark_failed();
            co_return;
        }
        ctx->prepared_change = std::move(change_);
        mark_executed();
    }

} // namespace components::operators
