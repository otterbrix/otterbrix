#include "operator_portal.hpp"

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>

namespace components::operators {

    operator_portal_fetch_t::operator_portal_fetch_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     uint64_t portal_id,
                                                     std::string name,
                                                     uint64_t count,
                                                     bool is_move)
        : read_only_operator_t(resource, std::move(log), operator_type::portal_fetch)
        , portal_id_(portal_id)
        , name_(std::move(name))
        , count_(count)
        , is_move_(is_move)
        , rows_(resource) {}

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_portal_fetch_t::source_next(pipeline::context_t* ctx) {
        using result_t = core::result_wrapper_t<std::optional<vector::data_chunk_t>>;
        if (!fetched_) {
            fetched_ = true;
            auto fetched = co_await ctx->runner->fetch_portal(ctx, portal_id_, name_, count_, is_move_);
            if (fetched.has_error()) {
                co_return result_t{fetched.error()};
            }
            rows_ = std::move(fetched.value().chunks);
            written_ = fetched.value().rows;
        }
        if (next_ >= rows_.size()) {
            co_return result_t{std::optional<vector::data_chunk_t>{}};
        }
        co_return result_t{std::optional<vector::data_chunk_t>{std::move(rows_[next_++])}};
    }

    operator_portal_close_t::operator_portal_close_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     uint64_t portal_id,
                                                     std::string name)
        : read_only_operator_t(resource, std::move(log), operator_type::portal_close)
        , portal_id_(portal_id)
        , name_(std::move(name)) {}

    actor_zeta::unique_future<void> operator_portal_close_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (auto closed = co_await ctx->runner->close_portal(ctx, portal_id_, name_); closed.contains_error()) {
            set_error(closed);
            mark_failed();
            co_return;
        }
        mark_executed();
    }

} // namespace components::operators
