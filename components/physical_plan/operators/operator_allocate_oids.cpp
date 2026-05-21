#include "operator_allocate_oids.hpp"

#include <components/context/context.hpp>
#include <components/logical_plan/node_allocate_oids.hpp>
#include <services/disk/manager_disk.hpp>

#include <utility>

namespace components::operators {

    operator_allocate_oids_t::operator_allocate_oids_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       std::size_t count,
                                                       components::logical_plan::node_allocate_oids_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::allocate_oids)
        , count_(count)
        , target_node_(target_node) {}

    void operator_allocate_oids_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_allocate_oids_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (target_node_ == nullptr || count_ == 0 || ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }
        auto [_a, af] =
            actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::allocate_oids_batch, count_);
        auto batch = co_await std::move(af);
        target_node_->set_oids(std::move(batch));
        mark_executed();
        co_return;
    }

} // namespace components::operators