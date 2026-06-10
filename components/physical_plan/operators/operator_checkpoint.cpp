#include "operator_checkpoint.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_checkpoint_t::operator_checkpoint_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, std::move(log), operator_type::checkpoint) {}

    void operator_checkpoint_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_checkpoint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Flush dirty index btrees so a post-recovery rebuild starts from a
        // consistent on-disk index state.
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_fi, fif] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::flush_all_indexes,
                                               ctx->session);
            co_await std::move(fif);
        }

        // snapshot the current WAL id BEFORE the checkpoint so the per-table
        // W-TORN (prev/current) snapshot pins a known recovery boundary.
        services::wal::id_t wal_max_id{0};
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wi, wif] = actor_zeta::send(ctx->wal_address,
                                               &services::wal::manager_wal_replicate_t::current_wal_id,
                                               ctx->session);
            wal_max_id = co_await std::move(wif);
        }

        // checkpoint_all. No-op when disk is off.
        services::wal::id_t checkpoint_wal_id{0};
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_cp, cpf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::checkpoint_all,
                                               ctx->session,
                                               wal_max_id);
            checkpoint_wal_id = co_await std::move(cpf);
        }

        if (checkpoint_wal_id > services::wal::id_t{0} && ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto [_wt, wtf] = actor_zeta::send(ctx->wal_address,
                                               &services::wal::manager_wal_replicate_t::truncate_before,
                                               ctx->session,
                                               checkpoint_wal_id);
            co_await std::move(wtf);
        }

        mark_executed();
    }

} // namespace components::operators
