#pragma once

#include <components/context/context.hpp>
#include <components/log/log.hpp>
#include <components/table/transaction.hpp>
#include <services/dispatcher/txn_messages.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Undoes every write a transaction left behind. frame_resource only allocates the coroutine frame.
    actor_zeta::unique_future<void> revert_transaction(std::pmr::memory_resource* frame_resource,
                                                       pipeline::context_t* ctx,
                                                       log_t& log,
                                                       components::table::txn_abort_drain_t drain);

    // Undo commit that failed fo finish
    components::table::txn_abort_drain_t undo_of_commit(const services::dispatcher::txn_commit_drain_t& drain);

} // namespace components::operators
