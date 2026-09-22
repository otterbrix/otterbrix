#include "transaction.hpp"

namespace components::table {

    transaction_t::transaction_t(uint64_t transaction_id,
                                 uint64_t start_time,
                                 session::session_id_t session,
                                 transaction_scope_t scope,
                                 std::pmr::memory_resource* resource)
        : session_(session)
        , transaction_id_(transaction_id)
        , start_time_(start_time)
        , scope_(scope)
        , in_flight_snapshot_(resource)
        , pending_base_appends_(resource)
        , pending_base_deletes_(resource) {}

    void transaction_t::set_commit_id(uint64_t id) { commit_id_ = id; }

    void transaction_t::mark_committed() { state_ = transaction_state_t::committed; }

    void transaction_t::mark_aborted() { state_ = transaction_state_t::aborted; }

    void transaction_t::add_append(int64_t row_start, uint64_t count) { appends_.push_back({row_start, count}); }

} // namespace components::table
