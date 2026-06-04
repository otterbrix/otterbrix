#include "transaction.hpp"

namespace components::table {

    transaction_t::transaction_t(uint64_t transaction_id,
                                 uint64_t start_time,
                                 session::session_id_t session,
                                 std::pmr::memory_resource* resource)
        : session_(session)
        , transaction_id_(transaction_id)
        , start_time_(start_time)
        , in_flight_snapshot_(resource)
        , pending_base_appends_(resource)
        , pending_base_deletes_(resource) {}

    void transaction_t::set_commit_id(uint64_t id) { commit_id_ = id; }

    void transaction_t::mark_committed() { committed_ = true; }

    void transaction_t::mark_aborted() { aborted_ = true; }

    void transaction_t::add_append(int64_t row_start, uint64_t count) { appends_.push_back({row_start, count}); }

} // namespace components::table
