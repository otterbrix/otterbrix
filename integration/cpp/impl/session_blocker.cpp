#include "session_blocker.hpp"

namespace otterbrix::impl {

    using components::session::session_id_t;

    session_block_t::session_block_t(std::pmr::memory_resource* resource)
        : std::pmr::unordered_map<session_id_t, std::pair<bool, void*>>(resource) {}

    bool session_block_t::empty() noexcept {
        std::shared_lock lock(mutex_);
        return std::pmr::unordered_map<session_id_t, std::pair<bool, void*>>::empty();
    }

    size_t session_block_t::size() noexcept {
        std::shared_lock lock(mutex_);
        return std::pmr::unordered_map<session_id_t, std::pair<bool, void*>>::size();
    }

    void session_block_t::clear() noexcept {
        std::lock_guard lock(mutex_);
        return std::pmr::unordered_map<session_id_t, std::pair<bool, void*>>::clear();
    }

    bool session_block_t::set_value(const session_id_t& session, std::pair<bool, void*> value) {
        std::lock_guard lock(mutex_);
        // it is possible that there is someone trying to create new session with the same id
        //! if this is a problem, solution will be to generate a new session
        auto it = insert_or_assign(session, value);
        if (!value.first && !it.second) {
            // if value == true, it is a return call and should be possible
            // if value == false and there is already a session here, then it should be illegal
            return false;
        }
        return true;
    }

    void session_block_t::set_value_flag(const session_id_t& session, bool value) {
        std::lock_guard lock(mutex_);
        auto it = find(session);
        // this should be called when we already registered a session
        assert(it != end());
        it->second.first = value;
    }

    void session_block_t::remove_session(const session_id_t& session) {
        std::lock_guard lock(mutex_);
        erase(session);
    }

    std::pair<bool, void*> session_block_t::value(const session_id_t& session) {
        std::shared_lock lock(mutex_);
        return std::pmr::unordered_map<session_id_t, std::pair<bool, void*>>::at(session);
    }
} // namespace otterbrix::impl