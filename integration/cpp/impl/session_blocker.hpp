#pragma once

#include <cassert>
#include <components/session/session.hpp>
#include <memory_resource>
#include <shared_mutex>
#include <unordered_map>

namespace otterbrix::impl {

    class session_block_t : private std::pmr::unordered_map<components::session::session_id_t, std::pair<bool, void*>> {
    public:
        explicit session_block_t(std::pmr::memory_resource* resource);

        bool empty() noexcept;
        size_t size() noexcept;
        void clear() noexcept;
        // return false if there is a hash conflict
        bool set_value(const components::session::session_id_t& session, std::pair<bool, void*> value);
        // when there is no intention of reading value
        void set_value_flag(const components::session::session_id_t& session, bool value);
        // sets pair.first to true, and writes value to par.second address
        template<typename T>
        void set_value(const components::session::session_id_t& session, T value);
        void remove_session(const components::session::session_id_t& session);
        std::pair<bool, void*> value(const components::session::session_id_t& session);

    private:
        std::shared_mutex mutex_;
    };

    template<typename T>
    void session_block_t::set_value(const components::session::session_id_t& session, T value) {
        std::lock_guard lock(mutex_);
        auto it = find(session);
        // this should be called when we already registered a session
        assert(it != end() && "session_block_t: session is not registered when value is being set");
        assert(!it->second.first && "session_block_t: value was already set");
        *reinterpret_cast<T*>(it->second.second) = std::forward<T>(value);
        it->second.first = true;
    }
} // namespace otterbrix::impl