#include "session.hpp"
#include <atomic>
#include <memory>

namespace components::session {

    namespace {
        std::atomic_int64_t uniq_counter = 0;
    }
    auto session_id_t::data() const -> std::uint64_t { return data_ * counter_; }

    bool session_id_t::operator==(const session_id_t& other) const {
        return data_ == other.data_ && counter_ == other.counter_;
    }

    bool session_id_t::operator==(const session_id_t& other) {
        return data_ == other.data_ && counter_ == other.counter_;
    }

    bool session_id_t::operator<(const session_id_t& other) const {
        return data_ < other.data_ || (data_ == other.data_ && counter_ < other.counter_);
    }

    std::size_t session_id_t::hash() const { return std::hash<uint64_t>{}(data_) ^ std::hash<uint64_t>{}(counter_); }
    session_id_t::session_id_t()
        : data_(0)
        , counter_(0) {
        uniq_counter.fetch_add(1);
        counter_ = static_cast<uint64_t>(uniq_counter);
        data_ = static_cast<uint64_t>(std::time(nullptr)); //todo recanting
    }

    session_id_t session_id_t::generate_uid() { return session_id_t{}; }
} // namespace components::session
