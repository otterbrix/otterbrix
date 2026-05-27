#pragma once

#include <cstdint>
#include <ctime>
#include <string>

namespace components::session {

    class session_id_t final {
    public:
        session_id_t();
        std::size_t hash() const;
        bool operator==(const session_id_t& other) const;
        bool operator==(const session_id_t& other);
        bool operator<(const session_id_t& other) const;
        auto data() const -> std::uint64_t;

        /**
         * @brief Generate new unique session id
         *
         * @return session_id_t new unique id
         */
        static session_id_t generate_uid();

    private:
        std::uint64_t data_;
        std::uint64_t counter_;
    };

} // namespace components::session

namespace std {
    template<>
    struct hash<components::session::session_id_t> {
        std::size_t operator()(components::session::session_id_t const& s) const noexcept { return s.hash(); }
    };
} // namespace std
