#pragma once
#include <atomic>
#include <cstdint>
#include <string>

namespace services::wal {

    using id_t = std::uint64_t;
    using atomic_id_t = std::atomic<id_t>;

    inline void next_id(atomic_id_t& id, id_t stride = 1) { id += stride; }

    inline void next_id(id_t& id, id_t stride = 1) { id += stride; }

    inline id_t id_from_string(const std::string& value) {
        if (value.empty()) {
            return 0;
        }
        return std::strtoull(value.data(), nullptr, 10);
    }

} //namespace services::wal
