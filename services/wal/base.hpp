#pragma once
#include <atomic>
#include <charconv>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <string>
#include <string_view>

#include <components/catalog/catalog_oids.hpp>

namespace services::wal {

    using id_t = std::uint64_t;
    using atomic_id_t = std::atomic<id_t>;
    using buffer_t = std::pmr::string;
    using size_tt = std::uint32_t;
    using crc32_t = std::uint32_t;

    // THE one classification of a directory name under the WAL root. A database
    // directory is named std::to_string(oid) AND NOTHING ELSE — the worker's own
    // constructor is the only writer of these names. from_chars over the whole
    // name, round-tripped through to_string, accepts exactly what the engine
    // writes; everything else is foreign content. The manager's startup scan and
    // wal_reader_t's replay walk MUST agree on this — when they did not, a
    // foreign-named directory was replayed at startup while nothing managed it
    // and its wal ids never bounded the id allocator.
    // Returns false when the name is foreign; on success writes the oid into out.
    inline bool parse_database_dir_name(const std::string& name, components::catalog::oid_t& out) {
        unsigned long parsed = 0;
        const char* first = name.data();
        const char* last = first + name.size();
        const auto [ptr, ec] = std::from_chars(first, last, parsed);
        if (ec != std::errc{} || ptr != last || std::to_string(parsed) != name ||
            parsed > std::numeric_limits<components::catalog::oid_t>::max()) {
            return false;
        }
        out = static_cast<components::catalog::oid_t>(parsed);
        return true;
    }

    inline void next_id(atomic_id_t& id, id_t stride = 1) { id += stride; }

    inline void next_id(id_t& id, id_t stride = 1) { id += stride; }

    inline id_t id_from_string(const std::string& value) {
        if (value.empty()) {
            return 0;
        }
        return std::strtoull(value.data(), nullptr, 10);
    }

} //namespace services::wal
