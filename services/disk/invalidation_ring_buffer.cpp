#include "invalidation_ring_buffer.hpp"

namespace services::disk {

    std::uint64_t invalidation_ring_buffer_t::push(invalidation_event_t event) noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::uint64_t version = next_version_++;
        event.version = version;
        ring_[(version - 1) % CAPACITY] = event;
        return version;
    }

    std::uint64_t invalidation_ring_buffer_t::latest_version() const noexcept {
        std::lock_guard<std::mutex> lock(mutex_);
        return next_version_ - 1;
    }

    invalidation_ring_buffer_t::snapshot_t
    invalidation_ring_buffer_t::since(std::uint64_t since_version) const {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot_t out;
        const std::uint64_t latest = next_version_ - 1;
        out.latest_version = latest;

        if (latest == 0 || since_version >= latest) {
            return out;
        }

        const std::uint64_t pushed = next_version_ - 1;
        if (pushed > CAPACITY && since_version + CAPACITY < pushed) {
            out.overflow = true;
            return out;
        }

        const std::uint64_t first_v = since_version + 1;
        out.events.reserve(static_cast<std::size_t>(latest - since_version));
        for (std::uint64_t v = first_v; v <= latest; ++v) {
            out.events.push_back(ring_[(v - 1) % CAPACITY]);
        }
        return out;
    }

} // namespace services::disk
