
#pragma once

#include "ddl_result.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <vector>

namespace services::disk {

    // M4 / Decision 2 (catalog-migration-to-postgresql-style.md §3 lines 144–150):
    // Pull-based ring buffer of invalidation events. Lives inside manager_disk_t; the
    // dispatcher's plan cache (M5) periodically pulls `since(last_seen_version)` to
    // invalidate stale resolutions.
    //
    // Capacity is fixed at 4096; on overflow (more than 4096 DDLs since the last consumer
    // pull) `since()` returns a sentinel result with `overflow=true` so the caller can
    // perform a full plan-cache reset (matches PostgreSQL's sinval-pattern).
    //
    // Concurrency: today disk runs on one actor thread (single producer / single consumer),
    // but the mutex is held over both push and since to keep the API safe under future
    // multi-threaded disk experiments. The cost is one uncontended lock per DDL — negligible
    // next to the storage write that just happened.
    class invalidation_ring_buffer_t {
    public:
        static constexpr std::size_t CAPACITY = 4096;

        struct snapshot_t {
            std::vector<invalidation_event_t> events;
            // True when more than CAPACITY events have been pushed since `since_version` —
            // events list is empty and the consumer must reset its cache wholesale.
            bool overflow{false};
            // The version at the time of the snapshot (== latest version pushed). Consumer
            // stores this and passes it back next time as `since_version`.
            std::uint64_t latest_version{0};
        };

        invalidation_ring_buffer_t() noexcept = default;

        invalidation_ring_buffer_t(const invalidation_ring_buffer_t&) = delete;
        invalidation_ring_buffer_t& operator=(const invalidation_ring_buffer_t&) = delete;

        // Push an event. Assigns a fresh monotonic version, returns it.
        std::uint64_t push(invalidation_event_t event) noexcept;

        // Latest assigned version (0 if nothing pushed yet).
        std::uint64_t latest_version() const noexcept;

        // Returns events with version > since_version, in push order.
        // If the consumer fell more than CAPACITY events behind, overflow=true and events
        // is empty; consumer must invalidate everything.
        snapshot_t since(std::uint64_t since_version) const;

    private:
        mutable std::mutex mutex_;
        std::array<invalidation_event_t, CAPACITY> ring_{};
        // next_version_ is the version that will be assigned to the next push().
        // Versions are 1-based; latest_version() == next_version_ - 1; 0 means "no events".
        std::uint64_t next_version_{1};
    };

} // namespace services::disk
