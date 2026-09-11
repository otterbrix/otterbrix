#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory_resource>

namespace core::pmr {

    // A pass-through memory_resource that counts what goes through it. Two jobs:
    //   * as an upstream, to see how much a resource under test really took -- and gave back;
    //   * installed as the process default (see default_resource_window_t), to prove a subsystem
    //     never falls back to the global arena.
    // An instrument, not a production path: every counter costs an atomic. A pooling resource
    // layered on top counts chunks, not the caller's requests.
    class counting_resource_t final : public std::pmr::memory_resource {
    public:
        // Leaves the upstream uncaptured so arm() can still name the process default. Until
        // something is captured, allocations are served by new_delete_resource().
        counting_resource_t() noexcept = default;

        explicit counting_resource_t(std::pmr::memory_resource* upstream) noexcept
            : upstream_(upstream) {}

        counting_resource_t(const counting_resource_t&) = delete;
        counting_resource_t& operator=(const counting_resource_t&) = delete;

        // Captures the CURRENT default resource as upstream -- but only if nothing is captured
        // yet, so a deallocation arriving after this resource is uninstalled still reaches the
        // resource that actually served it. Zeroes the counters either way.
        // Never captures itself: arming while already installed as the default would recurse.
        void arm() noexcept {
            std::pmr::memory_resource* current = std::pmr::get_default_resource();
            capture(current != this ? current : std::pmr::new_delete_resource());
            reset();
        }

        void reset() noexcept {
            allocations_.store(0, std::memory_order_relaxed);
            deallocations_.store(0, std::memory_order_relaxed);
            allocated_bytes_.store(0, std::memory_order_relaxed);
        }

        std::size_t allocations() const noexcept { return allocations_.load(std::memory_order_relaxed); }
        std::size_t deallocations() const noexcept { return deallocations_.load(std::memory_order_relaxed); }

        // A running total of what was ever asked for, not what is live: reset() zeroes it,
        // deallocate() does not.
        std::size_t allocated_bytes() const noexcept { return allocated_bytes_.load(std::memory_order_relaxed); }

        // Blocks handed out and not yet given back. Signed on purpose: freeing a block this
        // resource never served reads as a negative number instead of wrapping to a huge one.
        std::int64_t outstanding() const noexcept {
            return static_cast<std::int64_t>(allocations()) - static_cast<std::int64_t>(deallocations());
        }

        // Never null: an uncaptured resource answers new_delete_resource(), which is what it
        // would allocate from.
        std::pmr::memory_resource* upstream_resource() const noexcept {
            std::pmr::memory_resource* captured = upstream_.load(std::memory_order_acquire);
            return captured != nullptr ? captured : std::pmr::new_delete_resource();
        }

    private:
        // Latches `candidate` if nothing is captured yet; answers whoever won. do_allocate goes
        // through it too, so a resource that served a block before anyone armed it keeps serving
        // that block's deallocation from the same upstream.
        std::pmr::memory_resource* capture(std::pmr::memory_resource* candidate) noexcept {
            std::pmr::memory_resource* expected = nullptr;
            if (upstream_.compare_exchange_strong(expected,
                                                  candidate,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                return candidate;
            }
            return expected;
        }

        void* do_allocate(std::size_t bytes, std::size_t alignment) override {
            std::pmr::memory_resource* upstream = capture(std::pmr::new_delete_resource());
            allocations_.fetch_add(1, std::memory_order_relaxed);
            allocated_bytes_.fetch_add(bytes, std::memory_order_relaxed);
            return upstream->allocate(bytes, alignment);
        }

        void do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) override {
            std::pmr::memory_resource* upstream = capture(std::pmr::new_delete_resource());
            deallocations_.fetch_add(1, std::memory_order_relaxed);
            upstream->deallocate(ptr, bytes, alignment);
        }

        bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

        std::atomic<std::pmr::memory_resource*> upstream_{nullptr};
        std::atomic<std::size_t> allocations_{0};
        std::atomic<std::size_t> deallocations_{0};
        std::atomic<std::size_t> allocated_bytes_{0};
    };

    // Installs `resource` as the process default for a scope and puts the previous one back --
    // the previous one, not new_delete_resource(). Assumes one thread: set_default_resource is
    // process-wide.
    class default_resource_window_t final {
    public:
        explicit default_resource_window_t(std::pmr::memory_resource* resource) noexcept
            : previous_(std::pmr::set_default_resource(resource)) {}

        default_resource_window_t(const default_resource_window_t&) = delete;
        default_resource_window_t& operator=(const default_resource_window_t&) = delete;

        ~default_resource_window_t() { std::pmr::set_default_resource(previous_); }

    private:
        std::pmr::memory_resource* previous_;
    };

    // The probe that measures the process default. Immortal on purpose: memory it hands out
    // inside a window may be freed long after the window closes -- and after static destructors
    // have run. The pointer stays reachable, so LSan sees no leak.
    inline counting_resource_t& process_default_probe() {
        static counting_resource_t* probe = new counting_resource_t();
        return *probe;
    }

    // arm()s a probe and installs it as the process default for a scope: what it counts is
    // exactly what the scope took from the global arena.
    class default_resource_counter_t final {
    public:
        default_resource_counter_t()
            : default_resource_counter_t(process_default_probe()) {}

        explicit default_resource_counter_t(counting_resource_t& probe) noexcept
            : probe_(probe)
            , window_(armed(probe)) {}

        default_resource_counter_t(const default_resource_counter_t&) = delete;
        default_resource_counter_t& operator=(const default_resource_counter_t&) = delete;

        std::size_t allocations() const noexcept { return probe_.allocations(); }
        std::size_t allocated_bytes() const noexcept { return probe_.allocated_bytes(); }

    private:
        // arm() has to run before the probe becomes the default; a static helper in the
        // mem-initialiser is what orders it without a comma operator.
        static std::pmr::memory_resource* armed(counting_resource_t& probe) noexcept {
            probe.arm();
            return &probe;
        }

        counting_resource_t& probe_;
        default_resource_window_t window_;
    };

} // namespace core::pmr
