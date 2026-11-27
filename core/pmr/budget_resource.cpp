#include "budget_resource.hpp"

namespace core::pmr {

    namespace {
        // used + reserved can pass the limit by what live reservations already allocated (counted twice)
        std::uint64_t room_left(std::uint64_t limit, std::uint64_t used, std::uint64_t reserved) noexcept {
            if (used >= limit) {
                return 0;
            }
            const std::uint64_t unused = limit - used;
            return reserved >= unused ? 0 : unused - reserved;
        }
    } // namespace

    budget_resource_t::reservation_t::reservation_t(budget_resource_t* budget, std::uint64_t bytes) noexcept
        : budget_(budget)
        , bytes_(bytes) {}

    budget_resource_t::reservation_t::reservation_t(reservation_t&& other) noexcept
        : budget_(other.budget_)
        , bytes_(other.bytes_) {
        other.budget_ = nullptr;
        other.bytes_ = 0;
    }

    budget_resource_t::reservation_t& budget_resource_t::reservation_t::operator=(reservation_t&& other) noexcept {
        if (this != &other) {
            if (budget_) {
                budget_->release_(bytes_);
            }
            budget_ = other.budget_;
            bytes_ = other.bytes_;
            other.budget_ = nullptr;
            other.bytes_ = 0;
        }
        return *this;
    }

    budget_resource_t::reservation_t::~reservation_t() {
        if (budget_) {
            budget_->release_(bytes_);
        }
    }

    bool budget_resource_t::set_limit(std::uint64_t limit) noexcept {
        const std::uint64_t used = used_.load(std::memory_order_relaxed);
        const std::uint64_t reserved = reserved_.load(std::memory_order_relaxed);
        if (used > limit || reserved > limit - used) {
            return false;
        }
        limit_.store(limit, std::memory_order_relaxed);
        return true;
    }

    std::uint64_t budget_resource_t::available() const noexcept {
        return room_left(limit_.load(std::memory_order_relaxed),
                         used_.load(std::memory_order_relaxed),
                         reserved_.load(std::memory_order_relaxed));
    }

    budget_resource_t::reservation_t budget_resource_t::try_reserve(std::uint64_t bytes) noexcept {
        std::uint64_t reserved = reserved_.load(std::memory_order_relaxed);
        do {
            const std::uint64_t room =
                room_left(limit_.load(std::memory_order_relaxed), used_.load(std::memory_order_relaxed), reserved);
            if (bytes > room) {
                return reservation_t{};
            }
        } while (!reserved_.compare_exchange_weak(reserved, reserved + bytes, std::memory_order_relaxed));
        return reservation_t(this, bytes);
    }

    void budget_resource_t::wait_for_release(std::uint64_t seen) const noexcept {
        releases_.wait(seen, std::memory_order_acquire);
    }

    void budget_resource_t::release_(std::uint64_t bytes) noexcept {
        reserved_.fetch_sub(bytes, std::memory_order_relaxed);
        releases_.fetch_add(1, std::memory_order_release);
        releases_.notify_all();
    }

} // namespace core::pmr
