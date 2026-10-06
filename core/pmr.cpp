#include "pmr.hpp"

#include <core/config.hpp>

#include <cstring>

namespace core::pmr {

    namespace {

        std::unique_ptr<std::pmr::memory_resource> make_backing([[maybe_unused]] std::pmr::memory_resource* upstream) {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
            // A pooled sub-block overflow stays inside a block ASAN considers live and is never
            // reported; the tracer gives ASAN one redzoned block per object instead.
            return std::make_unique<resource_tracer_t>(upstream);
#elif defined(OTTERBRIX_TSAN_ENABLED)
            return nullptr;
#else
            return std::make_unique<std::pmr::synchronized_pool_resource>(upstream);
#endif
        }

    } // namespace

    otterbrix_resource::otterbrix_resource()
        : otterbrix_resource(std::pmr::new_delete_resource()) {}

    otterbrix_resource::otterbrix_resource(std::pmr::memory_resource* upstream)
        : upstream_(upstream)
        , backing_(make_backing(upstream_)) {}

    otterbrix_resource::~otterbrix_resource() = default;

    // TSAN: no pool. TSAN cannot see through synchronized_pool_resource's internal mutex and reports
    // a block reused across threads as a race. The poison write shows TSAN the whole block being
    // freed: its own free() marks only the first 1 KB.
    void* otterbrix_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
#if defined(OTTERBRIX_TSAN_ENABLED)
        return upstream_->allocate(bytes, alignment);
#else
        return backing_->allocate(bytes, alignment);
#endif
    }

    void otterbrix_resource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
#if defined(OTTERBRIX_TSAN_ENABLED)
        std::memset(p, 0xDE, bytes);
        upstream_->deallocate(p, bytes, alignment);
#else
        backing_->deallocate(p, bytes, alignment);
#endif
    }

    bool otterbrix_resource::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
        return this == &other;
    }

    void* arena_resource_t::upstream_counter_t::do_allocate(std::size_t bytes, std::size_t alignment) {
        ++allocations_;
        return upstream_->allocate(bytes, alignment);
    }

    void arena_resource_t::upstream_counter_t::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
        upstream_->deallocate(p, bytes, alignment);
    }

    bool arena_resource_t::upstream_counter_t::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
        return this == &other;
    }

    arena_resource_t::arena_resource_t(std::pmr::memory_resource* upstream)
        : upstream_(upstream)
        , buffer_(&upstream_)
        , pieces_(&upstream_) {}

    std::size_t arena_resource_t::upstream_allocations() const noexcept { return upstream_.allocations(); }

    arena_resource_t::~arena_resource_t() { release(); }

    void arena_resource_t::release() {
        for (const piece_t& piece : pieces_) {
            pieces_.get_allocator().resource()->deallocate(piece.pointer, piece.bytes, piece.alignment);
        }
        pieces_.clear();
        buffer_.release();
    }

    void* arena_resource_t::do_allocate(std::size_t bytes, std::size_t alignment) {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
        void* pointer = pieces_.get_allocator().resource()->allocate(bytes, alignment);
        pieces_.push_back(piece_t{pointer, bytes, alignment});
        return pointer;
#else
        return buffer_.allocate(bytes, alignment);
#endif
    }

    void arena_resource_t::do_deallocate(void*, std::size_t, std::size_t) {}

    bool arena_resource_t::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
        return this == &other;
    }

} // namespace core::pmr
