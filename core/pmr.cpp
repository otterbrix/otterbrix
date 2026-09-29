#include "pmr.hpp"

#include <core/config.hpp>

// clang answers neither __SANITIZE_ADDRESS__ (GCC) nor _ADDRESS_SANITIZER (MSVC) -- only
// __has_feature(address_sanitizer) -- so without that arm an ASAN build on clang silently kept the pool.
#if defined(__SANITIZE_ADDRESS__) || defined(_ADDRESS_SANITIZER)
#define OTTERBRIX_ADDRESS_SANITIZER 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define OTTERBRIX_ADDRESS_SANITIZER 1
#endif
#endif

namespace core::pmr {

    namespace {

#if !defined(OTTERBRIX_ADDRESS_SANITIZER) && defined(OTTERBRIX_TSAN_ENABLED)
        // TSAN cannot see through synchronized_pool_resource's internal mutex and reports memory
        // reused between threads as a race; new/delete it understands natively.
        class forwarding_resource_t final : public std::pmr::memory_resource {
        public:
            explicit forwarding_resource_t(std::pmr::memory_resource* upstream)
                : upstream_(upstream) {}

        private:
            void* do_allocate(std::size_t bytes, std::size_t alignment) override {
                return upstream_->allocate(bytes, alignment);
            }
            void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
                upstream_->deallocate(p, bytes, alignment);
            }
            bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }

            std::pmr::memory_resource* upstream_;
        };
#endif

        std::unique_ptr<std::pmr::memory_resource> make_backing(std::pmr::memory_resource* upstream) {
#if defined(OTTERBRIX_ADDRESS_SANITIZER)
            // A pooled sub-block overflow stays inside a block ASAN considers live and is never
            // reported; the tracer gives ASAN one redzoned block per object instead.
            return std::make_unique<resource_tracer_t>(upstream);
#elif defined(OTTERBRIX_TSAN_ENABLED)
            return std::make_unique<forwarding_resource_t>(upstream);
#else
            return std::make_unique<std::pmr::synchronized_pool_resource>(upstream);
#endif
        }

    } // namespace

    otterbrix_resource::otterbrix_resource()
        : otterbrix_resource(std::pmr::new_delete_resource()) {}

    otterbrix_resource::otterbrix_resource(std::pmr::memory_resource* upstream)
        : upstream_(upstream)
        , backing_(make_backing(upstream)) {}

    otterbrix_resource::~otterbrix_resource() = default;

    std::pmr::memory_resource* otterbrix_resource::upstream_resource() const noexcept { return upstream_; }

    void* otterbrix_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
        return backing_->allocate(bytes, alignment);
    }

    void otterbrix_resource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
        backing_->deallocate(p, bytes, alignment);
    }

    bool otterbrix_resource::do_is_equal(const std::pmr::memory_resource& other) const noexcept {
        return this == &other;
    }

} // namespace core::pmr
