// Reproduction / regression guard for the boost::lockfree eviction-queue
// bootstrap allocation footprint (the b1/b2 regression vs a13).
//
// eviction_queue_t::q is a `boost::lockfree::queue<buffer_eviction_node_t*>`
// constructed with a runtime capacity. boost's variable-sized freelist EAGERLY
// reserves `capacity + 1` heap nodes at construction (freelist_stack ctor loops
// `n+1` times), each via an over-aligned allocator (the node is
// `alignas(cacheline)`, so allocation bypasses `operator new` and the pmr
// resource_tracer — it goes straight to aligned_alloc/posix_memalign, i.e. it is
// UNTRACKED and unmaskable). Building the queue with capacity 256 therefore
// reserves 257 nodes up front; those nodes only leak if the owning queue is
// never destroyed (a destroyed queue frees them all — asserted below).
//
// This test injects a counting allocator (via the public `boost::lockfree::allocator<>`
// policy) into a queue with the SAME element type as production
// (`buffer_eviction_node_t*`) to make the otherwise-invisible node reservation
// observable, deterministically and cross-platform (no ASAN, no interposition).
//
// LIMITATION (intentional): the production `eviction_queue_t::q` uses boost's
// DEFAULT allocator, which is not injectable, so this exercises the same node
// type and freelist mechanism, not the live engine instance. The engine-level
// teardown gate lives in integration/cpp/test/test_engine_lifecycle.cpp.

#include <atomic>
#include <cstddef>
#include <new>

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <catch2/catch_test_macros.hpp>

#include <components/table/storage/buffer_pool.hpp>

namespace {

    // Process-global node allocation/free counters for the counting allocator.
    // Only our counting_queue below routes through this allocator, so these
    // counters are unaffected by any other queue in the test binary (which use
    // boost's default allocator). Reset at the top of each TEST_CASE.
    std::atomic<long> g_node_allocs{0};
    std::atomic<long> g_node_frees{0};

    // Standard-allocator-shaped counter that returns OVER-ALIGNED memory. boost
    // rebinds this to the freelist `node` type, which is `alignas(cacheline_bytes)`
    // (64 on x86_64 Linux, 128 on arm64 macOS); over-aligning every allocation to
    // 128 is a safe superset that satisfies either. Each freelist node is allocated
    // one at a time, so one allocate() call == one node.
    constexpr std::size_t kNodeAlign = 128;

    template<class T>
    struct counting_alloc {
        using value_type = T;

        counting_alloc() = default;
        template<class U>
        counting_alloc(const counting_alloc<U>&) noexcept {}

        template<class U>
        struct rebind {
            using other = counting_alloc<U>;
        };

        T* allocate(std::size_t n) {
            g_node_allocs.fetch_add(1, std::memory_order_relaxed);
            return static_cast<T*>(::operator new(n * sizeof(T), std::align_val_t(kNodeAlign)));
        }
        void deallocate(T* p, std::size_t n) noexcept {
            g_node_frees.fetch_add(1, std::memory_order_relaxed);
            ::operator delete(p, n * sizeof(T), std::align_val_t(kNodeAlign));
        }

        template<class U>
        bool operator==(const counting_alloc<U>&) const noexcept {
            return true;
        }
        template<class U>
        bool operator!=(const counting_alloc<U>&) const noexcept {
            return false;
        }
    };

    using node_ptr = components::table::storage::buffer_eviction_node_t*;
    using counting_queue = boost::lockfree::queue<node_ptr, boost::lockfree::allocator<counting_alloc<char>>>;

} // namespace

TEST_CASE("eviction queue: capacity 256 eagerly reserves 257 nodes (b1/b2 regression signature)",
          "[buffer_pool][leak-repro]") {
    g_node_allocs = 0;
    g_node_frees = 0;
    {
        counting_queue q(256);
        // boost freelist reserves capacity+1 nodes at construction, all up front.
        CHECK(g_node_allocs.load() == 257);
        CHECK(g_node_frees.load() == 0);
    }
    // A destroyed queue frees every reserved node — this is WHY otterbrix's own
    // ASAN/LSan CI is green, and why the reported leak requires a never-destroyed
    // owner rather than mere over-reservation.
    CHECK(g_node_frees.load() == g_node_allocs.load());
    CHECK(g_node_allocs.load() == 257);
}

TEST_CASE("eviction queue: capacity 0 reserves nothing eagerly and grows lazily (a13 / Edit 1)",
          "[buffer_pool][leak-repro]") {
    components::table::storage::buffer_eviction_node_t node_obj;

    g_node_allocs = 0;
    g_node_frees = 0;
    {
        counting_queue q(0);
        // Only the single dummy/sentinel node is reserved (capacity 0 + 1).
        CHECK(g_node_allocs.load() == 1);

        // push() on an empty freelist lazily allocates exactly one node on demand
        // (production enqueue uses q.push(), not bounded_push()), so capacity 0 is
        // functionally safe — it just defers the first malloc to the first eviction.
        REQUIRE(q.push(&node_obj));
        CHECK(g_node_allocs.load() == 2);

        node_ptr out = nullptr;
        REQUIRE(q.pop(out));
        CHECK(out == &node_obj);
    }
    // Net zero: everything reserved/grown is freed on destruction.
    CHECK(g_node_frees.load() == g_node_allocs.load());
    CHECK(g_node_allocs.load() == 2);
}
