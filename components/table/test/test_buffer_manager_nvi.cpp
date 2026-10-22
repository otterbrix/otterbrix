#include <catch2/catch_test_macros.hpp>

#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>

#include <cstdlib>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace {
    using namespace components::table::storage;

    class every_customization_point_t final : public buffer_manager_t {
        core::result_wrapper_t<buffer_handle_t> allocate_impl(memory_tag, uint64_t, bool) final { std::abort(); }
        core::result_wrapper_t<bool> reallocate_impl(std::shared_ptr<block_handle_t>&, uint64_t) final { std::abort(); }
        core::result_wrapper_t<buffer_handle_t> pin_impl(std::shared_ptr<block_handle_t>&) final { std::abort(); }
        void prefetch_impl(std::vector<std::shared_ptr<block_handle_t>>&) final { std::abort(); }
        void unpin_impl(block_handle_t*) final { std::abort(); }
        uint64_t block_allocation_size_impl() const final { std::abort(); }
        uint64_t block_size_impl() const final { std::abort(); }
        core::result_wrapper_t<std::shared_ptr<block_handle_t>> register_transient_memory_impl(uint64_t,
                                                                                               uint64_t) final {
            std::abort();
        }
        core::result_wrapper_t<std::shared_ptr<block_handle_t>> register_small_memory_impl(memory_tag, uint64_t) final {
            std::abort();
        }
        core::result_wrapper_t<bool> reserve_memory_impl(uint64_t) final { std::abort(); }
        void free_reserved_memory_impl(uint64_t) final { std::abort(); }
        std::vector<memory_info_t> get_memory_usage_info_impl() const final { std::abort(); }
        core::result_wrapper_t<bool> set_memory_limit_impl(uint64_t) final { std::abort(); }
        std::unique_ptr<file_buffer_t>
        construct_manager_buffer_impl(uint64_t, std::unique_ptr<file_buffer_t>&&, file_buffer_type) final {
            std::abort();
        }
        buffer_pool_t& buffer_pool_impl() const final { std::abort(); }
        std::pmr::memory_resource* resource_impl() const noexcept final { std::abort(); }
    };

    template<typename M>
    constexpr bool any_customization_point_is_public =
        requires(M& m, memory_tag tag, uint64_t n, bool b) { m.allocate_impl(tag, n, b); } ||
        requires(M& m, std::shared_ptr<block_handle_t>& h, uint64_t n) { m.reallocate_impl(h, n); } ||
        requires(M& m, std::shared_ptr<block_handle_t>& h) { m.pin_impl(h); } ||
        requires(M& m, std::vector<std::shared_ptr<block_handle_t>>& hs) { m.prefetch_impl(hs); } ||
        requires(M& m, block_handle_t* h) { m.unpin_impl(h); } ||
        requires(const M& m) { m.block_allocation_size_impl(); } || requires(const M& m) { m.block_size_impl(); } ||
        requires(M& m, uint64_t n) { m.register_transient_memory_impl(n, n); } ||
        requires(M& m, memory_tag tag, uint64_t n) { m.register_small_memory_impl(tag, n); } ||
        requires(M& m, uint64_t n) { m.reserve_memory_impl(n); } ||
        requires(M& m, uint64_t n) { m.free_reserved_memory_impl(n); } ||
        requires(const M& m) { m.get_memory_usage_info_impl(); } ||
        requires(M& m, uint64_t n) { m.set_memory_limit_impl(n); } ||
        requires(M& m, uint64_t n, std::unique_ptr<file_buffer_t>&& src, file_buffer_type type) {
            m.construct_manager_buffer_impl(n, std::move(src), type);
        } || requires(const M& m) { m.buffer_pool_impl(); } || requires(const M& m) { m.resource_impl(); };

} // namespace

TEST_CASE("buffer manager: every customization point is a private pure virtual") {
    STATIC_REQUIRE(std::is_abstract_v<buffer_manager_t>);
    STATIC_REQUIRE_FALSE(std::is_abstract_v<every_customization_point_t>);
    STATIC_REQUIRE_FALSE(std::is_abstract_v<standard_buffer_manager_t>);
}

TEST_CASE("buffer manager: no customization point is callable from outside") {
    STATIC_REQUIRE_FALSE(any_customization_point_is_public<buffer_manager_t>);
    STATIC_REQUIRE_FALSE(any_customization_point_is_public<standard_buffer_manager_t>);
    STATIC_REQUIRE_FALSE(any_customization_point_is_public<every_customization_point_t>);
}
