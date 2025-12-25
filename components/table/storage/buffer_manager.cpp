#include "buffer_manager.hpp"

#include "buffer_pool.hpp"

namespace components::table::storage {

    std::shared_ptr<block_handle_t> buffer_manager_t::register_transient_memory(uint64_t, uint64_t) {
        throw std::logic_error(
            "Incorrect call: This type of buffer_manager_t can not create 'transient-memory' blocks");
    }

    std::shared_ptr<block_handle_t> buffer_manager_t::register_small_memory(uint64_t size) {
        return register_small_memory(memory_tag::BASE_TABLE, size);
    }

    std::shared_ptr<block_handle_t> buffer_manager_t::register_small_memory(memory_tag, uint64_t) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t can not create 'small-memory' blocks");
    }

    void buffer_manager_t::reserve_memory(uint64_t) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t can not reserve memory");
    }
    void buffer_manager_t::free_reserved_memory(uint64_t) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t can not free reserved memory");
    }

    void buffer_manager_t::set_memory_limit(uint64_t) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t can not set a memory limit");
    }

    buffer_pool_t& buffer_manager_t::buffer_pool() const {
        throw std::logic_error("This type of buffer_manager_t does not have a buffer pool");
    }

    uint64_t buffer_manager_t::query_max_memory() const { return buffer_pool().query_max_memory(); }

    std::unique_ptr<file_buffer_t>
    buffer_manager_t::construct_manager_buffer(uint64_t, std::unique_ptr<file_buffer_t>&&, file_buffer_type) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t can not construct managed buffers");
    }

    void buffer_manager_t::add_to_eviction_queue(std::shared_ptr<block_handle_t>&) {
        throw std::logic_error("Incorrect call: This type of buffer_manager_t does not support 'add_to_eviction_queue");
    }

} // namespace components::table::storage