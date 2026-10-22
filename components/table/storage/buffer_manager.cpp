#include "buffer_manager.hpp"

#include "buffer_pool.hpp"

namespace components::table::storage {

    uint64_t buffer_manager_t::query_max_memory() const { return buffer_pool().query_max_memory(); }

} // namespace components::table::storage