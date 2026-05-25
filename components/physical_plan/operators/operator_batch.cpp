#include "operator_batch.hpp"

namespace components::operators {
    operator_batch_t::operator_batch_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks)
        : read_only_operator_t(resource, log_t{}, operator_type::batch) {
        // Defence-in-depth: callers (e.g. operator_group.cpp's global-aggregate
        // path) construct this with an explicitly empty vector. Keep the
        // operator_data_t invariant — at least one (possibly empty) chunk so
        // downstream operators that read chunks.front() do not crash.
        if (chunks.empty()) {
            std::pmr::vector<types::complex_logical_type> empty_types(resource);
            chunks.emplace_back(resource, empty_types, 0);
        }
        set_output(make_operator_data(resource, std::move(chunks)));
        mark_executed();
    }
} // namespace components::operators
