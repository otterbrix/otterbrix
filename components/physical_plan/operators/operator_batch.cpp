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

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_batch_t::source_next(pipeline::context_t* /*ctx*/) {
        // Stream the pre-materialized chunks one at a time; once past the end, return
        // the 0-column drain sentinel so execute_pipeline stops the pump.
        if (output_ && emit_index_ < output_->chunks().size()) {
            auto& chunk = output_->chunks()[emit_index_++];
            co_return chunk.partial_copy(resource_, 0, chunk.size());
        }
        co_return vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0};
    }
} // namespace components::operators
