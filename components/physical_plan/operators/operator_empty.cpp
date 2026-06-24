#include "operator_empty.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::operators {

    operator_empty_t::operator_empty_t(std::pmr::memory_resource* resource, operator_data_ptr&& data)
        : read_only_operator_t(resource, log_t{}, operator_type::empty) {
        output_ = std::move(data);
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    operator_empty_t::source_next(pipeline::context_t* /*ctx*/) {
        // Stream the held chunks one at a time; once exhausted, the 0-column drain
        // sentinel stops execute_pipeline's pump.
        if (output_ && emit_index_ < output_->chunks().size()) {
            auto& chunk = output_->chunks()[emit_index_++];
            co_return chunk.partial_copy(resource_, 0, chunk.size());
        }
        co_return vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0};
    }

    void operator_empty_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::operators