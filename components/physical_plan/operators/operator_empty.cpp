#include "operator_empty.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::operators {

    operator_empty_t::operator_empty_t(std::pmr::memory_resource* resource, operator_data_ptr&& data)
        : read_only_operator_t(resource, log_t{}, operator_type::empty) {
        output_ = std::move(data);
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_empty_t::source_next(pipeline::context_t* /*ctx*/) {
        if (output_ && emit_index_ < output_->chunks().size()) {
            auto& chunk = output_->chunks()[emit_index_++];
            co_return chunk.partial_copy(resource_, 0, chunk.size());
        }
        co_return std::nullopt;
    }

} // namespace components::operators