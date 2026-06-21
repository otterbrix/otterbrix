#include "operator_raw_data.hpp"

namespace components::operators {

    // Raw (literal) data already honours the ≤DEFAULT_VECTOR_CAPACITY bound (every
    // data_chunk_t does), so the chunk is adopted as the single batch as-is.
    operator_raw_data_t::operator_raw_data_t(vector::data_chunk_t&& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        auto* resource = chunk.resource();
        chunks_vector_t chunks(resource);
        chunks.emplace_back(std::move(chunk));
        output_ = make_operator_data(resource, std::move(chunks));
    }

    operator_raw_data_t::operator_raw_data_t(const vector::data_chunk_t& chunk)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        auto* resource = chunk.resource();
        vector::data_chunk_t copy(resource, chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
        chunk.copy(copy, 0);
        chunks_vector_t chunks(resource);
        chunks.emplace_back(std::move(copy));
        output_ = make_operator_data(resource, std::move(chunks));
    }

    // Multi-chunk literal data: each input chunk already honours the ≤DEFAULT_VECTOR_CAPACITY
    // bound, so copy them across as the output batch one-for-one.
    operator_raw_data_t::operator_raw_data_t(const std::pmr::vector<vector::data_chunk_t>& src_chunks)
        : read_only_operator_t(nullptr, log_t{}, operator_type::raw_data) {
        auto* resource = src_chunks.empty() ? std::pmr::get_default_resource() : src_chunks.front().resource();
        chunks_vector_t chunks(resource);
        chunks.reserve(src_chunks.size());
        for (const auto& chunk : src_chunks) {
            vector::data_chunk_t copy(resource, chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
            chunk.copy(copy, 0);
            chunks.emplace_back(std::move(copy));
        }
        output_ = make_operator_data(resource, std::move(chunks));
    }

    std::pmr::memory_resource* operator_raw_data_t::resource() const noexcept { return output_->resource(); }

    void operator_raw_data_t::on_execute_impl(pipeline::context_t*) {}

} // namespace components::operators
