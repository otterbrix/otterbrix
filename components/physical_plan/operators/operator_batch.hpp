#pragma once

#include "operator.hpp"
#include <boost/intrusive_ptr.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {
    class operator_batch_t final : public read_only_operator_t {
    public:
        operator_batch_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks);

        const std::vector<vector::data_chunk_t>& chunks() const noexcept { return chunks_; }
        std::vector<vector::data_chunk_t>& chunks() noexcept { return chunks_; }

    private:
        void on_execute_impl(pipeline::context_t*) override {}

        std::vector<vector::data_chunk_t> chunks_;
    };

    using operator_batch_ptr = boost::intrusive_ptr<operator_batch_t>;

    inline operator_batch_ptr make_operator_batch(std::pmr::memory_resource* resource,
                                                  std::vector<vector::data_chunk_t>&& chunks) {
        return {new operator_batch_t(resource, std::move(chunks))};
    }
} // namespace components::operators
