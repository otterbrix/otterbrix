#pragma once

#include <components/vector/vector.hpp>

#include <algorithm>
#include <cstdint>
#include <memory_resource>
#include <span>
#include <vector>

namespace components::table {

    struct row_ids_t {
        vector::vector_t ids;
        uint64_t count{0};
    };

    using row_id_batch_t = std::pmr::vector<row_ids_t>;

    [[nodiscard]] row_ids_t make_row_id_window(std::pmr::memory_resource* resource, uint64_t count);

    template<typename NextId>
    [[nodiscard]] row_id_batch_t
    make_row_id_batch(std::pmr::memory_resource* resource, uint64_t count, NextId&& next_id) {
        row_id_batch_t batch(resource);
        batch.reserve((count + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY);
        for (uint64_t offset = 0; offset < count; offset += vector::DEFAULT_VECTOR_CAPACITY) {
            auto& window = batch.emplace_back(
                make_row_id_window(resource, std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, count - offset)));
            auto* ids = window.ids.data<int64_t>();
            for (uint64_t index = 0; index < window.count; ++index) {
                ids[index] = next_id();
            }
        }
        return batch;
    }

    [[nodiscard]] row_id_batch_t make_row_id_batch(std::pmr::memory_resource* resource, std::span<const int64_t> ids);
    [[nodiscard]] row_id_batch_t make_row_id_range(std::pmr::memory_resource* resource, int64_t start, uint64_t count);

} // namespace components::table
