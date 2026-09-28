#include "index_types.hpp"

#include <components/vector/vector_operations.hpp>

namespace services::index {

    void append_key_batch(key_batch_t* target, const key_batch_t& source, std::pmr::memory_resource* resource) {
        using components::vector::DEFAULT_VECTOR_CAPACITY;
        target->ids.reserve(target->ids.size() + source.size());
        size_t offset = 0;
        for (const auto& [keys, rows] : source.keys) {
            const int64_t* ids = source.ids.data() + offset;
            offset += rows;
            size_t copied = 0;
            while (copied < rows) {
                if (target->keys.empty() || target->keys.back().second == DEFAULT_VECTOR_CAPACITY) {
                    target->keys.emplace_back(
                        components::vector::vector_t(resource, keys.type(), DEFAULT_VECTOR_CAPACITY),
                        size_t{0});
                }
                auto& [target_keys, target_rows] = target->keys.back();
                const size_t taken = std::min(DEFAULT_VECTOR_CAPACITY - target_rows, rows - copied);
                components::vector::vector_ops::copy(keys, target_keys, copied + taken, copied, target_rows);
                target->ids.insert(target->ids.end(), ids + copied, ids + copied + taken);
                target_rows += taken;
                copied += taken;
            }
        }
    }

} // namespace services::index
