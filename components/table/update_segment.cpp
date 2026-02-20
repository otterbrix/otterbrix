#include "update_segment.hpp"

#include <algorithm>

#include "column_data.hpp"
#include "column_segment.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_manager.hpp"
#include <components/vector/vector.hpp>

namespace components::table {

    static uint64_t sort_indexing_vector(std::pmr::memory_resource* resource,
                                         vector::indexing_vector_t& indexing,
                                         uint64_t count,
                                         int64_t* ids) {
        assert(count > 0);

        bool is_sorted = true;
        for (uint64_t i = 1; i < count; i++) {
            auto prev_index = indexing.get_index(i - 1);
            auto index = indexing.get_index(i);
            if (ids[index] <= ids[prev_index]) {
                is_sorted = false;
                break;
            }
        }
        if (is_sorted) {
            return count;
        }
        vector::indexing_vector_t sorted_indexing(resource, count);
        for (uint64_t i = 0; i < count; i++) {
            sorted_indexing.set_index(i, indexing.get_index(i));
        }
        std::sort(sorted_indexing.data(), sorted_indexing.data() + count, [&](uint64_t l, uint64_t r) {
            return ids[l] < ids[r];
        });
        uint64_t pos = 1;
        for (uint64_t i = 1; i < count; i++) {
            auto prev_index = sorted_indexing.get_index(i - 1);
            auto index = sorted_indexing.get_index(i);
            assert(ids[index] >= ids[prev_index]);
            if (ids[prev_index] != ids[index]) {
                sorted_indexing.set_index(pos++, index);
            }
        }

        indexing = sorted_indexing;
        assert(pos > 0);
        return pos;
    }

    static void check_for_conflicts(undo_buffer_pointer_t next_ptr,
                                    int64_t* ids,
                                    const vector::indexing_vector_t& indexing,
                                    uint64_t count,
                                    int64_t offset) {
        while (next_ptr.is_set()) {
            auto pin = next_ptr.pin();
            auto& info = pin.update_info();
            uint64_t i = 0, j = 0;
            auto tuples = info.tuples();
            while (true) {
                auto id = ids[indexing.get_index(i)] - offset;
                if (id == tuples[j]) {
                    throw std::logic_error("Conflict on update!");
                } else if (id < tuples[j]) {
                    i++;
                    if (i == count) {
                        break;
                    }
                } else {
                    j++;
                    if (j == info.N) {
                        break;
                    }
                }
            }
            next_ptr = info.next;
        }
    }

    update_info_t* create_empty_update_info(uint64_t type_size, uint64_t, std::unique_ptr<std::byte[]>& data) {
        data = std::make_unique<std::byte[]>(update_info_t::allocation_size(type_size));
        auto update_info = reinterpret_cast<update_info_t*>(data.get());
        update_info->initialize();
        return update_info;
    }

    static void merge_update_info_range_validity(update_info_t& current,
                                                 uint64_t start,
                                                 uint64_t end,
                                                 uint64_t result_offset,
                                                 vector::validity_mask_t& result_mask) {
        auto tuples = current.tuples();
        auto info_data = current.data<bool>();
        for (uint64_t i = 0; i < current.N; i++) {
            auto tuple_idx = tuples[i];
            if (tuple_idx < start) {
                continue;
            } else if (tuple_idx >= end) {
                break;
            }
            auto result_idx = result_offset + tuple_idx - start;
            result_mask.set(result_idx, info_data[i]);
        }
    }

    static void
    merge_validity_info(update_info_t& current, uint64_t result_offset, vector::validity_mask_t& result_mask) {
        auto tuples = current.tuples();
        auto info_data = current.data<bool>();
        for (uint64_t i = 0; i < current.N; i++) {
            result_mask.set(tuples[i] + result_offset, info_data[i]);
        }
    }

    undo_buffer_pointer_t undo_buffer_reference::buffer_pointer() { return {*entry, position}; }

    undo_buffer_reference undo_buffer_pointer_t::pin() const {
        assert(entry->capacity >= position);
        return undo_buffer_reference(*entry, entry->buffer_manager.pin(entry->block), position);
    }

    undo_buffer_reference undo_buffer_allocator_t::allocate(uint64_t alloc_len) {
        assert(!head || head->position <= head->capacity);
        storage::buffer_handle_t handle;
        if (!head || head->position + alloc_len > head->capacity) {
            auto block_size = buffer_manager.block_size();
            uint64_t capacity;
            if (!head && alloc_len <= 4096) {
                capacity = 4096;
            } else {
                capacity = block_size;
            }
            if (capacity < alloc_len) {
                capacity = vector::next_power_of_two(alloc_len);
            }
            auto entry = std::make_unique<undo_buffer_entry_t>(buffer_manager);
            if (capacity < block_size) {
                entry->block = buffer_manager.register_small_memory(storage::memory_tag::TRANSACTION, capacity);
                handle = buffer_manager.pin(entry->block);
            } else {
                handle = buffer_manager.allocate(storage::memory_tag::TRANSACTION, capacity, false);
                entry->block = handle.block_handle()->shared_from_this();
            }
            entry->capacity = capacity;
            entry->position = 0;
            if (head) {
                head->prev = entry.get();
                entry->next = std::move(head);
            } else {
                tail = entry.get();
            }
            head = std::move(entry);
        } else {
            handle = buffer_manager.pin(head->block);
        }
        uint64_t current_position = head->position;
        head->position += alloc_len;
        return undo_buffer_reference(*head, std::move(handle), current_position);
    }

    uint32_t* update_info_t::tuples() {
        return reinterpret_cast<uint32_t*>(reinterpret_cast<std::byte*>(this) + sizeof(update_info_t));
    }

    std::byte* update_info_t::values() {
        return reinterpret_cast<std::byte*>(this) + sizeof(update_info_t) + sizeof(uint32_t) * max;
    }

    types::logical_value_t update_info_t::value(uint64_t index) {
        auto& type = segment->column_data_->type();

        auto tuple_data = values();
        switch (type.type()) {
            case types::logical_type::VALIDITY:
                return types::logical_value_t(segment->column_data_->resource(), reinterpret_cast<bool*>(tuple_data)[index]);
            case types::logical_type::INTEGER:
                return types::logical_value_t(segment->column_data_->resource(), reinterpret_cast<int32_t*>(tuple_data)[index]);
            default:
                throw std::logic_error("Unimplemented type for update_info_t::value");
        }
    }

    bool update_info_t::has_prev() const { return prev.entry; }

    bool update_info_t::has_next() const { return next.entry; }

    uint64_t update_info_t::allocation_size(uint64_t type_size) {
        return storage::align_value<uint64_t>(sizeof(update_info_t) +
                                              (sizeof(uint32_t) + type_size) * vector::DEFAULT_VECTOR_CAPACITY);
    }

    update_segment_t::update_segment_t(column_data_t& data)
        : type_(data.type().to_physical_type())
        , type_size_(data.type().size())
        , heap_(data.resource())
        , column_data_(&data) {}

    bool update_segment_t::has_updates() const { return root_.get() != nullptr; }

    bool update_segment_t::has_uncommitted_updates(uint64_t vector_index) {
        auto read_lock = std::shared_lock(m_);
        auto entry = update_node(vector_index);
        if (!entry.is_set()) {
            return false;
        }
        auto pin = entry.pin();
        auto& info = pin.update_info();
        if (info.has_next()) {
            return true;
        }
        return false;
    }

    bool update_segment_t::has_updates(uint64_t vector_index) {
        auto read_lock = std::shared_lock(m_);
        return update_node(vector_index).is_set();
    }

    bool update_segment_t::has_updates(int64_t start_row_idx, int64_t end_row_idx) {
        auto read_lock = std::shared_lock(m_);
        if (!root_) {
            return false;
        }
        uint64_t base_vector_index = static_cast<uint64_t>(start_row_idx) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t end_vector_index = static_cast<uint64_t>(end_row_idx) / vector::DEFAULT_VECTOR_CAPACITY;
        for (uint64_t i = base_vector_index; i <= end_vector_index; i++) {
            auto entry = update_node(i);
            if (entry.is_set()) {
                return true;
            }
        }
        return false;
    }

    void update_segment_t::fetch_updates(uint64_t vector_index, uint64_t result_offset, vector::vector_t& result) {
        auto lock_handle = std::shared_lock(m_);
        auto node = update_node(vector_index);
        if (!node.is_set()) {
            return;
        }
        assert(result.get_vector_type() == vector::vector_type::FLAT);
        auto pin = node.pin();
        fetch_update(pin.update_info(), result_offset, result);
    }

    void update_segment_t::fetch_committed(uint64_t vector_index, uint64_t result_offset, vector::vector_t& result) {
        auto lock_handle = std::shared_lock(m_);
        auto node = update_node(vector_index);
        if (!node.is_set()) {
            return;
        }
        assert(result.get_vector_type() == vector::vector_type::FLAT);
        auto pin = node.pin();
        fetch_committed(pin.update_info(), result_offset, result);
    }

    void update_segment_t::fetch_committed_range(int64_t start_row, uint64_t count, vector::vector_t& result) {
        assert(count > 0);
        if (!root_) {
            return;
        }
        assert(result.get_vector_type() == vector::vector_type::FLAT);

        uint64_t end_row = static_cast<uint64_t>(start_row) + count;
        uint64_t start_vector = static_cast<uint64_t>(start_row) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t end_vector = (end_row - 1) / vector::DEFAULT_VECTOR_CAPACITY;
        assert(start_vector <= end_vector);

        for (uint64_t vector_idx = start_vector; vector_idx <= end_vector; vector_idx++) {
            auto entry = update_node(vector_idx);
            if (!entry.is_set()) {
                continue;
            }
            auto pin = entry.pin();
            uint64_t start_in_vector = vector_idx == start_vector ? static_cast<uint64_t>(start_row) -
                                                                        start_vector * vector::DEFAULT_VECTOR_CAPACITY
                                                                  : 0;
            uint64_t end_in_vector = vector_idx == end_vector ? end_row - end_vector * vector::DEFAULT_VECTOR_CAPACITY
                                                              : vector::DEFAULT_VECTOR_CAPACITY;
            assert(start_in_vector < end_in_vector);
            assert(end_in_vector > 0 && end_in_vector <= vector::DEFAULT_VECTOR_CAPACITY);
            uint64_t result_offset =
                vector_idx * vector::DEFAULT_VECTOR_CAPACITY + start_in_vector - static_cast<uint64_t>(start_row);
            fetch_committed_range(pin.update_info(), start_in_vector, end_in_vector, result_offset, result);
        }
    }

    void update_segment_t::update(uint64_t column_index,
                                  vector::vector_t& update,
                                  int64_t* ids,
                                  uint64_t count,
                                  vector::vector_t& base_data) {
        auto write_lock = std::unique_lock(m_);

        update.flatten(count);

        if (count == 0) {
            return;
        }

        vector::indexing_vector_t indexing(update.resource(), 0, count);
        count = sort_indexing_vector(update.resource(), indexing, count, ids);
        assert(count > 0);

        auto first_id = ids[indexing.get_index(0)];
        uint64_t vector_index =
            static_cast<uint64_t>(first_id - column_data_->start()) / vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t vector_offset =
            static_cast<uint64_t>(column_data_->start()) + vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        initialize_update_info(vector_index);

        assert(first_id >= column_data_->start());

        if (root_->info[vector_index].is_set()) {
            auto root_pointer = root_->info[vector_index];
            auto root_pin = root_pointer.pin();
            auto& base_info = root_pin.update_info();

            undo_buffer_reference node_ref;
            check_for_conflicts(base_info.next, ids, indexing, count, static_cast<int64_t>(vector_offset));

            std::unique_ptr<std::byte[]> update_info_data;
            update_info_t* node;
            node = create_empty_update_info(type_size_, count, update_info_data);
            node->segment = this;
            node->vector_index = vector_index;
            node->N = 0;
            node->column_index = column_index;

            node->next = base_info.next;
            if (node->next.is_set()) {
                auto next_pin = node->next.pin();
                auto& next_info = next_pin.update_info();
                next_info.prev = node_ref.buffer_pointer();
            }
            node->prev = root_pointer;
            base_info.next = node_ref.is_set() ? node_ref.buffer_pointer() : undo_buffer_pointer_t();

            merge_update(base_info, base_data, *node, update, ids, count, indexing);
        } else {
            uint64_t alloc_size = update_info_t::allocation_size(type_size_);
            auto handle = root_->buffer_allocator.allocate(alloc_size);
            auto& update_info = handle.update_info();
            update_info.initialize();
            update_info.column_index = column_index;

            initialize_update_info(update_info, ids, indexing, count, vector_index, vector_offset);

            std::unique_ptr<std::byte[]> update_info_data;
            undo_buffer_reference node_ref;
            update_info_t* transaction_node = create_empty_update_info(type_size_, count, update_info_data);

            initialize_update_info(*transaction_node, ids, indexing, count, vector_index, vector_offset);

            initialize_update(*transaction_node, base_data, update_info, update, indexing);

            update_info.next = node_ref.is_set() ? node_ref.buffer_pointer() : undo_buffer_pointer_t();
            update_info.prev = undo_buffer_pointer_t();
            transaction_node->next = undo_buffer_pointer_t();
            transaction_node->prev = handle.buffer_pointer();
            transaction_node->column_index = column_index;

            root_->info[vector_index] = handle.buffer_pointer();
        }
    }

    void update_segment_t::fetch_row(int64_t row_id, vector::vector_t& result, uint64_t result_idx) {
        uint64_t vector_index = static_cast<uint64_t>(row_id - column_data_->start()) / vector::DEFAULT_VECTOR_CAPACITY;
        auto entry = update_node(vector_index);
        if (!entry.is_set()) {
            return;
        }
        uint64_t row_in_vector =
            static_cast<uint64_t>(row_id - column_data_->start()) - vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        auto pin = entry.pin();
        fetch_row(pin.update_info(), row_in_vector, result, result_idx);
    }

    bool update_segment_t::check_row(int64_t row_id, const table_filter_t* filter) {
        uint64_t vector_index = static_cast<uint64_t>(row_id - column_data_->start()) / vector::DEFAULT_VECTOR_CAPACITY;
        auto entry = update_node(vector_index);
        if (!entry.is_set()) {
            return true;
        }
        uint64_t row_in_vector =
            static_cast<uint64_t>(row_id - column_data_->start()) - vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        auto pin = entry.pin();
        return check_row(pin.update_info(), row_in_vector, filter);
    }

    bool update_segment_t::row_is_updated(int64_t row_id) {
        uint64_t vector_index = static_cast<uint64_t>(row_id - column_data_->start()) / vector::DEFAULT_VECTOR_CAPACITY;
        auto entry = update_node(vector_index);
        if (!entry.is_set()) {
            return false;
        }
        uint64_t row_in_vector =
            static_cast<uint64_t>(row_id - column_data_->start()) - vector_index * vector::DEFAULT_VECTOR_CAPACITY;
        auto pin = entry.pin();
        auto& info = pin.update_info();
        bool found = false;
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            auto tuples = current->tuples();
            auto it = std::lower_bound(tuples, tuples + current->N, static_cast<uint32_t>(row_in_vector));
            if (it != tuples + current->N && *it == static_cast<uint32_t>(row_in_vector)) {
                found = true;
            }
        });
        return found;
    }

    void update_segment_t::cleanup_update_internal(update_info_t& info) {
        assert(info.has_prev());
        auto prev = info.prev;
        {
            auto pin = prev.pin();
            auto& prev_info = pin.update_info();
            prev_info.next = info.next;
        }
        if (info.has_next()) {
            auto next = info.next;
            auto next_pin = next.pin();
            auto& next_info = next_pin.update_info();
            next_info.prev = prev;
        }
    }

    void update_segment_t::cleanup_update(update_info_t& info) {
        auto lock_handle = std::unique_lock(m_);
        cleanup_update_internal(info);
    }

    core::string_heap_t& update_segment_t::heap() noexcept { return heap_; }

    undo_buffer_pointer_t update_segment_t::update_node(uint64_t vector_idx) const {
        if (!root_) {
            return undo_buffer_pointer_t();
        }
        if (vector_idx >= root_->info.size()) {
            return undo_buffer_pointer_t();
        }
        return root_->info[vector_idx];
    }

    void update_segment_t::initialize_update_info(uint64_t vector_idx) {
        if (!root_) {
            root_ = std::make_unique<update_node_t>(column_data_->block_manager().buffer_manager);
        }
        if (vector_idx < root_->info.size()) {
            return;
        }
        root_->info.reserve(vector_idx + 1);
        for (uint64_t i = root_->info.size(); i <= vector_idx; i++) {
            root_->info.emplace_back();
        }
    }

    void update_segment_t::initialize_update_info(update_info_t& info,
                                                  int64_t* ids,
                                                  const vector::indexing_vector_t& indexing,
                                                  uint64_t count,
                                                  uint64_t vector_index,
                                                  uint64_t vector_offset) {
        info.segment = this;
        info.vector_index = vector_index;
        info.prev = undo_buffer_pointer_t();
        info.next = undo_buffer_pointer_t();

        info.N = static_cast<uint32_t>(count);
        auto tuples = info.tuples();
        for (uint64_t i = 0; i < count; i++) {
            auto idx = indexing.get_index(i);
            auto id = ids[idx];
            assert(uint64_t(id) >= vector_offset && uint64_t(id) < vector_offset + vector::DEFAULT_VECTOR_CAPACITY);
            tuples[i] = static_cast<uint32_t>(static_cast<uint64_t>(id) - vector_offset);
        }
    }

    int64_t update_segment_t::start() const { return column_data_->start(); }

    void update_segment_t::initialize_update_validity(update_info_t& base_info,
                                                      const vector::vector_t& base_data,
                                                      update_info_t& update_info,
                                                      const vector::vector_t& update,
                                                      const vector::indexing_vector_t& indexing) {
        auto& update_mask = update.validity();
        auto tuple_data = update_info.data<bool>();

        if (!update_mask.all_valid()) {
            for (uint64_t i = 0; i < update_info.N; i++) {
                auto idx = indexing.get_index(i);
                tuple_data[i] = update_mask.row_is_valid(idx);
            }
        } else {
            for (uint64_t i = 0; i < update_info.N; i++) {
                tuple_data[i] = true;
            }
        }

        auto& base_mask = base_data.validity();
        auto base_tuple_data = base_info.data<bool>();
        auto base_tuples = base_info.tuples();
        if (!base_mask.all_valid()) {
            for (uint64_t i = 0; i < base_info.N; i++) {
                base_tuple_data[i] = base_mask.row_is_valid(base_tuples[i]);
            }
        } else {
            for (uint64_t i = 0; i < base_info.N; i++) {
                base_tuple_data[i] = true;
            }
        }
    }

    void
    update_segment_t::fetch_committed_validity(update_info_t& info, uint64_t result_offset, vector::vector_t& result) {
        auto& result_mask = result.validity();
        merge_validity_info(info, result_offset, result_mask);
    }

    void update_segment_t::merge_validity_loop(update_info_t& base_info,
                                               const vector::vector_t& base_data,
                                               update_info_t& update_info,
                                               const vector::vector_t& update,
                                               int64_t* ids,
                                               uint64_t count,
                                               const vector::indexing_vector_t& indexing) {
        auto& base_validity = base_data.validity();
        auto& update_validity = update.validity();
        merge_update_loop_internal<bool, vector::validity_mask_t>(
            base_info,
            &base_validity,
            update_info,
            &update_validity,
            ids,
            count,
            indexing,
            [](const vector::validity_mask_t* data, uint64_t index) { return data->row_is_valid(index); });
    }

    void update_segment_t::fetch_committed_range_validity(update_info_t& info,
                                                          uint64_t start,
                                                          uint64_t end,
                                                          uint64_t result_offset,
                                                          vector::vector_t& result) {
        auto& result_mask = result.validity();
        merge_update_info_range_validity(info, start, end, result_offset, result_mask);
    }

    void
    update_segment_t::update_merge_validity(update_info_t& info, uint64_t result_offset, vector::vector_t& result) {
        auto& result_mask = result.validity();
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            merge_validity_info(*current, result_offset, result_mask);
        });
    }

    void update_segment_t::fetch_row_validity(update_info_t& info,
                                              uint64_t row_index,
                                              vector::vector_t& result,
                                              uint64_t result_index) {
        auto& result_mask = result.validity();
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            auto info_data = current->data<bool>();
            auto tuples = current->tuples();
            auto it = std::lower_bound(tuples, tuples + current->N, row_index);
            if (it != tuples + current->N && *it == row_index) {
                result_mask.set(result_index, info_data[it - tuples]);
            }
        });
    }

    bool update_segment_t::check_row_validity(update_info_t& info, uint64_t row_index, const table_filter_t* filter) {
        bool result = true;
        update_info_t::update_for_transaction(info, [&](update_info_t* current) {
            auto info_data = current->data<bool>();
            auto tuples = current->tuples();
            auto it = std::lower_bound(tuples, tuples + current->N, row_index);
            if (it != tuples + current->N && *it == row_index) {
                const auto& const_filter = filter->cast<constant_filter_t>();
                result = const_filter.compare(info_data[it - tuples]);
            }
        });
        return result;
    }
} // namespace components::table