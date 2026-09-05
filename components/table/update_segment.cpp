#include "update_segment.hpp"

#include <algorithm>
#include <cstdio>

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

    // NOTE ON WRITE-WRITE CONFLICTS: a check_for_conflicts() used to stand here and walk
    // base_info.next. That chain was NEVER BUILT (see the note inside update()), so the walk
    // visited zero nodes and the check could not fire — it is deleted as the dead half of the
    // same fiction, not as a behaviour change. Real update-vs-update conflict detection needs
    // a real per-transaction chain first.

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

    void report_unreachable_update_node(const char* where, const core::error_t& error) {
        std::fprintf(stderr,
                     "components::table::%s: the update node could not be pinned (%d: %s); the read below it is "
                     "left without that overlay\n",
                     where,
                     static_cast<int>(error.type),
                     error.what.c_str());
    }

    // A default-constructed reference names no node, and undo_buffer_pointer_t() is exactly
    // that. This used to read `return {*entry, position};`, which forms a reference to *nullptr
    // for such a reference. (The historical caller — update()'s never-assigned `node_ref` —
    // is gone; the guard stays because the contract "an unset reference has no pointer" is
    // this type's own, not that caller's.)
    undo_buffer_pointer_t undo_buffer_reference::buffer_pointer() {
        if (!entry) {
            return undo_buffer_pointer_t();
        }
        return {*entry, position};
    }

    // NO SILENT FAILURE ON THIS PATH (rule 6).
    //
    // This used to swallow the refusal in an assert and, under NDEBUG, return
    // undo_buffer_reference(*entry, buffer_handle_t{}, position). Every consumer then calls
    // update_info(), which is reinterpret_cast<update_info_t*>(handle.ptr() + position) -- with
    // an empty handle that is `nullptr + position`, i.e. literally a base_info that is not an
    // address, produced by the one function whose job is to hand out addresses. The premise the
    // assert rested on ("a resident TRANSACTION block cannot fail to pin") is also not something
    // this function can check: standard_buffer_manager_t::pin only takes the no-op fast path
    // while the block is still block_state::LOADED, and the reload branch below it reports
    // out_of_memory, data_corruption and io_error.
    //
    // So the refusal is returned. The one caller that owns an error channel forwards it
    // (update_segment_t::update); the read paths that do not report it and
    // stop -- see report_unreachable_update_node above for where the missing channel starts.
    core::result_wrapper_t<undo_buffer_reference> undo_buffer_pointer_t::pin() const {
        // These two stay asserts, and the distinction is the whole point of the change above:
        // they are CALLER preconditions (every caller guards with is_set(), and a position
        // beyond the entry is a bug in whoever built the pointer), not runtime failures the
        // caller could be told about. What was wrong before was asserting away a RUNTIME
        // refusal that has an error value and then continuing on a fabricated address.
        // An error_t here would also need a resource to put its message on, and a pointer has
        // none -- std::pmr::string with no resource is the forbidden default resource.
        assert(entry && "undo_buffer_pointer_t::pin: the pointer names no entry");
        assert(entry->capacity >= position);
        auto pinned = entry->buffer_manager.pin(entry->block);
        if (pinned.has_error()) {
            return pinned.convert_error<undo_buffer_reference>();
        }
        return undo_buffer_reference(*entry, std::move(pinned.value()), position);
    }

    core::result_wrapper_t<undo_buffer_reference> undo_buffer_allocator_t::allocate(uint64_t alloc_len) {
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
                // Genuine fresh transaction-memory allocation — the OOM-able site.
                auto registered = buffer_manager.register_small_memory(storage::memory_tag::TRANSACTION, capacity);
                if (registered.has_error()) {
                    return registered.convert_error<undo_buffer_reference>();
                }
                entry->block = std::move(registered.value());
                auto pinned = buffer_manager.pin(entry->block);
                if (pinned.has_error()) {
                    return pinned.convert_error<undo_buffer_reference>();
                }
                handle = std::move(pinned.value());
            } else {
                auto allocated = buffer_manager.allocate(storage::memory_tag::TRANSACTION, capacity, false);
                if (allocated.has_error()) {
                    return allocated.convert_error<undo_buffer_reference>();
                }
                handle = std::move(allocated.value());
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
            auto pinned = buffer_manager.pin(head->block);
            if (pinned.has_error()) {
                return pinned.convert_error<undo_buffer_reference>();
            }
            handle = std::move(pinned.value());
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

    bool update_segment_t::has_updates(uint64_t vector_index) { return update_node(vector_index).is_set(); }

    bool update_segment_t::has_updates(int64_t start_row_idx, int64_t end_row_idx) {
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
        auto node = update_node(vector_index);
        if (!node.is_set()) {
            return;
        }
        assert(result.get_vector_type() == vector::vector_type::FLAT);
        auto pin = node.pin();
        if (pin.has_error()) {
            report_unreachable_update_node("update_segment_t::fetch_updates", pin.error());
            return;
        }
        fetch_update(pin.value().update_info(), result_offset, result);
    }

    void update_segment_t::fetch_committed(uint64_t vector_index, uint64_t result_offset, vector::vector_t& result) {
        auto node = update_node(vector_index);
        if (!node.is_set()) {
            return;
        }
        assert(result.get_vector_type() == vector::vector_type::FLAT);
        auto pin = node.pin();
        if (pin.has_error()) {
            report_unreachable_update_node("update_segment_t::fetch_committed", pin.error());
            return;
        }
        fetch_committed(pin.value().update_info(), result_offset, result);
    }

    void update_segment_t::fetch_committed_range(int64_t start_row,
                                                 uint64_t count,
                                                 vector::vector_t& result,
                                                 uint64_t result_offset_base) {
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
            if (pin.has_error()) {
                report_unreachable_update_node("update_segment_t::fetch_committed_range", pin.error());
                continue;
            }
            uint64_t start_in_vector = vector_idx == start_vector ? static_cast<uint64_t>(start_row) -
                                                                        start_vector * vector::DEFAULT_VECTOR_CAPACITY
                                                                  : 0;
            uint64_t end_in_vector = vector_idx == end_vector ? end_row - end_vector * vector::DEFAULT_VECTOR_CAPACITY
                                                              : vector::DEFAULT_VECTOR_CAPACITY;
            assert(start_in_vector < end_in_vector);
            assert(end_in_vector > 0 && end_in_vector <= vector::DEFAULT_VECTOR_CAPACITY);
            uint64_t result_offset = vector_idx * vector::DEFAULT_VECTOR_CAPACITY + start_in_vector -
                                     static_cast<uint64_t>(start_row) + result_offset_base;
            fetch_committed_range(pin.value().update_info(), start_in_vector, end_in_vector, result_offset, result);
        }
    }

    core::result_wrapper_t<bool> update_segment_t::update(uint64_t column_index,
                                                          vector::vector_t& update,
                                                          int64_t* ids,
                                                          uint64_t count,
                                                          vector::vector_t& base_data) {
        update.flatten(count);

        if (count == 0) {
            return true;
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
            if (root_pin.has_error()) {
                return root_pin.convert_error<bool>(); // out_of_memory / data_corruption / io_error
            }
            auto& base_info = root_pin.value().update_info();

            // WHAT THIS LEG REALLY IS, said plainly: an IN-PLACE merge into the vector's one
            // root node. The per-transaction undo machinery that used to be sketched here was
            // FICTION — it built the "transaction node" into a function-local heap buffer and
            // linked it through an `undo_buffer_reference node_ref` that was default-constructed
            // and NEVER assigned, so every link it wrote was the null pointer: base_info.next
            // stayed unset forever, next_info.prev was CLEARED through a branch that could not
            // even be entered (node->next copies the always-unset base_info.next), and the node
            // itself died with the frame. Nothing ever walked a chain, so check_for_conflicts
            // iterated zero nodes and no write-write conflict could fire. The pretence is
            // excised rather than kept: updates in this tree publish into the root node
            // immediately and rollback of updates is UNIMPLEMENTED — a caller that needs it
            // must build a real per-transaction chain, not un-comment this one.
            //
            // merge_update still needs a scratch update_info_t: phase 1 of
            // merge_update_loop_internal composes the superseded values into its arrays before
            // phase 2 rewrites base_info. The scratch is named for what it is and dropped.
            std::unique_ptr<std::byte[]> undo_scratch_data;
            update_info_t* undo_scratch = create_empty_update_info(type_size_, count, undo_scratch_data);
            undo_scratch->segment = this;
            undo_scratch->vector_index = vector_index;
            undo_scratch->N = 0;
            undo_scratch->column_index = column_index;
            undo_scratch->next = undo_buffer_pointer_t();
            undo_scratch->prev = undo_buffer_pointer_t();

            merge_update(base_info, base_data, *undo_scratch, update, ids, count, indexing);
        } else {
            uint64_t alloc_size = update_info_t::allocation_size(type_size_);
            auto allocated = root_->buffer_allocator.allocate(alloc_size);
            if (allocated.has_error()) {
                return allocated.convert_error<bool>(); // out_of_memory
            }
            auto& handle = allocated.value();
            auto& update_info = handle.update_info();
            update_info.initialize();
            update_info.column_index = column_index;

            initialize_update_info(update_info, ids, indexing, count, vector_index, vector_offset);

            // Same excision as the merge leg above: the "transaction node" here was a
            // function-local scratch whose links were all null (node_ref never assigned).
            // initialize_update still wants it — its second half writes the superseded base
            // values into the scratch's arrays — so it stays as a named scratch and dies here.
            std::unique_ptr<std::byte[]> undo_scratch_data;
            update_info_t* undo_scratch = create_empty_update_info(type_size_, count, undo_scratch_data);

            initialize_update_info(*undo_scratch, ids, indexing, count, vector_index, vector_offset);

            initialize_update(*undo_scratch, base_data, update_info, update, indexing);

            update_info.next = undo_buffer_pointer_t();
            update_info.prev = undo_buffer_pointer_t();

            root_->info[vector_index] = handle.buffer_pointer();
        }
        return true;
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
        if (pin.has_error()) {
            report_unreachable_update_node("update_segment_t::fetch_row", pin.error());
            return;
        }
        fetch_row(pin.value().update_info(), row_in_vector, result, result_idx);
    }

    core::string_buffer_t& update_segment_t::heap() noexcept { return heap_; }

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

} // namespace components::table