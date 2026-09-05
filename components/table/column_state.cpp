#include "column_state.hpp"

#include "column_data.hpp"
#include "column_segment.hpp"
#include "storage/block_handle.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_manager.hpp"

namespace components::table {

    void column_scan_state::initialize(const types::complex_logical_type& type,
                                       const std::vector<storage_index_t>& children) {
        if (type.type() == types::logical_type::VALIDITY) {
            return;
        }
        if (type.to_physical_type() == types::physical_type::STRUCT) {
            auto& struct_children = type.child_types();
            child_states.resize(struct_children.size() + 1);

            if (children.empty()) {
                scan_child_column.resize(struct_children.size(), true);
                for (uint64_t i = 0; i < struct_children.size(); i++) {
                    child_states[i + 1].initialize(struct_children[i]);
                }
            } else {
                scan_child_column.resize(struct_children.size(), false);
                for (uint64_t i = 0; i < children.size(); i++) {
                    auto& child = children[i];
                    auto index = child.primary_index();
                    auto& child_indexes = child.child_indexes();
                    scan_child_column[index] = true;
                    child_states[index + 1].initialize(struct_children[index], child_indexes);
                }
            }
        } else if (type.to_physical_type() == types::physical_type::LIST) {
            child_states.resize(2);
            child_states[1].initialize(type.child_type());
        } else if (type.to_physical_type() == types::physical_type::ARRAY) {
            child_states.resize(2);
            child_states[1].initialize(type.child_type());
        } else {
            child_states.resize(1);
        }
    }

    void column_scan_state::initialize(const types::complex_logical_type& type) {
        std::vector<storage_index_t> children;
        initialize(type, children);
    }

    void column_scan_state::next(uint64_t count) {
        for (auto& child_state : child_states) {
            child_state.next(count);
        }
    }

    storage::buffer_handle_t* column_fetch_state::get_or_insert_handle(column_segment_t& segment) {
        return get_or_insert_handle(segment.block);
    }

    storage::buffer_handle_t*
    column_fetch_state::get_or_insert_handle(std::shared_ptr<storage::block_handle_t>& block) {
        auto primary_id = block->block_id();

        auto entry = handles.find(primary_id);
        if (entry == handles.end()) {
            auto& buffer_manager = block->block_manager.buffer_manager;
            auto pinned = buffer_manager.pin(block);
            if (pinned.has_error()) {
                fetch_error = pinned.error();
                return nullptr;
            }
            auto pinned_entry = handles.insert({primary_id, std::move(pinned.value())});
            return &pinned_entry.first->second;
        } else {
            return &entry->second;
        }
    }

    uncompressed_string_segment_state::~uncompressed_string_segment_state() {
        while (head) {
            head = std::move(head->next);
        }
    }

    std::shared_ptr<storage::block_handle_t>
    uncompressed_string_segment_state::handle(storage::block_manager_t& manager, uint64_t block_id) {
        auto entry = handles_.find(block_id);
        if (entry != handles_.end()) {
            return entry->second;
        }
        auto result = manager.register_block(block_id);
        handles_.insert(std::make_pair(block_id, result));
        return result;
    }

    bool uncompressed_string_segment_state::register_block(storage::block_manager_t& manager, uint64_t block_id) {
        if (handles_.find(block_id) != handles_.end()) {
            // Already registered: the persisted list named the same block twice. The writer
            // dedupes (persist_string_overflow), so this is a corrupt pointer stream, and the
            // caller must SEE it -- it latches the false and column_data_t::initialize_column
            // reports data_corruption. Not a throw: this runs while a table is being opened,
            // where an exception would take the host process down (rules 2/6).
            return false;
        }
        // register_block() only hands out (or revives) a weak-registry entry for an EXISTING
        // file block -- it allocates nothing from the free list, so reopening a table with
        // big strings still allocates ZERO new blocks. The handle is UNLOADED and
        // is_reloadable() (id < MAXIMUM_BLOCK), so the pool may evict and re-read it exactly
        // like a packed data/validity block.
        auto result = manager.register_block(block_id);
        handles_.insert(std::make_pair(block_id, std::move(result)));
        on_disk_blocks.push_back(block_id);
        return true;
    }

    std::shared_ptr<storage::block_handle_t> uncompressed_string_segment_state::registered_handle(uint64_t block_id) {
        auto entry = handles_.find(block_id);
        if (entry == handles_.end()) {
            return nullptr;
        }
        return entry->second;
    }

} // namespace components::table