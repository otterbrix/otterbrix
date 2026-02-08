#include "column_state.hpp"

#include "column_data.hpp"
#include "column_segment.hpp"
#include "storage/block_handle.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_manager.hpp"
#include <regex>

namespace components::table {

    bool constant_filter_t::compare(const types::logical_value_t& value) const {
        if (filter_type == expressions::compare_type::regex) {
            auto str = std::string(value.value<std::string_view>());
            auto pattern = std::string(constant.value<std::string_view>());
            return std::regex_search(str, std::regex(pattern));
        }
        auto comp = value.compare(constant);
        if (comp == types::compare_t::equals) {
            switch (filter_type) {
                case expressions::compare_type::eq:
                case expressions::compare_type::gte:
                case expressions::compare_type::lte:
                case expressions::compare_type::all_true:
                    return true;
                default:
                    return false;
            }
        } else if (comp == types::compare_t::less) {
            switch (filter_type) {
                case expressions::compare_type::ne:
                case expressions::compare_type::lt:
                case expressions::compare_type::lte:
                case expressions::compare_type::all_true:
                    return true;
                default:
                    return false;
            }
        } else {
            switch (filter_type) {
                case expressions::compare_type::ne:
                case expressions::compare_type::gt:
                case expressions::compare_type::gte:
                case expressions::compare_type::all_true:
                    return true;
                default:
                    return false;
            }
        }
    }

    bool constant_filter_t::equals(const table_filter_t& other) const {
        return constant == other.cast<constant_filter_t>().constant && table_filter_t::equals(other);
    }

    std::unique_ptr<table_filter_t> constant_filter_t::copy() const {
        return std::make_unique<constant_filter_t>(filter_type, constant, table_indices);
    }

    bool conjunction_filter_t::equals(const table_filter_t& other) const {
        return table_filter_t::equals(other) && child_filters == other.cast<conjunction_filter_t>().child_filters;
    }

    std::unique_ptr<table_filter_t> conjunction_or_filter_t::copy() const {
        return std::make_unique<conjunction_or_filter_t>();
    }

    std::unique_ptr<table_filter_t> conjunction_and_filter_t::copy() const {
        return std::make_unique<conjunction_and_filter_t>();
    }

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

    storage::buffer_handle_t& column_fetch_state::get_or_insert_handle(column_segment_t& segment) {
        auto primary_id = segment.block->block_id();

        auto entry = handles.find(primary_id);
        if (entry == handles.end()) {
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);
            auto pinned_entry = handles.insert({primary_id, std::move(handle)});
            return pinned_entry.first->second;
        } else {
            return entry->second;
        }
    }

    uncompressed_string_segment_state::~uncompressed_string_segment_state() {
        while (head) {
            head = std::move(head->next);
        }
    }

    std::shared_ptr<storage::block_handle_t>
    uncompressed_string_segment_state::handle(storage::block_manager_t& manager, uint32_t block_id) {
        std::lock_guard lock(block_lock_);
        auto entry = handles_.find(block_id);
        if (entry != handles_.end()) {
            return entry->second;
        }
        auto result = manager.register_block(block_id);
        handles_.insert(std::make_pair(block_id, result));
        return result;
    }

    void uncompressed_string_segment_state::register_block(storage::block_manager_t& manager, uint32_t block_id) {
        std::lock_guard lock(block_lock_);
        auto entry = handles_.find(block_id);
        if (entry != handles_.end()) {
            throw std::runtime_error("uncompressed_string_segment_state::register_block - block id " +
                                     std::to_string(block_id) + " already exists");
        }
        auto result = manager.register_block(block_id);
        handles_.insert(std::make_pair(block_id, std::move(result)));
        on_disk_blocks.push_back(block_id);
    }

} // namespace components::table
