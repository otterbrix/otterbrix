#include "segment_tree.hpp"
#include <algorithm>
#include <cstring>

namespace core::b_plus_tree {

    segment_tree_t::iterator::iterator(segment_tree_t* seg_tree, segment_tree_t::block_metadata* metadata)
        : seg_tree_(seg_tree)
        , metadata_(metadata) {
        get_block();
    }

    segment_tree_t::iterator::iterator(const iterator& other)
        : seg_tree_(other.seg_tree_)
        , metadata_(other.metadata_) {
        get_block();
    }

    segment_tree_t::iterator::iterator(iterator&& other) noexcept
        : seg_tree_(other.seg_tree_)
        , metadata_(other.metadata_)
        , block_(other.block_) {}

    void segment_tree_t::iterator::get_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
            seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].last_used =
                std::chrono::system_clock::now();
        } else {
            block_ = nullptr;
        }
    }
    void segment_tree_t::iterator::load_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            if (!seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block) {
                seg_tree_->load_segment_(metadata_);
                block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
            }
        } else {
            assert(false && "segment_tree::iterator: out of range");
        }
    }

    segment_tree_t::r_iterator::r_iterator(segment_tree_t* seg_tree, segment_tree_t::block_metadata* metadata)
        : seg_tree_(seg_tree)
        , metadata_(metadata) {
        get_block();
    }

    segment_tree_t::r_iterator::r_iterator(const r_iterator& other)
        : seg_tree_(other.seg_tree_)
        , metadata_(other.metadata_) {
        get_block();
    }

    segment_tree_t::r_iterator::r_iterator(r_iterator&& other) noexcept
        : seg_tree_(other.seg_tree_)
        , metadata_(other.metadata_)
        , block_(other.block_) {}

    void segment_tree_t::r_iterator::get_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
            seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].last_used =
                std::chrono::system_clock::now();
        } else {
            block_ = nullptr;
        }
    }
    void segment_tree_t::r_iterator::load_block() {
        if (metadata_ < seg_tree_->metadata_end_ && metadata_ >= seg_tree_->metadata_begin_) {
            if (!seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get()) {
                seg_tree_->load_segment_(metadata_);
                block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
            }
        } else {
            assert(false && "segment_tree::r_iterator: out of range");
        }
    }

    segment_tree_t::segment_tree_t(std::pmr::memory_resource* resource,
                                   index_t (*func)(const item_data&),
                                   std::unique_ptr<filesystem::file_handle_t> file)
        : resource_(resource)
        , key_func_(func)
        , file_(std::move(file)) {
        header_ = static_cast<header_t*>(resource_->allocate(header_size, alignof(size_t)));
        header_->segments_count_ = 0;
        header_->item_count_ = 0;
        header_->unique_id_count_ = 0;
        metadata_begin_ = reinterpret_cast<block_metadata*>(header_ + 1);
        metadata_end_ = metadata_begin_;
    }

    segment_tree_t::~segment_tree_t() {
        file_.reset();
        resource_->deallocate(header_, header_size, alignof(size_t));
    }

    bool segment_tree_t::append(data_ptr_t data, uint32_t size) { return append(item_data{data, size}); }

    bool segment_tree_t::append(item_data item) {
        index_t index = key_func_(item);
        return append(index, item);
    }

    bool segment_tree_t::append(const index_t& index, item_data item) {
        if (segments_.empty()) {
            segments_.reserve(2);
            string_storage_.reserve(2);
            insert_segment_(segments_.end(), construct_new_node_(item));
            header_->unique_id_count_++;
        } else {
            // reserve +2 items to be sure that iterators won't be invalidated
            if (segments_.size() + 2 > segments_.capacity()) {
                segments_.reserve(segments_.size() * 2 + 1);
                string_storage_.reserve(segments_.size() * 2 + 1);
            }

            metadata_range range = find_range_(index);
            block_metadata* metadata = range.begin - (range.begin == metadata_end_);
            it append_node = segments_.begin() + (metadata - metadata_begin_);
            // check if item exists
            bool index_exists = false;
            for (block_metadata* meta = range.begin; meta <= range.end && meta != metadata_end_; meta++) {
                it node = segments_.begin() + (meta - metadata_begin_);
                if (!node->block) {
                    load_segment_(meta);
                }
                index_exists |= node->block->contains_index(index);
                if (node->block->contains(index, item)) {
                    return false;
                }
            }
            header_->unique_id_count_ += !index_exists;
            // go back to regular append
            metadata = range.begin - (range.begin == metadata_end_);
            append_node = segments_.begin() + (metadata - metadata_begin_);

            if (!append_node->block) {
                load_segment_(metadata);
            }

            if (append_node->block->is_memory_available(item.size)) {
                append_node->block->append(index, item);
                append_node->last_used = std::chrono::system_clock::now();
                append_node->modified = true;
                update_metadata_(append_node, metadata);
            } else {
                // check if doc can go to the neighbouring blocks
                if (append_node->block->max_index() <= index) {
                    // try next block
                    ++append_node;
                    ++metadata;

                    if (append_node == segments_.end()) {
                        insert_segment_(append_node, construct_new_node_(index, item));
                    } else {
                        if (!append_node->block) {
                            load_segment_(metadata);
                        }
                        if (append_node->block->is_memory_available(item.size)) {
                            append_node->block->append(index, item);
                            append_node->last_used = std::chrono::system_clock::now();
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                        } else {
                            insert_segment_(append_node, construct_new_node_(index, item));
                        }
                    }
                } else if (metadata->min_index >= index) {
                    if (metadata == metadata_begin_) {
                        insert_segment_(append_node, construct_new_node_(index, item));
                    } else {
                        // try block before
                        --append_node;
                        --metadata;

                        if (!append_node->block) {
                            load_segment_(metadata);
                        }
                        if (append_node->block->is_memory_available(item.size)) {
                            append_node->block->append(index, item);
                            append_node->last_used = std::chrono::system_clock::now();
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                        } else {
                            // nothing left but to split this block
                            append_node->last_used = std::chrono::system_clock::now();
                            std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result;
                            try {
                                split_result = append_node->block->split_append(index, item);
                            } catch (...) {
                                unload_old_segments_();
                                split_result = append_node->block->split_append(index, item);
                            }
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                            if (split_result.second) {
                                insert_segment_(
                                    append_node + 1,
                                    node_t{std::move(split_result.second), std::chrono::system_clock::now(), true});
                            }
                            insert_segment_(
                                append_node + 1,
                                node_t{std::move(split_result.first), std::chrono::system_clock::now(), true});
                        }
                    }
                } else {
                    // nothing left but to split this block
                    append_node->last_used = std::chrono::system_clock::now();
                    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result;
                    try {
                        split_result = append_node->block->split_append(index, item);
                    } catch (...) {
                        unload_old_segments_();
                        split_result = append_node->block->split_append(index, item);
                    }
                    append_node->modified = true;
                    update_metadata_(append_node, metadata);
                    if (split_result.second) {
                        insert_segment_(append_node + 1,
                                        node_t{std::move(split_result.second), std::chrono::system_clock::now(), true});
                    }
                    insert_segment_(append_node + 1,
                                    node_t{std::move(split_result.first), std::chrono::system_clock::now(), true});
                }
            }
        }

        header_->item_count_++;
        return true;
    }

    bool segment_tree_t::remove(data_ptr_t data, uint32_t size) { return remove(item_data{data, size}); }

    bool segment_tree_t::remove(item_data item) {
        index_t index = key_func_(item);
        return remove(index, item);
    }

    bool segment_tree_t::remove(const index_t& index, item_data item) {
        if (segments_.empty()) {
            return false;
        }

        metadata_range range = find_range_(index);
        block_metadata* metadata = range.begin;
        it remove_node = segments_.begin() + (range.begin - metadata_begin_);

        if (range.begin == range.end) {
            if (range.begin == metadata_end_) {
                return false;
            } else {
                if (!remove_node->block) {
                    load_segment_(metadata);
                }
                if (!remove_node->block->contains_index(index)) {
                    return false;
                } else {
                    size_t list_count = remove_node->block->item_count(index);
                    if (!remove_node->block->remove(item)) {
                        return false;
                    }
                    remove_node->modified = true;
                    remove_node->last_used = std::chrono::system_clock::now();
                    update_metadata_(remove_node, metadata);
                    if (list_count == 1) {
                        header_->unique_id_count_--;
                    }
                    header_->item_count_--;
                    // only way range.begin == range.end is is this block contains other indices
                    // so don`t have to delete it
                    return true;
                }
            }
        }

        for (auto meta = range.begin; meta != range.end; meta++) {
            metadata = meta;
            remove_node = segments_.begin() + (meta - metadata_begin_);
            if (!remove_node->block) {
                load_segment_(metadata);
            }

            if (remove_node->block->contains(index, item)) {
                // since items are unique, we do not have to check other blocks after
                if (remove_node->block->item_count(index) == 1) {
                    header_->unique_id_count_--;
                }
                remove_node->block->remove(index, item);

                if (remove_node->block->count() == 0) {
                    remove_segment_(remove_node);
                    header_->item_count_--;
                    return true;
                }
                break;
            }
        }

        remove_node->modified = true;
        remove_node->last_used = std::chrono::system_clock::now();
        update_metadata_(remove_node, metadata);

        // check if any of neighbouring blocks could be merged

        if (static_cast<double>(remove_node->block->available_memory()) /
                static_cast<double>(remove_node->block->block_size()) >
            merge_check) {
            it left = remove_node == segments_.begin() ? segments_.end() : remove_node - 1;
            it right = remove_node + 1;

            if (left != segments_.end() && !left->block) {
                load_segment_(metadata - 1);
            }
            if (right != segments_.end() && !right->block) {
                load_segment_(metadata + 1);
            }

            if (right != segments_.end() && remove_node->block->available_memory() >= right->block->occupied_memory()) {
                remove_node->modified = true;
                remove_node->block->merge(std::move(right->block));
                update_metadata_(remove_node, metadata);
                remove_segment_(right);
            } else if (left != segments_.end() &&
                       left->block->available_memory() >= remove_node->block->occupied_memory()) {
                left->block->merge(std::move(remove_node->block));
                left->modified = true;
                update_metadata_(left, metadata - 1);
                remove_segment_(remove_node);
            }
        }
        header_->item_count_--;
        return true;
    }

    bool segment_tree_t::remove_index(const index_t& index) {
        if (segments_.empty()) {
            return false;
        }

        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        block_metadata* metadata = range.begin;
        it remove_node = segments_.begin() + (metadata - metadata_begin_);
        if (!remove_node->block) {
            load_segment_(metadata);
        }

        if (!remove_node->block->contains_index(index)) {
            return false;
        }

        // blocks in the middle will only contains required index
        // check only first and last
        // but we have to record item count
        metadata_range delete_range = range;
        size_t count = 0;
        if (remove_node->block->unique_indices_count() == 1) {
            // keep in delete range
        } else {
            // remove required index, do not delete a block
            count += remove_node->block->item_count(index);
            remove_node->block->remove_index(index);
            remove_node->last_used = std::chrono::system_clock::now();
            remove_node->modified = true;
            update_metadata_(remove_node, metadata);
            delete_range.begin++;
        }

        if (range.end - range.begin > 1) {
            metadata = range.end - 1;
            remove_node = segments_.begin() + (metadata - metadata_begin_);
            if (remove_node->block->unique_indices_count() == 1) {
                // keep in delete range
            } else {
                // remove required index, do not delete a block
                count += remove_node->block->item_count(index);
                remove_node->block->remove_index(index);
                remove_node->last_used = std::chrono::system_clock::now();
                remove_node->modified = true;
                update_metadata_(remove_node, metadata);
                delete_range.end--;
            }
        }

        for (block_metadata* meta = delete_range.begin; meta != delete_range.end && meta < metadata_end_; meta++) {
            it node = segments_.begin() + (meta - metadata_begin_);

            if (!node->block) {
                load_segment_(meta);
            }
            count += node->block->count();
        }

        remove_range_(delete_range);
        header_->unique_id_count_--;
        header_->item_count_ -= count;
        return true;
    }

    [[nodiscard]] std::unique_ptr<segment_tree_t>
    segment_tree_t::split(std::unique_ptr<filesystem::file_handle_t> file) {
        // make no sense to split tree with 0 blocks
        assert(metadata_begin_ != metadata_end_);
        assert(header_->unique_id_count_ > 1);

        std::unique_ptr<segment_tree_t> splited_tree =
            std::make_unique<segment_tree_t>(resource_, key_func_, std::move(file));

        size_t split_size = header_->unique_id_count_ / 2;
        index_t prev_index{};
        splited_tree->segments_.reserve(segments_.size());
        splited_tree->string_storage_.reserve(string_storage_.size());
        for (auto metadata = metadata_end_ - 1; metadata >= metadata_begin_; metadata--) {
            it node = segments_.begin() + (metadata - metadata_begin_);
            if (!node->block) {
                load_segment_(metadata);
            }

            size_t count = node->block->unique_indices_count();
            assert(count != 0);
            // if indices are the same unique counter will be 1 less
            count -= prev_index == node->block->max_index();
            if (count <= split_size) {
                prev_index = node->block->min_index();
                // move this block to split_tree
                size_t item_count = node->block->count();
                node->modified = true;
                splited_tree->insert_segment_(splited_tree->segments_.begin(), std::move(*node));
                remove_segment_(node);
                split_size -= count;
                header_->item_count_ -= item_count;
                header_->unique_id_count_ -= count;
                splited_tree->header_->item_count_ += item_count;
                splited_tree->header_->unique_id_count_ += count;
            } else {
                // split required amount from that block and break the loop
                auto split_unique = static_cast<uint32_t>(split_size + (prev_index == node->block->min_index()));
                if (split_unique == 0 || split_unique == node->block->unique_indices_count()) {
                    break;
                }
                uint32_t item_count = node->block->count();
                splited_tree->insert_segment_(splited_tree->segments_.begin(),
                                              segment_tree_t::node_t{node->block->split_uniques(split_unique),
                                                                     std::chrono::system_clock::now(),
                                                                     true});
                item_count -= node->block->count();
                update_metadata_(node, metadata);
                header_->item_count_ -= item_count;
                header_->unique_id_count_ -= split_size;
                splited_tree->header_->item_count_ += item_count;
                splited_tree->header_->unique_id_count_ += split_size;
                break;
            }
        }

        return splited_tree;
    }

    void segment_tree_t::balance_with(std::unique_ptr<segment_tree_t>& other) {
        assert(min_index() > other->max_index() || max_index() < other->min_index());
        assert(header_->unique_id_count_ != other->header_->unique_id_count_ && header_->unique_id_count_ != 0 &&
               other->header_->unique_id_count_ != 0);
        // easier to check it where it is needed, then to add 2 new cases for it
        assert(header_->unique_id_count_ < other->header_->unique_id_count_);

        // we also have to make sure that same indices won't be split
        size_t rebalance_size =
            (header_->unique_id_count_ + other->header_->unique_id_count_) / 2 - header_->unique_id_count_;
        segments_.reserve(segments_.size() + other->segments_.size());
        string_storage_.reserve(string_storage_.size() + other->string_storage_.size());
        if (min_index() > other->max_index()) {
            index_t prev_index{};
            for (block_metadata* metadata = other->metadata_end_ - 1; metadata >= other->metadata_begin_; metadata--) {
                it node = other->segments_.begin() + (metadata - other->metadata_begin_);
                if (!node->block) {
                    other->load_segment_(metadata);
                }

                size_t count = node->block->unique_indices_count();
                assert(count != 0);
                // if indices are the same unique counter will be 1 less
                count -= prev_index == node->block->max_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->min_index();
                    // move this block
                    size_t item_count = node->block->count();
                    node->modified = true;
                    insert_segment_(segments_.begin(), std::move(*node));
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += count;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= count;
                } else {
                    // split required amount from that block and break the loop
                    auto split_unique =
                        static_cast<uint32_t>(rebalance_size + (prev_index == node->block->min_index()));
                    if (split_unique == 0 || split_unique == node->block->unique_indices_count()) {
                        break;
                    }
                    uint32_t item_count = node->block->count();
                    insert_segment_(segments_.begin(),
                                    segment_tree_t::node_t{node->block->split_uniques(split_unique),
                                                           std::chrono::system_clock::now(),
                                                           true});
                    item_count -= node->block->count();
                    assert(segments_.begin()->block->count() != 0 && "incorrect node split");
                    assert(node->block->count() != 0 && "incorrect node split");
                    other->update_metadata_(node, metadata);
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += rebalance_size;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= rebalance_size;
                    break;
                }
            }
        } else {
            index_t prev_index{};
            for (block_metadata* metadata = other->metadata_begin_; metadata < other->metadata_end_;) {
                it node = other->segments_.begin() + (metadata - other->metadata_begin_);
                if (!node->block) {
                    other->load_segment_(metadata);
                }

                size_t count = node->block->unique_indices_count();
                assert(count != 0);
                // if indices are the same unique counter will be 1 less
                count -= prev_index == node->block->min_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->max_index();
                    // move this block
                    size_t item_count = node->block->count();
                    node->modified = true;
                    insert_segment_(segments_.end(), std::move(*node));
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += count;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= count;
                } else {
                    // split required amount from that block and break the loop
                    if (count - rebalance_size == 0 || count - rebalance_size == node->block->unique_indices_count()) {
                        break;
                    }
                    size_t item_count = node->block->count();
                    std::unique_ptr<block_t> temp_block_ptr =
                        node->block->split_uniques(static_cast<uint32_t>(count - rebalance_size));
                    assert(temp_block_ptr->count() != 0 && "incorrect node split");
                    assert(node->block->count() != 0 && "incorrect node split");
                    insert_segment_(
                        segments_.end(),
                        segment_tree_t::node_t{std::move(node->block), std::chrono::system_clock::now(), true});
                    node->block = std::move(temp_block_ptr);
                    item_count -= node->block->count();
                    node->modified = true;
                    other->update_metadata_(node, metadata);
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += rebalance_size;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= rebalance_size;
                    break;
                }
            }
        }
    }

    void segment_tree_t::merge(std::unique_ptr<segment_tree_t>& other) {
        assert(header_->item_count_ != 0 && other->header_->item_count_ != 0);
        assert(min_index() > other->max_index() || max_index() < other->min_index());

        header_->unique_id_count_ += other->header_->unique_id_count_;
        header_->item_count_ += other->header_->item_count_;
        other->header_->item_count_ = 0;
        other->header_->unique_id_count_ = 0;
        segments_.reserve(segments_.size() + other->segments_.size());
        string_storage_.reserve(string_storage_.size() + other->string_storage_.size());
        if (min_index() > other->max_index()) {
            // insert all at begin pos
            header_->unique_id_count_ += other->header_->unique_id_count_;
            header_->item_count_ += other->header_->item_count_;
            while (!other->segments_.empty()) {
                if (!(other->segments_.end() - 1)->block) {
                    other->load_segment_(other->metadata_end_ - 1);
                }
                insert_segment_(segments_.begin(), std::move(*(other->segments_.end() - 1)));
                segments_.begin()->modified = true;
                other->remove_segment_(other->segments_.end() - 1);
            }
        } else {
            // insert all at end pos
            while (!other->segments_.empty()) {
                if (!other->segments_.begin()->block) {
                    other->load_segment_(other->metadata_begin_);
                }
                other->segments_.begin()->modified = true;
                insert_segment_(segments_.end(), std::move(*(other->segments_.begin())));
                other->remove_segment_(other->segments_.begin());
            }
        }
        other->header_->item_count_ = 0;
        other->header_->unique_id_count_ = 0;
    }

    bool segment_tree_t::contains_index(const index_t& index) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        auto node = segments_.begin() + (range.begin - metadata_begin_);
        if (!node->block) {
            load_segment_(range.begin);
        }

        return node->block->contains_index(index);
    }

    bool segment_tree_t::contains(item_data item) {
        auto index = key_func_(item);
        return contains(index, item);
    }

    bool segment_tree_t::contains(const index_t& index, item_data item) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }

            return node->block->contains(index, item);
        }
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }
            if (node->block->contains(index, item)) {
                return true;
            }
        }
        return false;
    }

    size_t segment_tree_t::item_count(const index_t& index) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return 0;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }

            return node->block->item_count(index);
        }
        size_t total = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }
            total += node->block->item_count(index);
        }
        return total;
    }

    segment_tree_t::item_data segment_tree_t::get_item(const index_t& index, size_t position) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return item_data();
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }

            return node->block->get_item(index, static_cast<uint32_t>(position));
        }
        size_t skipped_count = 0;
        size_t current_count = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }
            current_count = node->block->item_count(index);
            if (skipped_count + current_count > position) {
                return node->block->get_item(index,
                                             static_cast<uint32_t>(skipped_count + current_count - position - 1));
            }
            skipped_count += current_count;
        }
        return item_data();
    }

    void segment_tree_t::get_items(std::vector<item_data>& result, const index_t& index) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }

            return node->block->get_items(result, index);
        }

        // Running out of memory should be extreamly rare, so it will be faster to do one vector reserve
        size_t total_size = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!node->block) {
                load_segment_(range.begin);
            }
            total_size += node->block->item_count(index);
        }
        result.reserve(total_size);
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            // even if go through the same segments, we could have run out of memory while loading them
            // in which case first batch could be unloaded back, so we check again
            if (!node->block) {
                load_segment_(range.begin);
            }
            node->block->get_items(result, index);
        }
    }

    segment_tree_t::index_t segment_tree_t::min_index() const {
        if (metadata_begin_ == metadata_end_) {
            return std::numeric_limits<index_t>::min();
        } else {
            return metadata_begin_->min_index;
        }
    }

    segment_tree_t::index_t segment_tree_t::max_index() const {
        if (metadata_begin_ == metadata_end_) {
            return std::numeric_limits<index_t>::max();
        } else {
            return (metadata_end_ - 1)->max_index;
        }
    }

    size_t segment_tree_t::blocks_count() const { return segments_.size(); }

    size_t segment_tree_t::count() const { return header_->item_count_; }

    size_t segment_tree_t::unique_indices_count() const { return header_->unique_id_count_; }

    void segment_tree_t::flush() {
        close_gaps_();

        /*  header_  */
        file_->write(static_cast<void*>(header_), header_size, 0);

        /*  BLOCKS  */
        // TODO: it would be faster to flush blocks in offset order, instead of their id
        block_metadata* metadata = metadata_begin_;
        for (auto segment = segments_.begin(); segment != segments_.end(); segment++, metadata++) {
            // if segment is not loaded, it does not have to be flushed
            if (segment->block.get()) {
                assert(segment->block->count() != 0 && "block is empty");
                if (segment->modified) {
                    segment->block->recalculate_checksum();
                    file_->write(segment->block->internal_buffer(), metadata->size, metadata->file_offset);
                    segment->modified = false;
                }
            }
        }
        file_->truncate(static_cast<int64_t>(gap_tracker_.empty_spaces().front().offset));
        file_->sync();
    }

    void segment_tree_t::clean_load() {
        using components::types::physical_type;
        segments_.clear();
        string_storage_.clear();
        file_->seek(0);
        file_->read(static_cast<void*>(header_), header_size);
        metadata_end_ = metadata_begin_ + header_->segments_count_;
        gap_tracker_.init(file_->file_size(), INVALID_SIZE);

        segments_.reserve(header_->segments_count_);
        string_storage_.reserve(header_->segments_count_);
        // TODO: it would be faster to load blocks in offset order, instead of their id (especially on hard drives)
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            // call directly because there is no need to modify header
            segments_.emplace_back(
                node_t{create_initialize(resource_, key_func_, static_cast<uint32_t>(metadata->size)),
                       std::chrono::system_clock::now(),
                       false});
            string_storage_.emplace_back();
            file_->read(segments_.back().block->internal_buffer(), metadata->size, metadata->file_offset);
            assert(segments_.back().block->varify_checksum() && "block was modified outside of segment tree");
            assert(segments_.back().block->count() != 0 && "block is empty");
            segments_.back().block->restore_block();
            if (metadata->min_index.type() == physical_type::STRING) {
                auto min_index = segments_.back().block->min_index();
                string_storage_.back().first =
                    std::make_unique<std::pmr::string>(min_index.value<physical_type::STRING>(), resource_);
                metadata->min_index = index_t(*string_storage_.back().first);
            }
            if (metadata->max_index.type() == physical_type::STRING) {
                auto max_index = segments_.back().block->max_index();
                string_storage_.back().second =
                    std::make_unique<std::pmr::string>(max_index.value<physical_type::STRING>(), resource_);
                metadata->max_index = index_t(*string_storage_.back().second);
            }
        }
    }

    void segment_tree_t::lazy_load() {
        using components::types::physical_type;
        segments_.clear();
        string_storage_.clear();
        file_->seek(0);
        file_->read(static_cast<void*>(header_), header_size);
        metadata_end_ = metadata_begin_ + header_->segments_count_;
        gap_tracker_.init(file_->file_size(), std::numeric_limits<size_t>::max());

        segments_.reserve(header_->segments_count_);
        string_storage_.reserve(header_->segments_count_);
        // NOTE: while index_t is not stored on stack entirely
        // we have to load block to get min/max indices from it, if requires any heap allocations
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            segments_.emplace_back(node_t{nullptr, std::chrono::system_clock::now(), false});
            string_storage_.emplace_back();
            if (metadata->min_index.type() == physical_type::STRING ||
                metadata->max_index.type() == physical_type::STRING) {
                load_segment_(metadata);
                update_metadata_(segments_.end() - 1, metadata);
                segments_.back().block = nullptr;
            }
        }
    }

    segment_tree_t::metadata_range segment_tree_t::find_range_(const index_t& index) const {
        metadata_range result;
        result.begin =
            std::lower_bound(metadata_begin_,
                             metadata_end_,
                             index,
                             [](const block_metadata& meta, const index_t& index) { return meta.max_index < index; });
        result.end =
            std::lower_bound(result.begin, metadata_end_, index, [](const block_metadata& meta, const index_t& index) {
                return meta.min_index <= index;
            });
        return result;
    }

    // TODO: add neighbouring blocks merging if needed
    void segment_tree_t::remove_range_(metadata_range range) {
        if (range.begin == range.end) {
            return;
        }
        for (auto it = range.begin; it != range.end; it++) {
            gap_tracker_.remove_gap({it->file_offset, it->size});
        }
        gap_tracker_.clean_gaps();
        std::memmove(range.begin, range.end, static_cast<size_t>(metadata_end_ - range.end) * block_metadata_size);
        metadata_end_ -= range.end - range.begin;

        segments_.erase(segments_.begin() + (range.begin - metadata_begin_),
                        segments_.begin() + (range.end - metadata_begin_));
        string_storage_.erase(string_storage_.begin() + (range.begin - metadata_begin_),
                              string_storage_.begin() + (range.end - metadata_begin_));
        header_->segments_count_ = segments_.size();
    }

    segment_tree_t::node_t segment_tree_t::construct_new_node_(const index_t& index, item_data item) {
        std::unique_ptr<block_t> b_tree_ptr;
        try {
            b_tree_ptr =
                create_initialize(resource_,
                                  key_func_,
                                  align_to_block_size(item.size + block_t::header_size + block_t::metadata_size));
        } catch (...) {
            unload_old_segments_();
            b_tree_ptr =
                create_initialize(resource_,
                                  key_func_,
                                  align_to_block_size(item.size + block_t::header_size + block_t::metadata_size));
        }
        b_tree_ptr->append(index, item); // always true
        return {std::move(b_tree_ptr), std::chrono::system_clock::now(), true};
    }

    segment_tree_t::node_t segment_tree_t::construct_new_node_(item_data item) {
        auto index = key_func_(item);
        return construct_new_node_(index, item);
    }

    void segment_tree_t::load_segment_(block_metadata* metadata) {
        it node = segments_.begin() + (metadata - metadata_begin_);

        // if there is not enough memory, flush old blocks
        try {
            node->block = create_initialize(resource_, key_func_, static_cast<uint32_t>(metadata->size));
        } catch (...) {
            unload_old_segments_();
            node->block = create_initialize(resource_, key_func_, static_cast<uint32_t>(metadata->size));
        }

        file_->read(node->block->internal_buffer(), metadata->size, metadata->file_offset);
        assert(node->block->count() && "block stored on disk should not be empty");
        assert(node->block->varify_checksum() && "block was modified outside of segment tree");
        node->block->restore_block();
        node->last_used = std::chrono::system_clock::now();
        node->modified = false;
    }

    void segment_tree_t::unload_old_segments_() {
        close_gaps_();
        std::vector<std::pair<std::chrono::time_point<std::chrono::system_clock>, size_t>> blocks_to_unload;
        blocks_to_unload.reserve(segments_.size());
        for (size_t i = 0; i < segments_.size(); i++) {
            if (segments_[i].block.get() != nullptr) {
                blocks_to_unload.emplace_back(segments_[i].last_used, i);
            }
        }
        std::sort(blocks_to_unload.begin(), blocks_to_unload.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        size_t half_size = blocks_to_unload.size() / 2;
        for (size_t i = 0; i < half_size; i++) {
            size_t num = blocks_to_unload[i].second;
            assert(segments_[num].block->count() && "block stored on disk should not be empty");
            segments_[num].block->recalculate_checksum();
            file_->write(segments_[num].block->internal_buffer(),
                         (metadata_begin_ + num)->size,
                         (metadata_begin_ + num)->file_offset);
            segments_[num].block = nullptr;
            segments_[num].modified = false;
        }
    }

    void segment_tree_t::insert_segment_(it pos, node_t&& node) {
        node.last_used = std::chrono::system_clock::now();
        auto index = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + index;
        std::memmove(metadata + 1, metadata, (segments_.size() - static_cast<size_t>(index)) * block_metadata_size);
        metadata->file_offset = gap_tracker_.find_gap(node.block->block_size());
        metadata->size = node.block->block_size();
        segments_.insert(pos, std::move(node));
        string_storage_.emplace(string_storage_.begin() + index);
        update_metadata_(pos, metadata);
        metadata_end_++;
        header_->segments_count_ = segments_.size();
    }

    void segment_tree_t::remove_segment_(it pos) {
        auto index = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + index;
        gap_tracker_.remove_gap({metadata->file_offset, metadata->size});
        std::memmove(metadata, metadata + 1, (segments_.size() - static_cast<size_t>(index)) * block_metadata_size);
        metadata_end_--;

        segments_.erase(pos);
        string_storage_.erase(string_storage_.begin() + index);
        header_->segments_count_ = segments_.size();
    }

    void segment_tree_t::update_metadata_(it pos, block_metadata* metadata) {
        using components::types::physical_type;
        index_t min_index = pos->block->min_index();
        auto index_storage = string_storage_.begin() + (pos - segments_.begin());
        if (min_index.type() == physical_type::STRING) {
            index_storage->first =
                std::make_unique<std::pmr::string>(min_index.value<physical_type::STRING>(), resource_);
            metadata->min_index = index_t(*index_storage->first); //change reference to internal storage
        } else {
            metadata->min_index = min_index;
        }
        index_t max_index = pos->block->max_index();
        if (max_index.type() == physical_type::STRING) {
            index_storage->second =
                std::make_unique<std::pmr::string>(max_index.value<physical_type::STRING>(), resource_);
            metadata->max_index = index_t(*index_storage->second); //change reference to internal storage
        } else {
            metadata->max_index = max_index;
        }
    }

    void segment_tree_t::close_gaps_() {
        gap_tracker_.clean_gaps();
        auto& gaps = gap_tracker_.empty_spaces();
        while (gaps.size() > 1) {
            // TODO: try to close gaps with existing blocks
            size_t i = 0;
            for (block_metadata* it = metadata_begin_; it < metadata_end_; it++, i++) {
                if (it->file_offset > gaps.front().offset) {
                    it->file_offset -= gaps.front().size;
                    segments_[i].modified = true;
                }
            }
            for (size_t i = 1; i < gaps.size(); i++) {
                gaps[i].offset -= gaps.front().size;
            }
            gaps.back().size += gaps.front().size;
            gaps.erase(gaps.begin());
        }
    }

} // namespace core::b_plus_tree