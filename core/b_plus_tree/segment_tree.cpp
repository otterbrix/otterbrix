#include "segment_tree.hpp"

#include "absl/crc/crc32c.h"

#include <core/file/local_file_system.hpp>

#include <algorithm>
#include <cstring>

#ifdef DEV_MODE
#include <atomic>
#endif

namespace core::b_plus_tree {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_leaf_flushes{0};
        std::atomic<uint64_t> g_leaf_flushes_without_changes{0};
    } // namespace

    uint64_t leaf_flushes() noexcept { return g_leaf_flushes.load(std::memory_order_relaxed); }
    uint64_t leaf_flushes_without_changes() noexcept {
        return g_leaf_flushes_without_changes.load(std::memory_order_relaxed);
    }
    void reset_leaf_flushes() noexcept {
        g_leaf_flushes.store(0, std::memory_order_relaxed);
        g_leaf_flushes_without_changes.store(0, std::memory_order_relaxed);
    }
#endif

#ifdef DEV_MODE
    namespace {
        size_t g_max_segments_override = 0;
    } // namespace

    void dev_set_max_segments(size_t limit) noexcept { g_max_segments_override = limit; }
    size_t max_segments_limit() noexcept {
        return g_max_segments_override != 0 ? g_max_segments_override : segment_tree_t::max_segments;
    }
#else
    namespace {
        constexpr size_t max_segments_limit() noexcept { return segment_tree_t::max_segments; }
    } // namespace
#endif

    std::string_view to_string(load_failure_t failure) noexcept {
        switch (failure) {
            case load_failure_t::none:
                return "none";
            case load_failure_t::data_corruption:
                return "data corruption: what came off the disk is not what was written to it";
            case load_failure_t::io_error:
                return "io error: the file would not hand a block over";
            case load_failure_t::out_of_memory:
                return "out of memory: no room for a block, even after evicting this leaf";
            case load_failure_t::capacity_exceeded:
                return "capacity exceeded: this leaf's metadata array is full";
        }
        return "unknown";
    }

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
            // one block per step
            seg_tree_->begin_operation_();
            if (!seg_tree_->try_load_(metadata_)) {
                block_ = nullptr;
                return;
            }
            block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
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
            seg_tree_->begin_operation_();
            if (!seg_tree_->try_load_(metadata_)) {
                block_ = nullptr;
                return;
            }
            block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
        } else {
            assert(false && "segment_tree::r_iterator: out of range");
        }
    }

    segment_tree_t::segment_tree_t(core::pmr::budget_resource_t* resource,
                                   index_t (*func)(const item_data&),
                                   std::unique_ptr<filesystem::file_handle_t> file)
        : resource_(resource)
        , key_func_(func)
        , file_(std::move(file)) {
        initialize_header_region_();
    }

    segment_tree_t::segment_tree_t(core::pmr::budget_resource_t* resource,
                                   index_t (*func)(const item_data&),
                                   filesystem::local_file_system_t& fs,
                                   filesystem::path_t file_path)
        : resource_(resource)
        , key_func_(func)
        , fs_(&fs)
        , file_path_(std::move(file_path)) {
        initialize_header_region_();
    }

    void segment_tree_t::initialize_header_region_() {
        header_ = static_cast<header_t*>(resource_->allocate(header_size, alignof(size_t)));
        // without zeroing, leftover pool bytes past metadata_end_ leak into the file, differing across runs
        std::memset(static_cast<void*>(header_), 0, header_size);
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

    segment_tree_t::file_lease_t segment_tree_t::lease_file_() const {
        if (file_) {
            return file_lease_t{file_.get(), nullptr};
        }
        // FILE_CREATE, deliberately — a leaf built but never flushed has no file until its first flush.
        auto opened = filesystem::open_file(*fs_,
                                            file_path_,
                                            filesystem::file_flags::READ | filesystem::file_flags::WRITE |
                                                filesystem::file_flags::FILE_CREATE);
        if (!opened) {
            return file_lease_t{};
        }
        return file_lease_t{opened.get(), std::move(opened)};
    }

    size_t segment_tree_t::header_region_checksum_() const {
        const auto* region = reinterpret_cast<const char*>(header_) + sizeof(header_->header_checksum_);
        const size_t region_size = header_size - sizeof(header_->header_checksum_);
        return static_cast<size_t>(static_cast<uint32_t>(absl::ComputeCrc32c(absl::string_view(region, region_size))));
    }

    bool segment_tree_t::append(data_ptr_t data, uint32_t size) { return append(item_data{data, size}); }

    bool segment_tree_t::append(item_data item) {
        index_t index = key_func_(item);
        return append(index, item);
    }

    bool segment_tree_t::append(const index_t& index, item_data item) {
        if (abandoned()) {
            return false;
        }
        begin_operation_();
        mark_dirty_();
        if (segments_.empty()) {
            segments_.reserve(2);
            string_storage_.reserve(2);
            node_t fresh = construct_new_node_(item);
            if (!fresh.block) { // nothing counted yet on this path, so false leaves the leaf as it was
                return false;
            }
            if (!insert_segment_(segments_.end(), std::move(fresh))) {
                return false;
            }
            header_->unique_id_count_++;
        } else {
            if (segments_.size() + 2 > segments_.capacity()) { // +2 so iterators below won't be invalidated
                segments_.reserve(segments_.size() * 2 + 1);
                string_storage_.reserve(segments_.size() * 2 + 1);
            }

            metadata_range range = find_range_(index);
            block_metadata* metadata = range.begin - (range.begin == metadata_end_);
            it append_node = segments_.begin() + (metadata - metadata_begin_);
            bool index_exists = false;
            for (block_metadata* meta = range.begin; meta <= range.end && meta != metadata_end_; meta++) {
                it node = segments_.begin() + (meta - metadata_begin_);
                if (!try_load_(meta)) {
                    return false; // nothing changed yet
                }
                index_exists |= node->block->contains_index(index);
                if (node->block->contains(index, item)) {
                    return false;
                }
            }
            metadata = range.begin - (range.begin == metadata_end_);
            append_node = segments_.begin() + (metadata - metadata_begin_);

            if (!try_load_(metadata)) {
                return false;
            }
            header_->unique_id_count_ += !index_exists;

            if (append_node->block->is_memory_available(item.size)) {
                append_node->block->append(index, item);
                append_node->last_used = std::chrono::system_clock::now();
                append_node->modified = true;
                update_metadata_(append_node, metadata);
            } else {
                if (append_node->block->max_index() <= index) {
                    ++append_node;
                    ++metadata;

                    if (append_node == segments_.end()) {
                        node_t fresh = construct_new_node_(index, item);
                        if (!fresh.block) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                        if (!insert_segment_(append_node, std::move(fresh))) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                    } else {
                        if (!try_load_(metadata)) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                        if (append_node->block->is_memory_available(item.size)) {
                            append_node->block->append(index, item);
                            append_node->last_used = std::chrono::system_clock::now();
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                        } else {
                            node_t fresh = construct_new_node_(index, item);
                            if (!fresh.block) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                            if (!insert_segment_(append_node, std::move(fresh))) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                        }
                    }
                } else if (metadata->min_index >= index) {
                    if (metadata == metadata_begin_) {
                        node_t fresh = construct_new_node_(index, item);
                        if (!fresh.block) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                        if (!insert_segment_(append_node, std::move(fresh))) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                    } else {
                        --append_node;
                        --metadata;

                        if (!try_load_(metadata)) {
                            header_->unique_id_count_ -= !index_exists;
                            return false;
                        }
                        if (append_node->block->is_memory_available(item.size)) {
                            append_node->block->append(index, item);
                            append_node->last_used = std::chrono::system_clock::now();
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                        } else {
                            // room for both halves is reserved before split_append() takes the block apart
                            if (!reserve_segments_(2)) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                            append_node->last_used = std::chrono::system_clock::now();
                            std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result =
                                append_node->block->split_append(index, item);
                            if (!split_result.first) {
                                unload_old_segments_();
                                split_result = append_node->block->split_append(index, item);
                            }
                            if (!split_result.first) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                            if (split_result.second &&
                                !insert_segment_(
                                    append_node + 1,
                                    node_t{std::move(split_result.second), std::chrono::system_clock::now(), true})) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                            if (!insert_segment_(
                                    append_node + 1,
                                    node_t{std::move(split_result.first), std::chrono::system_clock::now(), true})) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                        }
                    }
                } else {
                    if (!reserve_segments_(2)) { // same reservation as the split above
                        header_->unique_id_count_ -= !index_exists;
                        return false;
                    }
                    append_node->last_used = std::chrono::system_clock::now();
                    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result =
                        append_node->block->split_append(index, item);
                    if (!split_result.first) {
                        unload_old_segments_();
                        split_result = append_node->block->split_append(index, item);
                    }
                    if (!split_result.first) {
                        header_->unique_id_count_ -= !index_exists;
                        return false;
                    }
                    append_node->modified = true;
                    update_metadata_(append_node, metadata);
                    if (split_result.second &&
                        !insert_segment_(
                            append_node + 1,
                            node_t{std::move(split_result.second), std::chrono::system_clock::now(), true})) {
                        header_->unique_id_count_ -= !index_exists;
                        return false;
                    }
                    if (!insert_segment_(
                            append_node + 1,
                            node_t{std::move(split_result.first), std::chrono::system_clock::now(), true})) {
                        header_->unique_id_count_ -= !index_exists;
                        return false;
                    }
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
        begin_operation_();
        mark_dirty_();
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
                if (!try_load_(metadata)) {
                    return false;
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
                    return true;
                }
            }
        }

        for (auto meta = range.begin; meta != range.end; meta++) {
            metadata = meta;
            remove_node = segments_.begin() + (meta - metadata_begin_);
            if (!try_load_(metadata)) {
                return false; // earlier blocks were only read
            }

            if (remove_node->block->contains(index, item)) { // items are unique; other blocks need no check
                bool last_copy_in_block = remove_node->block->item_count(index) == 1;
                // a key straddling several blocks decrements unique_id_count_ only once it's gone from all of
                // them; asked before the removal, so a block that will not load leaves the leaf as it was
                bool index_still_present = false;
                if (last_copy_in_block) {
                    for (block_metadata* probe = range.begin; probe != range.end; probe++) {
                        if (probe == meta) {
                            continue;
                        }
                        it node = segments_.begin() + (probe - metadata_begin_);
                        if (!try_load_(probe)) {
                            return false;
                        }
                        if (node->block->contains_index(index)) {
                            index_still_present = true;
                            break;
                        }
                    }
                }
                remove_node->block->remove(index, item);
                if (last_copy_in_block && !index_still_present) {
                    header_->unique_id_count_--;
                }

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

        if (static_cast<double>(remove_node->block->available_memory()) /
                static_cast<double>(remove_node->block->block_size()) >
            merge_check) {
            it left = remove_node == segments_.begin() ? segments_.end() : remove_node - 1;
            it right = remove_node + 1;

            // the item is already gone; a neighbour that will not load just leaves this merge undone
            if (left != segments_.end() && !try_load_(metadata - 1)) {
                left = segments_.end();
            }
            if (right != segments_.end() && !try_load_(metadata + 1)) {
                right = segments_.end();
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
        begin_operation_();
        mark_dirty_();
        if (segments_.empty()) {
            return false;
        }

        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        block_metadata* metadata = range.begin;
        it remove_node = segments_.begin() + (metadata - metadata_begin_);
        if (!try_load_(metadata)) {
            return false;
        }

        if (!remove_node->block->contains_index(index)) {
            return false;
        }
        // every block of the range is read below (the ends to trim, the middles to count), so all of them
        // are brought in before anything changes
        for (block_metadata* meta = range.begin + 1; meta < range.end; meta++) {
            if (!try_load_(meta)) {
                return false;
            }
        }

        // only the first and last block of the range may keep other indices; middles are deleted whole
        metadata_range delete_range = range;
        size_t count = 0;
        if (remove_node->block->unique_indices_count() == 1) {
            // keep in delete range
        } else {
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
            count += node->block->count();
        }

        remove_range_(delete_range);
        header_->unique_id_count_--;
        header_->item_count_ -= count;
        return true;
    }

    [[nodiscard]] std::unique_ptr<segment_tree_t>
    segment_tree_t::split(std::unique_ptr<filesystem::file_handle_t> file) {
        return split_into_(std::make_unique<segment_tree_t>(resource_, key_func_, std::move(file)));
    }

    [[nodiscard]] std::unique_ptr<segment_tree_t> segment_tree_t::split(filesystem::path_t new_file_path) {
        // only a lazy-mode leaf can hand out a lazy sibling: the fs it opens through is its own
        assert(fs_ != nullptr && "segment_tree_t::split(path) requires a lazy-mode leaf");
        return split_into_(std::make_unique<segment_tree_t>(resource_, key_func_, *fs_, std::move(new_file_path)));
    }

    [[nodiscard]] std::unique_ptr<segment_tree_t>
    segment_tree_t::split_into_(std::unique_ptr<segment_tree_t> splited_tree) {
        assert(metadata_begin_ != metadata_end_);
        assert(header_->unique_id_count_ > 1);
        begin_operation_();
        splited_tree->begin_operation_();

        size_t split_size = header_->unique_id_count_ / 2;
        index_t prev_index{};
        splited_tree->segments_.reserve(segments_.size());
        splited_tree->string_storage_.reserve(string_storage_.size());
        for (auto metadata = metadata_end_ - 1; metadata >= metadata_begin_; metadata--) {
            it node = segments_.begin() + (metadata - metadata_begin_);
            // a block that will not load ends the walk: what moved so far is a whole, consistent split
            // an unread stand-in answers unique_indices_count()==0; the subtraction below would wrap
            if (!try_load_(metadata) || node->unreadable || node->block->unique_indices_count() == 0) {
                break;
            }

            size_t count = node->block->unique_indices_count();
            count -= prev_index == node->block->max_index(); // one less when indices are the same
            if (count <= split_size) {
                prev_index = node->block->min_index();
                size_t item_count = node->block->count();
                node->modified = true;
                if (!splited_tree->insert_segment_(
                        splited_tree->segments_.begin(),
                        std::move(*node),
                        std::move(string_storage_[static_cast<size_t>(metadata - metadata_begin_)]))) {
                    break; // destination leaf is full and poisoned itself; this side's bytes stay readable
                }
                remove_segment_(node);
                split_size -= count;
                header_->item_count_ -= item_count;
                header_->unique_id_count_ -= count;
                splited_tree->header_->item_count_ += item_count;
                splited_tree->header_->unique_id_count_ += count;
            } else {
                auto split_unique = static_cast<uint32_t>(split_size + (prev_index == node->block->min_index()));
                if (split_unique == 0 || split_unique == node->block->unique_indices_count()) {
                    break;
                }
                // room reserved before split_uniques() moves items out, so a later refusal can't destroy them
                if (!splited_tree->reserve_segments_(1)) {
                    break;
                }
                uint32_t item_count = node->block->count();
                std::unique_ptr<block_t> moved = node->block->split_uniques(split_unique);
                if (!moved) {
                    report_failure_(load_failure_t::out_of_memory);
                    break;
                }
                if (!splited_tree->insert_segment_(
                        splited_tree->segments_.begin(),
                        segment_tree_t::node_t{std::move(moved), std::chrono::system_clock::now(), true})) {
                    break;
                }
                item_count -= node->block->count(); // bytes changed; must be written or the split unwinds on reload
                node->modified = true;
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
        begin_operation_();
        other->begin_operation_();
        mark_dirty_(); // both this leaf and the neighbour it trades blocks with change
        other->mark_dirty_();
        assert(min_index() > other->max_index() || max_index() < other->min_index());
        assert(header_->unique_id_count_ != other->header_->unique_id_count_ && header_->unique_id_count_ != 0 &&
               other->header_->unique_id_count_ != 0);
        assert(header_->unique_id_count_ < other->header_->unique_id_count_);

        size_t rebalance_size =
            (header_->unique_id_count_ + other->header_->unique_id_count_) / 2 - header_->unique_id_count_;
        segments_.reserve(segments_.size() + other->segments_.size());
        string_storage_.reserve(string_storage_.size() + other->string_storage_.size());
        if (min_index() > other->max_index()) {
            index_t prev_index{};
            for (block_metadata* metadata = other->metadata_end_ - 1; metadata >= other->metadata_begin_; metadata--) {
                it node = other->segments_.begin() + (metadata - other->metadata_begin_);
                if (!other->try_load_(metadata) || node->unreadable || node->block->unique_indices_count() == 0) {
                    break; // same stand-in hazard as the walk in split() above
                }

                size_t count = node->block->unique_indices_count();
                count -= prev_index == node->block->max_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->min_index();
                    size_t item_count = node->block->count();
                    node->modified = true;
                    if (!insert_segment_(
                            segments_.begin(),
                            std::move(*node),
                            std::move(
                                other->string_storage_[static_cast<size_t>(metadata - other->metadata_begin_)]))) {
                        break;
                    }
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += count;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= count;
                } else {
                    auto split_unique =
                        static_cast<uint32_t>(rebalance_size + (prev_index == node->block->min_index()));
                    if (split_unique == 0 || split_unique == node->block->unique_indices_count()) {
                        break;
                    }
                    if (!reserve_segments_(1)) { // same as in split(): room is asked for before splitting
                        break;
                    }
                    uint32_t item_count = node->block->count();
                    std::unique_ptr<block_t> moved = node->block->split_uniques(split_unique);
                    if (!moved) {
                        report_failure_(load_failure_t::out_of_memory);
                        break;
                    }
                    if (!insert_segment_(
                            segments_.begin(),
                            segment_tree_t::node_t{std::move(moved), std::chrono::system_clock::now(), true})) {
                        break;
                    }
                    item_count -= node->block->count();
                    assert(segments_.begin()->block->count() != 0 && "incorrect node split");
                    assert(node->block->count() != 0 && "incorrect node split");
                    node->modified = true; // donor's block was shrunk in place, so it has to be written
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
                if (!other->try_load_(metadata) || node->unreadable || node->block->unique_indices_count() == 0) {
                    break; // same stand-in hazard as the walk in split() above
                }

                size_t count = node->block->unique_indices_count();
                count -= prev_index == node->block->min_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->max_index();
                    size_t item_count = node->block->count();
                    node->modified = true;
                    if (!insert_segment_(
                            segments_.end(),
                            std::move(*node),
                            std::move(
                                other->string_storage_[static_cast<size_t>(metadata - other->metadata_begin_)]))) {
                        break;
                    }
                    other->remove_segment_(node);
                    rebalance_size -= count;
                    header_->item_count_ += item_count;
                    header_->unique_id_count_ += count;
                    other->header_->item_count_ -= item_count;
                    other->header_->unique_id_count_ -= count;
                } else {
                    if (count - rebalance_size == 0 || count - rebalance_size == node->block->unique_indices_count()) {
                        break;
                    }
                    // same "room before taking the block apart" rule as the branch above
                    if (!reserve_segments_(1)) {
                        break;
                    }
                    size_t item_count = node->block->count();
                    std::unique_ptr<block_t> temp_block_ptr =
                        node->block->split_uniques(static_cast<uint32_t>(count - rebalance_size));
                    if (!temp_block_ptr) {
                        report_failure_(load_failure_t::out_of_memory);
                        break;
                    }
                    assert(temp_block_ptr->count() != 0 && "incorrect node split");
                    assert(node->block->count() != 0 && "incorrect node split");
                    if (!insert_segment_(
                            segments_.end(),
                            segment_tree_t::node_t{std::move(node->block), std::chrono::system_clock::now(), true})) {
                        node->block = std::move(temp_block_ptr);
                        break;
                    }
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

    bool segment_tree_t::merge(std::unique_ptr<segment_tree_t>& other) {
        assert(header_->item_count_ != 0 && other->header_->item_count_ != 0);
        assert(min_index() > other->max_index() || max_index() < other->min_index());

        // all or nothing: btree_t deletes the leaf merged from, so a partial merge would orphan the rest
        if (poisoned() || other->poisoned()) {
            return false;
        }
        if (segments_.size() + other->segments_.size() > max_segments_limit()) {
            // Refused without touching either side, so neither is poisoned by asking.
            report_failure_(load_failure_t::capacity_exceeded);
            return false;
        }
        begin_operation_();
        other->begin_operation_();
        // all or nothing extends to loading: every block that moves is brought in before the first one does
        for (block_metadata* metadata = other->metadata_begin_; metadata < other->metadata_end_; metadata++) {
            if (!other->try_load_(metadata)) {
                return false;
            }
        }

        mark_dirty_();
        other->mark_dirty_();
        header_->unique_id_count_ += other->header_->unique_id_count_;
        header_->item_count_ += other->header_->item_count_;
        other->header_->item_count_ = 0;
        other->header_->unique_id_count_ = 0;
        segments_.reserve(segments_.size() + other->segments_.size());
        string_storage_.reserve(string_storage_.size() + other->string_storage_.size());
        if (min_index() > other->max_index()) {
            header_->unique_id_count_ += other->header_->unique_id_count_;
            header_->item_count_ += other->header_->item_count_;
            while (!other->segments_.empty()) {
                // cannot refuse: room reserved, neither side poisoned — the only two things insert_segment_() checks
                const bool moved = insert_segment_(segments_.begin(),
                                                   std::move(*(other->segments_.end() - 1)),
                                                   std::move(other->string_storage_.back()));
                assert(moved && "merge was cleared and then refused anyway");
                if (!moved) {
                    return false;
                }
                segments_.begin()->modified = true;
                other->remove_segment_(other->segments_.end() - 1);
            }
        } else {
            while (!other->segments_.empty()) {
                other->segments_.begin()->modified = true;
                const bool moved = insert_segment_(segments_.end(),
                                                   std::move(*(other->segments_.begin())),
                                                   std::move(other->string_storage_.front()));
                assert(moved && "merge was cleared and then refused anyway");
                if (!moved) {
                    return false;
                }
                other->remove_segment_(other->segments_.begin());
            }
        }
        other->header_->item_count_ = 0;
        other->header_->unique_id_count_ = 0;
        return true;
    }

    // Readers answer "nothing" for a block that will not load; out_of_memory on the channel says the answer is void.
    bool segment_tree_t::contains_index(const index_t& index) {
        begin_operation_();
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        auto node = segments_.begin() + (range.begin - metadata_begin_);
        if (!try_load_(range.begin)) {
            return false;
        }
        return node->block->contains_index(index);
    }

    bool segment_tree_t::contains(item_data item) {
        auto index = key_func_(item);
        return contains(index, item);
    }

    bool segment_tree_t::contains(const index_t& index, item_data item) {
        begin_operation_();
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!try_load_(range.begin)) {
                return false;
            }
            return node->block->contains(index, item);
        }
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!try_load_(it)) {
                return false;
            }
            if (node->block->contains(index, item)) {
                return true;
            }
        }
        return false;
    }

    size_t segment_tree_t::item_count(const index_t& index) {
        begin_operation_();
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return 0;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!try_load_(range.begin)) {
                return 0;
            }
            return node->block->item_count(index);
        }
        size_t total = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!try_load_(it)) {
                return 0; // a partial count would read as a real one
            }
            total += node->block->item_count(index);
        }
        return total;
    }

    segment_tree_t::item_data segment_tree_t::get_item(const index_t& index, size_t position) {
        begin_operation_();
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return item_data();
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!try_load_(range.begin)) {
                return item_data();
            }
            return node->block->get_item(index, static_cast<uint32_t>(position));
        }
        size_t skipped_count = 0;
        size_t current_count = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!try_load_(it)) {
                return item_data();
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
        begin_operation_();
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return;
        }
        if (range.begin == range.end) {
            auto node = segments_.begin() + (range.begin - metadata_begin_);
            if (!try_load_(range.begin)) {
                return;
            }
            return node->block->get_items(result, index);
        }

        // all of the range is brought in before any item is handed out, so a refusal adds nothing
        size_t total_size = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            if (!try_load_(it)) {
                return;
            }
            total_size += node->block->item_count(index);
        }
        result.reserve(total_size);
        for (auto it = range.begin; it != range.end; it++) {
            segments_[static_cast<size_t>(it - metadata_begin_)].block->get_items(result, index);
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

    bool segment_tree_t::flush() {
        if (poisoned()) { // an unread block's in-memory stand-in is EMPTY; refuse rather than write it over real rows
            return false;
        }
        // an untouched leaf is already correct on disk, and btree_t::flush() walks every leaf — skip the cost
        if (!dirty_.exchange(false, std::memory_order_acq_rel)) {
#ifdef DEV_MODE
            // a segment marked modified while not resident is never evidence of a missing mark_dirty_()
            for ([[maybe_unused]] const auto& segment : segments_) {
                assert(!(segment.block.get() && segment.modified) &&
                       "clean leaf carries a modified resident block — a mutation path forgot mark_dirty_()");
            }
            assert(gap_tracker_.empty_spaces().size() <= 1 &&
                   "clean leaf still has gaps to close — a mutation path forgot mark_dirty_()");
#endif
            return true;
        }
#ifdef DEV_MODE
        bool wrote_any_block = false;
#endif
        auto file = lease_file_();
        if (!file) {
            mark_dirty_(); // no handle, no flush: same refusal shape as a failed header write below
            return false;
        }
        close_gaps_();
        // close_gaps_() reads a block before relocating it, so a leaf can become poisoned inside flush()
        if (poisoned()) {
            mark_dirty_();
            return false;
        }

        // every failure below leaves the leaf dirty: `modified` clears only once bytes reach the file
        bool ok = true;

        header_->header_checksum_ = header_region_checksum_(); // sealed as the region goes out, for read_header_
        if (!file->write(static_cast<void*>(header_), header_size, 0)) {
            ok = false;
        }

        // TODO: it would be faster to flush blocks in offset order, instead of their id
        block_metadata* metadata = metadata_begin_;
        for (auto segment = segments_.begin(); segment != segments_.end(); segment++, metadata++) {
            if (segment->block.get()) {
                if (segment->unreadable) {
                    // belt and braces: checks the flag on the BLOCK (travels with it), not just the leaf-wide one
                    ok = false;
                    continue;
                }
                assert(segment->block->count() != 0 && "block is empty");
                if (segment->modified) {
#ifdef DEV_MODE
                    wrote_any_block = true;
#endif
                    segment->block->recalculate_checksum();
                    if (file->write(segment->block->internal_buffer(), metadata->size, metadata->file_offset)) {
                        segment->modified = false;
                    } else {
                        ok = false;
                    }
                }
            }
        }
        if (!file->truncate(static_cast<int64_t>(gap_tracker_.empty_spaces().front().offset))) {
            ok = false;
        }
        if (!file->sync()) {
            ok = false;
        }
        if (!ok) {
            mark_dirty_(); // not durable: the next flush must try again instead of skipping the leaf
        }
#ifdef DEV_MODE
        g_leaf_flushes.fetch_add(1, std::memory_order_relaxed);
        if (!wrote_any_block) {
            g_leaf_flushes_without_changes.fetch_add(1, std::memory_order_relaxed);
        }
#endif
        return ok;
    }

    bool segment_tree_t::read_header_(filesystem::file_handle_t& file) {
        // segments_count_ places metadata_end_; a dropped read leaves the PREVIOUS header in place
        if (file.file_size() == 0) { // created but never written to; the flush that hasn't happened yet will write it
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            return true;
        }
        if (!file.read(static_cast<void*>(header_), header_size, 0)) {
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::io_error);
            return false;
        }
        // checksum before any field, or a flipped bit would be believed if the structural check below still passed
        if (header_->header_checksum_ != header_region_checksum_()) {
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::data_corruption);
            return false;
        }
        if (header_->segments_count_ > max_segments) {
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::data_corruption);
            return false;
        }
        metadata_end_ = metadata_begin_ + header_->segments_count_;
        return true;
    }

    void segment_tree_t::clean_load() {
        clean_load_();
        recount_layout_();
    }

    void segment_tree_t::lazy_load() {
        lazy_load_();
        recount_layout_();
    }

    void segment_tree_t::recount_layout_() noexcept {
        size_t stored = 0;
        size_t largest = 0;
        bool view = false;
        for (const block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            stored += metadata->size;
            largest = std::max(largest, metadata->size);
            view = view || metadata->min_index.is_view() || metadata->max_index.is_view();
        }
        stored_bytes_.store(stored, std::memory_order_relaxed);
        largest_block_.store(largest, std::memory_order_relaxed);
        view_keyed_.store(view, std::memory_order_relaxed);
    }

    size_t segment_tree_t::key_copy_bytes_(const block_t& block) {
        size_t result = 0;
        for (const index_t& key : {block.min_index(), block.max_index()}) {
            if (key.is_view()) {
                result += key.view_bytes().size() + 1; // + the terminator std::pmr::string keeps
            }
        }
        return result;
    }

    size_t segment_tree_t::unloaded_bytes() const {
        size_t result = 0;
        const block_metadata* metadata = metadata_begin_;
        for (const auto& segment : segments_) {
            if (!segment.block || segment.unreadable) { // a stand-in keeps its buffer; the reload asks for a new one
                result += metadata->size;
            }
            metadata++;
        }
        return result;
    }

    size_t segment_tree_t::evict() {
        auto file = lease_file_();
        if (!file) { // nothing can be written out, so nothing may leave memory
            report_failure_(load_failure_t::io_error);
            return 0;
        }
        size_t freed = 0;
        bool wrote = false;
        block_metadata* metadata = metadata_begin_;
        for (auto& segment : segments_) {
            block_metadata* current = metadata++;
            if (!segment.block || segment.unreadable) {
                continue; // a stand-in is empty; writing it would overwrite rows still on the device
            }
            if (segment.modified) {
                segment.block->recalculate_checksum();
                if (!file->write(segment.block->internal_buffer(), current->size, current->file_offset)) {
                    report_failure_(load_failure_t::io_error);
                    continue; // write didn't land: keep resident and modified
                }
                wrote = true;
            }
            freed += segment.block->block_size();
            segment.block = nullptr;
            segment.modified = false;
        }
        if (wrote) {
            mark_dirty_(); // blocks went out without the header write and fsync flush() does
        }
        return freed;
    }

    void segment_tree_t::clean_load_() {
        segments_.clear();
        string_storage_.clear();
        unreadable_segments_.store(0, std::memory_order_release);
        abandoned_.store(false, std::memory_order_release);
        auto file = lease_file_();
        if (!file) { // no handle is the read that never happened: same shape as read_header_'s failed read
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::io_error);
            gap_tracker_.init(header_size, INVALID_SIZE);
            dirty_.store(false, std::memory_order_release);
            return;
        }
        if (!read_header_(*file)) {
            gap_tracker_.init(file->file_size(), INVALID_SIZE);
            dirty_.store(false, std::memory_order_release);
            return;
        }
        gap_tracker_.init(file->file_size(), INVALID_SIZE);

        segments_.reserve(header_->segments_count_);
        string_storage_.reserve(header_->segments_count_);
        // TODO: it would be faster to load blocks in offset order, instead of their id (especially on hard drives)
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            // a block is the only thing that makes a view-keyed metadata entry usable — see abandon_leaf_()
            const bool view_keyed = metadata->min_index.is_view() || metadata->max_index.is_view();
            segments_.emplace_back(node_t{nullptr, std::chrono::system_clock::now(), false});
            string_storage_.emplace_back();
            segments_.back().block = create_initialize(resource_, key_func_, static_cast<uint32_t>(metadata->size));
            if (!segments_.back().block) {
                // no room: not corruption, but a caller that asked for a clean load didn't get one
                if (view_keyed) {
                    abandon_leaf_(load_failure_t::out_of_memory);
                    return;
                }
                report_failure_(load_failure_t::out_of_memory);
                continue;
            }
            if (!file->read(segments_.back().block->internal_buffer(), metadata->size, metadata->file_offset)) {
                if (view_keyed) {
                    abandon_leaf_(load_failure_t::io_error);
                    return;
                }
                poison_segment_(segments_.end() - 1, load_failure_t::io_error);
                continue;
            }
            if (!segments_.back().block->varify_checksum()) {
                if (view_keyed) {
                    abandon_leaf_(load_failure_t::data_corruption);
                    return;
                }
                poison_segment_(segments_.end() - 1, load_failure_t::data_corruption);
                continue;
            }
            segments_.back().block->restore_block();
            assert(segments_.back().block->count() != 0 && "block is empty");
            const auto copies = resource_->try_reserve(view_keyed ? key_copy_bytes_(*segments_.back().block) : 0);
            if (!copies) {
                abandon_leaf_(load_failure_t::out_of_memory);
                return;
            }
            if (metadata->min_index.is_view()) {
                metadata->min_index = owned_key_(segments_.back().block->min_index(), string_storage_.back().first);
            }
            if (metadata->max_index.is_view()) {
                metadata->max_index = owned_key_(segments_.back().block->max_index(), string_storage_.back().second);
            }
        }

        // dirty_ starts true (a BUILT leaf was never written); else every restart reflushed every leaf
        dirty_.store(false, std::memory_order_release);
    }

    void segment_tree_t::lazy_load_() {
        segments_.clear();
        string_storage_.clear();
        unreadable_segments_.store(0, std::memory_order_release);
        abandoned_.store(false, std::memory_order_release);
        auto file = lease_file_();
        if (!file) {
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::io_error);
            gap_tracker_.init(header_size, std::numeric_limits<size_t>::max());
            dirty_.store(false, std::memory_order_release);
            return;
        }
        if (!read_header_(*file)) {
            gap_tracker_.init(file->file_size(), std::numeric_limits<size_t>::max());
            dirty_.store(false, std::memory_order_release);
            return;
        }
        gap_tracker_.init(file->file_size(), std::numeric_limits<size_t>::max());

        segments_.reserve(header_->segments_count_);
        string_storage_.reserve(header_->segments_count_);
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            segments_.emplace_back(node_t{nullptr, std::chrono::system_clock::now(), false});
            string_storage_.emplace_back();
            if (metadata->min_index.is_view() || metadata->max_index.is_view()) {
                load_segment_(metadata);
                // an unread stand-in's min/max_index() are the range's extremes, not this block's
                if (!segments_.back().block || segments_.back().unreadable) {
                    abandon_leaf_(last_failure_of_this_leaf_());
                    return;
                }
                const auto copies = resource_->try_reserve(key_copy_bytes_(*segments_.back().block));
                if (!copies) {
                    abandon_leaf_(load_failure_t::out_of_memory);
                    return;
                }
                update_metadata_(segments_.end() - 1, metadata);
                segments_.back().block = nullptr;
            }
        }

        // Loaded state matches the file by definition — see clean_load().
        dirty_.store(false, std::memory_order_release);
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
        mark_dirty_();
        if (range.begin == range.end) {
            return;
        }
        for (auto it = range.begin; it != range.end; it++) {
            gap_tracker_.remove_gap({it->file_offset, it->size});
            stored_bytes_.fetch_sub(it->size, std::memory_order_relaxed);
        }
        gap_tracker_.clean_gaps();
        std::memmove(range.begin, range.end, static_cast<size_t>(metadata_end_ - range.end) * block_metadata_size);
        metadata_end_ -= range.end - range.begin;

        for (auto node = segments_.begin() + (range.begin - metadata_begin_);
             node != segments_.begin() + (range.end - metadata_begin_);
             node++) {
            clear_segment_poison_(node);
        }
        segments_.erase(segments_.begin() + (range.begin - metadata_begin_),
                        segments_.begin() + (range.end - metadata_begin_));
        string_storage_.erase(string_storage_.begin() + (range.begin - metadata_begin_),
                              string_storage_.begin() + (range.end - metadata_begin_));
        header_->segments_count_ = segments_.size();
    }

    segment_tree_t::node_t segment_tree_t::construct_new_node_(const index_t& index, item_data item) {
        const uint32_t size = align_to_block_size(item.size + block_t::header_size + block_t::metadata_size);
        std::unique_ptr<block_t> b_tree_ptr = create_initialize(resource_, key_func_, size);
        if (!b_tree_ptr) {
            unload_old_segments_();
            b_tree_ptr = create_initialize(resource_, key_func_, size);
        }
        if (!b_tree_ptr) {
            report_failure_(load_failure_t::out_of_memory);
            return {nullptr, std::chrono::system_clock::now(), false};
        }
        b_tree_ptr->append(index, item); // always true
        return {std::move(b_tree_ptr), std::chrono::system_clock::now(), true};
    }

    segment_tree_t::node_t segment_tree_t::construct_new_node_(item_data item) {
        auto index = key_func_(item);
        return construct_new_node_(index, item);
    }

    void segment_tree_t::poison_segment_(it node, load_failure_t failure) {
        // a valid block holding nothing, so callers behind try_load_() never test for null
        node->block->reset();
        if (!node->unreadable) {
            node->unreadable = true;
            // counted rather than latched (see flush()), so a block reading back later makes the leaf writable again
            unreadable_segments_.fetch_add(1, std::memory_order_acq_rel);
        }
        node->modified = false;
        node->last_used = std::chrono::system_clock::now();
        report_failure_(failure);
    }

    void segment_tree_t::clear_segment_poison_(it node) noexcept {
        if (node->unreadable) {
            node->unreadable = false;
            unreadable_segments_.fetch_sub(1, std::memory_order_acq_rel);
        }
    }

    bool segment_tree_t::try_load_(block_metadata* metadata) {
        it node = segments_.begin() + (metadata - metadata_begin_);
        node->operation = operation_; // stamped before loading: the load's own eviction must not take it
        // a refused read leaves a non-null, empty stand-in; checking only `!block` would never retry it
        if (!node->block || node->unreadable) {
            load_segment_(metadata);
        }
        return node->block != nullptr;
    }

    bool segment_tree_t::reserve_segments_(size_t count) noexcept {
        if (segments_.size() + count <= max_segments_limit()) {
            return true;
        }
        // btree_t bounds a leaf at MAX_NODE_CAPACITY (8192), one more than max_segments (8191) fits
        abandoned_.store(true, std::memory_order_release);
        report_failure_(load_failure_t::capacity_exceeded);
        return false;
    }

    void segment_tree_t::abandon_leaf_(load_failure_t failure) {
        // a view min/max index is a pointer into this process; find_range_() must not dereference it stale
        segments_.clear();
        string_storage_.clear();
        std::memset(static_cast<void*>(header_), 0, header_size);
        metadata_end_ = metadata_begin_;
        unreadable_segments_.store(0, std::memory_order_release);
        abandoned_.store(true, std::memory_order_release);
        report_failure_(failure);
        dirty_.store(false, std::memory_order_release);
    }

    void segment_tree_t::load_segment_(block_metadata* metadata) {
        it node = segments_.begin() + (metadata - metadata_begin_);

        const auto block_size = static_cast<uint32_t>(metadata->size);
        std::unique_ptr<block_t> fresh = create_initialize(resource_, key_func_, block_size);
        if (!fresh) {
            unload_old_segments_();
            fresh = create_initialize(resource_, key_func_, block_size);
        }
        if (!fresh) { // the node stays as it was: unloaded, or still its stand-in
            report_failure_(load_failure_t::out_of_memory);
            return;
        }
        node->block = std::move(fresh);

        auto file = lease_file_();
        if (!file) { // no handle is the read that never happened: poison as a refused read would
            poison_segment_(node, load_failure_t::io_error);
            return;
        }
        if (!file->read(node->block->internal_buffer(), metadata->size, metadata->file_offset)) {
            poison_segment_(node, load_failure_t::io_error);
            return;
        }
        // not an assert (would vanish under -DNDEBUG): stands between a changed byte and a served row
        if (!node->block->varify_checksum()) {
            poison_segment_(node, load_failure_t::data_corruption);
            return;
        }
        node->block->restore_block();
        assert(node->block->count() && "block stored on disk should not be empty");
        last_failure_.store(load_failure_t::none, std::memory_order_release);
        clear_segment_poison_(node); // bytes arrived and checked out; whatever this cost the leaf, it doesn't any more
        node->last_used = std::chrono::system_clock::now();
        node->modified = false;
    }

    void segment_tree_t::unload_old_segments_() {
        // not just eviction: it writes the evicted blocks without the header write or fsync flush() does
        mark_dirty_();
        // deliberately does not close gaps: runs on a failed load allocation, and close_gaps_ itself loads blocks
        std::vector<std::pair<std::chrono::time_point<std::chrono::system_clock>, size_t>> blocks_to_unload;
        blocks_to_unload.reserve(segments_.size());
        for (size_t i = 0; i < segments_.size(); i++) {
            // a block the running operation loaded is still in its hands
            if (segments_[i].block.get() != nullptr && segments_[i].operation != operation_) {
                blocks_to_unload.emplace_back(segments_[i].last_used, i);
            }
        }
        std::sort(blocks_to_unload.begin(), blocks_to_unload.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.first < rhs.first;
        });
        auto file = lease_file_();
        if (!file) { // nothing can be written out, so nothing may leave memory
            report_failure_(load_failure_t::io_error);
            return;
        }
        size_t half_size = blocks_to_unload.size() / 2;
        for (size_t i = 0; i < half_size; i++) {
            size_t num = blocks_to_unload[i].second;
            if (segments_[num].unreadable) {
                continue; // an empty stand-in; writing it would overwrite rows still on the device
            }
            assert(segments_[num].block->count() && "block stored on disk should not be empty");
            if (segments_[num].modified) {
                segments_[num].block->recalculate_checksum();
                if (!file->write(segments_[num].block->internal_buffer(),
                                 (metadata_begin_ + num)->size,
                                 (metadata_begin_ + num)->file_offset)) {
                    // write didn't land: keep resident and modified, or flush() skips it and loses the rows
                    report_failure_(load_failure_t::io_error);
                    continue;
                }
            }
            segments_[num].block = nullptr; // only now: bytes are on the device, or never differed from it
            segments_[num].modified = false;
        }
    }

    bool segment_tree_t::insert_segment_(it pos, node_t&& node, key_copies_t&& keys) {
        if (node.unreadable) {
            return false; // flag travels with the EMPTY stand-in, or a clean destination would flush it over real rows
        }
        // reachable only near MAX_NODE_CAPACITY; otterbrix's own index uses DEFAULT_NODE_CAPACITY
        if (!reserve_segments_(1)) {
            return false;
        }
        mark_dirty_();
        node.last_used = std::chrono::system_clock::now();
        node.operation = operation_; // a block arriving mid-operation is in that operation's hands too
        auto index = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + index;
        std::memmove(metadata + 1, metadata, (segments_.size() - static_cast<size_t>(index)) * block_metadata_size);
        metadata->file_offset = gap_tracker_.find_gap(node.block->block_size());
        metadata->size = node.block->block_size();
        stored_bytes_.fetch_add(metadata->size, std::memory_order_relaxed);
        if (metadata->size > largest_block_.load(std::memory_order_relaxed)) {
            largest_block_.store(metadata->size, std::memory_order_relaxed);
        }
        segments_.insert(pos, std::move(node));
        string_storage_.emplace(string_storage_.begin() + index, std::move(keys));
        update_metadata_(pos, metadata);
        metadata_end_++;
        header_->segments_count_ = segments_.size();
        return true;
    }

    void segment_tree_t::remove_segment_(it pos) {
        mark_dirty_();
        clear_segment_poison_(pos); // the segment is leaving, so whatever it cost the leaf goes with it
        auto index = pos - segments_.begin();
        block_metadata* metadata = metadata_begin_ + index;
        gap_tracker_.remove_gap({metadata->file_offset, metadata->size});
        stored_bytes_.fetch_sub(metadata->size, std::memory_order_relaxed);
        std::memmove(metadata, metadata + 1, (segments_.size() - static_cast<size_t>(index)) * block_metadata_size);
        metadata_end_--;

        segments_.erase(pos);
        string_storage_.erase(string_storage_.begin() + index);
        header_->segments_count_ = segments_.size();
    }

    segment_tree_t::index_t segment_tree_t::owned_key_(const index_t& key, std::unique_ptr<std::pmr::string>& copy) {
        if (!key.is_view()) {
            return key;
        }
        view_keyed_.store(true, std::memory_order_relaxed);
        // an unchanged key keeps its copy: btree_t's estimate counts only copies of keys that change
        if (!copy || *copy != key.view_bytes()) {
            copy = std::make_unique<std::pmr::string>(key.view_bytes(), resource_);
        }
        return key.rebased(copy->data()); // the block's bytes can leave memory; the copy stays
    }

    void segment_tree_t::update_metadata_(it pos, block_metadata* metadata) {
        mark_dirty_();
        auto index_storage = string_storage_.begin() + (pos - segments_.begin());
        metadata->min_index = owned_key_(pos->block->min_index(), index_storage->first);
        metadata->max_index = owned_key_(pos->block->max_index(), index_storage->second);
    }

    void segment_tree_t::close_gaps_() {
        begin_operation_(); // a block read to move must still be in memory when its offset changes
        gap_tracker_.clean_gaps();
        auto& gaps = gap_tracker_.empty_spaces();
        while (gaps.size() > 1) {
            // TODO: try to close gaps with existing blocks
            size_t i = 0;
            // marked here, not at function entry: flush() calls close_gaps_() itself, and marking unconditionally
            // would dirty every leaf on every flush
            mark_dirty_();
            // read every block that has to move before any offset changes, and give up on the whole
            // pass if one won't come — else its relocated metadata points where nothing is written
            for (block_metadata* it = metadata_begin_; it < metadata_end_; it++, i++) {
                if (it->file_offset > gaps.front().offset) {
                    if (!try_load_(it) || segments_[i].unreadable) {
                        return;
                    }
                }
            }
            i = 0;
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