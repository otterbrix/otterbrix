#include "segment_tree.hpp"
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
            seg_tree_->ensure_loaded_(metadata_);
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
            seg_tree_->ensure_loaded_(metadata_);
            block_ = seg_tree_->segments_[static_cast<size_t>(metadata_ - seg_tree_->metadata_begin_)].block.get();
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
        // flush() writes this whole region to disk, but only the counters and the metadata entries
        // actually in use are ever assigned. Without zeroing, everything past metadata_end_ is
        // whatever the pool handed over, and that heap content is written to the file — a leak, and
        // the reason two runs of the same operations produced different bytes.
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

    bool segment_tree_t::append(data_ptr_t data, uint32_t size) { return append(item_data{data, size}); }

    bool segment_tree_t::append(item_data item) {
        index_t index = key_func_(item);
        return append(index, item);
    }

    bool segment_tree_t::append(const index_t& index, item_data item) {
        mark_dirty_();
        if (segments_.empty()) {
            segments_.reserve(2);
            string_storage_.reserve(2);
            node_t fresh = construct_new_node_(item);
            if (!fresh.block) {
                // The resource refused a block and had nothing to evict. Nothing has been counted
                // yet on this path, so answering false leaves the leaf exactly as it was.
                return false;
            }
            if (!insert_segment_(segments_.end(), std::move(fresh))) {
                return false;
            }
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
                ensure_loaded_(meta);
                index_exists |= node->block->contains_index(index);
                if (node->block->contains(index, item)) {
                    return false;
                }
            }
            header_->unique_id_count_ += !index_exists;
            // go back to regular append
            metadata = range.begin - (range.begin == metadata_end_);
            append_node = segments_.begin() + (metadata - metadata_begin_);

            ensure_loaded_(metadata);

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
                        ensure_loaded_(metadata);
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
                        // try block before
                        --append_node;
                        --metadata;

                        ensure_loaded_(metadata);
                        if (append_node->block->is_memory_available(item.size)) {
                            append_node->block->append(index, item);
                            append_node->last_used = std::chrono::system_clock::now();
                            append_node->modified = true;
                            update_metadata_(append_node, metadata);
                        } else {
                            // nothing left but to split this block
                            // split_append() takes this block APART -- it moves items out of it,
                            // and may put the new one into it -- before either half can be handed
                            // anywhere. A refusal afterwards destroys what is already out, so the
                            // room for both halves is asked for first. Two, because that is the
                            // most this path can need; one slot short of the ceiling it declines
                            // an append it might have fitted, which is the safe direction.
                            if (!reserve_segments_(2)) {
                                header_->unique_id_count_ -= !index_exists;
                                return false;
                            }
                            append_node->last_used = std::chrono::system_clock::now();
                            std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result =
                                split_append_nothrow(*append_node->block, index, item);
                            if (!split_result.first) {
                                unload_old_segments_();
                                split_result = split_append_nothrow(*append_node->block, index, item);
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
                    // nothing left but to split this block
                    // split_append() takes this block APART -- it moves items out of it,
                    // and may put the new one into it -- before either half can be handed
                    // anywhere. A refusal afterwards destroys what is already out, so the
                    // room for both halves is asked for first. Two, because that is the
                    // most this path can need; one slot short of the ceiling it declines
                    // an append it might have fitted, which is the safe direction.
                    if (!reserve_segments_(2)) {
                        header_->unique_id_count_ -= !index_exists;
                        return false;
                    }
                    append_node->last_used = std::chrono::system_clock::now();
                    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_result =
                        split_append_nothrow(*append_node->block, index, item);
                    if (!split_result.first) {
                        unload_old_segments_();
                        split_result = split_append_nothrow(*append_node->block, index, item);
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
                    if (!insert_segment_(append_node + 1,
                                         node_t{std::move(split_result.first),
                                                std::chrono::system_clock::now(),
                                                true})) {
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
                ensure_loaded_(metadata);
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
            ensure_loaded_(metadata);

            if (remove_node->block->contains(index, item)) {
                // since items are unique, we do not have to check other blocks after
                bool last_copy_in_block = remove_node->block->item_count(index) == 1;
                remove_node->block->remove(index, item);
                if (last_copy_in_block) {
                    // unique_id_count_ counts distinct keys across the whole segment tree,
                    // and one key can straddle several blocks of the range. Decrementing on
                    // every block-local extinction charged a K-block key K times: deleting a
                    // low-cardinality key spread over 2 blocks drove the counter 2 -> 0 while
                    // the other key's items were all still present. Decrement only when the
                    // key is gone from every candidate block.
                    bool index_still_present = false;
                    for (block_metadata* probe = range.begin; probe != range.end; probe++) {
                        it node = segments_.begin() + (probe - metadata_begin_);
                        ensure_loaded_(probe);
                        if (node->block->contains_index(index)) {
                            index_still_present = true;
                            break;
                        }
                    }
                    if (!index_still_present) {
                        header_->unique_id_count_--;
                    }
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

        // check if any of neighbouring blocks could be merged

        if (static_cast<double>(remove_node->block->available_memory()) /
                static_cast<double>(remove_node->block->block_size()) >
            merge_check) {
            it left = remove_node == segments_.begin() ? segments_.end() : remove_node - 1;
            it right = remove_node + 1;

            if (left != segments_.end()) {
                ensure_loaded_(metadata - 1);
            }
            if (right != segments_.end()) {
                ensure_loaded_(metadata + 1);
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
        ensure_loaded_(metadata);

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
            // The range's FIRST block is loaded at the top of this function; its LAST one was not.
            // After lazy_load() only the metadata is resident, so this dereference was on a null
            // block whenever one index spanned more than a single block.
            ensure_loaded_(metadata);
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

            ensure_loaded_(meta);
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
            ensure_loaded_(metadata);

            // THE STAND-IN IS NOT A BLOCK THIS WALK MAY TOUCH. poison_segment_() leaves an EMPTY
            // one in place of a block whose bytes did not arrive, and the arithmetic below is
            // built on "a resident block holds something": unique_indices_count() answers 0, the
            // subtraction under it turns that into SIZE_MAX (an empty block answers max_index()
            // with numeric_limits<index_t>::max(), which is what a default-constructed
            // prev_index is), and split_uniques() then reads one metadata entry PAST the end of
            // the allocation. The other way round -- when prev_index already has a real value --
            // count stays 0, 0 fits in any budget, and the stand-in is moved WHOLE into a leaf
            // that never failed to read anything and would flush it over the rows.
            //
            // Neither is a case to compute through. Stop: this leaf keeps the segment and its
            // metadata, so the bytes stay readable from its own file, and it is poisoned, so it
            // writes nothing over them.
            if (!node->block || node->unreadable || node->block->unique_indices_count() == 0) {
                break;
            }

            size_t count = node->block->unique_indices_count();
            // if indices are the same unique counter will be 1 less
            count -= prev_index == node->block->max_index();
            if (count <= split_size) {
                prev_index = node->block->min_index();
                // move this block to split_tree
                size_t item_count = node->block->count();
                node->modified = true;
                if (!splited_tree->insert_segment_(splited_tree->segments_.begin(), std::move(*node))) {
                    // The destination leaf is full and has poisoned itself. Stop here rather than
                    // move another block: this leaf's segment keeps its metadata, so its bytes are
                    // still readable from the file, and neither side will flush over them.
                    break;
                }
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
                // ASK FOR THE ROOM BEFORE TAKING THE ITEMS OUT. split_uniques() moves them out of
                // this block and hands them back in a new one, and it used to be called inside the
                // argument list of a call that can REFUSE -- at which point the temporary holding
                // them was destroyed while this block had already lost them. The refusal was
                // supposed to be the safe outcome.
                if (!splited_tree->reserve_segments_(1)) {
                    break;
                }
                uint32_t item_count = node->block->count();
                if (!splited_tree->insert_segment_(splited_tree->segments_.begin(),
                                                   segment_tree_t::node_t{node->block->split_uniques(split_unique),
                                                                          std::chrono::system_clock::now(),
                                                                          true})) {
                    break;
                }
                item_count -= node->block->count();
                // split_uniques() moved items OUT of this block, so its bytes changed and it has to
                // be written. Without this the file keeps the pre-split block and the moved items
                // come back on the next load — present in both trees at once.
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
        // Both sides change: this leaf and the neighbour it trades blocks with.
        mark_dirty_();
        other->mark_dirty_();
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
                other->ensure_loaded_(metadata);

                // THE STAND-IN IS NOT A BLOCK THIS WALK MAY TOUCH. poison_segment_() leaves an EMPTY
                // one in place of a block whose bytes did not arrive, and the arithmetic below is
                // built on "a resident block holds something": unique_indices_count() answers 0, the
                // subtraction under it turns that into SIZE_MAX (an empty block answers max_index()
                // with numeric_limits<index_t>::max(), which is what a default-constructed
                // prev_index is), and split_uniques() then reads one metadata entry PAST the end of
                // the allocation. The other way round -- when prev_index already has a real value --
                // count stays 0, 0 fits in any budget, and the stand-in is moved WHOLE into a leaf
                // that never failed to read anything and would flush it over the rows.
                //
                // Neither is a case to compute through. Stop: this leaf keeps the segment and its
                // metadata, so the bytes stay readable from its own file, and it is poisoned, so it
                // writes nothing over them.
                if (!node->block || node->unreadable || node->block->unique_indices_count() == 0) {
                    break;
                }

                size_t count = node->block->unique_indices_count();
                // if indices are the same unique counter will be 1 less
                count -= prev_index == node->block->max_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->min_index();
                    // move this block
                    size_t item_count = node->block->count();
                    node->modified = true;
                    if (!insert_segment_(segments_.begin(), std::move(*node))) {
                        break;
                    }
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
                    // Same as in split(): split_uniques() empties part of the donor's block
                    // before anything can be handed anywhere, so the room is asked for first.
                    if (!reserve_segments_(1)) {
                        break;
                    }
                    uint32_t item_count = node->block->count();
                    if (!insert_segment_(segments_.begin(),
                                         segment_tree_t::node_t{node->block->split_uniques(split_unique),
                                                                std::chrono::system_clock::now(),
                                                                true})) {
                        break;
                    }
                    item_count -= node->block->count();
                    assert(segments_.begin()->block->count() != 0 && "incorrect node split");
                    assert(node->block->count() != 0 && "incorrect node split");
                    // Same as in split(): the donor's block was shrunk in place, so it has to be
                    // written.
                    node->modified = true;
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
                other->ensure_loaded_(metadata);

                // THE STAND-IN IS NOT A BLOCK THIS WALK MAY TOUCH. poison_segment_() leaves an EMPTY
                // one in place of a block whose bytes did not arrive, and the arithmetic below is
                // built on "a resident block holds something": unique_indices_count() answers 0, the
                // subtraction under it turns that into SIZE_MAX (an empty block answers max_index()
                // with numeric_limits<index_t>::max(), which is what a default-constructed
                // prev_index is), and split_uniques() then reads one metadata entry PAST the end of
                // the allocation. The other way round -- when prev_index already has a real value --
                // count stays 0, 0 fits in any budget, and the stand-in is moved WHOLE into a leaf
                // that never failed to read anything and would flush it over the rows.
                //
                // Neither is a case to compute through. Stop: this leaf keeps the segment and its
                // metadata, so the bytes stay readable from its own file, and it is poisoned, so it
                // writes nothing over them.
                if (!node->block || node->unreadable || node->block->unique_indices_count() == 0) {
                    break;
                }

                size_t count = node->block->unique_indices_count();
                // if indices are the same unique counter will be 1 less
                count -= prev_index == node->block->min_index();
                if (count <= rebalance_size) {
                    prev_index = node->block->max_index();
                    // move this block
                    size_t item_count = node->block->count();
                    node->modified = true;
                    if (!insert_segment_(segments_.end(), std::move(*node))) {
                        break;
                    }
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
                    // The room has to be there BEFORE the block is taken apart: the refusal path
                    // below puts the split-off half back and drops the half it was handing over,
                    // so a refusal after the split destroys rows rather than declining to move
                    // them.
                    if (!reserve_segments_(1)) {
                        break;
                    }
                    size_t item_count = node->block->count();
                    std::unique_ptr<block_t> temp_block_ptr =
                        node->block->split_uniques(static_cast<uint32_t>(count - rebalance_size));
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

        // ALL OR NOTHING, and that is not a preference. btree_t DELETES the leaf it merged from:
        // whatever a partial merge leaves behind is no longer named by anything above, so those
        // rows are not "still readable from the source file", they are orphaned. Everything that
        // could stop the move is therefore asked before anything moves.
        if (poisoned() || other->poisoned()) {
            // One side holds an empty stand-in for a block whose bytes did not arrive. This is
            // the move that would carry it into a leaf that flushes -- and the move after which
            // the leaf that still has the real bytes gets deleted.
            return false;
        }
        if (segments_.size() + other->segments_.size() > max_segments_limit()) {
            // Refused without touching either side, so neither is poisoned by asking.
            report_failure_(load_failure_t::capacity_exceeded);
            return false;
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
            // insert all at begin pos
            header_->unique_id_count_ += other->header_->unique_id_count_;
            header_->item_count_ += other->header_->item_count_;
            while (!other->segments_.empty()) {
                other->ensure_loaded_(other->metadata_end_ - 1);
                // Cannot refuse: the room was reserved and neither side is poisoned, which are
                // the only two things insert_segment_() turns down.
                const bool moved = insert_segment_(segments_.begin(), std::move(*(other->segments_.end() - 1)));
                assert(moved && "merge was cleared and then refused anyway");
                if (!moved) {
                    return false;
                }
                segments_.begin()->modified = true;
                other->remove_segment_(other->segments_.end() - 1);
            }
        } else {
            // insert all at end pos
            while (!other->segments_.empty()) {
                other->ensure_loaded_(other->metadata_begin_);
                other->segments_.begin()->modified = true;
                const bool moved = insert_segment_(segments_.end(), std::move(*(other->segments_.begin())));
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

    bool segment_tree_t::contains_index(const index_t& index) {
        metadata_range range = find_range_(index);
        if (range.begin == metadata_end_) {
            return false;
        }
        auto node = segments_.begin() + (range.begin - metadata_begin_);
        ensure_loaded_(range.begin);

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
            ensure_loaded_(range.begin);

            return node->block->contains(index, item);
        }
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            ensure_loaded_(range.begin);
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
            ensure_loaded_(range.begin);

            return node->block->item_count(index);
        }
        size_t total = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            ensure_loaded_(range.begin);
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
            ensure_loaded_(range.begin);

            return node->block->get_item(index, static_cast<uint32_t>(position));
        }
        size_t skipped_count = 0;
        size_t current_count = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            ensure_loaded_(range.begin);
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
            ensure_loaded_(range.begin);

            return node->block->get_items(result, index);
        }

        // Running out of memory should be extreamly rare, so it will be faster to do one vector reserve
        size_t total_size = 0;
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            ensure_loaded_(range.begin);
            total_size += node->block->item_count(index);
        }
        result.reserve(total_size);
        for (auto it = range.begin; it != range.end; it++) {
            auto node = segments_.begin() + (it - metadata_begin_);
            // even if go through the same segments, we could have run out of memory while loading them
            // in which case first batch could be unloaded back, so we check again
            ensure_loaded_(range.begin);
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

    bool segment_tree_t::flush() {
        if (poisoned()) {
            // A block of this leaf could not be read back, and what stands in its place in memory
            // is EMPTY. close_gaps_() would relocate it, the writer below would write it, and
            // either one puts nothing over the rows that are still on the device -- which is how a
            // single refused read used to cost a whole block at the next flush. Refuse instead,
            // touch no bytes, and leave the leaf dirty so nothing counts it as written.
            //
            // Loud, not fatal: the tree still opens, the readable leaves still flush, and the
            // files can still be deleted.
            return false;
        }
        // A leaf nobody touched is already correct on disk: its blocks, its header and its length
        // were written by the flush that made it clean. Rewriting it would cost a header write, a
        // truncate and an fsync for nothing — and btree_t::flush() walks EVERY leaf, so that
        // nothing was being paid once per leaf on every statement.
        if (!dirty_.exchange(false, std::memory_order_acq_rel)) {
#ifdef DEV_MODE
            // Safety net for a coarse hand-maintained flag: if a mutation path ever forgets
            // mark_dirty_(), the change never reaches the disk and is lost at restart, silently.
            // The condition mirrors the writer below (`block.get()` && `modified`) — a segment
            // marked modified while NOT resident is not written by flush() with or without this
            // early return, so it is not evidence of a missing mark_dirty_().
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
        close_gaps_();
        // close_gaps_() has to READ a block before it can relocate it, and that read can be the
        // one that fails -- so a leaf can become poisoned INSIDE the flush, after the check at the
        // top of this function has already been passed.
        if (poisoned()) {
            mark_dirty_();
            return false;
        }

        // Every failure below leaves the leaf dirty again and returns false: the dirty flag was
        // cleared on entry (so a mutation during the write is not lost), and a block's `modified`
        // flag may only be cleared once its bytes reached the file. Otherwise a full disk reports
        // success, the leaf calls itself clean, the next flush skips it, and the rows are gone.
        bool ok = true;

        /*  header_  */
        if (!file_->write(static_cast<void*>(header_), header_size, 0)) {
            ok = false;
        }

        /*  BLOCKS  */
        // TODO: it would be faster to flush blocks in offset order, instead of their id
        block_metadata* metadata = metadata_begin_;
        for (auto segment = segments_.begin(); segment != segments_.end(); segment++, metadata++) {
            // if segment is not loaded, it does not have to be flushed
            if (segment->block.get()) {
                if (segment->unreadable) {
                    // BELT AND BRACES for the promise made at node_t::unreadable: the writer looks
                    // at the flag that belongs to the BLOCK it is about to write, not only at the
                    // leaf-wide one, because the flag is what travels when a block changes leaves.
                    // Unreachable while the refusal above stands; it is here so that no future way
                    // of acquiring one of these can end with it on the device.
                    ok = false;
                    continue;
                }
                assert(segment->block->count() != 0 && "block is empty");
                if (segment->modified) {
#ifdef DEV_MODE
                    wrote_any_block = true;
#endif
                    segment->block->recalculate_checksum();
                    if (file_->write(segment->block->internal_buffer(), metadata->size, metadata->file_offset)) {
                        segment->modified = false;
                    } else {
                        ok = false;
                    }
                }
            }
        }
        if (!file_->truncate(static_cast<int64_t>(gap_tracker_.empty_spaces().front().offset))) {
            ok = false;
        }
        if (!file_->sync()) {
            ok = false;
        }
        if (!ok) {
            // Not durable: make sure the next flush tries again instead of skipping the leaf.
            mark_dirty_();
        }
#ifdef DEV_MODE
        g_leaf_flushes.fetch_add(1, std::memory_order_relaxed);
        if (!wrote_any_block) {
            g_leaf_flushes_without_changes.fetch_add(1, std::memory_order_relaxed);
        }
#endif
        return ok;
    }

    bool segment_tree_t::read_header_() {
        // THE HEADER SIZES EVERYTHING ELSE. header_->segments_count_ places metadata_end_, and
        // every lookup below walks the array between metadata_begin_ and it. It used to be read
        // with the result dropped and its content unexamined: a read that did not happen left the
        // PREVIOUS header in place, and a count larger than the region holds walked off the end of
        // the header allocation on every later find.
        if (file_->file_size() == 0) {
            // A leaf file that was created and never written to. Nothing to read, and nothing
            // wrong: the leaf is empty and the flush that has not happened yet will write it. The
            // read below would report EOF for this, and calling that a failure would refuse to
            // flush a leaf that has no reason not to be flushed.
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            return true;
        }
        if (!file_->read(static_cast<void*>(header_), header_size, 0)) {
            std::memset(static_cast<void*>(header_), 0, header_size);
            metadata_end_ = metadata_begin_;
            abandoned_.store(true, std::memory_order_release);
            report_failure_(load_failure_t::io_error);
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
        using components::types::physical_type;
        segments_.clear();
        string_storage_.clear();
        // A load replaces this leaf's state with the file's, so whatever the last load met is no
        // longer what this leaf holds. Anything the load below meets is set again.
        unreadable_segments_.store(0, std::memory_order_release);
        abandoned_.store(false, std::memory_order_release);
        if (!read_header_()) {
            // The leaf is empty and openable, and flush() will not write that emptiness anywhere.
            gap_tracker_.init(file_->file_size(), INVALID_SIZE);
            dirty_.store(false, std::memory_order_release);
            return;
        }
        gap_tracker_.init(file_->file_size(), INVALID_SIZE);

        segments_.reserve(header_->segments_count_);
        string_storage_.reserve(header_->segments_count_);
        // TODO: it would be faster to load blocks in offset order, instead of their id (especially on hard drives)
        for (block_metadata* metadata = metadata_begin_; metadata < metadata_end_; metadata++) {
            // A block behind a STRING-typed metadata entry is the ONLY thing that can make that
            // entry usable -- see abandon_leaf_().
            const bool string_keyed = metadata->min_index.type() == physical_type::STRING ||
                                      metadata->max_index.type() == physical_type::STRING;
            // call directly because there is no need to modify header
            segments_.emplace_back(node_t{nullptr, std::chrono::system_clock::now(), false});
            string_storage_.emplace_back();
            segments_.back().block =
                create_initialize_nothrow(resource_, key_func_, static_cast<uint32_t>(metadata->size));
            if (!segments_.back().block) {
                // No room for this block. That is not a corruption and, for an integer-keyed
                // entry, not even an error the caller has to act on: an unloaded segment is
                // exactly what lazy_load() produces, and the next question about it loads it then.
                // It IS reported, because a caller that asked for a clean load did not get one.
                if (string_keyed) {
                    abandon_leaf_(load_failure_t::out_of_memory);
                    return;
                }
                report_failure_(load_failure_t::out_of_memory);
                continue;
            }
            if (!file_->read(segments_.back().block->internal_buffer(), metadata->size, metadata->file_offset)) {
                if (string_keyed) {
                    abandon_leaf_(load_failure_t::io_error);
                    return;
                }
                poison_segment_(segments_.end() - 1, load_failure_t::io_error);
                continue;
            }
            if (!segments_.back().block->varify_checksum()) {
                if (string_keyed) {
                    abandon_leaf_(load_failure_t::data_corruption);
                    return;
                }
                poison_segment_(segments_.end() - 1, load_failure_t::data_corruption);
                continue;
            }
            segments_.back().block->restore_block();
            assert(segments_.back().block->count() != 0 && "block is empty");
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

        // Everything above replaced this leaf's state with the file's, so by definition it now
        // matches the file and there is nothing to write. dirty_ starts true because a leaf that
        // was BUILT has never been written; a leaf that was LOADED has. Without this, the first
        // flush after any restart rewrote and fsynced every leaf of every index.
        dirty_.store(false, std::memory_order_release);
    }

    void segment_tree_t::lazy_load() {
        using components::types::physical_type;
        segments_.clear();
        string_storage_.clear();
        unreadable_segments_.store(0, std::memory_order_release);
        abandoned_.store(false, std::memory_order_release);
        if (!read_header_()) {
            gap_tracker_.init(file_->file_size(), std::numeric_limits<size_t>::max());
            dirty_.store(false, std::memory_order_release);
            return;
        }
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
                // A block that could not be read is an EMPTY stand-in, and an empty block answers
                // min_index()/max_index() with the extremes of the index range -- copying those
                // into the metadata would tell find_range_() that this block covers EVERY key. And
                // NOT copying them leaves the stale pointer that came off the file. Neither is
                // usable, which is why the leaf gives up here rather than half-answering.
                if (!segments_.back().block || segments_.back().unreadable) {
                    abandon_leaf_(last_failure_of_this_leaf_());
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
        // Same shape as load_segment_(): ask, evict, ask again, and answer with a value either
        // way. The `catch (...)` that used to be here retried INSIDE the catch, where a second
        // refusal had nothing to catch it and left the process.
        std::unique_ptr<block_t> b_tree_ptr = create_initialize_nothrow(resource_, key_func_, size);
        if (!b_tree_ptr) {
            unload_old_segments_();
            b_tree_ptr = create_initialize_nothrow(resource_, key_func_, size);
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
        // The stand-in is a VALID block that happens to hold nothing: reset() zeroes the count in
        // the buffer and re-derives the cursors from it, so every reader below walks a well-formed
        // empty block instead of a metadata array sized by whatever the file (or the pool) left in
        // header_->count_. That is what makes the call sites behind ensure_loaded_() safe without
        // asking each of them to test for a null block.
        node->block->reset();
        if (!node->unreadable) {
            node->unreadable = true;
            // The leaf as a whole stops being writable while this is not zero: see flush().
            // Counted rather than latched, so that a block which reads back on a later attempt
            // makes the leaf writable again instead of wedging it until the process restarts.
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

    void segment_tree_t::ensure_loaded_(block_metadata* metadata) {
        it node = segments_.begin() + (metadata - metadata_begin_);
        // A segment whose read was refused holds an EMPTY STAND-IN, not nothing -- so the
        // `if (!block)` guard this replaced never asked about it again, and one refused read kept
        // answering "nothing here" for the life of the process even after the device was fine.
        if (!node->block || node->unreadable) {
            load_segment_(metadata);
        }
    }

    bool segment_tree_t::reserve_segments_(size_t count) noexcept {
        if (segments_.size() + count <= max_segments_limit()) {
            return true;
        }
        // THE END OF THE METADATA ARRAY. metadata_end_++ moves a pointer inside the header
        // allocation and nothing else bounds it: max_segments entries fit, and btree_t bounds a
        // leaf at max_node_capacity_ unique indices -- which at MAX_NODE_CAPACITY is 8192, one
        // MORE than fits. Refuse rather than write past the allocation, and give the leaf up so
        // the state the refusal leaves behind cannot reach the device through a flush.
        //
        // Given up as a WHOLE, not counted like an unreadable block: nothing about the leaf will
        // make room later, so unlike a refused read this is not a condition that can lift by
        // itself, and only a load that replaces the leaf clears it.
        abandoned_.store(true, std::memory_order_release);
        report_failure_(load_failure_t::capacity_exceeded);
        return false;
    }

    void segment_tree_t::abandon_leaf_(load_failure_t failure) {
        // A metadata entry whose min or max index is a STRING does not carry the string: it
        // carries a POINTER, and the pointer that came off the file belongs to the process that
        // wrote it. What makes it usable again is reading the block and re-deriving the string
        // from the block's own bytes -- which is exactly why lazy_load() loads those blocks and
        // nothing else. If that read did not happen, nothing in this leaf can be compared against:
        // find_range_() would dereference a stale pointer on the very first lookup.
        //
        // So the leaf gives up as a whole rather than half-answering: empty in memory, untouched
        // on the device, refusing to flush, and saying why.
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
        // If there is not enough memory, write the oldest resident blocks out and ask again. The
        // refusal arrives as a nullptr, not as a throw -- see create_initialize_nothrow().
        node->block = create_initialize_nothrow(resource_, key_func_, block_size);
        if (!node->block) {
            unload_old_segments_();
            node->block = create_initialize_nothrow(resource_, key_func_, block_size);
        }
        if (!node->block) {
            // Nothing this leaf owns could be turned into room for one block. Say so and leave the
            // segment UNLOADED -- which is exactly the state lazy_load() leaves every segment it
            // did not touch in, so nothing below is surprised by it.
            report_failure_(load_failure_t::out_of_memory);
            return;
        }

        if (!file_->read(node->block->internal_buffer(), metadata->size, metadata->file_offset)) {
            // The bytes never arrived. The buffer holds whatever create_initialize() put there,
            // and restoring a block out of that is how a refused read used to become an empty
            // block that the next flush wrote over the real one.
            poison_segment_(node, load_failure_t::io_error);
            return;
        }
        // THE CHECK THAT USED TO BE AN ASSERT AND THEREFORE DID NOT EXIST UNDER -DNDEBUG. It costs
        // one CRC32C pass over the block on every load, which is the same pass flush() already
        // pays on every block it writes -- and it is the only thing standing between a changed
        // byte on the device and a row served as if it were the row that was stored.
        if (!node->block->varify_checksum()) {
            poison_segment_(node, load_failure_t::data_corruption);
            return;
        }
        node->block->restore_block();
        assert(node->block->count() && "block stored on disk should not be empty");
        last_failure_.store(load_failure_t::none, std::memory_order_release);
        // The bytes arrived and checked out. Whatever this segment cost the leaf before, it does
        // not cost it any more -- and if it was the only one, the leaf can be written again.
        clear_segment_poison_(node);
        node->last_used = std::chrono::system_clock::now();
        node->modified = false;
    }

    void segment_tree_t::unload_old_segments_() {
        // Not just a memory eviction: it WRITES the evicted blocks to the file and clears their
        // `modified` flags, without writing the header and without fsync. So after an unload the
        // leaf looks unmodified while its file still needs the header and the sync.
        mark_dirty_();
        // Deliberately does NOT close gaps. close_gaps_ now loads a block before relocating it,
        // and load_segment_ calls this function when the allocation fails — closing gaps here
        // would close that loop. Skipping it is safe: every block below is written at its current
        // metadata offset, which stays valid whether or not the file has holes in it. Compaction
        // is flush()'s job, and flush() calls close_gaps_ itself.
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
            if (segments_[num].unreadable) {
                // An empty stand-in for a block this leaf could not read. Writing it would put it
                // over the rows that are still on the device, so it is not written and not
                // dropped either -- it stays where it is and costs the memory it costs.
                continue;
            }
            assert(segments_[num].block->count() && "block stored on disk should not be empty");
            if (segments_[num].modified) {
                segments_[num].block->recalculate_checksum();
                if (!file_->write(segments_[num].block->internal_buffer(),
                                  (metadata_begin_ + num)->size,
                                  (metadata_begin_ + num)->file_offset)) {
                    // THE WRITE DID NOT LAND, and the two lines below used to run anyway: the
                    // block left memory and was marked clean, so flush() skipped it and every row
                    // in it was gone -- silently, with flush() still answering true. ENOSPC here
                    // used to cost half the resident blocks of the leaf.
                    //
                    // Keep it resident and keep it modified. The leaf is already dirty (marked at
                    // the top of this function), so the next flush writes it again.
                    report_failure_(load_failure_t::io_error);
                    continue;
                }
            }
            // Only now: the bytes are on the device, or they were never different from it.
            segments_[num].block = nullptr;
            segments_[num].modified = false;
        }
    }

    bool segment_tree_t::insert_segment_(it pos, node_t&& node) {
        if (node.unreadable) {
            // THE PROMISE MADE AT node_t::unreadable, KEPT WHERE THE BLOCK CHANGES HANDS. The
            // stand-in is EMPTY, the flag travels with it and the leaf-wide refusal does not --
            // so a destination that never failed to read anything would flush it, over the rows
            // that are still in the donor's file. The donor already said why on the channel.
            return false;
        }
        // Reachable only with one block per unique index (items around half a block each) and a
        // tree built with a capacity near MAX_NODE_CAPACITY; otterbrix's own index uses
        // DEFAULT_NODE_CAPACITY.
        if (!reserve_segments_(1)) {
            return false;
        }
        // Changes header_->segments_count_ and the block metadata array, which live in the header.
        mark_dirty_();
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
        return true;
    }

    void segment_tree_t::remove_segment_(it pos) {
        mark_dirty_();
        // The segment is leaving the leaf, so whatever it cost the leaf goes with it.
        clear_segment_poison_(pos);
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
        mark_dirty_();
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
            // Moves blocks inside the file: both the metadata (which lives in the header) and the
            // affected blocks have to be rewritten. Marked here rather than at function entry —
            // flush() calls close_gaps_() itself, and marking unconditionally would make every
            // leaf dirty again on every flush.
            mark_dirty_();
            // Read every block that has to move BEFORE any offset changes, and give up on the
            // whole pass if one of them will not come. Relocation only rewrites the metadata; the
            // bytes are moved by flush()'s writer, which skips segments whose block is not
            // resident -- so a relocated block that could not be read would point its metadata at
            // an address nothing is ever written to, and an offset already lowered for an earlier
            // block would leave a half-compacted file. Two passes keep it all or nothing.
            for (block_metadata* it = metadata_begin_; it < metadata_end_; it++, i++) {
                if (it->file_offset > gaps.front().offset) {
                    ensure_loaded_(it);
                    if (!segments_[i].block || segments_[i].unreadable) {
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