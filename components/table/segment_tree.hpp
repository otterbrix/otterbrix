#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <memory>
#include <vector>

namespace components::table {

    template<class T>
    class segment_base_t {
    public:
        segment_base_t(int64_t start, uint64_t count)
            : start(start)
            , count(count) {}

        int64_t start;
        std::atomic<uint64_t> count;
        // Rejected: a `next` link. Every erase and replace had to re-point it by hand, and a forgotten one
        // dangled; the successor is the entry after this one.
        uint64_t index = 0;
    };

    // Rejected: a lock. A tree has one owner (the table's disk agent), so no second thread ever waited on
    // it, while the refused-append unwind took it again from under itself (test_unwind_limits L0/L0b).
    template<class T>
    class segment_tree_t {
        class segment_iteration_helper;

    public:
        segment_tree_t() = default;
        segment_tree_t(const segment_tree_t&) = delete;
        segment_tree_t(segment_tree_t&&) = delete;
        segment_tree_t& operator=(const segment_tree_t&) = delete;
        segment_tree_t& operator=(segment_tree_t&&) = delete;

        bool is_empty() const noexcept { return nodes_.empty(); }
        uint64_t segment_count() const noexcept { return nodes_.size(); }
        T* root_segment() const noexcept { return nodes_.empty() ? nullptr : nodes_.front().get(); }
        T* last_segment() const noexcept { return nodes_.empty() ? nullptr : nodes_.back().get(); }
        // Negative numbers start from the back.
        T* segment_at(int64_t index) const noexcept {
            if (index < 0) {
                index += static_cast<int64_t>(nodes_.size());
                if (index < 0) {
                    return nullptr;
                }
            }
            return static_cast<uint64_t>(index) < nodes_.size() ? nodes_[static_cast<uint64_t>(index)].get() : nullptr;
        }
        T* next_segment(const T* segment) const noexcept {
            if (!segment) {
                return nullptr;
            }
            assert(has_segment(segment));
            return segment_at(static_cast<int64_t>(segment->index) + 1);
        }
        bool has_segment(const T* segment) const noexcept {
            return segment && segment->index < nodes_.size() && nodes_[segment->index].get() == segment;
        }

        // False when no segment brackets `row_number`, never throws: a throw here
        // would unwind across the disk agent's mailbox into a coroutine whose
        // unhandled_exception() is empty, hanging the statement instead of failing it (rules
        // 2/9). Every caller reports the miss on its own error channel.
        bool try_segment_index(int64_t row_number, uint64_t& result) const noexcept {
            if (nodes_.empty()) {
                return false;
            }
            uint64_t lower = 0;
            uint64_t upper = nodes_.size() - 1;
            while (lower <= upper) {
                uint64_t index = (lower + upper) / 2;
                assert(index < nodes_.size());
                const auto& entry = *nodes_[index];
                if (row_number < entry.start) {
                    // Half-open guard: index 0 cannot move `upper` lower without underflowing the unsigned
                    // cursor (index - 1 wraps to UINT64_MAX). A row_number below the first segment's start
                    // has no containing segment -- stop instead of looping into an out-of-range index.
                    if (index == 0) {
                        break;
                    }
                    upper = index - 1;
                } else if (row_number >= entry.start + static_cast<int64_t>(entry.count)) {
                    lower = index + 1;
                } else {
                    result = index;
                    return true;
                }
            }
            return false;
        }
        T* get_segment(int64_t row_number) const noexcept {
            uint64_t index;
            return try_segment_index(row_number, index) ? nodes_[index].get() : nullptr;
        }

        const std::vector<std::unique_ptr<T>>& reference_segments() const noexcept { return nodes_; }
        // Takes the list out of this tree, which is left empty.
        std::vector<std::unique_ptr<T>> move_segments() { return std::move(nodes_); }
        segment_iteration_helper segments() const { return segment_iteration_helper(*this); }

        void append_segment(std::unique_ptr<T> segment) {
            assert(segment);
            segment->index = nodes_.size();
            nodes_.push_back(std::move(segment));
        }

        // The new node carries the SAME row range; only its block backing differs. The old node is freed here.
        void replace_segment_at_index(uint64_t index, std::unique_ptr<T> new_node) {
            assert(new_node);
            assert(index < nodes_.size());
            assert(new_node->start == nodes_[index]->start);
            assert(new_node->count.load() == nodes_[index]->count.load());
            new_node->index = index;
            nodes_[index] = std::move(new_node);
        }

        void erase_segments(uint64_t segment_start) {
            if (segment_start >= nodes_.size()) {
                return;
            }
            nodes_.erase(nodes_.begin() + static_cast<int64_t>(segment_start), nodes_.end());
        }

        // False = a gap between nodes (a broken tree invariant). Never throws.
        [[nodiscard]] bool contiguous() const noexcept {
            for (uint64_t i = 1; i < nodes_.size(); i++) {
                if (nodes_[i]->start != nodes_[i - 1]->start + static_cast<int64_t>(nodes_[i - 1]->count)) {
                    return false;
                }
            }
            return true;
        }

    private:
        std::vector<std::unique_ptr<T>> nodes_;

        // Rejected: an end iterator taken at begin(). A loop body that erased the tail read past the new end;
        // the iterator compares its position with the live size instead.
        class segment_iteration_helper {
            class segment_iterator;

        public:
            explicit segment_iteration_helper(const segment_tree_t& tree)
                : tree_(tree) {}

            segment_iterator begin() const { return segment_iterator(tree_); }
            std::default_sentinel_t end() const { return std::default_sentinel; }

        private:
            const segment_tree_t& tree_;

            class segment_iterator {
            public:
                explicit segment_iterator(const segment_tree_t& tree)
                    : tree_(tree) {}

                segment_iterator& operator++() {
                    ++index_;
                    return *this;
                }
                bool operator!=(std::default_sentinel_t) const { return index_ < tree_.nodes_.size(); }
                T& operator*() const {
                    assert(index_ < tree_.nodes_.size());
                    return *tree_.nodes_[index_];
                }

            private:
                const segment_tree_t& tree_;
                uint64_t index_ = 0;
            };
        };
    };

} // namespace components::table
