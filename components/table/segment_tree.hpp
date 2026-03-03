#pragma once

#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace components::table {

    template<class T>
    class segment_base_t {
    public:
        segment_base_t(int64_t start, uint64_t count)
            : start(start)
            , count(count)
            , next(nullptr) {}

        int64_t start;
        std::atomic<uint64_t> count;
        T* next;
        uint64_t index = 0;
    };

    template<class T>
    struct segment_node_t {
        int64_t row_start;
        std::unique_ptr<T> node;
    };

    template<class T, bool SUPPORTS_LAZY_LOADING = false>
    class segment_tree_t {
        class segment_iteration_helper;

    public:
        explicit segment_tree_t()
            : finished_loading_(true) {}
        virtual ~segment_tree_t() = default;

        std::unique_lock<std::mutex> lock() { return std::unique_lock(node_lock_); }

        bool is_empty(std::unique_lock<std::mutex>& l) { return root_segment(l) == nullptr; }

        T* root_segment() {
            auto l = lock();
            return root_segment(l);
        }

        T* root_segment(std::unique_lock<std::mutex>& l) {
            if (nodes_.empty()) {
                load_next_segment(l);
            }
            return root_segment_internal();
        }
        std::vector<segment_node_t<T>> move_segments(std::unique_lock<std::mutex>& l) {
            load_all_segments(l);
            return std::move(nodes_);
        }
        std::vector<segment_node_t<T>> move_segments() {
            auto l = lock();
            return move_segments(l);
        }

        const std::vector<segment_node_t<T>>& reference_segments(std::unique_lock<std::mutex>& l) {
            load_all_segments(l);
            return nodes_;
        }
        const std::vector<segment_node_t<T>>& reference_segments() {
            auto l = lock();
            return reference_segments(l);
        }

        uint64_t segment_count() {
            auto l = lock();
            return segment_count(l);
        }
        uint64_t segment_count(std::unique_lock<std::mutex>&) { return nodes_.size(); }
        T* segment_at(int64_t index) {
            auto l = lock();
            return segment_at(l, index);
        }
        T* segment_at(std::unique_lock<std::mutex>& l, int64_t index) {
            if (index < 0) {
                load_all_segments(l);
                index += static_cast<int64_t>(nodes_.size());
                if (index < 0) {
                    return nullptr;
                }
                return nodes_[static_cast<uint64_t>(index)].node.get();
            } else {
                while (uint64_t(index) >= nodes_.size() && load_next_segment(l)) {
                }
                if (uint64_t(index) >= nodes_.size()) {
                    return nullptr;
                }
                return nodes_[static_cast<uint64_t>(index)].node.get();
            }
        }
        T* next_segment(T* segment) {
            if (!SUPPORTS_LAZY_LOADING) {
                return segment->next;
            }
            if (finished_loading_) {
                return segment->next;
            }
            auto l = lock();
            return next_segment(l, segment);
        }
        T* next_segment(std::unique_lock<std::mutex>& l, T* segment) {
            if (!segment) {
                return nullptr;
            }
            assert(nodes_[segment->index].node.get() == segment);
            return segment_at(l, static_cast<int64_t>(segment->index + 1));
        }

        T* last_segment(std::unique_lock<std::mutex>& l) {
            load_all_segments(l);
            if (nodes_.empty()) {
                return nullptr;
            }
            return nodes_.back().node.get();
        }
        T* get_segment(int64_t row_number) {
            auto l = lock();
            return get_segment(l, row_number);
        }
        T* get_segment(std::unique_lock<std::mutex>& l, int64_t row_number) {
            return nodes_[segment_index(l, row_number)].node.get();
        }

        void append_segment_internal(std::unique_lock<std::mutex>&, std::unique_ptr<T> segment) {
            assert(segment);
            if (!nodes_.empty()) {
                nodes_.back().node->next = segment.get();
            }
            segment_node_t<T> node;
            segment->index = nodes_.size();
            segment->next = nullptr;
            node.row_start = segment->start;
            node.node = std::move(segment);
            nodes_.push_back(std::move(node));
        }
        void append_segment(std::unique_ptr<T> segment) {
            auto l = lock();
            append_segment(l, std::move(segment));
        }
        void append_segment(std::unique_lock<std::mutex>& l, std::unique_ptr<T> segment) {
            load_all_segments(l);
            append_segment_internal(l, std::move(segment));
        }
        bool has_segment(T* segment) {
            auto l = lock();
            return has_segment(l, segment);
        }
        bool has_segment(std::unique_lock<std::mutex>&, T* segment) {
            return segment->index < nodes_.size() && nodes_[segment->index].node.get() == segment;
        }

        void replace(segment_tree_t<T>& other) {
            auto l = lock();
            replace(l, other);
        }
        void replace(std::unique_lock<std::mutex>& l, segment_tree_t<T>& other) {
            other.load_all_segments(l);
            nodes_ = std::move(other.nodes_);
        }

        void erase_segments(std::unique_lock<std::mutex>& l, uint64_t segment_start) {
            load_all_segments(l);
            if (segment_start >= nodes_.size() - 1) {
                return;
            }
            nodes_.erase(nodes_.begin() + static_cast<int64_t>(segment_start) + 1, nodes_.end());
        }

        uint64_t segment_index(std::unique_lock<std::mutex>& l, int64_t row_number) {
            uint64_t segment_index;
            if (try_segment_index(l, row_number, segment_index)) {
                return segment_index;
            }
            throw std::runtime_error("Could not find node in column segment tree");
        }

        bool try_segment_index(std::unique_lock<std::mutex>& l, int64_t row_number, uint64_t& result) {
            while (nodes_.empty() ||
                   row_number >= nodes_.back().row_start + static_cast<int64_t>(nodes_.back().node->count)) {
                if (!load_next_segment(l)) {
                    break;
                }
            }
            if (nodes_.empty()) {
                return false;
            }
            uint64_t lower = 0;
            uint64_t upper = nodes_.size() - 1;
            while (lower <= upper) {
                uint64_t index = (lower + upper) / 2;
                assert(index < nodes_.size());
                auto& entry = nodes_[index];
                assert(entry.row_start == entry.node->start);
                if (row_number < entry.row_start) {
                    upper = index - 1;
                } else if (row_number >= entry.row_start + static_cast<int64_t>(entry.node->count)) {
                    lower = index + 1;
                } else {
                    result = index;
                    return true;
                }
            }
            return false;
        }

        segment_iteration_helper segments() { return segment_iteration_helper(*this); }

        std::vector<T*> copy_segments() {
            auto l = lock();
            load_all_segments(l);
            std::vector<T*> result;
            result.reserve(nodes_.size());
            for (auto& entry : nodes_) {
                result.push_back(entry.node.get());
            }
            return result;
        }

        void reinitialize() {
            if (nodes_.empty()) {
                return;
            }
            int64_t offset = nodes_[0].node->start;
            for (auto& entry : nodes_) {
                if (entry.node->start != offset) {
                    throw std::runtime_error("In segment_tree_t::reinitialize - gap found between nodes!");
                }
                entry.row_start = offset;
                offset += static_cast<int64_t>(entry.node->count);
            }
        }

    protected:
        std::atomic<bool> finished_loading_;

        virtual std::unique_ptr<T> load_segment() { return nullptr; }

    private:
        std::vector<segment_node_t<T>> nodes_;
        std::mutex node_lock_;

        T* root_segment_internal() { return nodes_.empty() ? nullptr : nodes_[0].node.get(); }

        class segment_iteration_helper {
            class segment_iterator;

        public:
            explicit segment_iteration_helper(segment_tree_t& tree)
                : tree_(tree) {}

            segment_iterator begin() { return segment_iterator(tree_, tree_.root_segment()); }
            segment_iterator end() { return segment_iterator(tree_, nullptr); }

        private:
            segment_tree_t& tree_;

            class segment_iterator {
            public:
                segment_iterator(segment_tree_t& tree, T* current)
                    : tree(tree)
                    , current(current) {}

                segment_tree_t& tree;
                T* current;

                void next() { current = tree.next_segment(current); }

                segment_iterator& operator++() {
                    next();
                    return *this;
                }
                bool operator!=(const segment_iterator& other) const { return current != other.current; }
                T& operator*() const {
                    assert(current);
                    return *current;
                }
            };
        };

        bool load_next_segment(std::unique_lock<std::mutex>& l) {
            if (!SUPPORTS_LAZY_LOADING) {
                return false;
            }
            if (finished_loading_) {
                return false;
            }
            auto result = load_segment();
            if (result) {
                append_segment_internal(l, std::move(result));
                return true;
            }
            return false;
        }

        void load_all_segments(std::unique_lock<std::mutex>& l) {
            if (!SUPPORTS_LAZY_LOADING) {
                return;
            }
            while (load_next_segment(l)) {
            }
        }
    };

} // namespace components::table