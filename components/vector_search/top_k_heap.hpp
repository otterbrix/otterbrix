#pragma once

#include <algorithm>
#include <cstddef>
#include <limits>
#include <queue>
#include <utility>
#include <vector>

namespace components::vector_search {

    struct scored_entry_t {
        std::size_t row_id;
        double distance;
    };

    // Fixed-size Top-K heap, O(N log K). farthest=true keeps the K largest.
    class top_k_heap_t {
    public:
        explicit top_k_heap_t(std::size_t k, bool farthest = false)
            : k_(k)
            , farthest_(farthest) {}

        bool push(std::size_t row_id, double distance) {
            if (k_ == 0) {
                return false;
            }
            const double key = make_key(distance);
            if (heap_.size() < k_) {
                heap_.push({{row_id, distance}, key});
                return true;
            }
            if (key < heap_.top().key) {
                heap_.pop();
                heap_.push({{row_id, distance}, key});
                return true;
            }
            return false;
        }

        [[nodiscard]] double worst_distance() const {
            if (heap_.empty()) {
                return std::numeric_limits<double>::infinity();
            }
            return heap_.top().entry.distance;
        }

        [[nodiscard]] std::size_t size() const { return heap_.size(); }
        [[nodiscard]] std::size_t capacity() const { return k_; }
        [[nodiscard]] bool full() const { return heap_.size() >= k_; }

        // Drain best-first (ascending; descending in farthest mode).
        [[nodiscard]] std::vector<scored_entry_t> drain_sorted() {
            std::vector<scored_entry_t> results;
            results.reserve(heap_.size());
            while (!heap_.empty()) {
                results.push_back(heap_.top().entry);
                heap_.pop();
            }
            std::reverse(results.begin(), results.end());
            return results;
        }

    private:
        struct heap_item_t {
            scored_entry_t entry;
            double key;

            bool operator<(const heap_item_t& other) const { return key < other.key; }
        };

        [[nodiscard]] double make_key(double distance) const { return farthest_ ? -distance : distance; }

        std::size_t k_;
        bool farthest_;
        std::priority_queue<heap_item_t> heap_;
    };

} // namespace components::vector_search
