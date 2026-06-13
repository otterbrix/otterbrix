#pragma once

#include "distance_metrics.hpp"
#include "top_k_heap.hpp"

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace components::vector_search {

    using knn_result_t = std::vector<scored_entry_t>;

    // Exact kNN over a row-major flat array; nearest-first.
    template<typename T>
    knn_result_t knn_exact_search(const T* data,
                                  std::size_t num_vectors,
                                  std::size_t dim,
                                  const T* query,
                                  std::size_t k,
                                  metric_type metric) {
        if (dim == 0) {
            throw std::invalid_argument("knn_exact_search: dimension must be > 0");
        }
        if (k == 0) {
            return {};
        }

        top_k_heap_t heap(k);

        for (std::size_t i = 0; i < num_vectors; ++i) {
            const T* vec = data + i * dim;
            double dist = compute_distance(vec, query, dim, metric);
            heap.push(i, dist);
        }

        return heap.drain_sorted();
    }

    template<typename T>
    knn_result_t knn_exact_search(const std::vector<std::vector<T>>& data,
                                  const std::vector<T>& query,
                                  std::size_t k,
                                  metric_type metric) {
        if (query.empty()) {
            throw std::invalid_argument("knn_exact_search: query vector must not be empty");
        }
        if (data.empty() || k == 0) {
            return {};
        }
        std::size_t dim = query.size();

        top_k_heap_t heap(k);

        for (std::size_t i = 0; i < data.size(); ++i) {
            if (data[i].size() != dim) {
                continue;
            }
            double dist = compute_distance(data[i].data(), query.data(), dim, metric);
            heap.push(i, dist);
        }

        return heap.drain_sorted();
    }

} // namespace components::vector_search
