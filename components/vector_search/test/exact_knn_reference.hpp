#pragma once

// Test oracle for exact kNN.
//
// It mirrors the production vector-search operator's exact path, which scores
// every candidate row with compute_distance() and keeps the best K via
// top_k_heap_t. Both primitives are reused verbatim here, so the oracle and the
// engine share a single definition of "distance" and "Top-K" — there is no
// second, drifting implementation of the kNN math to keep in sync.

#include <vector_search/distance_metrics.hpp>
#include <vector_search/top_k_heap.hpp>

#include <cstddef>
#include <vector>

namespace components::vector_search::test {

    // Row-major flat array (num_vectors × dim).
    template<typename T>
    inline std::vector<scored_entry_t> exact_knn(const T* data,
                                                 std::size_t num_vectors,
                                                 std::size_t dim,
                                                 const T* query,
                                                 std::size_t k,
                                                 metric_type metric) {
        top_k_heap_t heap(k);
        for (std::size_t i = 0; i < num_vectors; ++i) {
            heap.push(i, compute_distance(data + i * dim, query, dim, metric));
        }
        return heap.drain_sorted();
    }

    // Vector-of-vectors; rows whose dimension differs from the query are skipped.
    template<typename T>
    inline std::vector<scored_entry_t> exact_knn(const std::vector<std::vector<T>>& data,
                                                 const std::vector<T>& query,
                                                 std::size_t k,
                                                 metric_type metric) {
        const std::size_t dim = query.size();
        top_k_heap_t heap(k);
        for (std::size_t i = 0; i < data.size(); ++i) {
            if (data[i].size() != dim) {
                continue;
            }
            heap.push(i, compute_distance(data[i].data(), query.data(), dim, metric));
        }
        return heap.drain_sorted();
    }

} // namespace components::vector_search::test
