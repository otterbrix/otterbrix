#pragma once

#include "distance_metrics.hpp"
#include "top_k_heap.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace hnswlib {
    template<typename T>
    class HierarchicalNSW;
    template<typename T>
    class SpaceInterface;
} // namespace hnswlib

namespace components::vector_search {

    // HNSW hyperparameters.
    struct hnsw_params_t {
        std::size_t M = 16;
        std::size_t ef_construction = 64;
        std::size_t ef_search = 40;
        std::size_t max_elements = 100000;
        std::size_t random_seed = 100;
    };

    // Approximate kNN index (HNSW).
    class hnsw_index_t {
    public:
        hnsw_index_t(std::size_t dim, metric_type metric, const hnsw_params_t& params = {});
        ~hnsw_index_t();

        hnsw_index_t(const hnsw_index_t&) = delete;
        hnsw_index_t& operator=(const hnsw_index_t&) = delete;
        hnsw_index_t(hnsw_index_t&&) noexcept;
        hnsw_index_t& operator=(hnsw_index_t&&) noexcept;

        // replace_deleted: reuse a tombstone.
        void add(std::size_t row_id, const float* vec, bool replace_deleted = false);
        void add(std::size_t row_id, const double* vec, bool replace_deleted = false);

        // Tombstone; false if absent.
        bool mark_delete(std::size_t row_id);

        [[nodiscard]] std::vector<scored_entry_t> search(const float* query, std::size_t k) const;
        [[nodiscard]] std::vector<scored_entry_t> search(const double* query, std::size_t k) const;

        // 0 resets to default.
        void set_ef_search(std::size_t ef);

        [[nodiscard]] std::size_t dim() const noexcept { return dim_; }
        [[nodiscard]] metric_type metric() const noexcept { return metric_; }
        [[nodiscard]] std::size_t size() const noexcept;
        [[nodiscard]] std::size_t deleted_count() const noexcept;
        [[nodiscard]] std::size_t live_count() const noexcept;

        // Visit live points (for rebuild).
        void for_each_live(const std::function<void(std::size_t row_id, const float* vec)>& fn) const;

        void save(const std::string& path) const;
        void load(const std::string& path);

    private:
        const float* prepare_input(const float* src, std::vector<float>& buf) const;
        const float* prepare_input(const double* src, std::vector<float>& buf) const;
        void ensure_capacity_for_one(bool replace_deleted);

        std::size_t dim_;
        metric_type metric_;
        hnsw_params_t params_;
        std::size_t default_ef_search_{0};
        std::unique_ptr<hnswlib::SpaceInterface<float>> space_;
        std::unique_ptr<hnswlib::HierarchicalNSW<float>> index_;
    };

} // namespace components::vector_search
