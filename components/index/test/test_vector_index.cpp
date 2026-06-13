// Tests for vector_index_t — HNSW index inside the index_t hierarchy.

#include <catch2/catch.hpp>
#include <components/expressions/key.hpp>
#include <components/index/vector_index.hpp>
#include <components/types/logical_value.hpp>
#include <vector_search/distance_metrics.hpp>

#include <algorithm>
#include <memory_resource>
#include <random>
#include <set>
#include <vector>

using namespace components::index;
using components::vector_search::metric_type;
using components::vector_search::hnsw_params_t;

namespace {

    struct dataset_t {
        std::vector<std::vector<float>> vectors;
        std::vector<float> query;
    };

    dataset_t make_random_dataset(std::size_t n, std::size_t dim, uint32_t seed) {
        std::mt19937 rng(seed);
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
        dataset_t ds;
        ds.vectors.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            std::vector<float> v(dim);
            for (auto& x : v) x = dist(rng);
            ds.vectors.push_back(std::move(v));
        }
        ds.query.resize(dim);
        for (auto& x : ds.query) x = dist(rng);
        return ds;
    }

    std::set<int64_t> brute_force_topk(const dataset_t& ds, std::size_t k, metric_type m) {
        std::vector<std::pair<double, int64_t>> all;
        all.reserve(ds.vectors.size());
        std::size_t dim = ds.query.size();
        for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
            double d = components::vector_search::compute_distance(ds.vectors[i].data(), ds.query.data(), dim, m);
            all.emplace_back(d, static_cast<int64_t>(i));
        }
        std::sort(all.begin(), all.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        std::set<int64_t> out;
        for (std::size_t i = 0; i < std::min(k, all.size()); ++i) {
            out.insert(all[i].second);
        }
        return out;
    }

    keys_base_storage_t make_keys(std::pmr::memory_resource* res, const std::string& name) {
        keys_base_storage_t keys(res);
        keys.emplace_back(res, name);
        return keys;
    }

    hnsw_params_t params_with(std::size_t max_elements, std::size_t ef_search = 100) {
        hnsw_params_t p;
        p.max_elements = max_elements;
        p.ef_search = ef_search;
        return p;
    }

} // namespace

TEST_CASE("vector_index::reports_vector_hnsw_type") {
    std::pmr::synchronized_pool_resource pool;
    auto keys = make_keys(&pool, "embedding");
    vector_index_t idx(&pool, "vidx", keys, 8, metric_type::l2, params_with(100));
    REQUIRE(idx.type() == components::logical_plan::index_type::vector_hnsw);
    REQUIRE(idx.name() == "vidx");
    REQUIRE(idx.dim() == 8);
    REQUIRE(idx.metric() == metric_type::l2);
}

TEST_CASE("vector_index::knn_search_recall_l2") {
    std::pmr::synchronized_pool_resource pool;
    auto keys = make_keys(&pool, "embedding");
    constexpr std::size_t dim = 32;
    auto ds = make_random_dataset(500, dim, 42);

    vector_index_t idx(&pool, "vidx", keys, dim, metric_type::l2, params_with(500));
    for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
        idx.add_vector(static_cast<int64_t>(i), ds.vectors[i].data());
    }
    REQUIRE(idx.size() == 500);

    auto results = idx.knn_search(ds.query.data(), dim, 10, metric_type::l2);
    REQUIRE(results.size() == 10);
    for (std::size_t i = 1; i < results.size(); ++i) {
        REQUIRE(results[i - 1].distance <= results[i].distance);
    }

    auto truth = brute_force_topk(ds, 10, metric_type::l2);
    std::size_t hits = 0;
    for (const auto& r : results) {
        if (truth.count(r.row_index)) ++hits;
    }
    double recall = static_cast<double>(hits) / static_cast<double>(truth.size());
    INFO("recall@10 = " << recall);
    REQUIRE(recall >= 0.9);
}

TEST_CASE("vector_index::knn_search_rejects_mismatched_dim_or_metric") {
    std::pmr::synchronized_pool_resource pool;
    auto keys = make_keys(&pool, "embedding");
    vector_index_t idx(&pool, "vidx", keys, 16, metric_type::l2, params_with(10));

    std::vector<float> bogus(16, 0.5f);
    REQUIRE(idx.knn_search(bogus.data(), 8, 5, metric_type::l2).empty());
    REQUIRE(idx.knn_search(bogus.data(), 16, 5, metric_type::cosine).empty());
}

TEST_CASE("vector_index::key_value_methods_are_safe_noops") {
    std::pmr::synchronized_pool_resource pool;
    auto keys = make_keys(&pool, "embedding");
    vector_index_t idx(&pool, "vidx", keys, 4, metric_type::l2, params_with(10));

    components::types::logical_value_t v{&pool, int64_t{0}};
    const core::date::timezone_offset_t tz{};
    auto rfind = idx.find(v, tz);
    REQUIRE(rfind.first == rfind.second);
    auto rlb = idx.lower_bound(v, tz);
    REQUIRE(rlb.first == rlb.second);
    auto rub = idx.upper_bound(v, tz);
    REQUIRE(rub.first == rub.second);
    REQUIRE(idx.cbegin() == idx.cend());

    idx.insert(v, int64_t{0}, tz);
    idx.remove(v, tz);
    REQUIRE(idx.size() == 0);
}
