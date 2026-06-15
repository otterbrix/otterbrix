// Tests for the HNSW approximate kNN index.

#include <catch2/catch.hpp>
#include <vector_search/distance_metrics.hpp>
#include <vector_search/hnsw_index.hpp>

#include <algorithm>
#include <random>
#include <set>
#include <vector>

using namespace components::vector_search;

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

    std::set<std::size_t> brute_force_topk(const dataset_t& ds, std::size_t k, metric_type m) {
        std::vector<std::pair<double, std::size_t>> all;
        all.reserve(ds.vectors.size());
        std::size_t dim = ds.query.size();
        for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
            double d = compute_distance(ds.vectors[i].data(), ds.query.data(), dim, m);
            all.emplace_back(d, i);
        }
        std::sort(all.begin(), all.end(),
                  [](const auto& a, const auto& b) { return a.first < b.first; });
        std::set<std::size_t> out;
        for (std::size_t i = 0; i < std::min(k, all.size()); ++i) {
            out.insert(all[i].second);
        }
        return out;
    }

    double recall_at_k(const std::vector<scored_entry_t>& got, const std::set<std::size_t>& truth) {
        if (truth.empty()) return 1.0;
        std::size_t hits = 0;
        for (const auto& e : got) {
            if (truth.count(e.row_id)) ++hits;
        }
        return static_cast<double>(hits) / static_cast<double>(truth.size());
    }

    hnsw_params_t default_params(std::size_t max_elements, std::size_t ef_search = 100) {
        hnsw_params_t p;
        p.max_elements = max_elements;
        p.ef_search = ef_search;
        return p;
    }

} // namespace

TEST_CASE("hnsw_index::build_and_search::l2") {
    auto ds = make_random_dataset(500, 32, 42);
    hnsw_index_t index(32, metric_type::l2, default_params(500, 64));
    for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
        index.add(i, ds.vectors[i].data());
    }
    REQUIRE(index.size() == 500);

    auto results = index.search(ds.query.data(), 10);
    REQUIRE(results.size() == 10);
    for (std::size_t i = 1; i < results.size(); ++i) {
        REQUIRE(results[i - 1].distance <= results[i].distance);
    }
}

TEST_CASE("hnsw_index::recall_at_k::l2") {
    for (auto dim : {16u, 64u}) {
        auto ds = make_random_dataset(1000, dim, 7);
        hnsw_index_t index(dim, metric_type::l2, default_params(1000));
        for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
            index.add(i, ds.vectors[i].data());
        }
        auto results = index.search(ds.query.data(), 10);
        auto truth = brute_force_topk(ds, 10, metric_type::l2);
        double recall = recall_at_k(results, truth);
        INFO("dim=" << dim << " recall=" << recall);
        REQUIRE(recall >= 0.9);
    }
}

TEST_CASE("hnsw_index::recall_at_k::cosine") {
    auto ds = make_random_dataset(1000, 64, 11);
    hnsw_index_t index(64, metric_type::cosine, default_params(1000));
    for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
        index.add(i, ds.vectors[i].data());
    }
    auto results = index.search(ds.query.data(), 10);
    auto truth = brute_force_topk(ds, 10, metric_type::cosine);
    double recall = recall_at_k(results, truth);
    INFO("cosine recall=" << recall);
    REQUIRE(recall >= 0.9);
}

TEST_CASE("hnsw_index::recall_at_k::inner_product") {
    auto ds = make_random_dataset(1000, 64, 99);
    hnsw_index_t index(64, metric_type::inner_product, default_params(1000));
    for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
        index.add(i, ds.vectors[i].data());
    }
    auto results = index.search(ds.query.data(), 10);
    auto truth = brute_force_topk(ds, 10, metric_type::inner_product);
    double recall = recall_at_k(results, truth);
    INFO("inner_product recall=" << recall);
    REQUIRE(recall >= 0.9);
}

TEST_CASE("hnsw_index::ef_search_affects_recall") {
    auto ds = make_random_dataset(2000, 64, 123);
    hnsw_params_t params;
    params.max_elements = 2000;
    params.M = 8;
    hnsw_index_t index(64, metric_type::l2, params);
    for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
        index.add(i, ds.vectors[i].data());
    }
    auto truth = brute_force_topk(ds, 10, metric_type::l2);

    index.set_ef_search(16);
    double low_recall = recall_at_k(index.search(ds.query.data(), 10), truth);

    index.set_ef_search(256);
    double high_recall = recall_at_k(index.search(ds.query.data(), 10), truth);

    INFO("low(ef=16)=" << low_recall << " high(ef=256)=" << high_recall);
    REQUIRE(high_recall >= low_recall);
}

TEST_CASE("hnsw_index::accepts_double_inputs") {
    auto ds_f = make_random_dataset(200, 16, 3);
    std::vector<std::vector<double>> ds_d;
    ds_d.reserve(ds_f.vectors.size());
    for (const auto& v : ds_f.vectors) {
        ds_d.emplace_back(v.begin(), v.end());
    }
    std::vector<double> query_d(ds_f.query.begin(), ds_f.query.end());

    hnsw_index_t index(16, metric_type::l2, default_params(200));
    for (std::size_t i = 0; i < ds_d.size(); ++i) {
        index.add(i, ds_d[i].data());
    }
    auto results = index.search(query_d.data(), 5);
    REQUIRE(results.size() == 5);
}
