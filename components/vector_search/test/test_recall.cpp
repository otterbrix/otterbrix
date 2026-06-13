// Recall@k tests for exact kNN.
//
// Exact kNN is, by definition, the ground truth — so recall@k must equal 1.0
// across all (N, D, K) configurations. We compute Top-K via knn_exact_search and
// independently via a naive brute-force linear scan, then verify the intersection
// of row IDs equals K.
//
// These tests directly back the "Доля корректных ответов" metric required by the
// diploma's exhaustive evaluation methodology.

#include <catch2/catch.hpp>
#include <vector_search/distance_metrics.hpp>
#include <vector_search/knn_search.hpp>

#include <algorithm>
#include <random>
#include <set>
#include <vector>

using namespace components::vector_search;

namespace {

    struct dataset_t {
        std::vector<std::vector<double>> vectors;
        std::vector<double> query;
    };

    dataset_t make_random_dataset(std::size_t n, std::size_t dim, uint32_t seed) {
        std::mt19937 rng(seed);
        std::uniform_real_distribution<double> dist(-1.0, 1.0);
        dataset_t ds;
        ds.vectors.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            std::vector<double> v(dim);
            for (auto& x : v) x = dist(rng);
            ds.vectors.push_back(std::move(v));
        }
        ds.query.resize(dim);
        for (auto& x : ds.query) x = dist(rng);
        return ds;
    }

    // Independent brute-force Top-K: sort all (distance, row) pairs and take the smallest K.
    std::set<std::size_t> brute_force_topk(const dataset_t& ds, std::size_t k, metric_type metric) {
        std::vector<std::pair<double, std::size_t>> all;
        all.reserve(ds.vectors.size());
        std::size_t dim = ds.query.size();
        for (std::size_t i = 0; i < ds.vectors.size(); ++i) {
            double d = compute_distance(ds.vectors[i].data(), ds.query.data(), dim, metric);
            all.emplace_back(d, i);
        }
        std::sort(all.begin(), all.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
        std::set<std::size_t> out;
        for (std::size_t i = 0; i < std::min(k, all.size()); ++i) {
            out.insert(all[i].second);
        }
        return out;
    }

    double measure_recall(const dataset_t& ds, std::size_t k, metric_type metric) {
        auto under_test = knn_exact_search(ds.vectors, ds.query, k, metric);
        auto ground_truth = brute_force_topk(ds, k, metric);
        std::size_t hits = 0;
        for (const auto& entry : under_test) {
            if (ground_truth.count(entry.row_id)) {
                ++hits;
            }
        }
        return static_cast<double>(hits) / static_cast<double>(k);
    }

} // namespace

TEST_CASE("vector_search::recall::exact_must_be_1.0::l2") {
    for (std::size_t n : {100u, 1000u}) {
        for (std::size_t d : {8u, 64u, 128u}) {
            for (std::size_t k : {1u, 10u, 50u}) {
                if (k > n) continue;
                auto ds = make_random_dataset(n, d, 42);
                double recall = measure_recall(ds, k, metric_type::l2);
                INFO("N=" << n << " D=" << d << " K=" << k);
                REQUIRE(recall == Approx(1.0));
            }
        }
    }
}

TEST_CASE("vector_search::recall::exact_must_be_1.0::cosine") {
    for (std::size_t n : {100u, 1000u}) {
        for (std::size_t d : {8u, 64u, 128u}) {
            for (std::size_t k : {1u, 10u, 50u}) {
                if (k > n) continue;
                auto ds = make_random_dataset(n, d, 7);
                double recall = measure_recall(ds, k, metric_type::cosine);
                INFO("N=" << n << " D=" << d << " K=" << k);
                REQUIRE(recall == Approx(1.0));
            }
        }
    }
}

TEST_CASE("vector_search::recall::exact_must_be_1.0::inner_product") {
    for (std::size_t n : {100u, 1000u}) {
        for (std::size_t d : {8u, 64u}) {
            for (std::size_t k : {1u, 10u, 50u}) {
                if (k > n) continue;
                auto ds = make_random_dataset(n, d, 99);
                double recall = measure_recall(ds, k, metric_type::inner_product);
                INFO("N=" << n << " D=" << d << " K=" << k);
                REQUIRE(recall == Approx(1.0));
            }
        }
    }
}

TEST_CASE("vector_search::recall::k_larger_than_n_returns_all") {
    auto ds = make_random_dataset(5, 16, 1);
    auto results = knn_exact_search(ds.vectors, ds.query, /*k=*/100, metric_type::l2);
    REQUIRE(results.size() == 5);
    // distances must be non-decreasing
    for (std::size_t i = 1; i < results.size(); ++i) {
        REQUIRE(results[i - 1].distance <= results[i].distance);
    }
}
