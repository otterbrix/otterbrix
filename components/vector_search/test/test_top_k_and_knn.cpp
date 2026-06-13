#include <catch2/catch.hpp>
#include <vector_search/distance_metrics.hpp>
#include <vector_search/knn_search.hpp>
#include <vector_search/top_k_heap.hpp>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <vector>

using namespace components::vector_search;

// ============================================================================
// top_k_heap tests
// ============================================================================

TEST_CASE("vector_search::top_k_heap::k_equals_1") {
    top_k_heap_t heap(1);

    heap.push(0, 5.0);
    REQUIRE(heap.size() == 1);
    REQUIRE(heap.worst_distance() == Approx(5.0));

    heap.push(1, 3.0); // closer → should replace
    REQUIRE(heap.size() == 1);
    REQUIRE(heap.worst_distance() == Approx(3.0));

    heap.push(2, 10.0); // farther → should NOT replace
    REQUIRE(heap.size() == 1);
    REQUIRE(heap.worst_distance() == Approx(3.0));

    auto results = heap.drain_sorted();
    REQUIRE(results.size() == 1);
    REQUIRE(results[0].row_id == 1);
    REQUIRE(results[0].distance == Approx(3.0));
}

TEST_CASE("vector_search::top_k_heap::k_equals_3") {
    top_k_heap_t heap(3);

    heap.push(0, 5.0);
    heap.push(1, 2.0);
    heap.push(2, 8.0);
    heap.push(3, 1.0);  // should evict 8.0
    heap.push(4, 10.0); // should NOT enter
    heap.push(5, 3.0);  // should evict 5.0

    REQUIRE(heap.size() == 3);

    auto results = heap.drain_sorted();
    REQUIRE(results.size() == 3);

    // sorted ascending by distance
    REQUIRE(results[0].row_id == 3);
    REQUIRE(results[0].distance == Approx(1.0));
    REQUIRE(results[1].row_id == 1);
    REQUIRE(results[1].distance == Approx(2.0));
    REQUIRE(results[2].row_id == 5);
    REQUIRE(results[2].distance == Approx(3.0));
}

TEST_CASE("vector_search::top_k_heap::k_larger_than_data") {
    top_k_heap_t heap(10);

    heap.push(0, 3.0);
    heap.push(1, 1.0);
    heap.push(2, 2.0);

    REQUIRE(heap.size() == 3);
    REQUIRE_FALSE(heap.full());

    auto results = heap.drain_sorted();
    REQUIRE(results.size() == 3);
    REQUIRE(results[0].distance == Approx(1.0));
    REQUIRE(results[1].distance == Approx(2.0));
    REQUIRE(results[2].distance == Approx(3.0));
}

TEST_CASE("vector_search::top_k_heap::k_equals_0") {
    top_k_heap_t heap(0);
    REQUIRE_FALSE(heap.push(0, 1.0));
    REQUIRE(heap.size() == 0);
    auto results = heap.drain_sorted();
    REQUIRE(results.empty());
}

TEST_CASE("vector_search::top_k_heap::duplicate_distances") {
    top_k_heap_t heap(3);

    heap.push(0, 2.0);
    heap.push(1, 2.0);
    heap.push(2, 2.0);
    heap.push(3, 2.0); // equal to worst — should NOT replace (strictly less check)

    REQUIRE(heap.size() == 3);
    auto results = heap.drain_sorted();
    REQUIRE(results.size() == 3);
    for (const auto& r : results) {
        REQUIRE(r.distance == Approx(2.0));
    }
}

TEST_CASE("vector_search::top_k_heap::empty_heap_worst_distance") {
    top_k_heap_t heap(5);
    REQUIRE(std::isinf(heap.worst_distance()));
}

// ============================================================================
// knn_exact_search tests
// ============================================================================

TEST_CASE("vector_search::knn_search::basic_l2") {
    // 5 vectors in 2D
    std::vector<std::vector<double>> data = {
        {0.0, 0.0}, // 0
        {1.0, 0.0}, // 1
        {0.0, 1.0}, // 2
        {5.0, 5.0}, // 3
        {0.5, 0.5}, // 4
    };

    std::vector<double> query = {0.0, 0.0};

    auto results = knn_exact_search(data, query, 3, metric_type::l2);
    REQUIRE(results.size() == 3);

    // nearest: row 0 (d=0), row 4 (d=0.5), row 1 or 2 (d=1.0)
    REQUIRE(results[0].row_id == 0);
    REQUIRE(results[0].distance == Approx(0.0));
    REQUIRE(results[1].row_id == 4);
    REQUIRE(results[1].distance == Approx(0.5)); // squared: 0.25+0.25=0.5
}

TEST_CASE("vector_search::knn_search::basic_cosine") {
    std::vector<std::vector<double>> data = {
        {1.0, 0.0},  // 0: points right
        {1.0, 1.0},  // 1: 45°
        {0.0, 1.0},  // 2: points up
        {-1.0, 0.0}, // 3: points left (opposite)
    };

    std::vector<double> query = {1.0, 0.0}; // points right

    auto results = knn_exact_search(data, query, 2, metric_type::cosine);
    REQUIRE(results.size() == 2);

    // nearest by cosine: row 0 (distance=0), row 1 (distance ≈ 0.293)
    REQUIRE(results[0].row_id == 0);
    REQUIRE(results[0].distance == Approx(0.0).margin(1e-10));
    REQUIRE(results[1].row_id == 1);
}

TEST_CASE("vector_search::knn_search::k_larger_than_n") {
    std::vector<std::vector<double>> data = {
        {1.0, 2.0},
        {3.0, 4.0},
    };
    std::vector<double> query = {0.0, 0.0};

    auto results = knn_exact_search(data, query, 10, metric_type::l2);
    REQUIRE(results.size() == 2);
}

TEST_CASE("vector_search::knn_search::empty_data") {
    std::vector<std::vector<double>> data;
    std::vector<double> query = {1.0, 2.0};

    auto results = knn_exact_search(data, query, 5, metric_type::l2);
    REQUIRE(results.empty());
}

TEST_CASE("vector_search::knn_search::k_equals_0") {
    std::vector<std::vector<double>> data = {{1.0, 2.0}};
    std::vector<double> query = {0.0, 0.0};

    auto results = knn_exact_search(data, query, 0, metric_type::l2);
    REQUIRE(results.empty());
}

TEST_CASE("vector_search::knn_search::raw_pointer_interface") {
    // 3 vectors in 3D, flat layout
    std::vector<double> flat = {
        1.0,
        0.0,
        0.0, // 0
        0.0,
        1.0,
        0.0, // 1
        0.0,
        0.0,
        1.0, // 2
    };
    std::vector<double> query = {1.0, 0.0, 0.0};

    auto results = knn_exact_search(flat.data(), 3, 3, query.data(), 2, metric_type::cosine);
    REQUIRE(results.size() == 2);
    REQUIRE(results[0].row_id == 0); // identical to query
    REQUIRE(results[0].distance == Approx(0.0).margin(1e-10));
}

TEST_CASE("vector_search::knn_search::mismatched_dimensions_skipped") {
    std::vector<std::vector<double>> data = {
        {1.0, 2.0, 3.0},
        {4.0, 5.0}, // wrong dimension → skipped
        {0.1, 0.2, 0.3},
    };
    std::vector<double> query = {1.0, 2.0, 3.0};

    auto results = knn_exact_search(data, query, 3, metric_type::l2);
    REQUIRE(results.size() == 2); // only 2 valid vectors
}

TEST_CASE("vector_search::knn_search::large_dataset") {
    constexpr std::size_t n = 10000;
    constexpr std::size_t dim = 16;
    constexpr std::size_t k = 5;

    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(-1.0, 1.0);

    // Generate random vectors
    std::vector<double> flat(n * dim);
    for (auto& v : flat) {
        v = dist(rng);
    }

    std::vector<double> query(dim);
    for (auto& v : query) {
        v = dist(rng);
    }

    auto results = knn_exact_search(flat.data(), n, dim, query.data(), k, metric_type::l2);
    REQUIRE(results.size() == k);

    // Verify ordering: distances should be non-decreasing
    for (std::size_t i = 1; i < results.size(); ++i) {
        REQUIRE(results[i].distance >= results[i - 1].distance);
    }

    // Brute-force verify: all non-result vectors should have distance >= worst result
    double worst = results.back().distance;
    std::vector<std::size_t> result_ids;
    for (const auto& r : results) {
        result_ids.push_back(r.row_id);
    }

    for (std::size_t i = 0; i < n; ++i) {
        if (std::find(result_ids.begin(), result_ids.end(), i) != result_ids.end()) {
            continue;
        }
        double d = compute_distance(flat.data() + i * dim, query.data(), dim, metric_type::l2);
        REQUIRE(d >= worst - 1e-10);
    }
}
