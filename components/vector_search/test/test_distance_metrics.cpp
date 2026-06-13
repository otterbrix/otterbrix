#include <catch2/catch.hpp>
#include <vector_search/distance_metrics.hpp>

#include <cmath>
#include <vector>

using namespace components::vector_search;

TEST_CASE("vector_search::distance_metrics::cosine::identical_vectors") {
    std::vector<double> a = {1.0, 2.0, 3.0};
    double dist = cosine_distance(a.data(), a.data(), a.size());
    REQUIRE(dist == Approx(0.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::cosine::orthogonal_vectors") {
    std::vector<double> a = {1.0, 0.0};
    std::vector<double> b = {0.0, 1.0};
    double dist = cosine_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(1.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::cosine::opposite_vectors") {
    std::vector<double> a = {1.0, 0.0, 0.0};
    std::vector<double> b = {-1.0, 0.0, 0.0};
    double dist = cosine_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(2.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::cosine::parallel_scaled") {
    std::vector<double> a = {1.0, 2.0, 3.0};
    std::vector<double> b = {2.0, 4.0, 6.0};
    double dist = cosine_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(0.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::cosine::zero_vector") {
    std::vector<double> a = {1.0, 2.0, 3.0};
    std::vector<double> zero = {0.0, 0.0, 0.0};
    double dist = cosine_distance(a.data(), zero.data(), a.size());
    REQUIRE(std::isinf(dist));
    REQUIRE(dist > 0.0);
}

TEST_CASE("vector_search::distance_metrics::cosine::known_value") {
    // cos(45°) = sqrt(2)/2 ≈ 0.7071, distance = 1 - 0.7071 ≈ 0.2929
    std::vector<double> a = {1.0, 0.0};
    std::vector<double> b = {1.0, 1.0};
    double dist = cosine_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(1.0 - std::sqrt(2.0) / 2.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::cosine::float_type") {
    std::vector<float> a = {1.0f, 2.0f, 3.0f};
    std::vector<float> b = {2.0f, 4.0f, 6.0f};
    double dist = cosine_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(0.0).margin(1e-6));
}

TEST_CASE("vector_search::distance_metrics::cosine::high_dim") {
    constexpr std::size_t dim = 128;
    std::vector<double> a(dim, 1.0);
    std::vector<double> b(dim, 1.0);
    double dist = cosine_distance(a.data(), b.data(), dim);
    REQUIRE(dist == Approx(0.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::l2::identical_vectors") {
    std::vector<double> a = {1.0, 2.0, 3.0};
    double dist = l2_distance(a.data(), a.data(), a.size());
    REQUIRE(dist == Approx(0.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::l2::known_value") {
    std::vector<double> a = {0.0, 0.0};
    std::vector<double> b = {3.0, 4.0};
    double dist = l2_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(5.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::l2::unit_distance") {
    std::vector<double> a = {0.0};
    std::vector<double> b = {1.0};
    double dist = l2_distance(a.data(), b.data(), a.size());
    REQUIRE(dist == Approx(1.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::l2_squared::preserves_order") {
    std::vector<double> query = {0.0, 0.0};
    std::vector<double> near = {1.0, 1.0};
    std::vector<double> far = {3.0, 4.0};

    double d_near = l2_distance_squared(query.data(), near.data(), query.size());
    double d_far = l2_distance_squared(query.data(), far.data(), query.size());

    REQUIRE(d_near < d_far);
    REQUIRE(d_near == Approx(2.0).margin(1e-10));
    REQUIRE(d_far == Approx(25.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::l2::high_dim") {
    constexpr std::size_t dim = 128;
    std::vector<double> a(dim, 0.0);
    std::vector<double> b(dim, 1.0);
    double dist = l2_distance(a.data(), b.data(), dim);
    // sqrt(128 * 1^2) = sqrt(128)
    REQUIRE(dist == Approx(std::sqrt(128.0)).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::compute_distance::dispatches_cosine") {
    std::vector<double> a = {1.0, 0.0};
    std::vector<double> b = {0.0, 1.0};

    // hnswlib scale: L2² over unit vectors = 2·(1−cosθ); orthogonal → 2.
    double dist = compute_distance(a.data(), b.data(), a.size(), metric_type::cosine);
    REQUIRE(dist == Approx(2.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::compute_distance::dispatches_inner_product") {
    std::vector<double> a = {2.0, 0.5};
    std::vector<double> b = {1.0, 2.0};

    // hnswlib scale: 1 − dot = 1 − 3 = −2.
    double dist = compute_distance(a.data(), b.data(), a.size(), metric_type::inner_product);
    REQUIRE(dist == Approx(-2.0).margin(1e-10));
}

TEST_CASE("vector_search::distance_metrics::compute_distance::dispatches_l2") {
    std::vector<double> a = {0.0, 0.0};
    std::vector<double> b = {3.0, 4.0};

    double dist = compute_distance(a.data(), b.data(), a.size(), metric_type::l2);
    // l2 dispatch uses squared distance
    REQUIRE(dist == Approx(25.0).margin(1e-10));
}
