#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

namespace components::vector_search {

    enum class metric_type : uint8_t
    {
        cosine,
        l2,
        inner_product
    };

    inline std::string metric_to_string(metric_type m) {
        switch (m) {
            case metric_type::cosine:
                return "cosine";
            case metric_type::l2:
                return "l2";
            case metric_type::inner_product:
                return "inner_product";
        }
        return "unknown";
    }

    // 1 - cosine; +inf for a zero-magnitude vector.
    template<typename T>
    double cosine_distance(const T* a, const T* b, std::size_t dim) {
        double dot = 0.0;
        double norm_a = 0.0;
        double norm_b = 0.0;

        for (std::size_t i = 0; i < dim; ++i) {
            auto va = static_cast<double>(a[i]);
            auto vb = static_cast<double>(b[i]);
            dot += va * vb;
            norm_a += va * va;
            norm_b += vb * vb;
        }

        constexpr double eps = std::numeric_limits<double>::epsilon();
        if (norm_a <= eps || norm_b <= eps) {
            return std::numeric_limits<double>::infinity();
        }

        double similarity = dot / (std::sqrt(norm_a) * std::sqrt(norm_b));
        if (similarity > 1.0) {
            similarity = 1.0;
        }
        if (similarity < -1.0) {
            similarity = -1.0;
        }
        return 1.0 - similarity;
    }

    // Squared L2 (ordering-preserving, avoids sqrt).
    template<typename T>
    double l2_distance_squared(const T* a, const T* b, std::size_t dim) {
        double sum = 0.0;
        for (std::size_t i = 0; i < dim; ++i) {
            double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
            sum += diff * diff;
        }
        return sum;
    }

    template<typename T>
    double l2_distance(const T* a, const T* b, std::size_t dim) {
        return std::sqrt(l2_distance_squared(a, b, dim));
    }

    // Negated dot product (smaller = closer).
    template<typename T>
    double inner_product_distance(const T* a, const T* b, std::size_t dim) {
        double dot = 0.0;
        for (std::size_t i = 0; i < dim; ++i) {
            dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
        }
        return -dot;
    }

    // kNN ordering key — same scales as the hnswlib spaces.
    template<typename T>
    double compute_distance(const T* a, const T* b, std::size_t dim, metric_type metric) {
        switch (metric) {
            case metric_type::cosine:
                return 2.0 * cosine_distance(a, b, dim);
            case metric_type::l2:
                return l2_distance_squared(a, b, dim);
            case metric_type::inner_product:
                return 1.0 + inner_product_distance(a, b, dim); // 1 − dot
        }
        return 0.0;
    }

} // namespace components::vector_search
