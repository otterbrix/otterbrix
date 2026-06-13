#pragma once

#include <components/catalog/results/ddl_result.hpp>

#include <components/catalog/catalog_oids.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>

namespace services::disk {

    struct resolve_namespace_result_t {
        bool found{false};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string name;

        resolve_namespace_result_t() = default;
        explicit resolve_namespace_result_t(std::pmr::memory_resource* /*resource*/) {}
    };

    // Field order: strings → uint64 → oid_t/int32 → bool.
    struct resolve_function_result_t {
        std::string name;
        std::string proargmatchers;
        std::string prorettype;
        std::uint64_t prouid{0};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::int32_t pronargs{0};
        bool found{false};

        resolve_function_result_t() = default;
        explicit resolve_function_result_t(std::pmr::memory_resource* /*resource*/) {}
    };
    // Layout guard: libstdc++ (string==32) → 120; libc++ (string==24) → 96.
    static_assert(sizeof(resolve_function_result_t) <= 120, "resolve_function_result_t layout regression");

} // namespace services::disk
