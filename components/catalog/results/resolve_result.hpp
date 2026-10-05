#pragma once

#include <components/catalog/results/ddl_result.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/proc_signature.hpp>

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

    // Field order: strings → signature → uint64 → oid_t → bool.
    struct resolve_function_result_t {
        std::string name;
        components::catalog::proc_signature_t signature;
        std::uint64_t prouid{0};
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        bool found{false};

        resolve_function_result_t() = default;
        explicit resolve_function_result_t(std::pmr::memory_resource* /*resource*/) {}
    };
    // Layout guard: libstdc++ (string==32) → 128; libc++ (string==24) → 104.
    static_assert(sizeof(resolve_function_result_t) <= 128, "resolve_function_result_t layout regression");

} // namespace services::disk
