#pragma once

#include <cstdint>
#include <string>

namespace components::catalog {

    // A kernel signature as its pg_proc row stores it (PostgreSQL 18: one row per signature).
    struct proc_signature_t {
        std::int32_t pronargs{0};
        std::string proargmatchers;
        std::string prorettype;

        bool operator==(const proc_signature_t&) const = default;
    };

} // namespace components::catalog
