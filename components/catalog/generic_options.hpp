#pragma once

#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace components::catalog {

    // OPTIONS (name 'value', ...) of a foreign server or table, one pg_foreign_option row each. Opaque to
    // the engine: only the server type's connector reads them.
    struct generic_option_t {
        std::pmr::string name;
        std::pmr::string value;

        generic_option_t(std::pmr::string name, std::pmr::string value)
            : name(std::move(name))
            , value(std::move(value)) {}
    };

    using generic_options_t = std::pmr::vector<generic_option_t>;

} // namespace components::catalog
