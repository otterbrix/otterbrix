#pragma once

#include <components/function/table_function.hpp>

#include <common/external_dependencies.hpp>

#include <cstdint>
#include <memory>

namespace components::tableref {
    struct table_ref_t {
        std::shared_ptr<otterbrix::external_dependency_t> external_dependency;
        uintptr_t arrow_factory = 0;
        std::unique_ptr<components::function::table_function_t> function;
    };

} // namespace components::tableref
