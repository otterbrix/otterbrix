#pragma once

#include <components/function/table_function.hpp>
#include <components/types/logical_value.hpp>

#include <common/external_dependencies.hpp>

#include <memory>
#include <vector>

namespace components::tableref {
    struct table_ref_t {
        std::shared_ptr<otterbrix::external_dependency_t> external_dependency;
        std::vector<components::types::logical_value_t> children;
        std::unique_ptr<components::function::table_function_t> function;
    };

} // namespace components::tableref
