#pragma once

#include <components/function/table_function.hpp>
#include <components/types/logical_value.hpp>

#include <core/external_dependencies.hpp>

#include <memory>
#include <vector>

namespace components::tableref {
    struct TableRef {
        std::shared_ptr<otterbrix::ExternalDependency> external_dependency;
        std::vector<components::types::logical_value_t> children;
        std::unique_ptr<components::function::TableFunction> function;
    };

} // namespace components::tableref
