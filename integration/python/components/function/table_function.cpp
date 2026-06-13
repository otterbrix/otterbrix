#include "table_function.hpp"
#include <components/tableref/tableref.hpp>
namespace components::function {
    TableFunctionBindInput::TableFunctionBindInput(std::vector<types::logical_value_t>& inputs, tableref::TableRef& ref)
        : inputs(inputs), ref(ref) {}

    GlobalTableFunctionState::~GlobalTableFunctionState() = default;

    LocalTableFunctionState::~LocalTableFunctionState() = default;

    TableFunction::TableFunction(std::string name, std::vector<types::complex_logical_type> arguments, table_function_t function,
            table_function_bind_t bind, table_function_init_global_t init_global, table_function_init_local_t init_local)
        : SimpleNamedParameterFunction(std::move(name), std::move(arguments)), bind(bind)
        , init_global(init_global), init_local(init_local), function(function) {}
} // namespace components::function
