#pragma once

#include "arrow_array_stream.hpp"

#include <components/function/table_function.hpp>
#include <components/vector/arrow/arrow_converter.hpp>
#include <components/vector/arrow/arrow_wrapper.hpp>

#include <core/external_dependencies.hpp>
#include <core/types/memory.hpp>

#include <memory>
#include <mutex>

namespace otterbrix {

    //! Table function that drains a python Arrow object through the canonical core
    //! components::vector::arrow path (schema_from_arrow + data_chunk_from_arrow).
    //!
    //! Conventions match PandasScanFunction so the replacement-scan dispatcher in
    //! python_replacement_scan.cpp can drive it generically:
    //!   inputs[0] : void* -> PythonTableArrowArrayStreamFactory*
    struct ArrowScanFunction : public components::function::TableFunction {
    public:
        ArrowScanFunction();

        static unique_ptr<components::function::FunctionData>
        ArrowScanBind(components::function::TableFunctionBindInput& input,
                      std::vector<components::types::complex_logical_type>& return_types,
                      std::vector<std::string>& names);

        static unique_ptr<components::function::GlobalTableFunctionState>
        ArrowScanInitGlobal(components::function::TableFunctionInitInput& input);

        static unique_ptr<components::function::LocalTableFunctionState>
        ArrowScanInitLocal(components::function::TableFunctionInitInput& input,
                           components::function::GlobalTableFunctionState* gstate);

        static void ArrowScanFunc(components::function::TableFunctionInput& data_p,
                                  components::vector::data_chunk_t& output);
    };

} // namespace otterbrix
