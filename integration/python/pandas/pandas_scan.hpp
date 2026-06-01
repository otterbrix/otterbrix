#pragma once

#include "pandas_bind.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>
#include <core/typedefs.hpp> 
#include <components/function/function.hpp>
#include <components/function/table_function.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>

namespace otterbrix {

struct PandasScanFunction : public components::function::TableFunction {
public:
    static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * components::vector::DEFAULT_VECTOR_CAPACITY;

public:
    PandasScanFunction();

    static unique_ptr<components::function::FunctionData> PandasScanBind(components::function::TableFunctionBindInput &input,
            vector<components::types::complex_logical_type> &return_types, vector<string> &names);

    static unique_ptr<components::function::GlobalTableFunctionState> PandasScanInitGlobal(components::function::TableFunctionInitInput &input);

    static unique_ptr<components::function::LocalTableFunctionState>PandasScanInitLocal(components::function::TableFunctionInitInput &input, 
            components::function::GlobalTableFunctionState *gstate);

    static idx_t PandasScanMaxThreads(const components::function::FunctionData *bind_data_p);

    static bool PandasScanParallelStateNext(const components::function::FunctionData *bind_data_p,
            components::function::LocalTableFunctionState *lstate, 
            components::function::GlobalTableFunctionState *gstate);

    //! The main pandas scan function: note that this can be called in parallel without the GIL
    //! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
    static void PandasScanFunc(components::function::TableFunctionInput &data_p, components::vector::data_chunk_t &output);

    static idx_t PandasScanGetBatchIndex(const components::function::FunctionData *bind_data_p,
            components::function::LocalTableFunctionState *local_state,
            components::function::GlobalTableFunctionState *global_state);

    // Helper function that transform pandas df names to make them work with our binder
    static py::object PandasReplaceCopiedNames(const py::object &original_df);

    static void PandasBackendScanSwitch(PandasColumnBindData &bind_data,
            idx_t count, idx_t offset, components::vector::vector_t &out);
};

} // namespace otterbrix
