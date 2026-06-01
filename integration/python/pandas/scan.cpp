#include "pandas_scan.hpp"
#include "pandas_bind.hpp"
#include "column/pandas_numpy_column.hpp"

#include <numpy/array_wrapper.hpp>
#include <numpy/numpy_scan.hpp>
#include <numpy/numpy_bind.hpp>
#include <components/function/table_function.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/tableref/tableref.hpp>
#include <core/string_util/string_util.hpp>
#include <core/external_dependencies.hpp>
#include <atomic>
#include <mutex>

namespace otterbrix {

using components::types::complex_logical_type;
using namespace components::function;


struct PandasScanFunctionData : public TableFunctionData {
    PandasScanFunctionData(py::handle df, idx_t row_count, vector<PandasColumnBindData> pandas_bind_data,
                           vector<complex_logical_type> sql_types, shared_ptr<DependencyItem> dependency)
        : df(df), row_count(row_count), lines_read(0), pandas_bind_data(std::move(pandas_bind_data))
        , sql_types(std::move(sql_types)), copied_df(std::move(dependency))  {
    }
    py::handle df;
    idx_t row_count;
    std::atomic<idx_t> lines_read;
    vector<PandasColumnBindData> pandas_bind_data;
    vector<complex_logical_type> sql_types;
    shared_ptr<DependencyItem> copied_df;

    ~PandasScanFunctionData() override {
         try {
              py::gil_scoped_acquire acquire;
              pandas_bind_data.clear();
         } catch (...) { // NOLINT
         }
    }
};

struct PandasScanLocalState : public LocalTableFunctionState {
    PandasScanLocalState(idx_t start, idx_t end) : start(start), end(end), batch_index(0) {
    }

    idx_t start;
    idx_t end;
    idx_t batch_index;
    vector<uint64_t> column_ids;
};

struct PandasScanGlobalState : public GlobalTableFunctionState {
    explicit PandasScanGlobalState(idx_t max_threads) : position(0), batch_index(0), max_threads(max_threads) {
    }

    std::mutex lock;
    idx_t position;
    idx_t batch_index;
    idx_t max_threads;

    idx_t MaxThreads() const override {
         return max_threads;
    }
};

PandasScanFunction::PandasScanFunction()
    : TableFunction("pandas_scan", {components::types::logical_type::POINTER}, PandasScanFunc, PandasScanBind, PandasScanInitGlobal,
                   PandasScanInitLocal) {
    }

unique_ptr<FunctionData> PandasScanFunction::PandasScanBind(TableFunctionBindInput &input,
                                                           vector<complex_logical_type> &return_types, vector<string> &names) {
    py::gil_scoped_acquire acquire;
    py::handle df(reinterpret_cast<PyObject *>(input.inputs[0].value<void*>()));

    vector<PandasColumnBindData> pandas_bind_data;

    auto is_py_dict = py::isinstance<py::dict>(df);
    if (is_py_dict) {
         NumpyBind::Bind(df, pandas_bind_data, return_types, names);
    } else {
         Pandas::Bind(df, pandas_bind_data, return_types, names);
    }
    auto df_columns = py::list(df.attr("keys")());

    auto &ref = input.ref;
    shared_ptr<DependencyItem> dependency_item;
    if (ref.external_dependency) {
        // This was created during the replacement scan if this was a pandas DataFrame (see python_replacement_scan.cpp)
        dependency_item = ref.external_dependency->GetDependency("copy");
        if (!dependency_item) {
            // This was created during the replacement if this was a numpy scan
            dependency_item = ref.external_dependency->GetDependency("data");
        }
    }

    auto get_fun = df.attr("__getitem__");
    idx_t row_count = py::len(get_fun(df_columns[0]));
    return make_unique<PandasScanFunctionData>(df, row_count, std::move(pandas_bind_data), return_types, dependency_item);
}

unique_ptr<GlobalTableFunctionState> PandasScanFunction::PandasScanInitGlobal(TableFunctionInitInput &input) {
    if (PyGILState_Check()) {
         throw std::runtime_error("PandasScan called but GIL was already held!");
    }
    return make_unique<PandasScanGlobalState>(PandasScanMaxThreads(input.bind_data.get()));
}

unique_ptr<LocalTableFunctionState> PandasScanFunction::PandasScanInitLocal(TableFunctionInitInput &input,
                                                                           GlobalTableFunctionState *gstate) {
    auto result = make_unique<PandasScanLocalState>(0, 0);
    result->column_ids = input.column_ids;
    PandasScanParallelStateNext(input.bind_data.get(), result.get(), gstate);
    return std::move(result);
}

idx_t PandasScanFunction::PandasScanMaxThreads(const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<PandasScanFunctionData>();
    return bind_data.row_count / PANDAS_PARTITION_COUNT + 1;
}

bool PandasScanFunction::PandasScanParallelStateNext(const FunctionData *bind_data_p,
                                                    LocalTableFunctionState *lstate,
                                                    GlobalTableFunctionState *gstate) {
    auto &bind_data = bind_data_p->Cast<PandasScanFunctionData>();
    auto &parallel_state = gstate->Cast<PandasScanGlobalState>();
    auto &state = lstate->Cast<PandasScanLocalState>();

    std::lock_guard<std::mutex> parallel_lock(parallel_state.lock);
    if (parallel_state.position >= bind_data.row_count) {
         return false;
    }
    state.start = parallel_state.position;
    parallel_state.position += PANDAS_PARTITION_COUNT;
    if (parallel_state.position > bind_data.row_count) {
         parallel_state.position = bind_data.row_count;
    }
    state.end = parallel_state.position;
    state.batch_index = parallel_state.batch_index++;
    return true;
}

void PandasScanFunction::PandasBackendScanSwitch(PandasColumnBindData &bind_data, idx_t count, idx_t offset,
                                                components::vector::vector_t &out) {
    auto backend = bind_data.pandas_col->Backend();
    switch (backend) {
    case PandasColumnBackend::NUMPY: {
         NumpyScan::Scan(bind_data, count, offset, out);
         break;
    }
    default: {
         throw std::runtime_error("Type not implemented for PandasColumnBackend");
    }
    }
}

//! The main pandas scan function: note that this can be called in parallel without the GIL
//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
void PandasScanFunction::PandasScanFunc(TableFunctionInput &data_p, components::vector::data_chunk_t &output) {
    auto &data = data_p.bind_data->Cast<PandasScanFunctionData>();
    auto &state = data_p.local_state->Cast<PandasScanLocalState>();

    if (state.start >= state.end) {
         if (!PandasScanParallelStateNext(data_p.bind_data.get(), data_p.local_state.get(),
                                          data_p.global_state.get())) {
              return;
         }
    }
    idx_t this_count = std::min((idx_t)components::vector::DEFAULT_VECTOR_CAPACITY, state.end - state.start);
    output.set_cardinality(this_count);
    for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
         auto col_idx = state.column_ids[idx];
         //components::table::COLUMN_IDENTIFIER_ROW_ID
         if (col_idx == (uint64_t) -1) {
              output.data[idx].sequence(state.start, 1, this_count);
         } else {
              PandasBackendScanSwitch(data.pandas_bind_data[col_idx], this_count, state.start, output.data[idx]);
         }
    }
    state.start += this_count;
    data.lines_read += this_count;
}

py::object PandasScanFunction::PandasReplaceCopiedNames(const py::object &original_df) {
    py::object copy_df = original_df.attr("copy")(false);
    auto df_columns = py::list(original_df.attr("columns"));
    vector<string> columns;
    for (const auto &str : df_columns) {
         columns.push_back(string(py::str(str)));
    }
    string_utils::DeduplicateColumns(columns);

    py::list new_columns(columns.size());
    for (idx_t i = 0; i < columns.size(); i++) {
         new_columns[i] = std::move(columns[i]);
    }
    copy_df.attr("columns") = std::move(new_columns);
    columns.clear();
    return copy_df;
}


} // namespace otterbrix
