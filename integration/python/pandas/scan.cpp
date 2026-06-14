#include "pandas_scan.hpp"
#include <memory>
#include "pandas_bind.hpp"
#include "column/pandas_numpy_column.hpp"

#include <numpy/array_wrapper.hpp>
#include <numpy/numpy_scan.hpp>
#include <numpy/numpy_bind.hpp>
#include <components/function/table_function.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/tableref/tableref.hpp>
#include <common/string_util/string_util.hpp>
#include <common/external_dependencies.hpp>
#include <atomic>
#include <memory_resource>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace otterbrix {

using components::types::complex_logical_type;
using namespace components::function;


struct pandas_scan_function_data_t : public table_function_data_t {
    pandas_scan_function_data_t(py::handle df, idx_t row_count, std::vector<pandas_column_bind_data_t> pandas_bind_data,
                           std::vector<complex_logical_type> sql_types, dependency_item_t* dependency)
        : df(df), row_count(row_count), lines_read(0), pandas_bind_data(std::move(pandas_bind_data))
        , sql_types(std::move(sql_types)), copied_df(dependency)  {
    }
    py::handle df;
    idx_t row_count;
    std::atomic<idx_t> lines_read;
    std::vector<pandas_column_bind_data_t> pandas_bind_data;
    std::vector<complex_logical_type> sql_types;
    //! Non-owning observer of the source/copy dependency. The owning external_dependency_t (held by
    //! the table_ref_t) outlives this function_data_t, so a borrow is sufficient to document the alias.
    dependency_item_t* copied_df;

    ~pandas_scan_function_data_t() override {
         try {
              py::gil_scoped_acquire acquire;
              pandas_bind_data.clear();
         } catch (...) { // NOLINT
         }
    }
};

struct pandas_scan_local_state_t : public local_table_function_state_t {
    pandas_scan_local_state_t(idx_t start, idx_t end) : start(start), end(end), batch_index(0) {
    }

    idx_t start;
    idx_t end;
    idx_t batch_index;
    std::vector<uint64_t> column_ids;
};

struct pandas_scan_global_state_t : public global_table_function_state_t {
    explicit pandas_scan_global_state_t(idx_t max_threads) : position(0), batch_index(0), max_threads_(max_threads) {
    }

    std::mutex lock;
    idx_t position;
    idx_t batch_index;
    idx_t max_threads_;

    idx_t max_threads() const override {
         return max_threads_;
    }
};

pandas_scan_function_t::pandas_scan_function_t()
    : table_function_t("pandas_scan", {components::types::logical_type::POINTER}, pandas_scan_func, pandas_scan_bind, pandas_scan_init_global,
                   pandas_scan_init_local) {
    }

std::unique_ptr<function_data_t> pandas_scan_function_t::pandas_scan_bind(table_function_bind_input_t &input,
                                                           std::vector<complex_logical_type> &return_types, std::vector<std::string> &names) {
    py::gil_scoped_acquire acquire;
    py::handle df(reinterpret_cast<PyObject *>(input.inputs[0].value<void*>()));

    std::vector<pandas_column_bind_data_t> pandas_bind_data;

    // NOTE: the table-function bind callback (table_function_bind_t) has a fixed C-ABI
    // signature with no threaded engine resource, so the binder resource is rooted here.
    auto* resource = std::pmr::get_default_resource();
    auto is_py_dict = py::isinstance<py::dict>(df);
    auto bind_error = is_py_dict
                          ? numpy_bind_t::bind(resource, df, pandas_bind_data, return_types, names)
                          : pandas_t::bind(resource, df, pandas_bind_data, return_types, names);
    if (bind_error.contains_error()) {
         // Locked table_function_bind_t contract surfaces failures via exception (DEFER: R3).
         throw std::runtime_error(std::string(bind_error.what));
    }
    auto df_columns = py::list(df.attr("keys")());

    auto &ref = input.ref;
    dependency_item_t* dependency_item = nullptr;
    if (ref.external_dependency) {
        // This was created during the replacement scan if this was a pandas data_frame (see python_replacement_scan.cpp)
        dependency_item = ref.external_dependency->get_dependency("copy");
        if (!dependency_item) {
            // This was created during the replacement if this was a numpy scan
            dependency_item = ref.external_dependency->get_dependency("data");
        }
    }

    auto get_fun = df.attr("__getitem__");
    idx_t row_count = py::len(get_fun(df_columns[0]));
    return std::make_unique<pandas_scan_function_data_t>(df, row_count, std::move(pandas_bind_data), return_types, dependency_item);
}

std::unique_ptr<global_table_function_state_t> pandas_scan_function_t::pandas_scan_init_global(table_function_init_input_t &input) {
    if (PyGILState_Check()) {
         throw std::runtime_error("PandasScan called but GIL was already held!");
    }
    return std::make_unique<pandas_scan_global_state_t>(pandas_scan_max_threads(input.bind_data.get()));
}

std::unique_ptr<local_table_function_state_t> pandas_scan_function_t::pandas_scan_init_local(table_function_init_input_t &input,
                                                                           global_table_function_state_t *gstate) {
    auto result = std::make_unique<pandas_scan_local_state_t>(0, 0);
    result->column_ids = input.column_ids;
    pandas_scan_parallel_state_next(input.bind_data.get(), result.get(), gstate);
    return result;
}

idx_t pandas_scan_function_t::pandas_scan_max_threads(const function_data_t *bind_data_p) {
    auto &bind_data = bind_data_p->cast<pandas_scan_function_data_t>();
    return bind_data.row_count / PANDAS_PARTITION_COUNT + 1;
}

bool pandas_scan_function_t::pandas_scan_parallel_state_next(const function_data_t *bind_data_p,
                                                    local_table_function_state_t *lstate,
                                                    global_table_function_state_t *gstate) {
    auto &bind_data = bind_data_p->cast<pandas_scan_function_data_t>();
    auto &parallel_state = gstate->cast<pandas_scan_global_state_t>();
    auto &state = lstate->cast<pandas_scan_local_state_t>();

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

void pandas_scan_function_t::pandas_backend_scan_switch(pandas_column_bind_data_t &bind_data, idx_t count, idx_t offset,
                                                components::vector::vector_t &out) {
    auto backend = bind_data.pandas_col->backend();
    switch (backend) {
    case pandas_column_backend_t::NUMPY: {
         auto scan_error = numpy_scan_t::scan(out.resource(), bind_data, count, offset, out);
         if (scan_error.contains_error()) {
             // Locked table_function_t contract surfaces failures via exception (DEFER: R3).
             throw std::runtime_error(std::string(scan_error.what));
         }
         break;
    }
    default: {
         throw std::runtime_error("Type not implemented for PandasColumnBackend");
    }
    }
}

//! The main pandas scan function: note that this can be called in parallel without the GIL
//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
void pandas_scan_function_t::pandas_scan_func(table_function_input_t &data_p, components::vector::data_chunk_t &output) {
    auto &data = data_p.bind_data->cast<pandas_scan_function_data_t>();
    auto &state = data_p.local_state->cast<pandas_scan_local_state_t>();

    if (state.start >= state.end) {
         if (!pandas_scan_parallel_state_next(data_p.bind_data.get(), data_p.local_state.get(),
                                          data_p.global_state.get())) {
              return;
         }
    }
    idx_t this_count = std::min(static_cast<idx_t>(components::vector::DEFAULT_VECTOR_CAPACITY), state.end - state.start);
    output.set_cardinality(this_count);
    for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
         auto col_idx = state.column_ids[idx];
         if (col_idx == static_cast<uint64_t>(-1)) {
              output.data[idx].sequence(static_cast<int64_t>(state.start), 1, this_count);
         } else {
              pandas_backend_scan_switch(data.pandas_bind_data[col_idx], this_count, state.start, output.data[idx]);
         }
    }
    state.start += this_count;
    data.lines_read += this_count;
}

py::object pandas_scan_function_t::pandas_replace_copied_names(const py::object &original_df) {
    py::object copy_df = original_df.attr("copy")(false);
    auto df_columns = py::list(original_df.attr("columns"));
    std::vector<std::string> columns;
    for (const auto &str : df_columns) {
         columns.push_back(std::string(py::str(str)));
    }
    string_utils::deduplicate_columns(columns);

    py::list new_columns(columns.size());
    for (idx_t i = 0; i < columns.size(); i++) {
         new_columns[i] = std::move(columns[i]);
    }
    copy_df.attr("columns") = std::move(new_columns);
    columns.clear();
    return copy_df;
}


} // namespace otterbrix
