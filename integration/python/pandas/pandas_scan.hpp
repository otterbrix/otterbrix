#pragma once

#include "pandas_bind.hpp"
#include <memory>

#include <pybind11/pybind_wrapper.hpp>
#include <common/typedefs.hpp> 
#include <components/function/function.hpp>
#include <components/function/table_function.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <string>
#include <vector>

namespace otterbrix {

struct pandas_scan_function_t : public components::function::table_function_t {
public:
    static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * components::vector::DEFAULT_VECTOR_CAPACITY;

public:
    pandas_scan_function_t();

    static std::unique_ptr<components::function::function_data_t> pandas_scan_bind(components::function::table_function_bind_input_t &input,
            std::vector<components::types::complex_logical_type> &return_types, std::vector<std::string> &names);

    static std::unique_ptr<components::function::global_table_function_state_t> pandas_scan_init_global(components::function::table_function_init_input_t &input);

    static std::unique_ptr<components::function::local_table_function_state_t>pandas_scan_init_local(components::function::table_function_init_input_t &input, 
            components::function::global_table_function_state_t *gstate);

    static idx_t pandas_scan_max_threads(const components::function::function_data_t *bind_data_p);

    static bool pandas_scan_parallel_state_next(const components::function::function_data_t *bind_data_p,
            components::function::local_table_function_state_t *lstate, 
            components::function::global_table_function_state_t *gstate);

    //! The main pandas scan function: note that this can be called in parallel without the GIL
    //! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
    static void pandas_scan_func(components::function::table_function_input_t &data_p, components::vector::data_chunk_t &output);

    static idx_t pandas_scan_get_batch_index(const components::function::function_data_t *bind_data_p,
            components::function::local_table_function_state_t *local_state,
            components::function::global_table_function_state_t *global_state);

    // Helper function that transform pandas df names to make them work with our binder
    static py::object pandas_replace_copied_names(const py::object &original_df);

    static void pandas_backend_scan_switch(pandas_column_bind_data_t &bind_data,
            idx_t count, idx_t offset, components::vector::vector_t &out);
};

} // namespace otterbrix
