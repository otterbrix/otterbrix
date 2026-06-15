#pragma once

#include "arrow_array_stream.hpp"

#include <components/function/table_function.hpp>
#include <components/vector/arrow/arrow_converter.hpp>
#include <components/vector/arrow/arrow_wrapper.hpp>

#include <common/external_dependencies.hpp>

#include <memory>
#include <mutex>

namespace otterbrix {

    //! arrow_table_t function that drains a python Arrow object through the canonical core
    //! components::vector::arrow path (schema_from_arrow + data_chunk_from_arrow).
    //!
    //! The replacement-scan dispatcher in python_replacement_scan.cpp drives it generically:
    //!   inputs[0] : void* -> python_table_arrow_array_stream_factory_t*
    struct arrow_scan_function_t : public components::function::table_function_t {
    public:
        arrow_scan_function_t();

        static std::unique_ptr<components::function::function_data_t>
        ArrowScanBind(components::function::table_function_bind_input_t& input,
                      std::vector<components::types::complex_logical_type>& return_types,
                      std::vector<std::string>& names);

        static std::unique_ptr<components::function::global_table_function_state_t>
        ArrowScanInitGlobal(components::function::table_function_init_input_t& input);

        static std::unique_ptr<components::function::local_table_function_state_t>
        ArrowScanInitLocal(components::function::table_function_init_input_t& input,
                           components::function::global_table_function_state_t* gstate);

        static void ArrowScanFunc(components::function::table_function_input_t& data_p,
                                  components::vector::data_chunk_t& output);
    };

} // namespace otterbrix
