#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/arrow/arrow.hpp>
#include <components/types/types.hpp>

#include <core/types/string.hpp>
#include <core/types/vector.hpp>

namespace otterbrix {

    void TransformOtterbrixToArrowChunk(ArrowSchema &arrow_schema, ArrowArray &data, py::list &batches);

    namespace pyarrow {
    
        py::object ToArrowTable(const vector<components::types::complex_logical_type> &types, 
                const vector<string> &names, const py::list &batches);
    
    } // namespace pyarrow

} // namespace otterbrix
