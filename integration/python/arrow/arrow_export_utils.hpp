#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/vector/arrow/arrow.hpp>
#include <components/types/types.hpp>
#include <string>
#include <vector>


namespace otterbrix {

    void TransformOtterbrixToArrowChunk(ArrowSchema& arrow_schema, ArrowArray& data, py::list& batches);

    namespace pyarrow {

        py::object ToArrowTable(const std::vector<components::types::complex_logical_type>& types,
                                const std::vector<std::string>& names,
                                const py::list& batches);

    } // namespace pyarrow

} // namespace otterbrix
