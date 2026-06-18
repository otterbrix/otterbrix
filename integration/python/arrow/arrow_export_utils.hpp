#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/vector/arrow/arrow.hpp>
#include <string>
#include <vector>

namespace otterbrix {

    void transform_otterbrix_to_arrow_chunk(ArrowSchema& arrow_schema, ArrowArray& data, py::list& batches);

    namespace pyarrow {

        py::object to_arrow_table(const std::vector<components::types::complex_logical_type>& types,
                                  const std::vector<std::string>& names,
                                  const py::list& batches);

    } // namespace pyarrow

} // namespace otterbrix
