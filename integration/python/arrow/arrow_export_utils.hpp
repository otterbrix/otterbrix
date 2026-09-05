#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <components/vector/arrow/arrow.hpp>
#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

    void transform_otterbrix_to_arrow_chunk(ArrowSchema& arrow_schema, ArrowArray& data, py::list& batches);

    namespace pyarrow {

        //! `resource` holds the schema's type list for the length of this call and nothing
        //! longer: the ArrowSchema is exported to pyarrow before it returns. It is an
        //! argument rather than the process default because rule 14 leaves no global to
        //! reach for, and because the only caller shape here -- a connection or a space --
        //! always has an arena where it stands.
        py::object to_arrow_table(std::pmr::memory_resource* resource,
                                  const std::vector<components::types::complex_logical_type>& types,
                                  const std::vector<std::string>& names,
                                  const py::list& batches);

    } // namespace pyarrow

} // namespace otterbrix
