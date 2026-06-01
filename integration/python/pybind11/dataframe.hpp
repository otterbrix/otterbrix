#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>

namespace otterbrix {

    class PandasDataFrame : public py::object {
    public:
        PandasDataFrame(const py::object &o) : py::object(o, borrowed_t {}) {
        }
        using py::object::object;

        public:
        static bool check_(const py::handle &object); // NOLINT
        static bool IsPyArrowBacked(const py::handle &df);
        static py::object ToArrowTable(const py::object &df);
    };

} // namespace otterbrix
