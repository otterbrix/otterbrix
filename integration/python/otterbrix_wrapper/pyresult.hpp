#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <native/python_objects.hpp>

#include <core/typedefs.hpp>

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <vector>


namespace otterbrix {
    class PyConnection;

    class PyResult {
    public:
        PyResult(PyConnection* env, components::cursor::cursor_t_ptr result,
                const std::vector<components::table::column_definition_t>& columns);
        ~PyResult();
        Optional<py::tuple> Fetchone();

        py::list Fetchmany(idx_t size);

        py::list Fetchall();
        
        PandasDataFrame FetchDF();

        void Close();

        bool IsClosed() const;

    private:
        PyConnection* env;

        components::cursor::cursor_t_ptr result;
        std::vector<components::table::column_definition_t> columns;

    };

} // namespace otterbrix
