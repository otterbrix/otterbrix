#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/dataframe.hpp>
#include <connection_environment/connection_environment.hpp>
#include <native/python_objects.hpp>

#include <core/types/memory.hpp>
#include <core/types/vector.hpp>
#include <core/typedefs.hpp>

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>


namespace otterbrix {
    class PyResult {
    public:
        PyResult(ConnectionEnvironment* env, components::cursor::cursor_t_ptr result,
                const vector<components::table::column_definition_t>& columns);
        ~PyResult();
        Optional<py::tuple> Fetchone();

        py::list Fetchmany(idx_t size);

        py::list Fetchall();
        
        PandasDataFrame FetchDF();

        void Close();

        bool IsClosed() const;

    private:
        ConnectionEnvironment* env;
        
        components::cursor::cursor_t_ptr result;
        vector<components::table::column_definition_t> columns;

    };

} // namespace otterbrix
