#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <components/table/data_table.hpp>
#include <components/tableref/tableref.hpp>
#include <core/types/memory.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>

#include <components/logical_plan/node_data.hpp>
#include <components/table/column_definition.hpp>
#include <utility>

namespace otterbrix {
    class Scan {
        public:
        //! Try to perform a replacement, returns NULL on error
        static unique_ptr<components::tableref::TableRef> 
            TryReplacementObject(const py::object &entry, const string &name);

        //! Perform a replacement or throw if it failed
        static unique_ptr<components::tableref::TableRef> 
            ReplacementObject(const py::object &entry, const string &name);
        
        static std::pair<components::logical_plan::node_data_ptr, unique_ptr<vector<components::table::column_definition_t>>>
            FetchObjectData(std::pmr::memory_resource* resource, unique_ptr<components::tableref::TableRef> ref) ;


    };
} // namespace otterbrix
