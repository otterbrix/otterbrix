#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <components/table/data_table.hpp>
#include <components/tableref/tableref.hpp>
#include <memory>

#include <components/logical_plan/node_data.hpp>
#include <components/table/column_definition.hpp>
#include <utility>
#include <string>
#include <vector>

namespace otterbrix {
    class Scan {
        public:
        //! Try to perform a replacement, returns NULL on error
        static std::unique_ptr<components::tableref::TableRef> 
            TryReplacementObject(const py::object &entry, const std::string &name);

        //! Perform a replacement or throw if it failed
        static std::unique_ptr<components::tableref::TableRef> 
            ReplacementObject(const py::object &entry, const std::string &name);
        
        static std::pair<components::logical_plan::node_data_ptr, std::unique_ptr<std::vector<components::table::column_definition_t>>>
            FetchObjectData(std::pmr::memory_resource* resource, std::unique_ptr<components::tableref::TableRef> ref) ;


    };
} // namespace otterbrix
