#pragma once

#include <components/table/data_table.hpp>
#include <components/tableref/tableref.hpp>
#include <memory>
#include <pybind11/pybind_wrapper.hpp>

#include <components/logical_plan/node_data.hpp>
#include <components/table/column_definition.hpp>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace otterbrix {
    class scan_t {
    public:
        //! Try to perform a replacement, returns NULL on error.
        //! `resource` is the arena the table_ref_t's argument list is built on. It is the
        //! CALLER's engine arena (py_connection_t hands over space->dispatcher()->resource()),
        //! which outlives the ref: the ref is consumed by fetch_object_data below, one call
        //! later, and the connection that owns the arena outlives both.
        static std::unique_ptr<components::tableref::table_ref_t>
        try_replacement_object(std::pmr::memory_resource* resource, const py::object& entry, const std::string& name);

        //! Perform a replacement or throw if it failed
        static std::unique_ptr<components::tableref::table_ref_t>
        replacement_object(std::pmr::memory_resource* resource, const py::object& entry, const std::string& name);

        static std::pair<components::logical_plan::node_data_ptr,
                         std::unique_ptr<std::vector<components::table::column_definition_t>>>
        fetch_object_data(std::pmr::memory_resource* resource, std::unique_ptr<components::tableref::table_ref_t> ref);
    };
} // namespace otterbrix
