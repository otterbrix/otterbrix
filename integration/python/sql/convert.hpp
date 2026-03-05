#pragma once

#include <memory_resource>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/logical_value.hpp>

namespace py = pybind11;

components::types::logical_value_t to_value(std::pmr::memory_resource* resource, const py::handle& obj);

auto to_pylist(const std::pmr::vector<std::string>& src) -> py::list;

auto to_statement(std::pmr::memory_resource* resource,
                  const py::handle& source,
                  components::logical_plan::node_aggregate_t*,
                  components::logical_plan::parameter_node_t* params) -> void;
auto test_to_statement(const py::handle& source) -> py::str;

auto pack_to_match(const py::object& object) -> py::list;
