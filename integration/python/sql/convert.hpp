#pragma once

#include <memory_resource>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include <components/document/document.hpp>
#include <components/physical_plan/collection/operators/sort/sort.hpp>

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace py = pybind11;

namespace components::document {
    class py_handle_decoder_t {
        py_handle_decoder_t() = delete;
        py_handle_decoder_t(const py_handle_decoder_t&) = delete;
        py_handle_decoder_t(py_handle_decoder_t&&) = delete;
        py_handle_decoder_t& operator=(const py_handle_decoder_t&) = delete;
        py_handle_decoder_t& operator=(py_handle_decoder_t&&) = delete;

    public:
        static document_ptr to_document(const py::handle& source, std::pmr::memory_resource* resource);
    };
} // namespace components::document

auto to_document(const py::handle& obj, std::pmr::memory_resource* resource) -> document_ptr;
auto from_document(const document_ptr& document) -> py::object;

auto from_object(const document_ptr& document, const std::string& key) -> py::object;
auto from_object(const document_ptr& document, uint32_t index) -> py::object;

auto to_pylist(const std::pmr::vector<std::string>& src) -> py::list;
auto to_pylist(const std::pmr::vector<components::document::document_id_t>& src) -> py::list;

auto to_sorter(const py::handle& sort_dict) -> components::collection::sort::sorter_t;
auto to_order(const py::object& order) -> components::collection::sort::order;

auto to_statement(std::pmr::memory_resource* resource,
                  const py::handle& source,
                  components::logical_plan::node_aggregate_t*,
                  components::logical_plan::parameter_node_t* params) -> void;
auto test_to_statement(const py::handle& source) -> py::str;

auto pack_to_match(const py::object& object) -> py::list;
