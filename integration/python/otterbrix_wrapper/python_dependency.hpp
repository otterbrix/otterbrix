#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/registered_py_object.hpp>

#include <common/external_dependencies.hpp>
#include <memory>

namespace otterbrix {
    
class python_dependency_item_t : public dependency_item_t {
public:
    explicit python_dependency_item_t(std::unique_ptr<registered_object_t> &&object);
    ~python_dependency_item_t() override;

public:
    static std::unique_ptr<dependency_item_t> create(py::object object);
    static std::unique_ptr<dependency_item_t> create(std::unique_ptr<registered_object_t> &&object);

public:
    std::unique_ptr<registered_object_t> object;
};

} // namespace otterbrix
