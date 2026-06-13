#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/registered_py_object.hpp>

#include <core/external_dependencies.hpp>
#include <memory>

namespace otterbrix {
    
class PythonDependencyItem : public DependencyItem {
public:
    explicit PythonDependencyItem(std::unique_ptr<RegisteredObject> &&object);
    ~PythonDependencyItem() override;

public:
    static std::unique_ptr<DependencyItem> Create(py::object object);
    static std::unique_ptr<DependencyItem> Create(std::unique_ptr<RegisteredObject> &&object);

public:
    std::unique_ptr<RegisteredObject> object;
};

} // namespace otterbrix
