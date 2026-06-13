#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/registered_py_object.hpp>

#include <core/external_dependencies.hpp>
#include <core/types/memory.hpp>

namespace otterbrix {
    
class PythonDependencyItem : public DependencyItem {
public:
    explicit PythonDependencyItem(unique_ptr<RegisteredObject> &&object);
    ~PythonDependencyItem() override;

public:
    static unique_ptr<DependencyItem> Create(py::object object);
    static unique_ptr<DependencyItem> Create(unique_ptr<RegisteredObject> &&object);

public:
    unique_ptr<RegisteredObject> object;
};

} // namespace otterbrix
