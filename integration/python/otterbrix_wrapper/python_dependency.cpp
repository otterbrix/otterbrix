#include "python_dependency.hpp"
#include <memory>

namespace otterbrix {
    
    PythonDependencyItem::PythonDependencyItem(std::unique_ptr<RegisteredObject> &&object) : object(std::move(object)) {
    }
    
    PythonDependencyItem::~PythonDependencyItem() { // NOLINT - cannot throw in exception
        py::gil_scoped_acquire gil;
        object.reset();
    }
    
    std::unique_ptr<DependencyItem> PythonDependencyItem::Create(py::object object) {
        auto registered_object = std::make_unique<RegisteredObject>(std::move(object));
        return std::make_unique<PythonDependencyItem>(std::move(registered_object));
    }
        
    std::unique_ptr<DependencyItem> PythonDependencyItem::Create(std::unique_ptr<RegisteredObject> &&object) {
        return std::make_unique<PythonDependencyItem>(std::move(object));
    }

} // namespace otterbrix

