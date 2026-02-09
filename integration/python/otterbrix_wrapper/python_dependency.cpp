#include "python_dependency.hpp"

namespace otterbrix {
    
    PythonDependencyItem::PythonDependencyItem(unique_ptr<RegisteredObject> &&object) : object(std::move(object)) {
    }
    
    PythonDependencyItem::~PythonDependencyItem() { // NOLINT - cannot throw in exception
        py::gil_scoped_acquire gil;
        object.reset();
    }
    
    unique_ptr<DependencyItem> PythonDependencyItem::Create(py::object object) {
        auto registered_object = make_unique<RegisteredObject>(std::move(object));
        return make_unique<PythonDependencyItem>(std::move(registered_object));
    }
        
    unique_ptr<DependencyItem> PythonDependencyItem::Create(unique_ptr<RegisteredObject> &&object) {
        return make_unique<PythonDependencyItem>(std::move(object));
    }

} // namespace otterbrix

