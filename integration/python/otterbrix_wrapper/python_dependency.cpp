#include "python_dependency.hpp"
#include <memory>

namespace otterbrix {

    python_dependency_item_t::python_dependency_item_t(std::unique_ptr<registered_object_t>&& object)
        : object(std::move(object)) {}

    python_dependency_item_t::~python_dependency_item_t() { // NOLINT - cannot throw in exception
        py::gil_scoped_acquire gil;
        object.reset();
    }

    std::unique_ptr<dependency_item_t> python_dependency_item_t::create(py::object object) {
        auto registered_object = std::make_unique<registered_object_t>(std::move(object));
        return std::make_unique<python_dependency_item_t>(std::move(registered_object));
    }

    std::unique_ptr<dependency_item_t> python_dependency_item_t::create(std::unique_ptr<registered_object_t>&& object) {
        return std::make_unique<python_dependency_item_t>(std::move(object));
    }

} // namespace otterbrix
