#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <memory>

#include <components/types/types.hpp>
#include <string>

namespace otterbrix {

    class PyGenericAlias : public py::object {
    public:
        using py::object::object;
    
    public:
        static bool check_(const py::handle &object);
    };
    
    class PyUnionType : public py::object {
    public:
        using py::object::object;
    
    public:
        static bool check_(const py::handle &object);
    };
    
    class OtterBrixPyType : public std::enable_shared_from_this<OtterBrixPyType> {
    public:
        explicit OtterBrixPyType(components::types::complex_logical_type type);
    
    public:
        static void Initialize(py::handle &m);
    
    public:
        bool Equals(const std::shared_ptr<OtterBrixPyType> &other) const;
        std::shared_ptr<OtterBrixPyType> GetAttribute(const std::string &name) const;
        py::list Children() const;
        std::string ToString() const;
        const components::types::complex_logical_type &Type() const;
        std::string GetId() const;
    
    private:
        components::types::complex_logical_type type;
    };

} // namespace otterbrix

