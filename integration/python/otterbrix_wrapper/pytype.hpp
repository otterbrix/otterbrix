#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <core/types/memory.hpp>
#include <core/types/string.hpp>

#include <components/types/types.hpp>

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
    
    class OtterBrixPyType : public enable_shared_from_this<OtterBrixPyType> {
    public:
        explicit OtterBrixPyType(components::types::complex_logical_type type);
    
    public:
        static void Initialize(py::handle &m);
    
    public:
        bool Equals(const shared_ptr<OtterBrixPyType> &other) const;
        shared_ptr<OtterBrixPyType> GetAttribute(const string &name) const;
        py::list Children() const;
        string ToString() const;
        const components::types::complex_logical_type &Type() const;
        string GetId() const;
    
    private:
    private:
        components::types::complex_logical_type type;
    };

} // namespace otterbrix

