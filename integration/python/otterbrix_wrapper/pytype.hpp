#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <memory>

#include <components/types/types.hpp>
#include <string>

namespace otterbrix {

    class py_generic_alias_t : public py::object {
    public:
        using py::object::object;

    public:
        static bool check_(const py::handle& object);
    };

    class py_union_type_t : public py::object {
    public:
        using py::object::object;

    public:
        static bool check_(const py::handle& object);
    };

    class otterbrix_py_type_t : public std::enable_shared_from_this<otterbrix_py_type_t> {
    public:
        explicit otterbrix_py_type_t(components::types::complex_logical_type type_);

    public:
        static void initialize(py::handle& m);

    public:
        bool equals(const std::shared_ptr<otterbrix_py_type_t>& other) const;
        std::shared_ptr<otterbrix_py_type_t> get_attribute(const std::string& name) const;
        py::list children() const;
        std::string to_string() const;
        const components::types::complex_logical_type& type() const;
        std::string get_id() const;

    private:
        components::types::complex_logical_type type_;
    };

} // namespace otterbrix
