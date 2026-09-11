#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <memory>

#include <components/types/types.hpp>
#include <integration/cpp/otterbrix.hpp>
#include <module_arena.hpp>
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
        //! A type built by a module factory: `type_` lives on that module's arena; `arena` is never null.
        otterbrix_py_type_t(module_arena_ptr arena, components::types::complex_logical_type type);

        //! Reads a type from a RELATION'S schema onto the ENGINE's arena (base_otterbrix_t::resource),
        //! since a copied nested type keeps the source's allocator — without this ctor, a STRUCT
        //! column's child vector outlived the connection that freed it. `space` is never null.
        otterbrix_py_type_t(boost::intrusive_ptr<otterbrix_t> space, components::types::complex_logical_type type);

    public:
        // `arena` is created in integration/python/main.cpp's PYBIND11_MODULE body; objects built
        // here hold a counted reference to keep it alive as long as their nested child lists need it.
        static void initialize(py::handle& m, const module_arena_ptr& arena);

    public:
        bool equals(const std::shared_ptr<otterbrix_py_type_t>& other) const;
        std::shared_ptr<otterbrix_py_type_t> get_attribute(const std::string& name) const;
        py::list children() const;
        std::string to_string() const;
        const components::types::complex_logical_type& type() const;
        std::string get_id() const;

    private:
        //! Derives a child/map-key/list-element type on the same arena and owner — safe since
        //! `complex_logical_type`'s copy keeps the source allocator.
        std::shared_ptr<otterbrix_py_type_t> derive(components::types::complex_logical_type value) const;

    private:
        //! Only this pybind class can stand on either arena; exactly one of these two is ever set
        //! (enforced by the ctors). Declared before `type_` so reverse-order destruction frees
        //! them last, after type_'s destructor has deallocated into whichever one it was built on.
        module_arena_ptr arena_;
        boost::intrusive_ptr<otterbrix_t> space_;
        components::types::complex_logical_type type_;
    };

} // namespace otterbrix
