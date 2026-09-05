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
        //! A type built by one of the MODULE'S factories: `type_` stands on the module's arena
        //! and this object holds a counted reference to it. `arena` is never null.
        otterbrix_py_type_t(module_arena_ptr arena, components::types::complex_logical_type type);

        //! A type read out of a RELATION'S schema (py_relation_t::column_types). Its bytes are
        //! on the ENGINE's arena -- base_otterbrix_t::resource is a member of the space, and a
        //! copy of a nested complex_logical_type keeps the source's allocator
        //! (struct_logical_type_extension copies with fields.get_allocator()) -- so the owner
        //! it has to hold is the SPACE, not the module's arena. Before this ctor existed the
        //! object held neither: `rel.types` on a STRUCT column handed Python a child vector
        //! sitting in a pool that the connection's destructor had already released.
        //! `space` is never null.
        otterbrix_py_type_t(boost::intrusive_ptr<otterbrix_t> space,
                            components::types::complex_logical_type type);

    public:
        // `arena` is the MODULE'S arena, created in the PYBIND11_MODULE body
        // (integration/python/main.cpp) and passed down through
        // otterbrix_py_typing_t::initialize. The `py::init` factories registered here
        // build types that the interpreter may hold for the rest of the process, and a
        // nested type keeps its child list in a pmr vector on this very arena, so the
        // arena must outlive them — which is exactly what holding a counted reference to it
        // in every one of these objects buys.
        static void initialize(py::handle& m, const module_arena_ptr& arena);

    public:
        bool equals(const std::shared_ptr<otterbrix_py_type_t>& other) const;
        std::shared_ptr<otterbrix_py_type_t> get_attribute(const std::string& name) const;
        py::list children() const;
        std::string to_string() const;
        const components::types::complex_logical_type& type() const;
        std::string get_id() const;

    private:
        //! Builds a type DERIVED from this one -- a child, a map key, a list element -- on the
        //! same arena, carrying the same owner. `complex_logical_type`'s copy keeps the source
        //! allocator, so the derived type's bytes really are on this object's arena and not on
        //! any other; copying the owner with them is what makes that safe.
        std::shared_ptr<otterbrix_py_type_t> derive(components::types::complex_logical_type value) const;

    private:
        //! ENUMERATION, NOT MEMORY. Of the five classes the binding registers with pybind11 --
        //! py_connection_t, py_relation_t, py_result_t, py_expression_t and this one -- this is
        //! the only one that can stand on the MODULE'S arena. The other four are built by a
        //! connection and stand on the ENGINE's arena (a member of the space), and each already
        //! holds an owning boost::intrusive_ptr<otterbrix_t> for exactly that reason:
        //! py_relation_t::space_, py_result_t::space, py_expression_t::space_,
        //! py_connection_t::space. ONE RULE, TWO ARENAS: hold a counted reference to whichever
        //! arena your bytes are on.
        //!
        //! This class is the one that can be on either, so it carries both slots and EXACTLY
        //! ONE of them is set -- the module's arena for a type the module's factories built,
        //! the space for a type read out of a relation's schema. The constructors are what
        //! enforce that; neither of them accepts a null owner.
        //!
        //! Both declared BEFORE `type_` so reverse-order member destruction frees them LAST:
        //! `type_` deallocates its STRUCT/MAP child vector into whichever of these two arenas
        //! it was built on.
        module_arena_ptr arena_;
        boost::intrusive_ptr<otterbrix_t> space_;
        components::types::complex_logical_type type_;
    };

} // namespace otterbrix
